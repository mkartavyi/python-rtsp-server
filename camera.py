import asyncio
import re
import time
from hashlib import md5
from typing import Dict, Optional, Union

from _config import Config
from log import Log
from rtp_buffer import RTPPacketBuffer

BufferKey = Union[str, int]


class Camera:
    def __init__(self, camera_hash: str):
        self.hash = camera_hash
        self.url = _parse_url(Config.cameras[camera_hash]['url'])
        self.udp_ports, self.track_ids = [], []
        self.description = {}
        self.session_id: Optional[str] = None
        self.rtp_info = None
        self.realm = None
        self.nonce = None
        self.cseq = 1
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.udp_transports: Dict[int, asyncio.DatagramTransport] = {}
        self.buffers: Dict[BufferKey, RTPPacketBuffer] = {}
        self._connect_lock = asyncio.Lock()
        self._play_lock = asyncio.Lock()
        self._interleave_task: Optional[asyncio.Task] = None
        self._connected = False
        self._playing = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._buffer_size = getattr(Config, 'rtp_buffer_size', 256)

    async def connect(self):
        """Open TCP socket and connect to the camera."""
        async with self._connect_lock:
            if self._connected:
                return

            self.udp_ports = self._get_self_udp_ports()

            try:
                self.reader, self.writer = await asyncio.open_connection(
                    self.url['host'], self.url['tcp_port'])
            except Exception as e:
                Log.print(f"Camera: error: can't connect [{self.hash}]: {e}")
                return

            await self._request('OPTIONS', self.url['url'])

            reply, code = await self._request(
                'DESCRIBE',
                self.url['url'],
                'Accept: application/sdp')

            if code == 401:
                self.realm, self.nonce = _get_auth_params(reply)

                reply, code = await self._request(
                    'DESCRIBE',
                    self.url['url'],
                    'Accept: application/sdp')

            self.description = _get_description(reply)
            self.track_ids = _get_track_ids(reply)

            reply, _code = await self._request(
                'SETUP',
                f'{self.url["url"]}/{self.track_ids[0]}',
                self._get_transport_line(0))

            self.session_id = _get_session_id(reply)

            if len(self.track_ids) > 1:
                await self._request(
                    'SETUP',
                    f'{self.url["url"]}/{self.track_ids[1]}',
                    self._get_transport_line(1),
                    f'Session: {self.session_id}')

            self.rtp_info = None
            self._connected = True

            Log.write(f'Camera: connected [{self.hash}]')

    async def play(self):
        """Start playing and push the stream to the shared RTP buffers."""
        if not self._connected:
            raise RuntimeError(f'Camera {self.hash} is not connected')

        async with self._play_lock:
            if self._playing:
                return

            cmd = (
                'PLAY',
                self.url['url'],
                f'Session: {self.session_id}',
                'Range: npt=0.000-')

            if Config.tcp_mode:
                reply, _code = await self._request(*cmd)
                self.rtp_info = _get_rtp_info(reply)
                self.buffers['tcp'] = RTPPacketBuffer(self._buffer_size)
                self._loop = asyncio.get_running_loop()
                self._interleave_task = asyncio.create_task(self._interleave())
            else:
                reply, _code = await self._request(*cmd)
                self.rtp_info = _get_rtp_info(reply)
                self._loop = asyncio.get_running_loop()
                self.buffers[0] = RTPPacketBuffer(self._buffer_size)
                await self._start_udp_server(0)

                if self.description['audio']:
                    self.buffers[1] = RTPPacketBuffer(self._buffer_size)
                    await self._start_udp_server(1)

            self._playing = True

    async def close(self):
        """Close all opened sockets, transports, and buffers."""
        self._playing = False

        if self._interleave_task:
            self._interleave_task.cancel()
            try:
                await self._interleave_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                Log.print(f'Camera: error while closing interleaved task [{self.hash}]: {e}')
            self._interleave_task = None

        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass

        for transport in list(self.udp_transports.values()):
            try:
                transport.close()
            except Exception:
                pass
        self.udp_transports.clear()

        await self._close_buffers()

        self.reader = None
        self.writer = None
        self._connected = False
        self._loop = None
        self.buffers = {}
        self.rtp_info = None

        Log.write(f'Camera: closed [{self.hash}]')

    async def create_reader(self, track: BufferKey):
        if track not in self.buffers:
            raise RuntimeError(f'Buffer for track {track} is not available')
        return await self.buffers[track].create_reader()

    def get_tracks(self):
        if Config.tcp_mode:
            return ['tcp']
        return sorted([idx for idx in self.buffers.keys() if isinstance(idx, int)])

    def send_udp(self, idx: int, data: bytes, host: str, port: int) -> None:
        transport = self.udp_transports.get(idx)
        if not transport:
            return
        try:
            transport.sendto(data, (host, port))
        except Exception as e:
            Log.print(f'Camera: error sending UDP packet [{self.hash}]: {e}')

    def handle_udp_packet(self, idx: int, data: bytes) -> None:
        buffer = self.buffers.get(idx)
        if not buffer:
            return
        loop = self._loop
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
        if loop:
            loop.create_task(buffer.append(data))

    async def _interleave(self):
        buffer = self.buffers.get('tcp')
        try:
            while self._playing and self.reader and buffer:
                frame = await self.reader.read(2048)
                if not frame:
                    Log.print(f'Camera: interleaved stream ended [{self.hash}]')
                    break
                await buffer.append(frame)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            Log.print(f'Camera: error while reading interleaved data [{self.hash}]: {e}')
        finally:
            if buffer:
                await buffer.close()

    async def _request(self, option, url, *lines):
        """Ask the camera option with given lines. Returns reply and status code."""
        self._write(option, url, *lines)

        data = await self.reader.read(2048)

        if data[0:1] == b'$':
            Log.print('Camera: read: interleaved binary data')
            return None, 200

        reply = data.decode()

        Log.print(f'~~~ Camera: read:\n{reply}')

        res = re.match(r'RTSP/1.0 (\d{3}) ([^\r\n]+)', reply)
        if not res:
            Log.print('Camera: error: invalid reply')
            return reply, 0
        return reply, int(res.group(1))

    def _write(self, option, url, *lines):
        cmd = f'{option} {url} RTSP/1.0\r\n' \
            f'CSeq: {self.cseq}\r\n'

        auth_line = self._get_auth_line(option)
        if auth_line:
            cmd += f'{auth_line}\r\n'

        for row in lines:
            if row:
                cmd += f'{row}\r\n'
        cmd += '\r\n'

        Log.print(f'~~~ Camera: write\n{cmd}')

        self.writer.write(cmd.encode())
        self.cseq += 1

    def _get_auth_line(self, option):
        """Encode auth "response" hash"""
        if not self.realm or not self.nonce:
            return
        ha1 = md5(f'{self.url["login"]}:{self.realm}:{self.url["password"]}'.encode('utf-8')).hexdigest()
        ha2 = md5(f'{option}:{self.url["url"]}'.encode('utf-8')).hexdigest()
        response = md5(f'{ha1}:{self.nonce}:{ha2}'.encode('utf-8')).hexdigest()
        line = f'Authorization: Digest username="{self.url["login"]}", ' \
            f'realm="{self.realm}", algorithm="MD5", nonce="{self.nonce}", ' \
            f'uri="{self.url["url"]}", response="{response}"'
        return line

    def _get_transport_line(self, idx):
        """Build new "Transport" line for given track index"""
        if Config.tcp_mode:
            channel = '0-1' if not idx else '2-3'
            return f'Transport: RTP/AVP/TCP;unicast;interleaved={channel}'

        return 'Transport: RTP/AVP;unicast;' \
            f'client_port={self.udp_ports[idx][0]}-{self.udp_ports[idx][1]}'

    def _get_self_udp_ports(self):
        """Calculate port number from free user ports range"""
        start_port = Config.start_udp_port
        idx = list(Config.cameras.keys()).index(self.hash) * 4
        return [
            [start_port + idx, start_port + idx + 1],
            [start_port + idx + 2, start_port + idx + 3]]

    async def _start_udp_server(self, idx):
        """Create datagram endpoint"""
        if idx in self.udp_transports:
            return

        loop = self._loop or asyncio.get_running_loop()

        try:
            transport, _protocol = await loop.create_datagram_endpoint(
                lambda: CameraUdpProtocol(self, idx),
                local_addr=('0.0.0.0', self.udp_ports[idx][0]))

            self.udp_transports[idx] = transport

        except Exception as e:
            Log.print(f"Camera: error: can't create_datagram_endpoint: {e}")

    async def _close_buffers(self):
        for buffer in list(self.buffers.values()):
            try:
                await buffer.close()
            except Exception:
                pass


class CameraUdpProtocol(asyncio.DatagramProtocol):
    """Callback called when connection to the camera is made"""
    def __init__(self, camera: Camera, idx: int):
        self.camera = camera
        self.idx = idx

    def datagram_received(self, data, addr):
        self.camera.handle_udp_packet(self.idx, data)


def _parse_url(url):
    """Get URL components"""
    rex = r'^((.+)://)?((.+?)(:(.+))?@)?(.+?)(:(\d+))?(/.*)?$'
    parsed_url = re.match(rex, url)
    if not parsed_url:
        raise RuntimeError('Invalid rtsp url')
    res = {
        'scheme': parsed_url.group(2) or 'rtsp',
        'login': parsed_url.group(4) or '',
        'password': parsed_url.group(6) or '',
        'host': parsed_url.group(7),
        'tcp_port': int(parsed_url.group(9) or 554),
        'path': parsed_url.group(10) or ''}
    res['url'] = f'{res["scheme"]}://{res["host"]}:{res["tcp_port"]}{res["path"]}'
    return res


def _get_auth_params(reply):
    """Search digest auth realm and nonce in reply"""
    realm_nonce = re.match(r'.+?\nWWW-Authenticate:.+?realm="(.+?)", ?nonce="(.+?)"', reply, re.DOTALL)
    if not realm_nonce:
        raise RuntimeError('Invalid digest auth reply')

    return realm_nonce.group(1), realm_nonce.group(2)


def _get_description(reply):
    """Search SDP (Session Description Protocol) in rtsp reply"""
    blocks = reply.split('\r\n\r\n', 2)
    if len(blocks) < 2:
        raise RuntimeError('Invalid DESCRIBE reply')

    sdp = blocks[1].strip()

    details = {'video': {}, 'audio': {}}

    res = re.match(r'.+?\nm=video (.+?)\r\n', sdp, re.DOTALL)
    if res:
        details['video'] = {'media': res.group(1), 'bandwidth': '', 'rtpmap': '', 'format': ''}

        res = re.match(r'.+?\nm=video .+?\nb=([^\r\n]+)', sdp, re.DOTALL)
        if res:
            details['video']['bandwidth'] = res.group(1)

        res = re.match(r'.+?\nm=video .+?\na=rtpmap:([^\r\n]+)/([^\r\n]+)', sdp, re.DOTALL)
        if res:
            details['video']['rtpmap'] = res.group(1) + '/' + res.group(2)
            details['video']['clk_freq'] = int(res.group(2))

        res = re.match(r'.+?\nm=video .+?\na=fmtp:([^\r\n]+)', sdp, re.DOTALL)
        if res:
            details['video']['format'] = res.group(1)

    res = re.match(r'.+?\nm=audio (.+?)\r\n', sdp, re.DOTALL)
    if res:
        details['audio'] = {'media': res.group(1), 'rtpmap': ''}

        res = re.match(r'.+?\nm=audio .+?\na=rtpmap:([^\r\n]+)/([^\r\n]+)', sdp, re.DOTALL)
        if res:
            details['audio']['rtpmap'] = res.group(1) + '/' + res.group(2)
            details['audio']['clk_freq'] = int(res.group(2))

    return details


def _get_track_ids(reply):
    """Search track ID in rtsp reply"""
    track_ids = re.findall(r'\na=control:.*?((?:track|stream).*?\d)', reply)
    if not track_ids:
        raise RuntimeError('Invalid track ID in reply')
    return track_ids


def _get_session_id(reply):
    """Search session ID in rtsp reply"""
    res = re.match(r'.+?\nSession: *([^;]+)', reply, re.DOTALL)
    if not res:
        raise RuntimeError('Invalid session ID')
    return res.group(1)


def _get_rtp_info(reply):
    """Search "RTP-Info" string in rtsp reply"""
    if not reply:
        return
    res = re.match(r'.+?\r\n(RTP-Info: .+?)\r\n', reply, re.DOTALL)
    if not res:
        raise RuntimeError('Invalid RTP-Info')
    rtp_info = res.group(1)

    seq = re.findall(r';seq=(\d+)', rtp_info)
    rtptime = re.findall(r';rtptime=(\d+)', rtp_info)
    if not seq or not rtptime:
        raise RuntimeError('Invalid RTP-Info')

    return {'seq': seq, 'rtptime': rtptime, 'starttime': time.time()}
