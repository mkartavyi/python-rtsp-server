import asyncio
import re
import string
import time
from random import choices, randrange
from urllib.parse import unquote, urlparse
from typing import Dict, List, Optional

from _config import Config
from shared import Shared, CameraState
from camera import Camera
from log import Log


class Client:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        peername = writer.get_extra_info('peername')
        self.host = peername[0]
        self.tcp_port = peername[1]
        self.camera_hash, self.session_id = None, None
        self.udp_ports = {}
        self._camera_state: Optional[CameraState] = None
        self._stream_tasks: List[asyncio.Task] = []
        self._stream_started = False
        self._transport_protocol: Optional[str] = None
        self._content_base: Optional[str] = None
        self._track_aliases: Dict[str, int] = {}

    @staticmethod
    async def listen():
        """ One listener for all clients
        """
        host = Config.rtsp_host if hasattr(Config, 'rtsp_host') else '0.0.0.0'
        Log.write(f'Client: start listening {host}:{Config.rtsp_port}')

        server = await asyncio.start_server(_handle, host, Config.rtsp_port)
        async with server:
            await server.serve_forever()

    async def handle(self, data):
        """ Communicate with clients
        """
        try:
            ask = data.decode()
        except (Exception,):
            Log.print(f"Client: warning: can't decode this ask, skipped:\n{data}")
            return

        option = await self._request(ask)
        self.session_id = _get_session_id(ask)

        if option == 'OPTIONS':
            await self._response('Public: OPTIONS, DESCRIBE, SETUP, TEARDOWN, PLAY')

        if option == 'DESCRIBE':
            sdp = self._get_description()
            await self._response(
                'Content-Type: application/sdp',
                f'Content-Length: {len(sdp) + 4}',
                f'Content-Base: {self._get_content_base()}',
                '',
                sdp)

        elif option == 'SETUP':
            await self._response(
                self._get_transport_line(ask),
                f'Session: {self.session_id};timeout=60')

        elif option == 'PLAY':
            # Now we are ready to share this instance
            state = self._camera_state
            if not state:
                raise RuntimeError('camera state is not initialised')
            state.clients[self.session_id] = self

            camera = state.camera
            if not camera:
                raise RuntimeError('camera is not initialised')

            # Start camera's playing before client's playing because we need to get RTP info first
            await camera.play()

            res = [f'Session: {self.session_id}']
            rtp_info = self._get_rtp_info()
            if rtp_info:
                res.append(rtp_info)

            await self._response(*res)

            await self._check_web_limit()

            info = f'Client: play [{self.camera_hash}] [{self.session_id}] [{self.host}] {self.user_agent}'
            Log.write(info, self.host)

            await self._start_stream(camera)

            # In TCP mode we'll stop listening rtsp
            if self._effective_transport() == 'tcp':
                return True  # Handling is over, stop self._handle loop

        elif option == 'TEARDOWN':
            await self._response(f'Session: {self.session_id}')

    async def write(self, frame):
        if self.writer.transport.is_closing():
            await self.close()
            return
        self.writer.write(frame)

    async def close(self):
        if not self.camera_hash:
            return
        await self._stop_stream()

        if self.writer:
            try:
                if not self.writer.transport.is_closing():
                    self.writer.close()
                    await self.writer.wait_closed()
            except (Exception,):
                pass

        state = self._camera_state or Shared.data.get(self.camera_hash)
        if not state:
            return

        clients = state.clients
        if not self.session_id or self.session_id not in clients:
            return

        del clients[self.session_id]

        Log.write(f'Client closed [{self.camera_hash}] [{self.session_id}] [{self.host}]', self.host)

        # If last client is closed, close the camera connection too
        if not clients:
            try:
                if state.camera:
                    await state.camera.close()
            except Exception as e:
                Log.print(f"Client: error: can't close the camera {self.camera_hash}: {e}")
            state.camera = None

    def _get_rtp_info(self):
        """ Build new "RTP-Info" line (for UDP mode only)
        """
        if not self._camera_state or not self._camera_state.camera:
            return

        camera = self._camera_state.camera
        rtp_info = camera.rtp_info
        if not rtp_info:
            return

        sdp = camera.description

        delta = time.time() - rtp_info['starttime']
        clock_frequency = sdp['video']['clk_freq']  # i.e. 90000 in SDP a=rtpmap:96 H26*/90000
        rtptime = int(rtp_info["rtptime"][0]) + int(delta * clock_frequency)

        base = self._get_content_base()
        res = f'RTP-Info: url={base}track1;' \
            f'seq={rtp_info["seq"][0]};rtptime={rtptime}'

        if len(rtp_info['seq']) < 2:
            return res

        clock_frequency = sdp['audio']['clk_freq']  # i.e. 8000 in SDP a=rtpmap:8 PCMA/8000
        rtptime = int(rtp_info["rtptime"][1]) + int(delta * clock_frequency)

        res += f',url={base}track2;' \
            f'seq={rtp_info["seq"][1]};rtptime={rtptime}'

        return res

    async def _request(self, ask):
        """ Parse client's ask
        """
        Log.print(f'~~~ Client: read\n{ask}')
        request_line = ask.split('\r\n', 1)[0]
        parts = request_line.split()
        if len(parts) < 3:
            raise RuntimeError('invalid ask')
        option, uri = parts[0], parts[1]

        parsed_uri = urlparse(uri)
        if parsed_uri.scheme not in {'rtsp', 'rtsps'}:
            raise RuntimeError('invalid ask')
        option, uri = parts[0], parts[1]

        parsed_uri = urlparse(uri)
        if parsed_uri.scheme not in {'rtsp', 'rtsps'}:
            raise RuntimeError('invalid ask')

        camera_path = parsed_uri.path.lstrip('/')
        if parsed_uri.params:
            camera_path = f'{camera_path};{parsed_uri.params}' if camera_path else parsed_uri.params
        if parsed_uri.query:
            camera_path = f'{camera_path}?{parsed_uri.query}' if camera_path else parsed_uri.query
        if parsed_uri.fragment:
            camera_path = f'{camera_path}#{parsed_uri.fragment}' if camera_path else parsed_uri.fragment

        camera_path = parsed_uri.path.lstrip('/')
        if parsed_uri.params:
            camera_path = f'{camera_path};{parsed_uri.params}' if camera_path else parsed_uri.params
        if parsed_uri.query:
            camera_path = f'{camera_path}?{parsed_uri.query}' if camera_path else parsed_uri.query
        if parsed_uri.fragment:
            camera_path = f'{camera_path}#{parsed_uri.fragment}' if camera_path else parsed_uri.fragment

        camera_path = parsed_uri.path.lstrip('/')
        if parsed_uri.params:
            camera_path = f'{camera_path};{parsed_uri.params}' if camera_path else parsed_uri.params
        if parsed_uri.query:
            camera_path = f'{camera_path}?{parsed_uri.query}' if camera_path else parsed_uri.query
        if parsed_uri.fragment:
            camera_path = f'{camera_path}#{parsed_uri.fragment}' if camera_path else parsed_uri.fragment

        camera_path = parsed_uri.path.lstrip('/')
        if parsed_uri.params:
            camera_path = f'{camera_path};{parsed_uri.params}' if camera_path else parsed_uri.params
        if parsed_uri.query:
            camera_path = f'{camera_path}?{parsed_uri.query}' if camera_path else parsed_uri.query
        if parsed_uri.fragment:
            camera_path = f'{camera_path}#{parsed_uri.fragment}' if camera_path else parsed_uri.fragment

        camera_path = parsed_uri.path.lstrip('/')
        if parsed_uri.params:
            camera_path = f'{camera_path};{parsed_uri.params}' if camera_path else parsed_uri.params
        if parsed_uri.query:
            camera_path = f'{camera_path}?{parsed_uri.query}' if camera_path else parsed_uri.query
        if parsed_uri.fragment:
            camera_path = f'{camera_path}#{parsed_uri.fragment}' if camera_path else parsed_uri.fragment

        self.cseq = _get_cseq(ask)
        self.user_agent = _get_user_agent(ask)

        if not self.camera_hash:
            camera_hash = unquote(camera_path)
            if camera_hash not in Shared.data:
                raise RuntimeError('invalid camera hash')

            self.camera_hash = camera_hash
            self._camera_state = Shared.data[camera_hash]

            self._content_base = self._build_content_base(parsed_uri)

            if not self._camera_state.camera:
                camera = Camera(self._camera_state.source)
                await camera.connect()
                if not camera.session_id:
                    raise RuntimeError(f'camera "{camera_hash}" is unavailable')
                self._camera_state.camera = camera

        return option

    async def _response(self, *lines):
        """ Reply to client with given params
        """
        reply = 'RTSP/1.0 200 OK\r\n' \
            f'CSeq: {self.cseq}\r\n'

        for row in lines:
            reply += f'{row}\r\n'

        reply += '\r\n'

        self.writer.write(reply.encode())

        Log.print(f'~~~ Client: write\n{reply}')

    async def _start_stream(self, camera: Camera):
        if self._stream_started:
            return

        self._stream_started = True

        if self._effective_transport() == 'tcp':
            task = asyncio.create_task(self._stream_tcp(camera))
            self._stream_tasks.append(task)
            return

        tracks = [idx for idx in camera.get_tracks() if idx in self.udp_ports]
        for idx in tracks:
            task = asyncio.create_task(self._stream_udp(camera, idx))
            self._stream_tasks.append(task)

    async def _stop_stream(self):
        if not self._stream_tasks:
            self._stream_started = False
            return

        for task in list(self._stream_tasks):
            task.cancel()

        for task in list(self._stream_tasks):
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                Log.print(f'Client: stream task error [{self.camera_hash}]: {e}')

        self._stream_tasks.clear()
        self._stream_started = False

    async def _stream_tcp(self, camera: Camera):
        reader = await camera.create_reader('tcp')
        try:
            while True:
                packet = await reader.read()
                if packet is None:
                    break
                if self.writer.transport.is_closing():
                    break
                self.writer.write(packet)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            Log.print(f'Client: tcp stream error [{self.camera_hash}]: {e}')
        finally:
            await reader.close()

    async def _stream_udp(self, camera: Camera, track_idx: int):
        reader = await camera.create_reader(track_idx)
        try:
            while True:
                packet = await reader.read()
                if packet is None:
                    break
                if track_idx not in self.udp_ports:
                    break
                udp_port = self.udp_ports[track_idx][0]
                camera.send_udp(track_idx, packet, self.host, udp_port)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            Log.print(f'Client: udp stream error [{self.camera_hash}]: {e}')
        finally:
            await reader.close()

    def _get_transport_line(self, ask):
        """Parse the requested transport and build a response line."""
        transport_header = _get_transport_header(ask)
        if not transport_header:
            if Config.tcp_mode:
                self._transport_protocol = 'tcp'
                return 'Transport: RTP/AVP/TCP;unicast;interleaved=0-1'
            raise RuntimeError('transport header is missing')

        candidates = [candidate.strip() for candidate in transport_header.split(',') if candidate.strip()]

        udp_candidate = None
        tcp_candidate = None
        for candidate in candidates:
            upper = candidate.upper()
            if 'MULTICAST' in upper:
                continue
            if 'RTP/AVP/TCP' in upper:
                if not tcp_candidate:
                    tcp_candidate = candidate
                continue
            if 'RTP/AVP' in upper and 'TCP' not in upper:
                if not udp_candidate:
                    udp_candidate = candidate

        chosen = None
        protocol = None
        if Config.tcp_mode and tcp_candidate:
            chosen = tcp_candidate
            protocol = 'tcp'
        elif udp_candidate:
            chosen = udp_candidate
            protocol = 'udp'
        elif tcp_candidate:
            chosen = tcp_candidate
            protocol = 'tcp'

        if not chosen:
            raise RuntimeError('unsupported transport requested')

        parts = [part.strip() for part in chosen.split(';') if part.strip()]
        if not parts:
            raise RuntimeError('invalid transport header')

        protocol_token, *params = parts

        if protocol == 'tcp':
            interleaved = None
            response_params = []
            has_unicast = False
            for param in params:
                lower = param.lower()
                if lower == 'unicast':
                    has_unicast = True
                    response_params.append('unicast')
                elif lower.startswith('interleaved='):
                    interleaved = param.split('=', 1)[1].strip()
                else:
                    response_params.append(param)

            if not has_unicast:
                response_params.insert(0, 'unicast')

            response_params.append(f'interleaved={interleaved or "0-1"}')

            self._transport_protocol = 'tcp'
            return f'Transport: {";".join([protocol_token] + response_params)}'

        udp_ports = _get_ports(chosen)
        track_idx = self._resolve_track_index(ask)
        if track_idx is None:
            track_idx = 0 if not self.udp_ports else max(self.udp_ports.keys(), default=-1) + 1

        self.udp_ports[track_idx] = udp_ports

        response_params = []
        has_unicast = False
        has_client_port = False
        for param in params:
            lower = param.lower()
            if lower == 'unicast':
                has_unicast = True
                response_params.append('unicast')
            elif lower == 'multicast':
                continue
            elif lower.startswith('client_port='):
                has_client_port = True
                value = param.split('=', 1)[1].strip()
                response_params.append(f'client_port={value}')
            elif lower.startswith('server_port='):
                continue
            elif lower.startswith('destination=') or lower.startswith('source='):
                continue
            elif lower.startswith('ttl='):
                continue
            else:
                response_params.append(param)

        if not has_unicast:
            response_params.insert(0, 'unicast')

        if not has_client_port:
            raise RuntimeError('invalid transport ports')

        response_params.extend(self._format_server_ports(track_idx))

        self._transport_protocol = 'udp'
        return f'Transport: {";".join([protocol_token] + response_params)}'

    def _get_description(self):
        """ Create new SDP based on original one from the camera
        """
        if not self._camera_state or not self._camera_state.camera:
            raise RuntimeError('camera is not initialised')

        sdp = self._camera_state.camera.description
        self._track_aliases = {}

        camera = self._camera_state.camera if self._camera_state else None
        if camera:
            for idx, track_id in enumerate(camera.track_ids):
                self._track_aliases[track_id.lower()] = idx

        res = 'v=0\r\n' \
            f'o=- {randrange(100000, 999999)} {randrange(1, 10)} IN IP4 {Config.local_ip}\r\n' \
            's=python-rtsp-server\r\n' \
            't=0 0'
        # 'a=range:npt=0-'

        if not sdp['video']:
            return res
        res += f'\r\nm=video {sdp["video"]["media"]}\r\n' \
            'c=IN IP4 0.0.0.0\r\n' \
            f'b={sdp["video"]["bandwidth"]}\r\n' \
            f'a=rtpmap:{sdp["video"]["rtpmap"]}\r\n' \
            f'a=fmtp:{sdp["video"]["format"]}\r\n' \
            'a=control:track1'
        self._track_aliases.setdefault('track1', 0)

        if not sdp['audio']:
            return res
        res += f'\r\nm=audio {sdp["audio"]["media"]}\r\n' \
            f'a=rtpmap:{sdp["audio"]["rtpmap"]}\r\n' \
            'a=control:track2'
        self._track_aliases.setdefault('track2', 1)
        return res

    async def _check_web_limit(self):
        """ Just drop old "external" connections
        """
        if not Config.web_limit or _get_client_type(self.host) == 'local':
            return
        web_sessions = []
        state = self._camera_state or Shared.data.get(self.camera_hash)
        if not state:
            return
        clients = state.clients
        for session_id, client in clients.items():
            if _get_client_type(client.host) == 'web':
                web_sessions.append(session_id)
        if len(web_sessions) > Config.web_limit:
            ws = web_sessions[:-Config.web_limit]
            for session_id in ws:
                Log.write('Client: web limit exceeded, close old connection')
                await clients[session_id].close()

    def _effective_transport(self) -> str:
        if self._transport_protocol:
            return self._transport_protocol
        return 'tcp' if Config.tcp_mode else 'udp'

    def _build_content_base(self, parsed_uri):
        netloc = parsed_uri.netloc
        if not netloc:
            host = getattr(Config, 'rtsp_host', None) or Config.local_ip
            if ':' not in host:
                netloc = f'{host}:{Config.rtsp_port}'
            else:
                netloc = host
        scheme = parsed_uri.scheme or 'rtsp'
        path = parsed_uri.path or '/'
        if not path.endswith('/'):
            path = f'{path}/'
        return self._ensure_trailing_slash(f'{scheme}://{netloc}{path}')

    def _get_content_base(self):
        if self._content_base:
            return self._ensure_trailing_slash(self._content_base)

        host = getattr(Config, 'rtsp_host', None) or Config.local_ip
        if ':' not in host:
            netloc = f'{host}:{Config.rtsp_port}'
        else:
            netloc = host
        path = f'/{self.camera_hash or ""}'
        if not path.endswith('/'):
            path += '/'
        return self._ensure_trailing_slash(f'rtsp://{netloc}{path}')

    @staticmethod
    def _ensure_trailing_slash(value: str) -> str:
        return value if value.endswith('/') else f'{value}/'

    def _resolve_track_index(self, ask: str) -> Optional[int]:
        request_line = ask.split('\r\n', 1)[0]
        parts = request_line.split()
        if len(parts) < 2:
            return None

        uri = parts[1]
        parsed_uri = urlparse(uri)
        segment = parsed_uri.path.rstrip('/').split('/')[-1].lower()
        if not segment:
            return None

        if segment in self._track_aliases:
            return self._track_aliases[segment]

        match = re.search(r'(trackid=|track)(\d+)', segment)
        if match:
            value = int(match.group(2))
            for alias in (f'track{value}', f'trackid={value}'):
                if alias in self._track_aliases:
                    return self._track_aliases[alias]
            if match.group(1) == 'track':
                return max(value - 1, 0)
            return value

        return None

    def _format_server_ports(self, track_idx: int) -> List[str]:
        params: List[str] = []
        camera = self._camera_state.camera if self._camera_state else None

        server_port: Optional[str] = None
        if camera and 0 <= track_idx < len(camera.udp_ports):
            ports = camera.udp_ports[track_idx]
            if len(ports) >= 2:
                server_port = f'server_port={ports[0]}-{ports[1]}'

        if not server_port:
            base_port = Config.start_udp_port + track_idx * 2
            server_port = f'server_port={base_port}-{base_port + 1}'

        params.append(server_port)

        if camera:
            extras = camera.get_transport_params(track_idx)
            if extras:
                ssrc = extras.get('ssrc')
                if ssrc:
                    params.append(f'ssrc={ssrc}')
                mode = extras.get('mode')
                if mode:
                    params.append(f'mode={mode}')

        return params


async def _handle(reader, writer):
    """ This callback function will be called every time a connection to the server is made
    """
    client = Client(reader, writer)
    Log.print(f'Client: new connection from {client.host}:{client.tcp_port}')

    while True:
        try:
            data = await reader.read(2048)
        except ConnectionResetError:
            await client.close()
            Log.print(f'Client: connection reset: {client.host}:{client.tcp_port}')
            return

        if data[0:1] == b'' or writer.transport.is_closing():
            await client.close()
            Log.print(f'Client: connection closed: {client.host}:{client.tcp_port}')
            return

        # Handle client connection
        try:
            if await client.handle(data):  # start TCP mode listening, handling is over
                return
        except Exception as e:
            Log.print(f"Client: error: can't handle request from {client.host}: {e}")
            await client.close()
            return


def _get_session_id(ask):
    """ Search session ID in rtsp ask
    """
    res = re.match(r'.+?\nSession: *([^;\r\n]+)', ask, re.DOTALL)
    if res:
        return res.group(1).strip()

    return ''.join(choices(string.ascii_lowercase + string.digits, k=9))


def _get_cseq(ask):
    """ Search CSeq in rtsp ask
    """
    res = re.match(r'.+?\r\nCSeq: (\d+)', ask, re.DOTALL)
    if not res:
        raise RuntimeError('invalid incoming CSeq')
    return int(res.group(1))


def _get_user_agent(ask):
    """ Search User-Agent in rtsp ask
        [ -~] means any ASCII character from the space to the tilde
    """
    res = re.match(r'.+?\r\nUser-Agent: ([ -~]+)\r\n', ask, re.DOTALL + re.IGNORECASE)
    if not res:
        return 'unknown user agent'
    return res.group(1)


def _get_transport_header(ask):
    """Extract the Transport header value from the request."""
    res = re.search(r'\nTransport:\s*([^\r\n]+)', ask, re.IGNORECASE)
    if not res:
        return None
    return res.group(1).strip()


def _get_ports(transport):
    """ Search port numbers in transport string
    """
    res = re.search(r'client_port=(\d+)-(\d+)', transport, re.IGNORECASE)
    if not res:
        raise RuntimeError('invalid transport ports')
    return [int(res.group(1)), int(res.group(2))]


def _get_client_type(host):
    if host == '127.0.0.1' \
        or host == 'localhost' \
            or (host.startswith('192.168.') and host != Config.local_ip):
        return 'local'
    return 'web'
