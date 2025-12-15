import socket


def _detect_local_ip():
    """Try to resolve the local IP address without failing on missing hostnames."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(('8.8.8.8', 80))
            return sock.getsockname()[0]
    except OSError:
        pass

    try:
        return socket.gethostbyname(socket.gethostname())
    except (OSError, socket.gaierror):
        return '127.0.0.1'


class Config:
    rtsp_host = '0.0.0.0'  # Client listener host
    rtsp_port = 4554       # Client listener port
    start_udp_port = 5550
    local_ip = _detect_local_ip()
    rtp_buffer_size = 256  # Number of RTP packets stored per track

    # Camera(s) settings.
    #    * The keys of this dictionary will be called "camera hash".
    #    * "path" can be used for the storage.
    #    * "url" must contain at least <protocol>://<host>
    #    * Optional: "storage_command" must contain at least two pairs of parentheses (for URL and output file name).
    #       Overrides the same named command from the "storage" section.
    #       Examples:
    #           ffmpeg -i {url} -c copy {filename}.mkv
    #           mencoder {url} -ovc copy -o {filename}.avi
    #           openRTSP -b 10000000 -i -w 1920 -h 1080 -f 15 {url} > {filename}.avi
    #       Note that these utilities aren't included and must be installed yourself.
    #
    cameras = {
        'some-URL-compatible-string/including-UTF-characters': {
            'path': 'some folder in the storage_path',
            'url': 'rtsp://[<login>:<password>@]<IP or host name>[:554][/<uri>]',
            # 'storage_command': 'any *nix command for saving rtsp stream to a file',
        },
        # The "source" field allows registering multiple RTSP paths that share the same
        # camera stream. Each alias must reference the key of a camera that defines the
        # "url" field.
        'cam-alias-example': {
            'source': 'some-URL-compatible-string/including-UTF-characters',
        },
        # Example: expose the same physical camera through two RTSP URLs ("cam1" and
        # "front-door"). Clients can connect to either path and will receive packets
        # from the single shared camera connection.
        'cam1': {
            'url': 'rtsp://admin:admin@192.168.1.1:554',
        },
        'front-door': {
            'source': 'cam1',
        },
    }

    # Force UDP or TCP protocol globally
    tcp_mode = False

    # Limit connections from the web. Set to 0 for unlimited connections
    web_limit = 2

    # Check UDP traffic from cameras, secs
    watchdog_interval = 30
    # Send RTSP keep-alives this often (seconds); set to 0 to disable
    camera_keepalive_interval = 25
    # Restart the upstream connection if no RTP packets arrive for this many seconds
    camera_watchdog_timeout = 20
    # Base delay between reconnect attempts when a camera drops
    camera_reconnect_delay = 3

    # Update this path if you want the log file somewhere else. Relative paths
    # are resolved against the working directory of the server process.
    log_file = 'python-rtsp-server.log'

    # Attention!
    # All files and subdirectories older than "storage_period_days" in this folder will be deleted!
    storage_path = 'absolute path to video monitoring storage folder'
    storage_period_days = 14
    storage_fragment_secs = 600
    # UDP mode:
    storage_command = 'ffmpeg -i {url} -c copy -v fatal -t {storage_fragment_secs} {filename}.mkv'
    # TCP mode:
    # storage_command = 'ffmpeg -rtsp_transport tcp -i {url} -c copy -v fatal -t {storage_fragment_secs} {filename}.mkv'
    storage_enable = False

    debug = True
