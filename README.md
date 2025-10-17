## python-rtsp-server

Python-rtsp-server is a lightweight, zero-dependency proxy and storage server
for several IP-cameras and multiple clients.

### Features:

* Reliable connection of clients in the local network. One connection to each camera, regardless of the number of clients.
* Minimum client connection latency.
* Low CPU load.
* Proxying streams from IP cameras to an unlimited number of clients in the local network, the ability to limit the number of web clients.
* Ability to save to hard disk, with fragmentation and daily rotation.
* Restoring of connection with cameras and recording to disk after a possible disconnection of cameras.

### Requirements:

Python 3.7+ is required. Optionally uses system-wide utilities for saving streams to file storage, such as ffmpeg, OpenRTSP or mencoder.

Compatible with Linux. Supports H.264, H.265 and H.265+ codecs.

Tested with Hikvision DS-2CD2023 and Rubetek RV-3414 IP-cameras, using VLC as a client.
There is a special mobile app for this server [on GitHub](https://github.com/vladpen/cams).

### Installation:

Copy config-example.py to private configuration file _config.py and edit _config.py.

Start the server:
```bash
python3 main.py
```

### Basic usage

UDP mode:
```bash
vlc rtsp://localhost:4554/camera-hash
```

TCP mode:
```bash
vlc --rtsp-tcp rtsp://localhost:4554/camera-hash
```

### Expose one camera under multiple RTSP paths

To publish the same physical camera via several URLs, register aliases in
`_config.py`. The primary entry must contain the camera `url`, while each alias
specifies a `source` pointing to that primary key:

```python
class Config:
    cameras = {
        'cam1': {
            'url': 'rtsp://admin:admin@192.168.1.1:554',
        },
        'front-door': {
            'source': 'cam1',
        },
    }
```

Clients can then connect to either `rtsp://<server>:4554/cam1` or
`rtsp://<server>:4554/front-door`; both URLs deliver packets from the same
shared camera connection.

### Start on boot with systemd

Create the service unit /etc/systemd/system/python-rtsp-server.service:

```bash
[Unit]
Description="video monitoring"

[Service]
ExecStart=/usr/bin/python3 /path-to-python-rtsp-server/main.py

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
sudo systemctl enable python-rtsp-server
sudo systemctl start python-rtsp-server
```

Discussion: [habr.com/ru/post/597363](https://habr.com/ru/post/597363).

*Copyright (c) 2021-2024 vladpen under MIT license. Use it with absolutely no warranty.*
