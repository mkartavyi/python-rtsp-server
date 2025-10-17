import asyncio
from _config import Config
from client import Client
from storage import Storage
from shared import Shared, CameraState


async def main():
    # Start one listener for all clients
    tasks = [asyncio.create_task(Client.listen())]

    base_states = {}

    # Register primary camera entries first
    for camera_hash, cfg in Config.cameras.items():
        source = cfg.get('source')
        if source:
            continue
        base_states[camera_hash] = CameraState(camera_hash, camera_hash)
        Shared.data[camera_hash] = base_states[camera_hash]

    # Register aliases that point to primary camera entries
    for camera_hash, cfg in Config.cameras.items():
        source = cfg.get('source', camera_hash)
        if source not in base_states:
            raise RuntimeError(f'Camera "{camera_hash}" references unknown source "{source}"')
        Shared.data[camera_hash] = base_states[source]

        # Start streams saving, if enabled
        if Config.storage_enable and not cfg.get('source'):
            s = Storage(camera_hash)
            tasks.append(asyncio.create_task(s.run()))
            tasks.append(asyncio.create_task(s.watchdog()))

    for t in tasks:
        await t


if __name__ == '__main__':
    asyncio.run(main())
