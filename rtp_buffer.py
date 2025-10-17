import asyncio
from collections import deque
from typing import Deque, Tuple, Optional


class RTPPacketBuffer:
    """Thread-safe buffer that keeps the last N RTP packets."""

    def __init__(self, max_packets: int):
        if max_packets <= 0:
            raise ValueError('max_packets must be positive')
        self._max_packets = max_packets
        self._buffer: Deque[Tuple[int, bytes]] = deque(maxlen=max_packets)
        self._next_index = 0
        self._condition = asyncio.Condition()
        self._closed = False

    async def append(self, packet: bytes) -> None:
        """Store a new packet and notify all waiting readers."""
        if packet is None:
            return
        async with self._condition:
            if self._closed:
                return
            self._buffer.append((self._next_index, packet))
            self._next_index += 1
            self._condition.notify_all()

    async def create_reader(self) -> "RTPPacketReader":
        async with self._condition:
            start_index = self._buffer[0][0] if self._buffer else self._next_index
            return RTPPacketReader(self, start_index)

    async def close(self) -> None:
        async with self._condition:
            self._closed = True
            self._condition.notify_all()

    async def _read(self, reader: "RTPPacketReader") -> Optional[bytes]:
        while True:
            async with self._condition:
                if self._closed:
                    return None

                if self._buffer and reader._index < self._buffer[0][0]:
                    reader._index = self._buffer[0][0]

                for idx, data in self._buffer:
                    if idx >= reader._index:
                        reader._index = idx + 1
                        return data

                await self._condition.wait()


class RTPPacketReader:
    """Reader with an independent cursor into the RTP packet buffer."""

    def __init__(self, buffer: RTPPacketBuffer, start_index: int):
        self._buffer = buffer
        self._index = start_index
        self._closed = False

    async def read(self) -> Optional[bytes]:
        if self._closed:
            return None
        return await self._buffer._read(self)

    async def close(self) -> None:
        self._closed = True
