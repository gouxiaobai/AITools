"""
数据包存储与管理
"""
import threading
import time
from dataclasses import dataclass, field
from typing import Optional, Callable


@dataclass
class Packet:
    id: int
    direction: str          # 'C2S' | 'S2C'
    action: Optional[int]
    raw: bytes              # 包体原始字节（不含长度头）
    decoded: dict           # 解码后的字典
    timestamp: float = field(default_factory=time.time)
    note: str = ""          # 用户备注


class PacketStore:
    def __init__(self, max_packets: int = 10000):
        self._packets: list[Packet] = []
        self._next_id = 0
        self._lock = threading.Lock()
        self._listeners: list[Callable[[Packet], None]] = []
        self._listeners_lock = threading.Lock()
        self.token: Optional[str] = None
        self._msg_id = 1
        self._msg_id_lock = threading.Lock()
        self._max_packets = max(1, int(max_packets))

    # ------------------------------------------------------------------ #
    #  写入                                                                #
    # ------------------------------------------------------------------ #

    def add(self, direction: str, raw: bytes, decoded: dict) -> Packet:
        action = self._extract_action(decoded)

        # 自动从客户端包中提取 token
        if direction == "C2S":
            comm = decoded.get("commReq")
            if isinstance(comm, dict) and "token" in comm:
                self.token = comm["token"]

        with self._lock:
            pkt = Packet(
                id=self._next_id,
                direction=direction,
                action=action,
                raw=raw,
                decoded=decoded,
            )
            self._next_id += 1
            self._packets.append(pkt)
            # Keep bounded memory usage for long-running sessions.
            if len(self._packets) > self._max_packets:
                overflow = len(self._packets) - self._max_packets
                del self._packets[:overflow]

        with self._listeners_lock:
            listeners = list(self._listeners)
        for fn in listeners:
            try:
                fn(pkt)
            except Exception:
                pass

        return pkt

    def add_listener(self, fn: Callable[[Packet], None]):
        with self._listeners_lock:
            self._listeners.append(fn)

    def remove_listener(self, fn: Callable[[Packet], None]):
        with self._listeners_lock:
            try:
                self._listeners.remove(fn)
            except ValueError:
                pass

    # ------------------------------------------------------------------ #
    #  读取                                                                #
    # ------------------------------------------------------------------ #

    def get_all(self, after_id: int = -1) -> list[Packet]:
        with self._lock:
            if after_id < 0:
                return list(self._packets)
            return [p for p in self._packets if p.id > after_id]

    def get_by_id(self, packet_id: int) -> Optional[Packet]:
        with self._lock:
            for p in self._packets:
                if p.id == packet_id:
                    return p
            return None

    def clear(self):
        with self._lock:
            self._packets.clear()
            self._next_id = 0

    # ------------------------------------------------------------------ #
    #  消息序号                                                            #
    # ------------------------------------------------------------------ #

    def next_msg_id(self) -> int:
        with self._msg_id_lock:
            mid = self._msg_id
            self._msg_id += 1
            return mid

    def peek_next_msg_id(self) -> int:
        with self._msg_id_lock:
            return self._msg_id

    # ------------------------------------------------------------------ #
    #  内部工具                                                            #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _extract_action(decoded: dict) -> Optional[int]:
        if "action" in decoded:
            return decoded["action"]
        comm = decoded.get("commResp")
        if isinstance(comm, dict):
            return comm.get("action")
        return None
