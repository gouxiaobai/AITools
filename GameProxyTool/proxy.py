"""
TCP MITM proxy.
Forwards traffic between client and game server, records packets, and supports packet injection.
"""
import socket
import struct
import threading
import time

from protocol import decode_c2s_payload, decode_s2c_payload, encode_packet, read_packet
from store import PacketStore


class Connection:
    """A bidirectional client <-> server TCP tunnel."""

    def __init__(self, client_sock: socket.socket, server_sock: socket.socket, store: PacketStore):
        self.client_sock = client_sock
        self.server_sock = server_sock
        self.store = store
        self.active = True
        self._send_lock = threading.Lock()

    def start(self):
        threading.Thread(target=self._forward_c2s, daemon=True).start()
        threading.Thread(target=self._forward_s2c, daemon=True).start()

    def close(self):
        self.active = False
        for sock in (self.client_sock, self.server_sock):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def _forward_c2s(self):
        """Intercept and forward: client -> server."""
        while self.active:
            raw = read_packet(self.client_sock)
            if raw is None:
                self.active = False
                break
            try:
                decoded = decode_c2s_payload(raw)
            except Exception as exc:
                decoded = {"_parse_error": str(exc), "_raw_hex": raw.hex()[:200]}

            self.store.add("C2S", raw, decoded)
            self._send_raw_to(self.server_sock, raw)

    def _forward_s2c(self):
        """Intercept and forward: server -> client."""
        while self.active:
            raw = read_packet(self.server_sock)
            if raw is None:
                self.active = False
                break
            try:
                decoded = decode_s2c_payload(raw)
            except Exception as exc:
                decoded = {"_parse_error": str(exc), "_raw_hex": raw.hex()[:200]}

            self.store.add("S2C", raw, decoded)
            self._send_raw_to(self.client_sock, raw)

    def _send_raw_to(self, sock: socket.socket, raw: bytes):
        """Send full packet with length header."""
        try:
            with self._send_lock:
                sock.sendall(struct.pack(">I", len(raw)) + raw)
        except OSError:
            self.active = False

    def inject(self, msg: dict) -> int:
        """Inject a custom packet to game server."""
        msg.setdefault("commReq", {})
        msg["commReq"].setdefault("sid", self.store.next_msg_id())
        msg["commReq"].setdefault("token", self.store.token or "")
        msg["commReq"].setdefault("cv", 1)
        sid = msg["commReq"]["sid"]

        data = encode_packet(msg)  # [4B len] + [XOR msgpack]
        try:
            with self._send_lock:
                self.server_sock.sendall(data)
            self.store.add("C2S", data[4:], msg)
            return sid
        except OSError as exc:
            raise RuntimeError(f"inject failed: {exc}")


class TCPProxy:
    """Listen on local port and transparently proxy to game server."""

    def __init__(self, listen_port: int, server_host: str | None, server_port: int | None, store: PacketStore):
        self.listen_port = listen_port
        self.server_host = server_host
        self.server_port = server_port
        self.store = store
        self._srv_sock: socket.socket | None = None
        self._connections: list[Connection] = []
        self._conn_lock = threading.Lock()
        self._running = False

    def set_target(self, host: str, port: int):
        self.server_host = host
        self.server_port = port
        print(f"[Proxy] discovered game server: {host}:{port}", flush=True)

    def start(self):
        if self._running:
            return
        self._srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv_sock.bind(("0.0.0.0", self.listen_port))
        self._srv_sock.listen(10)
        self._running = True
        print(f"[Proxy] listen 0.0.0.0:{self.listen_port} -> {self.server_host}:{self.server_port}", flush=True)
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def stop(self):
        if not self._running:
            return
        self._running = False

        if self._srv_sock is not None:
            try:
                self._srv_sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._srv_sock.close()
            except OSError:
                pass
            self._srv_sock = None

        with self._conn_lock:
            conns = list(self._connections)
            self._connections.clear()
        for conn in conns:
            conn.close()

    def _accept_loop(self):
        while self._running and self._srv_sock is not None:
            try:
                client_sock, addr = self._srv_sock.accept()
                print(f"[Proxy] new connection: {addr}", flush=True)
            except OSError:
                break

            waited = 0.0
            while self._running and not self.server_host and waited < 15:
                time.sleep(0.5)
                waited += 0.5

            if not self._running:
                try:
                    client_sock.close()
                except OSError:
                    pass
                break

            if not self.server_host:
                print("[Proxy] game server not discovered yet, close incoming connection", flush=True)
                client_sock.close()
                continue

            try:
                server_sock = socket.create_connection((self.server_host, self.server_port), timeout=10)
                server_sock.settimeout(None)
            except Exception as exc:
                print(f"[Proxy] connect game server failed: {exc}", flush=True)
                client_sock.close()
                continue

            conn = Connection(client_sock, server_sock, self.store)
            with self._conn_lock:
                self._connections.append(conn)
            conn.start()

    def inject(self, msg: dict, conn_index: int = 0) -> int:
        conns = self._active_connections()
        if not conns:
            raise RuntimeError("no active connection")
        if conn_index >= len(conns):
            raise RuntimeError(f"connection index out of range: {conn_index}, total {len(conns)}")
        return conns[conn_index].inject(msg)

    def active_count(self) -> int:
        return len(self._active_connections())

    def _active_connections(self) -> list[Connection]:
        with self._conn_lock:
            return [conn for conn in self._connections if conn.active]
