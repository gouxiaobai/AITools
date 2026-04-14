"""
协议编解码模块
格式：
  客户端发包：[4B 包长 大端序] + [XOR加密的 MessagePack 数据]
  服务端收包：[4B 包长 大端序] + [1B 压缩标志] + [XOR加密的 MessagePack 数据]
XOR密钥：max(1, 数据长度 % 256)
"""
import struct
import gzip
import msgpack

MAX_PACKET_SIZE = 10 * 1024 * 1024  # 10MB 防护上限


def xor_codec(data: bytes) -> bytes:
    """XOR 编解码（加密与解密共用同一函数）"""
    key = max(1, len(data) % 256)
    return bytes(b ^ key for b in data)


def encode_packet(msg: dict) -> bytes:
    """
    将消息字典编码为完整的发送格式
    返回：[4B 包长 大端序] + [XOR加密的 MessagePack 数据]
    """
    raw = msgpack.packb(msg, use_bin_type=True)
    encrypted = xor_codec(raw)
    return struct.pack(">I", len(encrypted)) + encrypted


def decode_c2s_payload(data: bytes) -> dict:
    """解码客户端→服务端包体（不含4字节长度头）"""
    decrypted = xor_codec(data)
    return msgpack.unpackb(decrypted, raw=False)


def decode_s2c_payload(data: bytes) -> dict:
    """解码服务端→客户端包体（不含4字节长度头，含1字节压缩标志）"""
    if len(data) < 1:
        raise ValueError("包体过短")
    compressed_flag = data[0]
    payload = data[1:]
    if compressed_flag == 1:
        payload = gzip.decompress(payload)
    decrypted = xor_codec(payload)
    return msgpack.unpackb(decrypted, raw=False)


def read_packet(sock) -> bytes | None:
    """
    从 socket 精确读取一个完整数据包
    返回包体字节（不含4字节长度头），连接断开返回 None
    """
    length_bytes = _recv_exact(sock, 4)
    if length_bytes is None:
        return None
    length = struct.unpack(">I", length_bytes)[0]
    if length <= 0 or length > MAX_PACKET_SIZE:
        return None
    return _recv_exact(sock, length)


def _recv_exact(sock, n: int) -> bytes | None:
    """精确读取 n 字节，连接断开或异常返回 None"""
    buf = b""
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except OSError:
            return None
        if not chunk:
            return None
        buf += chunk
    return buf
