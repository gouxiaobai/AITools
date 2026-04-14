import gzip
import struct
import unittest

import msgpack

from protocol import decode_c2s_payload, decode_s2c_payload, encode_packet, read_packet, xor_codec


class _FakeSocket:
    def __init__(self, chunks):
        self._chunks = list(chunks)

    def recv(self, n):
        if not self._chunks:
            return b""
        chunk = self._chunks.pop(0)
        if len(chunk) <= n:
            return chunk
        self._chunks.insert(0, chunk[n:])
        return chunk[:n]


class ProtocolTests(unittest.TestCase):
    def test_c2s_round_trip(self):
        msg = {"action": 19660, "commReq": {"sid": 1, "token": "abc"}}
        packet = encode_packet(msg)
        length = struct.unpack(">I", packet[:4])[0]
        self.assertEqual(length, len(packet) - 4)
        self.assertEqual(decode_c2s_payload(packet[4:]), msg)

    def test_s2c_decode_compressed(self):
        msg = {"commResp": {"action": 50006, "code": 0}, "items": [1, 2, 3]}
        packed = msgpack.packb(msg, use_bin_type=True)
        encrypted = xor_codec(packed)
        compressed = gzip.compress(encrypted)
        payload = bytes([1]) + compressed
        self.assertEqual(decode_s2c_payload(payload), msg)

    def test_read_packet_invalid_length(self):
        sock = _FakeSocket([struct.pack(">I", 0)])
        self.assertIsNone(read_packet(sock))


if __name__ == "__main__":
    unittest.main()
