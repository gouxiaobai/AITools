import unittest

from store import PacketStore


class StoreTests(unittest.TestCase):
    def test_capacity_limit(self):
        store = PacketStore(max_packets=3)
        for i in range(5):
            store.add("C2S", b"\x01", {"action": i})
        packets = store.get_all()
        self.assertEqual(len(packets), 3)
        self.assertEqual([p.action for p in packets], [2, 3, 4])

    def test_listener_add_remove(self):
        store = PacketStore()
        seen = []

        def listener(pkt):
            seen.append(pkt.id)

        store.add_listener(listener)
        store.add("C2S", b"\x01", {"action": 1})
        store.remove_listener(listener)
        store.add("C2S", b"\x01", {"action": 2})
        self.assertEqual(seen, [0])

    def test_next_msg_id_peek(self):
        store = PacketStore()
        self.assertEqual(store.peek_next_msg_id(), 1)
        self.assertEqual(store.next_msg_id(), 1)
        self.assertEqual(store.peek_next_msg_id(), 2)


if __name__ == "__main__":
    unittest.main()
