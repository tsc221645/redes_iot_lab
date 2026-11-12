import unittest
from decimal import Decimal
from codec import DIRECCIONES, encode, decode

class TestCodec(unittest.TestCase):
    def test_boundaries(self):
        for t in (0, 55.55, 110):
            for h in (0, 55, 100):
                for w in DIRECCIONES:
                    original = {'temperatura': t, 'humedad': h, 'direccion_viento': w}
                    payload = encode(original)
                    self.assertEqual(len(payload), 3)
                    self.assertEqual(decode(payload), original)
    def test_round_trip_samples(self):
        for temperature in (0, 0.01, 12.34, 55.55, 99.99, 110):
            for humidity in (0, 1, 55, 99, 100):
                for direction in DIRECCIONES:
                    data = {'temperatura': temperature, 'humedad': humidity, 'direccion_viento': direction}
                    self.assertEqual(decode(encode(data)), data)

    def test_layout_uses_24_bits(self):
        payload = encode({'temperatura': 110, 'humedad': 100, 'direccion_viento': 'NE'})
        self.assertEqual(payload, b'\xab\xe3\x27')
        self.assertEqual(int.from_bytes(payload, 'big'), (11000 << 10) | (100 << 3) | 7)

    def test_rejects_precision_types_and_ranges(self):
        for temperature in (55.555, 0.005, Decimal('NaN'), -0.01, 110.01):
            with self.subTest(temperature=temperature), self.assertRaises(ValueError):
                encode({'temperatura': temperature, 'humedad': 50, 'direccion_viento': 'N'})
        for humidity in (-1, 101, 55.9, True):
            with self.subTest(humidity=humidity), self.assertRaises(ValueError):
                encode({'temperatura': 55, 'humedad': humidity, 'direccion_viento': 'N'})
        with self.assertRaises(ValueError):
            encode({'temperatura': 55, 'humedad': 50, 'direccion_viento': 'NOPE'})

    def test_rejects_invalid_payload(self):
        for payload in (b'12', b'\xff\xff\xff'):
            with self.assertRaises(ValueError):
                decode(payload)

if __name__ == '__main__':
    unittest.main()
