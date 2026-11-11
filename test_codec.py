import unittest
from codec import DIRECCIONES, encode, decode
from producer import generar_medicion, serializar

class TestCodec(unittest.TestCase):
    def test_boundaries(self):
        for t in (0, 55.55, 110):
            for h in (0, 55, 100):
                for w in DIRECCIONES:
                    original = {'temperatura': t, 'humedad': h, 'direccion_viento': w}
                    payload = encode(original)
                    self.assertEqual(len(payload), 3)
                    self.assertEqual(decode(payload), original)
    def test_random_samples(self):
        for _ in range(1000):
            d = generar_medicion()
            self.assertEqual(decode(encode(d)), d)
    def test_invalid(self):
        with self.assertRaises(ValueError):
            decode(b'12')
        with self.assertRaises(ValueError):
            encode({'temperatura': 111, 'humedad': 50, 'direccion_viento': 'N'})

if __name__ == '__main__':
    unittest.main()
