"""Codificación exacta de telemetría en 24 bits."""
DIRECCIONES = ('N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE')

def encode(data: dict) -> bytes:
    temp = round(float(data['temperatura']) * 100)
    hum = int(data['humedad'])
    viento = data['direccion_viento']
    if not 0 <= temp <= 11000 or not 0 <= hum <= 100 or viento not in DIRECCIONES:
        raise ValueError('Medición fuera de rango')
    packed = (temp << 10) | (hum << 3) | DIRECCIONES.index(viento)
    return packed.to_bytes(3, 'big')

def decode(payload: bytes) -> dict:
    if len(payload) != 3:
        raise ValueError('El payload debe medir exactamente 3 bytes')
    packed = int.from_bytes(payload, 'big')
    temp = packed >> 10
    hum = (packed >> 3) & 0x7f
    viento = packed & 0x07
    if temp > 11000 or hum > 100:
        raise ValueError('Payload contiene valores fuera de rango')
    return {'temperatura': temp / 100, 'humedad': hum, 'direccion_viento': DIRECCIONES[viento]}
