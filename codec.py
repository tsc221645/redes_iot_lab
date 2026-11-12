"""Codificación exacta de telemetría en 24 bits."""
from decimal import Decimal, InvalidOperation

DIRECCIONES = ('N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE')

def encode(data: dict) -> bytes:
    try:
        temp_decimal = Decimal(str(data['temperatura']))
        hum = data['humedad']
        viento = data['direccion_viento']
    except (KeyError, TypeError, InvalidOperation) as exc:
        raise ValueError('Medicion invalida') from exc
    if not temp_decimal.is_finite():
        raise ValueError('La temperatura debe ser finita')
    temp_scaled = temp_decimal * 100
    if temp_scaled != temp_scaled.to_integral_value():
        raise ValueError('La temperatura debe tener como maximo dos decimales')
    temp = int(temp_scaled)
    if isinstance(hum, bool) or not isinstance(hum, int):
        raise ValueError('La humedad debe ser un entero')
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
