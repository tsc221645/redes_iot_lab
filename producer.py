import argparse
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from codec import DIRECCIONES, encode
from config import BOOTSTRAP, TOPIC, MODE, OFFLINE, kafka_security

def generar_medicion():
    # La guía dice 'Uniforme (Normal)' y pide centrada en 55; se usa normal truncada.
    return {
        'temperatura': round(min(110, max(0, random.gauss(55, 15))), 2),
        'humedad': round(min(100, max(0, random.gauss(55, 18)))),
        'direccion_viento': random.choice(DIRECCIONES),
    }

def serializar(data, mode):
    return encode(data) if mode == 'binary' else json.dumps(data, ensure_ascii=False).encode('utf-8')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['json', 'binary'], default=MODE)
    parser.add_argument('--interval', type=float, default=20)
    parser.add_argument('--count', type=int, default=0, help='0 = sin límite')
    args = parser.parse_args()
    if not 15 <= args.interval <= 30:
        parser.error('--interval debe estar entre 15 y 30 segundos')
    producer = None if OFFLINE else KafkaProducer(bootstrap_servers=BOOTSTRAP.split(','), **kafka_security())
    print(f'Productor: topic={TOPIC}, modo={args.mode}, offline={OFFLINE}', flush=True)
    try:
        n = 0
        while not args.count or n < args.count:
            data = generar_medicion()
            payload = serializar(data, args.mode)
            if producer:
                metadata = producer.send(TOPIC, key=b'sensor1', value=payload).get(timeout=15)
                status = f'partition={metadata.partition}, offset={metadata.offset}'
            else:
                with open('telemetria.jsonl', 'a', encoding='utf-8') as f:
                    f.write(json.dumps({'ts': datetime.now(timezone.utc).isoformat(), 'payload_hex': payload.hex(), 'mode': args.mode}) + '\n')
                status = 'guardado en telemetria.jsonl'
            print(f'{data} | payload={len(payload)} bytes | {status}', flush=True)
            n += 1
            if not args.count or n < args.count:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print('\nProductor detenido')
    finally:
        if producer:
            producer.flush(timeout=10)
            producer.close()

if __name__ == '__main__':
    main()
