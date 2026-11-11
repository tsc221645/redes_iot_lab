import argparse
import json
import time
from collections import deque
from datetime import datetime
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from codec import DIRECCIONES, decode
from config import BOOTSTRAP, TOPIC, MODE, OFFLINE, kafka_security

def deserializar(payload, mode):
    return decode(payload) if mode == 'binary' else json.loads(payload.decode('utf-8'))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['json', 'binary'], default=MODE)
    parser.add_argument('--limit', type=int, default=100)
    args = parser.parse_args()
    if args.limit < 2:
        parser.error('--limit debe ser al menos 2')
    times, temps, hums, winds = (deque(maxlen=args.limit) for _ in range(4))
    plt.ion()
    fig, axs = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
    fig.suptitle(f'Estación meteorológica IoT | {TOPIC} | {args.mode.upper()}')
    plt.subplots_adjust(hspace=.35, bottom=.12)
    axs[0].set_ylabel('Temperatura (°C)')
    axs[1].set_ylabel('Humedad (%)')
    axs[2].set_ylabel('Dirección del viento')
    axs[0].set_ylim(0, 110)
    axs[1].set_ylim(0, 100)
    axs[2].set_yticks(range(8), DIRECCIONES)
    axs[2].set_ylim(-.5, 7.5)
    lines = [axs[0].plot([], [], '-o', markersize=3)[0], axs[1].plot([], [], '-o', markersize=3)[0], axs[2].step([], [], where='post')[0]]
    for ax in axs:
        ax.grid(alpha=.3)
    def update(data, stamp):
        times.append(stamp)
        temps.append(data['temperatura'])
        hums.append(data['humedad'])
        winds.append(DIRECCIONES.index(data['direccion_viento']))
        x = list(range(len(times)))
        for line, y in zip(lines, (temps, hums, winds)):
            line.set_data(x, list(y))
        axs[2].set_xlim(0, max(2, len(times)))
        ticks = list(range(0, len(times), max(1, len(times)//8)))
        axs[2].set_xticks(ticks, [list(times)[i].strftime('%H:%M:%S') for i in ticks], rotation=30)
        fig.canvas.draw_idle()
        plt.pause(.05)
        print(f'{stamp:%H:%M:%S} | {data} | muestras={len(times)}', flush=True)
    consumer = None
    try:
        if OFFLINE:
            print('Modo offline: siguiendo telemetria.jsonl')
            offset = 0
            while plt.fignum_exists(fig.number):
                try:
                    with open('telemetria.jsonl', encoding='utf-8') as f:
                        f.seek(offset)
                        for row in f:
                            item = json.loads(row)
                            if item['mode'] == args.mode:
                                update(deserializar(bytes.fromhex(item['payload_hex']), args.mode), datetime.fromisoformat(item['ts']).astimezone())
                        offset = f.tell()
                except FileNotFoundError:
                    pass
                plt.pause(.5)
        else:
            consumer = KafkaConsumer(TOPIC, bootstrap_servers=BOOTSTRAP.split(','), group_id=f'iot-visual-{args.mode}', auto_offset_reset='latest', consumer_timeout_ms=1000, **kafka_security())
            print(f'Escuchando {TOPIC} ({args.mode})...')
            while plt.fignum_exists(fig.number):
                for _, messages in consumer.poll(timeout_ms=500).items():
                    for msg in messages:
                        try:
                            update(deserializar(msg.value, args.mode), datetime.fromtimestamp(msg.timestamp/1000))
                        except (ValueError, KeyError, UnicodeDecodeError, json.JSONDecodeError) as e:
                            print(f'Mensaje omitido: {e}')
                plt.pause(.05)
    except KeyboardInterrupt:
        print('\nConsumidor detenido')
    finally:
        if consumer:
            consumer.close()
        if plt.fignum_exists(fig.number):
            fig.savefig('grafica_meteorologica.png', dpi=180, bbox_inches='tight')
        plt.close(fig)

if __name__ == '__main__':
    main()
