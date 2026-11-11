# Estación meteorológica IoT — CC3067

## Arquitectura
`Sensor simulado (producer.py) -> Kafka Broker -> consumer.py -> gráficas en vivo`

## Instalación
Requiere Python 3.10+ y acceso de red al broker de la universidad.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # Windows PowerShell: Copy-Item .env.example .env
```

Edita `.env`: establece `KAFKA_TOPIC` con el carné de uno de los integrantes y `KAFKA_PASSWORD` con la contraseña indicada en el PDF de la actividad. No publiques el archivo `.env`.

## Ejecutar contra Kafka
En terminal 1:
```bash
python consumer.py --mode json
```
En terminal 2:
```bash
python producer.py --mode json --interval 20
```
Captura la terminal y las gráficas. Detén ambos con Ctrl+C o cierra la ventana del consumidor.

Para la fase de **3 bytes**, cambia el modo en ambas terminales:
```bash
python consumer.py --mode binary
python producer.py --mode binary --interval 20
```
El productor imprime `payload=3 bytes`; toma capturas del resultado y de las gráficas. Los modos deben coincidir. Se usa el mismo topic con distintos modos solo de forma secuencial, no simultánea.

## Modo offline para demostración sin broker
Configura `IOT_OFFLINE=true` en `.env`. Inicia primero el consumidor y después el productor con el mismo modo. Los mensajes se escriben a `telemetria.jsonl` y se siguen en tiempo real. **Esto NO demuestra conectividad Kafka**: solo permite probar simulación, codificación y gráficas sin el servidor.

## Pruebas
```bash
python -m unittest test_codec.py -v
```

## Codificación de 24 bits

| Campo | Bits | Representación |
|---|---:|---|
| Temperatura | 14 | `round(temperatura * 100)`; 0–11000 |
| Humedad | 7 | Entero 0–100 |
| Viento | 3 | Índice 0–7 de N, NO, O, SO, S, SE, E, NE |

Orden de bits: `[temperatura:14][humedad:7][viento:3]`.

```python
packed = (temp_entera << 10) | (humedad << 3) | viento_indice
payload = packed.to_bytes(3, 'big')
```

El campo `key=b'sensor1'` de Kafka **no forma parte de los 3 bytes del value**; Kafka añade sus propias cabeceras y metadatos. La restricción del laboratorio se interpreta como el tamaño del `value` (payload de aplicación).

## Evidencias para Canvas
1. Captura de las pruebas unitarias exitosas.
2. Captura del productor JSON y consumidor con las tres gráficas.
3. Captura del productor binario indicando `payload=3 bytes` y consumidor graficando los datos decodificados.
4. Capturas del broker/terminal que permitan demostrar publicación y consumo reales.
5. PDF con explicaciones y respuestas teóricas (archivo `respuestas.md`).

**Nota sobre la distribución:** la guía dice literalmente “Distribución Uniforme (Normal)” pero también solicita valores centrados en 55 con varianza elegida. Son distribuciones distintas; este proyecto usa normal truncada con media subyacente 55, desviación 15 para temperatura y 18 para humedad, con valores limitados a sus rangos válidos. La media observada de humedad puede variar levemente por truncamiento y redondeo.
