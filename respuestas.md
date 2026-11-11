# Respuestas teóricas — Estación Meteorológica IoT

## 3.1 JSON/SOAP

**¿A qué capa OSI pertenecen y por qué?** Se asocian principalmente con la capa 7, aplicación, porque son formatos/protocolos de representación e intercambio de información entre aplicaciones. JSON es un formato de datos y SOAP un protocolo de mensajería, no capas OSI independientes. La codificación/representación también puede relacionarse conceptualmente con las funciones de presentación (capa 6).

**Beneficios:** interoperabilidad, legibilidad, estructura consistente, facilidad de validación, mantenimiento, integración entre lenguajes y posibilidad de extender campos. JSON suele ser más compacto y sencillo que XML/SOAP, aunque ambos añaden sobrecarga frente a un formato binario.

## 3.3 Kafka Pub/Sub

**Ventajas y desventajas:** Pub/Sub desacopla productores de consumidores, permite múltiples consumidores, retención/reprocesamiento, tolerancia a fallos y escalabilidad mediante particiones. A cambio requiere administrar el broker, manejar autenticación, particiones, offsets, reintentos y posibles duplicados. Para un único sensor y un solo consumidor puede ser más complejo de lo necesario. El orden está garantizado dentro de cada partición, no globalmente entre particiones.

**Aplicaciones adecuadas e inadecuadas:** Kafka es útil para telemetría masiva, eventos de negocio, logs, analítica en tiempo real y pipelines con varios consumidores. Puede ser excesivo para dispositivos con recursos muy limitados, mensajes esporádicos de una sola fuente, o control de muy baja latencia que necesita respuesta inmediata; MQTT u otros mecanismos pueden ser más simples en esos escenarios.

## 3.4 Payload restringido

**Complejidades:** empaquetado y desempaquetado a nivel de bits, control de rango, precisión, versión del esquema, depuración más difícil, compatibilidad y necesidad de acordar el orden exacto de los campos. También se pierde autodescripción respecto a JSON.

**¿Cómo cabe temperatura en 14 bits?** Multiplicando por 100 para guardar los centésimos como entero: 110.00 °C se convierte en 11000, menor que 2^14 = 16384. En la decodificación se divide por 100. Quedan 14 bits temperatura + 7 humedad + 3 viento = 24 bits = 3 bytes.

**¿Y si humedad tuviera un decimal?** Se requerirían 1001 valores posibles (0.0 a 100.0), por lo que serían necesarios 10 bits. La suma sería 14 + 10 + 3 = 27 bits y no cabría en 3 bytes. Habría que reducir precisión o rango de algún campo, enviar humedad en un mensaje aparte, usar compresión dependiente de los datos, o aumentar el límite de bytes. Por ejemplo, reducir temperatura a resolución de 0.1 °C usaría 11 bits (0–1100), y 11+10+3=24 bits.

**¿Qué herramientas/parámetros Kafka ayudarían?** Compresión del productor (`compression_type` como gzip, snappy, lz4 o zstd, según soporte), `batch_size` y `linger_ms` para agrupar mensajes, particiones para distribuir carga y ajustes de retención para almacenamiento. Sin embargo, la compresión y el batching no garantizan que **cada value individual** mida 3 bytes: el límite debe cumplirse con la codificación binaria. Kafka tampoco reduce las cabeceras del protocolo ni el tamaño de la clave al reducir el value.
