import os
from dotenv import load_dotenv
load_dotenv()

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', '161.35.11.141:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'iot-estacion-demo')
USERNAME = os.getenv('KAFKA_USERNAME', '')
PASSWORD = os.getenv('KAFKA_PASSWORD', '')
MODE = os.getenv('IOT_MODE', 'binary').lower()
OFFLINE = os.getenv('IOT_OFFLINE', 'false').lower() in ('true', '1', 'yes')

def kafka_security():
    if not USERNAME or not PASSWORD:
        raise RuntimeError('Configura KAFKA_USERNAME y KAFKA_PASSWORD en .env')
    return dict(security_protocol='SASL_PLAINTEXT', sasl_mechanism='PLAIN', sasl_plain_username=USERNAME, sasl_plain_password=PASSWORD)
