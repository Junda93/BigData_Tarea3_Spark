import csv
import json
import time
from kafka import KafkaProducer

# Configuración del servidor Kafka y el nuevo tópico
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'ani_peajes_topic'  # Nuevo tópico de Kafka

# Inicializar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Ruta a tu archivo CSV de la ANI
CSV_FILE_PATH = 'recaudo_ani.csv'  # Ajusta con el nombre exacto de tu archivo

print(f"Iniciando la transmisión de datos al tópico '{TOPIC_NAME}'...")

try:
    with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Enviar cada fila formateada como JSON al tópico
            producer.send(TOPIC_NAME, value=row)
            print(f"Enviado: {row}")
            # Simular la frecuencia de transmisión (1 segundo entre registros)
            time.sleep(1)
            
except FileNotFoundError:
    print(f"Error: No se encontró el archivo en {CSV_FILE_PATH}")
except Exception as e:
    print(f"Error durante la ejecución: {e}")
finally:
    producer.flush()
    producer.close()
    print("Transmisión finalizada.")
