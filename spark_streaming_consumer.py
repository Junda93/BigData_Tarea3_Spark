from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("SparkStreamingANI") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema según la estructura del CSV/JSON de la ANI
# Ajusta los nombres de las columnas a los que tiene tu archivo CSV de la ANI
schema = StructType([
    StructField("Peaje", StringType(), True),
    StructField("Fecha", StringType(), True),
    StructField("Categoria", StringType(), True),
    StructField("Valor", StringType(), True)
])

# 3. Leer la transmisión desde el nuevo tópico de Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ani_peajes_topic") \
    .load()

# 4. Deserializar el mensaje JSON y extraer los campos
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Mostrar el flujo de datos en consola en tiempo real
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
