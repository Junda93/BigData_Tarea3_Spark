from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, round, desc

# 1. Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("Analisis_Batch_Recaudo_ANI") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n==================================================")
print(" 1. CARGA DEL CONJUNTO DE DATOS (BATCH)")
print("==================================================\n")

# 2. Cargar el dataset desde el archivo CSV
file_path = "Recaudo_Vehicular_ANI.csv"
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

print(f"Total de registros cargados: {df_raw.count()}")
print("\nEstructura del esquema detectado:")
df_raw.printSchema()

print("\nPrimeras 5 filas del dataset original:")
df_raw.show(5, truncate=False)

print("\n==================================================")
print(" 2. LIMPIEZA Y TRANSFORMACIÓN DE DATOS")
print("==================================================\n")

# 3. Limpieza: eliminar registros duplicados y nulos en columnas críticas
df_cleaned = df_raw.dropna(subset=["Peaje", "Recaudo"]).dropDuplicates()

# Convertir columnas numéricas para asegurar cálculos correctos
df_processed = df_cleaned.withColumn("Recaudo", col("Recaudo").cast("double")) \
                         .withColumn("RecaudoFsv", col("RecaudoFsv").cast("double")) \
                         .withColumn("RecaudoSobrante", col("RecaudoSobrante").cast("double"))

print(f"Total de registros tras la limpieza: {df_processed.count()}")

print("\n==================================================")
print(" 3. ANÁLISIS EXPLORATORIO DE DATOS (EDA) Y AGREGACIONES")
print("==================================================\n")

# A. Conteo total de estaciones de peaje analizadas
total_peajes = df_processed.select("Peaje").distinct().count()
print(f"Cantidad total de estaciones de peaje analizadas: {total_peajes}\n")

# B. Top 10 Peajes con mayor recaudo económico consolidado
print("--- TOP 10 PEAJES CON MAYOR RECAUDO ---")
top_peajes = df_processed.groupBy("Peaje") \
    .agg(round(_sum("Recaudo"), 2).alias("Recaudo_Total"),
         count("*").alias("Total_Registros")) \
    .orderBy(desc("Recaudo_Total"))
top_peajes.show(10, truncate=False)

# C. Análisis de Sobrantes y Fondo de Seguridad Vial (FSV) consolidado
print("--- RESUMEN CONSOLIDADO DE RECAUDO NACIONAL ---")
resumen_general = df_processed.select(
    round(_sum("Recaudo"), 2).alias("Grand_Total_Recaudo"),
    round(_sum("RecaudoFsv"), 2).alias("Total_Recaudo_FSV"),
    round(_sum("RecaudoSobrante"), 2).alias("Total_Sobrantes")
)
resumen_general.show(truncate=False)

print("\n==================================================")
print(" PROCESAMIENTO BATCH FINALIZADO CON ÉXITO")
print("==================================================\n")

spark.stop()
