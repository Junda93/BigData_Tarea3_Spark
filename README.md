# Tarea 3 - Procesamiento de Datos con Apache Spark y Kafka

**Curso:** Big Data 202016911-90  
**Universidad:** Universidad Nacional Abierta y a Distancia (UNAD)  
**Estudiante:** Juan David Calle Ceballos  

---

## 📌 Descripción del Proyecto
Este repositorio contiene la solución a la Tarea 3 del curso de Big Data, enfocada en la implementación de arquitecturas de procesamiento masivo en lote (Batch) mediante **Apache Spark** y la simulación e ingesta de eventos en tiempo real (Streaming) utilizando **Apache Kafka** integrado con **Spark Structured Streaming**[cite: 15].

---

## 📁 Estructura del Repositorio
* **`spark_batch_analysis.py`**: Script de Python/PySpark que ejecuta el análisis en lote sobre el dataset de Recaudo Vehicular ANI[cite: 15, 21].
* **`kafka_producer.py`**: Productor en Python que simula la generación de eventos de telemetría de sensores y los publica en el tópico de Kafka[cite: 15].
* **`spark_streaming_consumer.py`**: Consumidor de Spark Structured Streaming que procesa el flujo en tiempo real mediante ventanas de tiempo[cite: 15].
* **`README.md`**: Guía de documentación, arquitectura e instrucciones de ejecución del proyecto[cite: 24].

---

## 📊 1. Procesamiento Batch (PySpark)
Se utilizó el dataset transaccional oficial **Recaudo Vehicular ANI** del portal de Datos Abiertos de Colombia (`datos.gov.co`)[cite: 21]. El script ejecuta:
1. **Carga y Esquema:** Ingesta del archivo CSV infiriendo esquemas y encabezados en un DataFrame de PySpark[cite: 15].
2. **Limpieza y Transformación:** Eliminación de duplicados, filtrado de valores nulos y casteo explícito de datos numéricos[cite: 15].
3. **Análisis Exploratorio (EDA):** Agregación de métricas para identificar los peajes con mayor recaudo económico, consolidado nacional de ingresos y aportes al Fondo de Seguridad Vial (FSV)[cite: 15, 21].

---

## ⚡ 2. Procesamiento en Tiempo Real (Kafka + Spark Streaming)
Se implementó el laboratorio del **Anexo 3** para el análisis continuo de eventos[cite: 15, 16]:
- **Apache Kafka:** Configuración de un clúster local con ZooKeeper y creación del tópico `sensor_data`[cite: 16].
- **Spark Streaming:** Consumo continuo del flujo de datos en JSON, parseo de esquemas y agregaciones por ventanas de tiempo de 1 minuto para calcular promedios de temperatura y humedad por sensor[cite: 15, 16].

---

## 🛠️ Tecnologías e Infraestructura
* **Lenguaje:** Python 3.10
* **Procesamiento Distribuido:** Apache Spark 3.5.3 (PySpark / Spark SQL)[cite: 15, 16]
* **Mensajería Distribuida:** Apache Kafka 3.9.2 & ZooKeeper[cite: 15, 16]
* **Entorno:** Ubuntu 22.04 LTS en máquina virtual Oracle VM VirtualBox conectada vía SSH por PuTTY[cite: 16]

---

## 🚀 Instrucciones de Ejecución

### A. Ejecución del Procesamiento Batch
1. Descargar el dataset oficial en el directorio de trabajo:
   ```bash
   wget -O Recaudo_Vehicular_ANI.csv "[https://www.datos.gov.co/api/views/qac6-9mkr/rows.csv?accessType=DOWNLOAD](https://www.datos.gov.co/api/views/qac6-9mkr/rows.csv?accessType=DOWNLOAD)"
