# Tarea 3 – Procesamiento de datos con Apache Spark y Kafka

**Estudiante:** Mery Johana Vergara  
**Universidad:** Universidad Nacional Abierta y a Distancia – UNAD  
**Curso:** Big Data

## Descripción

Este repositorio contiene el ejercicio de procesamiento de datos de sensores con Kafka y Spark Structured Streaming. Los datos son simulados y representan temperatura y humedad de diez sensores.

El productor envía una lectura cada segundo, seleccionando aleatoriamente un sensor. Kafka recibe los mensajes en el tema `sensor_data`. Spark calcula los promedios por sensor en ventanas de un minuto y guarda los resultados en archivos JSON.

## Archivos

- `sensor_producer.py`: genera y envía las lecturas a Kafka.
- `spark_streaming.py`: procesa los mensajes y calcula los promedios.
- `grafica_sensores.py`: genera las gráficas del último minuto disponible.
- `grafica_sensores.png`: imagen obtenida durante la prueba.

## Entorno utilizado

- Ubuntu en VirtualBox.
- Python 3.10 y Java 11.
- Apache Kafka 3.9.2.
- Apache Spark 3.5.8, con Scala 2.12.
- Bibliotecas de Python: kafka-python y matplotlib.

## Ejecución

Los comandos siguientes corresponden a la máquina utilizada, con los programas en `/home/hadoop`. Kafka debe estar disponible en `localhost:9092` y debe existir el tema `sensor_data`.

### 1. Iniciar el productor

```bash
python3 /home/hadoop/sensor_producer.py
```

### 2. Ejecutar Spark en otra terminal

```bash
/home/hadoop/spark-3.5.8-bin-hadoop3/bin/spark-submit --driver-memory 512m --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 /home/hadoop/spark_streaming.py
```

Spark procesa los mensajes con un intervalo configurado de diez segundos y guarda los promedios en `/home/hadoop/resultados_sensores`.

### 3. Generar la gráfica

Una vez guardados los resultados, ejecutar en otra terminal:

```bash
python3 /home/hadoop/grafica_sensores.py
```

La imagen se guarda en `/home/hadoop/grafica_sensores.png`.

## Resultado

![Promedios de temperatura y humedad por sensor](grafica_sensores.png)

La gráfica muestra los promedios del último minuto disponible en los resultados. Es una imagen estática: para actualizarla es necesario ejecutar nuevamente el programa de gráficos.

## Alcance

Este ejercicio demuestra la integración entre un productor de datos, Kafka, Spark Structured Streaming y una visualización con Matplotlib. Corresponde al componente de sensores de la actividad.
