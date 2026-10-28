#!/usr/bin/env python3
"""Tarea 3 UNAD: analisis batch del CSV historico de TRM.
Ejecutar con spark-submit --master local[1] --driver-memory 512m analisis_trm.py
Entrada predeterminada: /home/hadoop/trm.csv
Salida: /home/hadoop/resultados_trm/ejecucion_FECHA_HORA (sin borrar anteriores).
Promedios por registro, no ponderados por dias de vigencia. Anios extremos parciales.
Referencia de fechas: https://spark.apache.org/docs/3.5.8/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_date.html
"""
import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession, functions as F


def guardar_csv(tabla, destino):
    # Solo resultados pequenos: escritura incremental sin depender de pandas.
    with destino.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(tabla.columns)
        for fila in tabla.toLocalIterator():
            w.writerow(list(fila))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--entrada', default='/home/hadoop/trm.csv')
    parser.add_argument('--salida', default='/home/hadoop/resultados_trm')
    args = parser.parse_args()
    entrada = Path(args.entrada).resolve()
    if not entrada.is_file():
        raise SystemExit('No se encuentra el CSV: ' + str(entrada))
    salida = Path(args.salida) / datetime.now().strftime('ejecucion_%Y%m%d_%H%M%S_%f')
    salida.mkdir(parents=True, exist_ok=False)
    spark = (SparkSession.builder.appName('Tarea3_TRM_Batch')
             .config('spark.sql.shuffle.partitions', '2')
             .config('spark.sql.ansi.enabled', 'false')
             .config('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        bruto = spark.read.option('header', True).option('mode', 'FAILFAST').csv(entrada.as_uri())
        columnas = ['VALOR', 'UNIDAD', 'VIGENCIADESDE', 'VIGENCIAHASTA']
        if bruto.columns != columnas:
            raise ValueError('Columnas inesperadas: ' + str(bruto.columns))
        inicial = bruto.count()
        nulos = bruto.select(*[
            F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ''), 1).otherwise(0)).alias(c)
            for c in columnas]).first().asDict()
        normal = bruto.select(*[F.trim(F.col(c)).alias(c) for c in columnas])
        tipado = normal.select(
            F.col('VALOR').cast('double').alias('valor'),
            F.upper('UNIDAD').alias('unidad'),
            F.to_date('VIGENCIADESDE', 'dd/MM/yyyy').alias('desde'),
            F.to_date('VIGENCIAHASTA', 'dd/MM/yyyy').alias('hasta'))
        valido = F.coalesce((F.col('valor') > 0) & ~F.isnan('valor') &
                            (F.col('valor') < float('inf')) & (F.col('unidad') == 'COP') &
                            F.col('desde').isNotNull() & F.col('hasta').isNotNull() &
                            (F.col('hasta') >= F.col('desde')), F.lit(False))
        rechazados = tipado.filter(~valido)
        validos = tipado.filter(valido)
        n_validos = validos.count()
        limpio = validos.dropDuplicates().withColumn('anio', F.year('desde')).withColumn('mes', F.month('desde')).cache()
        n_limpios = limpio.count()
        if not n_limpios:
            raise ValueError('No hay registros validos para analizar')
        calidad = {'registros_originales': inicial, 'nulos_o_vacios_por_columna': nulos,
                   'registros_invalidos': inicial - n_validos,
                   'duplicados_validos_eliminados': n_validos - n_limpios,
                   'registros_limpios': n_limpios}
        print('\nCONTROL DE CALIDAD\n' + json.dumps(calidad, indent=2))
        limpio.printSchema()
        limpio.orderBy('desde').show(5, truncate=False)
        estadisticas = limpio.select('valor').summary()
        estadisticas.show(truncate=False)
        anual = limpio.groupBy('anio').agg(F.count('*').alias('registros'),
                   F.avg('valor').alias('promedio'), F.min('valor').alias('minimo'),
                   F.max('valor').alias('maximo')).orderBy('anio')
        altos = limpio.filter(F.col('valor') > 5000).orderBy(F.desc('valor'))
        print('\nREGISTROS CON TRM MAYOR A 5000 COP/USD')
        altos.show(20, truncate=False)
        anual.show(50, truncate=False)
        for nombre, tabla in [('datos_limpios', limpio.orderBy('desde')),
                              ('estadisticas', estadisticas), ('resumen_anual', anual),
                              ('valores_mayores_5000', altos), ('rechazados', rechazados)]:
            guardar_csv(tabla, salida / (nombre + '.csv'))
        (salida / 'calidad.json').write_text(json.dumps(calidad, indent=2), encoding='utf-8')
        # Graficar solo el resumen anual agregado por Spark.
        filas = anual.collect()
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        anios = [r['anio'] for r in filas]
        fig, ax = plt.subplots(2, 1, figsize=(12, 8), constrained_layout=True)
        ax[0].plot(anios, [r['promedio'] for r in filas], color='#176c99', marker='o', markersize=3)
        ax[0].fill_between(anios, [r['minimo'] for r in filas], [r['maximo'] for r in filas], alpha=.18, label='Minimo y maximo')
        ax[0].set_title('TRM promedio por registro y rango anual')
        ax[0].set_ylabel('COP por USD')
        ax[0].legend()
        ax[1].bar(anios, [r['registros'] for r in filas], color='#176c99')
        ax[1].set_title('Cantidad de registros por ano de inicio de vigencia')
        ax[1].set_ylabel('Registros')
        ax[1].set_xlabel('Ano')
        for eje in ax:
            eje.grid(axis='y', alpha=.2)
        fig.suptitle('Historico de TRM | Fuente: trm.csv\nPromedios no ponderados por dias; anos extremos con cobertura parcial', fontsize=12)
        fig.savefig(salida / 'grafica_trm.png', dpi=160)
        plt.close(fig)
        (salida / 'notas.txt').write_text(
            'Fuente: CSV historico TRM, Datos Abiertos Colombia, mcec-87by.\n'
            'Analisis descriptivo; no predice tasas futuras ni explica causas economicas.\n'
            'Cada registro puede abarcar varios dias. Los promedios no se ponderan por dias.\n'
            'Se eliminan duplicados exactos y registros invalidos; no se imputan valores.\n'
            'Los valores altos validos se conservan. Los anos extremos son parciales.\n'
            'El CSV pequeno permite demostrar Spark, pero no medir escalabilidad industrial.\n', encoding='utf-8')
        print('\nANALISIS TERMINADO. Resultados en: ' + str(salida), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
