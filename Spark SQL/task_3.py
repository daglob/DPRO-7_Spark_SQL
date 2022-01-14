import pyspark.sql as pySparkSql
from pyspark.sql import Window
from pyspark.sql import functions as F

# Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021.
# (например: в россии вчера было 9150, сегодня 8763, итог: -387)
# (в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)

spark = pySparkSql.SparkSession.builder.appName('PysparkSQL-HW_Task-1').master('local').getOrCreate()
data = spark.read.csv('/opt/bitnami/spark/volume/Spark SQL/owid-covid-data.csv', header=True, inferSchema=True)
window = Window().partitionBy("iso_code").orderBy('date')
data \
    .withColumn('before_cases', F.lag('new_cases').over(window)) \
    .select(F.to_date(F.col('date')).alias('date'),
            'before_cases',
            'new_cases',
            (F.col('before_cases') - F.col('new_cases')).alias('delta')) \
    .where(F.col('iso_code') == 'RUS') \
    .where(F.col('date').between('2021-03-24', '2021-03-31')) \
    .show()
