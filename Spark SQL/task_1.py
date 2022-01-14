import pyspark.sql as pySparkSql
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Выберите 15 стран с наибольшим процентом переболевших на 31 марта
# (в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших)

spark = pySparkSql.SparkSession.builder.appName('PysparkSQL-HW_Task-1').master('local').getOrCreate()
data = spark.read.csv('/opt/bitnami/spark/volume/Spark SQL/owid-covid-data.csv', header=True, inferSchema=True)
data.withColumn('population', F.col('population').cast(T.DecimalType(18, 1))) \
    .withColumn('total_cases', F.col('total_cases').cast(T.DecimalType(18, 1))) \
    .select('iso_code',
            'location',
            'population',
            'total_cases',
            'date',
            (F.col('total_cases') / F.col('population') * 100).alias('percent')) \
    .where(F.col('date') == '2020-03-31') \
    .orderBy(F.col('percent').desc()) \
    .show(15)
