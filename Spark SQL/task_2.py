import pyspark.sql as pySparkSql
from pyspark.sql import Window
from pyspark.sql import functions as F

# Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021
# в отсортированном порядке по убыванию
# (в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)

spark = pySparkSql.SparkSession.builder.appName('PysparkSQL-HW_Task-1').master('local').getOrCreate()
data = spark.read.csv('/opt/bitnami/spark/volume/Spark SQL/owid-covid-data.csv', header=True, inferSchema=True)
window = Window().partitionBy('location')
data.withColumn('max_new_cases', F.max('new_cases').over(window)) \
    .select(F.to_date(F.col('date')).alias('date'),
            'location',
            'new_cases') \
    .where(F.col('date').between('2021-03-24', '2021-03-31')) \
    .where(F.col('location') != 'World') \
    .where(F.col('max_new_cases') == F.col('new_cases')) \
    .orderBy(F.col('max_new_cases').desc()) \
    .show(10)

# +----------+--------------------+---------+
# |      date|            location|new_cases|
# +----------+--------------------+---------+
# |2021-03-25|              Brazil| 100158.0|
# |2021-03-26|             Hungary|  11265.0|
# |2021-03-30|              Serbia|   9983.0|
# |2021-03-30|              Greece|   4322.0|
# |2021-03-29|              Kosovo|   4257.0|
# |2021-03-29|Bosnia and Herzeg...|   3755.0|
# |2021-03-24|             Moldova|   2273.0|
# |2021-03-26|               Kenya|   2008.0|
# |2021-03-31|     North Macedonia|   1511.0|
# |2021-03-25|       Cote d'Ivoire|    767.0|
# +----------+--------------------+---------+
# only showing top 10 rows
