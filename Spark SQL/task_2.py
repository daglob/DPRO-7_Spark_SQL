import pyspark.sql as pySparkSql
from pyspark.sql import Window
from pyspark.sql import functions as F

# Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021
# в отсортированном порядке по убыванию
# (в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)

spark = pySparkSql.SparkSession.builder.appName('PysparkSQL-HW_Task-1').master('local').getOrCreate()
data = spark.read.csv('/opt/bitnami/spark/volume/Spark SQL/owid-covid-data.csv', header=True, inferSchema=True)
filtered_data = data.select(F.to_date(F.col('date')).alias('date'),
                            'location',
                            'new_cases') \
    .where(F.col('date').between('2021-03-24', '2021-03-31')) \
    .where(F.col('location') != 'World')

window = Window().partitionBy('location')
filtered_data.withColumn('max_new_cases', F.max('new_cases').over(window)) \
    .where(F.col('max_new_cases') == F.col('new_cases')) \
    .orderBy(F.col('max_new_cases').desc()) \
    .drop('new_cases') \
    .show(10)

# +----------+--------------+-------------+
# |      date|      location|max_new_cases|
# +----------+--------------+-------------+
# |2021-03-24|        Europe|     256192.0|
# |2021-03-31|European Union|     216452.0|
# |2021-03-31|          Asia|     183350.0|
# |2021-03-25| South America|     148476.0|
# |2021-03-24| North America|     100760.0|
# |2021-03-25|        Brazil|     100158.0|
# |2021-03-24| United States|      86960.0|
# |2021-03-31|         India|      72330.0|
# |2021-03-24|        France|      65392.0|
# |2021-03-31|        Turkey|      39302.0|
# +----------+--------------+-------------+
# only showing top 10 rows
