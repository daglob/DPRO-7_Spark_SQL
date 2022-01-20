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


# +--------+--------------+-----------+-----------+----------+------------------+
# |iso_code|      location| population|total_cases|      date|           percent|
# +--------+--------------+-----------+-----------+----------+------------------+
# |     VAT|       Vatican|      809.0|        6.0|2020-03-31|0.7416563658838072|
# |     SMR|    San Marino|    33938.0|      236.0|2020-03-31|0.6953857033413872|
# |     AND|       Andorra|    77265.0|      376.0|2020-03-31|0.4866368989840160|
# |     LUX|    Luxembourg|   625976.0|     2178.0|2020-03-31|0.3479366621084514|
# |     ISL|       Iceland|   341250.0|     1135.0|2020-03-31|0.3326007326007326|
# |     ESP|         Spain| 46754783.0|    95923.0|2020-03-31|0.2051618975538823|
# |     CHE|   Switzerland|  8654618.0|    16605.0|2020-03-31|0.1918628875358797|
# |     LIE| Liechtenstein|    38137.0|       68.0|2020-03-31|0.1783045336549807|
# |     ITA|         Italy| 60461828.0|   105792.0|2020-03-31|0.1749732078891164|
# |     MCO|        Monaco|    39244.0|       52.0|2020-03-31|0.1325043318723881|
# |     AUT|       Austria|  9006400.0|    10180.0|2020-03-31|0.1130307337004797|
# |     BEL|       Belgium| 11589616.0|    12775.0|2020-03-31|0.1102279833947906|
# |OWID_EUN|European Union|444919060.0|   397713.0|2020-03-31|0.0893899667953088|
# |     DEU|       Germany| 83783945.0|    71808.0|2020-03-31|0.0857061576654095|
# |     NOR|        Norway|  5421242.0|     4641.0|2020-03-31|0.0856076891605282|
# +--------+--------------+-----------+-----------+----------+------------------+
# only showing top 15 rows