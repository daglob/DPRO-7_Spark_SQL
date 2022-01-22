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
    .where(F.col('date') == '2021-03-31') \
    .orderBy(F.col('percent').desc()) \
    .show(15)

# +--------+-------------+-----------+-----------+----------+-------------------+
# |iso_code|     location| population|total_cases|      date|            percent|
# +--------+-------------+-----------+-----------+----------+-------------------+
# |     AND|      Andorra|    77265.0|    12010.0|2021-03-31|15.5439073319096616|
# |     MNE|   Montenegro|   628062.0|    91218.0|2021-03-31|14.5237253646932946|
# |     CZE|      Czechia| 10708982.0|  1532332.0|2021-03-31|14.3088484040779973|
# |     SMR|   San Marino|    33938.0|     4730.0|2021-03-31|13.9371795627320408|
# |     SVN|     Slovenia|  2078932.0|   215602.0|2021-03-31|10.3708057791212026|
# |     LUX|   Luxembourg|   625976.0|    61642.0|2021-03-31| 9.8473423901235830|
# |     ISR|       Israel|  8655541.0|   833105.0|2021-03-31| 9.6251060447868019|
# |     USA|United States|331002647.0| 30462210.0|2021-03-31| 9.2030109958607068|
# |     SRB|       Serbia|  6804596.0|   600596.0|2021-03-31| 8.8263285579334908|
# |     BHR|      Bahrain|  1701583.0|   144445.0|2021-03-31| 8.4888600791145657|
# |     PAN|       Panama|  4314768.0|   355051.0|2021-03-31| 8.2287390654607617|
# |     PRT|     Portugal| 10196707.0|   821722.0|2021-03-31| 8.0586997351203678|
# |     EST|      Estonia|  1326539.0|   106424.0|2021-03-31| 8.0226815796595502|
# |     SWE|       Sweden| 10099270.0|   804886.0|2021-03-31| 7.9697443478588056|
# |     LTU|    Lithuania|  2722291.0|   216119.0|2021-03-31| 7.9388647282748244|
# +--------+-------------+-----------+-----------+----------+-------------------+
# only showing top 15 rows
