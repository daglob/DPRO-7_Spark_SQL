import pyspark.sql as pySparkSql

from pyspark.sql import functions as pySparkSqlFunc
from pyspark.sql import types as pySparkSqlTypes

# root
#  |-- iso_code: string (nullable = true)
#  |-- continent: string (nullable = true)
#  |-- location: string (nullable = true)
#  |-- date: string (nullable = true)
#  |-- total_cases: double (nullable = true)
#  |-- new_cases: double (nullable = true)
#  |-- new_cases_smoothed: double (nullable = true)
#  |-- total_deaths: double (nullable = true)
#  |-- new_deaths: double (nullable = true)
#  |-- new_deaths_smoothed: double (nullable = true)
#  |-- total_cases_per_million: double (nullable = true)
#  |-- new_cases_per_million: double (nullable = true)
#  |-- new_cases_smoothed_per_million: double (nullable = true)
#  |-- total_deaths_per_million: double (nullable = true)
#  |-- new_deaths_per_million: double (nullable = true)
#  |-- new_deaths_smoothed_per_million: double (nullable = true)
#  |-- reproduction_rate: double (nullable = true)
#  |-- icu_patients: double (nullable = true)
#  |-- icu_patients_per_million: double (nullable = true)
#  |-- hosp_patients: double (nullable = true)
#  |-- hosp_patients_per_million: double (nullable = true)
#  |-- weekly_icu_admissions: double (nullable = true)
#  |-- weekly_icu_admissions_per_million: double (nullable = true)
#  |-- weekly_hosp_admissions: double (nullable = true)
#  |-- weekly_hosp_admissions_per_million: double (nullable = true)
#  |-- new_tests: double (nullable = true)
#  |-- total_tests: double (nullable = true)
#  |-- total_tests_per_thousand: double (nullable = true)
#  |-- new_tests_per_thousand: double (nullable = true)
#  |-- new_tests_smoothed: double (nullable = true)
#  |-- new_tests_smoothed_per_thousand: double (nullable = true)
#  |-- positive_rate: double (nullable = true)
#  |-- tests_per_case: double (nullable = true)
#  |-- tests_units: string (nullable = true)
#  |-- total_vaccinations: double (nullable = true)
#  |-- people_vaccinated: double (nullable = true)
#  |-- people_fully_vaccinated: double (nullable = true)
#  |-- new_vaccinations: double (nullable = true)
#  |-- new_vaccinations_smoothed: double (nullable = true)
#  |-- total_vaccinations_per_hundred: double (nullable = true)
#  |-- people_vaccinated_per_hundred: double (nullable = true)
#  |-- people_fully_vaccinated_per_hundred: double (nullable = true)
#  |-- new_vaccinations_smoothed_per_million: double (nullable = true)
#  |-- stringency_index: double (nullable = true)
#  |-- population: double (nullable = true)
#  |-- population_density: double (nullable = true)
#  |-- median_age: double (nullable = true)
#  |-- aged_65_older: double (nullable = true)
#  |-- aged_70_older: double (nullable = true)
#  |-- gdp_per_capita: double (nullable = true)
#  |-- extreme_poverty: double (nullable = true)
#  |-- cardiovasc_death_rate: double (nullable = true)
#  |-- diabetes_prevalence: double (nullable = true)
#  |-- female_smokers: double (nullable = true)
#  |-- male_smokers: double (nullable = true)
#  |-- handwashing_facilities: double (nullable = true)
#  |-- hospital_beds_per_thousand: double (nullable = true)
#  |-- life_expectancy: double (nullable = true)
#  |-- human_development_index: double (nullable = true)

# Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021
# в отсортированном порядке по убыванию
# (в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)

spark = pySparkSql.SparkSession.builder.appName('PysparkSQL-HW_Task-1').master('local').getOrCreate()
data = spark.read.csv('/opt/bitnami/spark/volume/Spark SQL/owid-covid-data.csv', header=True, inferSchema=True)
data \
    .withColumn('population', pySparkSqlFunc.col('population').cast(pySparkSqlTypes.DecimalType(18, 1))) \
    .withColumn('total_cases', pySparkSqlFunc.col('total_cases').cast(pySparkSqlTypes.DecimalType(18, 1))) \
    .select('iso_code',
            'location',
            'population',
            'total_cases',
            'date',
            (pySparkSqlFunc.col('total_cases') / pySparkSqlFunc.col('population') * 100).alias('percent')) \
    .where(pySparkSqlFunc.col('date') == '2020-03-31') \
    .orderBy(pySparkSqlFunc.col('percent').desc()) \
    .show(15)

# 38928341.0
# 38928341.00
