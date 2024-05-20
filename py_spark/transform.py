from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("transform").getOrCreate()

data = [(1, "basheer", 40000),
        (2, "kalai", 40000)]

schema = ["id", "name", "salary"]

df = spark.createDataFrame(data, schema=schema)
df.show()

from pyspark.sql.functions import upper


def convertNameToUpper(df):
    return df.withColumn('name', upper(df.name))


def doubleThesalary(df):
    return df.withColumn('salary', (df.salary * 2))


df1 = df.transform(convertNameToUpper)
df1.show()

df2 = df.transform(doubleThesalary)
df2.show()
