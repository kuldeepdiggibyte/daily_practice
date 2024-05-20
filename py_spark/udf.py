from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("udf").getOrCreate()

data = [(1,"sachin tendulkar",200000,50000),
        (2,"raja ram monhan roy",300000,50000)]

schema = ["id","name","salary","bonus"]

df = spark.createDataFrame(data,schema=schema)
df.show()


def totalpay(s,b):

    return s+b

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

TotalPayment = udf(lambda a,b:totalpay(a,b),IntegerType())


df.withColumn('topay',TotalPayment(df.salary,df.bonus)).show()
