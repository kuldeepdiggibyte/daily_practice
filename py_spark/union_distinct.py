
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('union').getOrCreate()

Data = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns = ["employee_name","department","state","salary","age","bonus"]

df = spark.createDataFrame(data = Data, schema = columns)

df.printSchema()
df.show(truncate=False)

Data2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = Data2, schema = columns2)

df2.printSchema()
df2.show(truncate=False)


# union() to merge two DataFrames
unionDF = df.union(df2).distinct()
unionDF.show(truncate=False)

unionDF1 = df.union(df2)

print("DROP DUPLICATE INCOMING")
union_dup = unionDF1.dropDuplicates()
union_dup.show(truncate=False)

print("UNION_ALL")
unionAllDF = df.unionAll(df2)
unionAllDF.show(truncate=False)
