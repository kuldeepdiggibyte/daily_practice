from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext

sc=SparkContext("local[*]","StreamingExample")
ssc=StreamingContext(sc,5)
lines=ssc.textFileStream(r'C:\Users\KuldeeppralhadManago\Desktop\pythonProject2\instruction.txt')
words=lines.flatMap(lambda x:x.split(" "))
mapped_words=words.map(lambda x:(x,1))
reduced_words=mapped_words.reduceByKey(lambda x,y:x+y)
sorted_words=reduced_words.map(lambda x:(x[1],x[0])).transform(lambda x:x.sortByKey(False))
sorted_words.pprint()
ssc.start()

ssc.stop(stopSparkContext=True, stopGraceFully=True)