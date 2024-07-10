from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext(appName="DStream_QueueStream")
ssc = StreamingContext(sc, 2)

rddQueue = []
for i in range(3):
    rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 21)], 10)]
inputStream = ssc.queueStream(rddQueue)
mappedStream = inputStream.map(lambda x: (x % 10, 1))
reducedStream = mappedStream.reduceByKey(lambda a, b: a + b)
reducedStream.pprint()
ssc.start()
ssc.stop(stopSparkContext=True, stopGraceFully=True)



