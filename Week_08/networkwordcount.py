from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 5 second
sc = SparkContext("local[2]", "networkwordcount")
sc.setLogLevel("ERROR") # reduce spam of logging
# zet de sparkcontext om naar een streaming context dat om de 5 seconden uitgevoerd wordt
ssc = StreamingContext(sc, 5)
# lees data van de localhost-poort 19999
lines = ssc.socketTextStream('localhost',19999)

# zet elke lijn om naar zijn woorden (1 woorde per rij)
words = lines.flatMap(lambda line: line.split(" "))
# zet bij elk woord een 1-tje (deze gaan we optellen)
words = words.map(lambda word: (word,1))
# reduce van mapreduce (tel de 1-tjes op per woord)
counts = words.reduceByKey(lambda x,y: x+y)

counts.pprint()

# start de berekeningen
ssc.start()
# houd de applicatie actief
ssc.awaitTermination()
