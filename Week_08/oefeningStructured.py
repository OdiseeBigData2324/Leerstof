
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, col, length

spark = SparkSession.builder.master("local").appName("Oefening").getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the lines into words
words = lines.select(explode(split(lines.value, " ")).alias("word"))
lengths = words.withColumn("length", length(col("word")))

# Generate running word count
wordCounts = lengths.groupBy("length").count()

 # Start running the query that prints the running counts to the console
query = wordCounts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()
