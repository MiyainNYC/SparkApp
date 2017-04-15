from pyspark.sql import SQLContext
from pyspark import SparkContext
from operator import add
from pyspark.sql.functions import regexp_replace,col,trim,lower,desc
from pyspark.streaming import StreamingContext
import re
import sys

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "WordCount")
ssc = StreamingContext(sc, 1)
sqlContext = SQLContext(sc)
# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

data = sc.textFile(sys.argv[1])
print 'number of lines in file: %s' % data.count()

chars = data.map(lambda s:len(s)).reduce(add)

print 'number of characters in file:%s' % chars

words = data.flatMap(lambda line: re.split(' ', line.lower().strip()))

# words of more than three characters

words = words.filter(lambda x: len(x)>3)

words.collect()

# set count 1 per word

words = words.map(lambda w:(w,1))

words_df = sqlContext.createDataFrame(words,['word','count'])
words_df.show()

## replace punctuation
df_transformed = words_df.select(lower(trim(regexp_replace(col('word'),r'[.,\/#$%^&*()-_+=~!"\s]*',''))).alias('keywords'))

## take top 250 keywords and save them into csv
sqlContext.createDataFrame(df_transformed.groupby('keywords').count().sort(desc('count')).take(251)).toPandas().to_csv('keywords.csv')

250_words = sqlContext.createDataFrame(df_transformed.groupby('keywords').count().sort(desc('count')).take(251))
# Print the top 250 words
250_words.pprint()

ssc.start()
ssc.awaitTermination()
