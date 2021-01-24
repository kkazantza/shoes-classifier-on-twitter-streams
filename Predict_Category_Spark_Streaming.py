"""
 Counts words in UTF8 encoded, '\n' delimited text directly received from Kafka in every 2 seconds.
 Usage: direct_kafka_wordcount.py <broker_list> <topic>
"""
from __future__ import print_function

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
import re
from pyspark import sql
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, DecimalType
from pyspark.ml.feature import IndexToString


from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, VectorAssembler

def predict(rdd,bestModel):
    if(not rdd.isEmpty()):
        df = sqlContext.createDataFrame(rdd).toDF("descr")
        predictions = bestModel.transform(df)
        converter = IndexToString(inputCol="prediction", outputCol="label", labels=bestModel.stages[0].labels)
        labelReverse = converter.transform(predictions)
        print("predictions for tweet:")
        print(labelReverse.select("features","probability", "prediction", "label").show())
        labelReverse.write.mode('append').parquet('hdfs:///predictions/tweets_predictions.parquet')
    else:
        print("No data received")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    sqlContext = sql.SQLContext(sc)
    
    bestModel =  PipelineModel.load("hdfs:///models/logisticRegressionModel")

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])\
            .map(lambda x: (x, ))
    lines.pprint()
    lines.foreachRDD(lambda rdd: predict(rdd,bestModel))
    
    ssc.start()
    ssc.awaitTermination()


