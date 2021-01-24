# Shoes Classifier on Twitter Streams

The scope of this project is to train a classifier in a spark environment for determining the category (i.e. men’s shoes) from a large dataset of categorized (labeled) products and their descriptions and apply the classifier on streams of twitter tweets about shopping offers. It is executed on a 3 node gc cluster.

### Training Phase

For the classification task we built 2 classification models:
* Random Forest Classifier
* Multinomial logistic regression

Data Preprocessing Stages used
* Category Indexing - indexes each categorical column
* Tokenizer - converts sentences to lists of words
* HashingTF - generates term frequency vectors
* IDF - generates term frequency-inverse document frequency (TF-IDF) features
* VectorAssembler - a transformer that merges multiple columns into a vector column

Features used for the classification task:
* descr(encoded as tf-idf feature)

To train the classification models run Train_Model.py in cluster mode:

```
spark-submit Train_Model.py \
>     --master yarn \
>     --deploy-mode cluster \
>     --driver-memory 4g \
>     --executor-memory 2g \
>     --executor-cores 1 \
>     --queue thequeue
```
**Best Performance model is the Multinomial Logistic regression.**

### Kafka

* In order to run the producer to stream tweets and publish them to Kafka ‘offers’ topic execute command:

```
java -cp /opt/twitter4j/lib/*:/opt/kafka_2.11-0.10.1.0/libs/*:. KafkaTwitterProducer mZq3Qy2FDchEMjtvTxtoYzNmw ZVEsmnyBArmseznVfPoEbTKbFk03h2YQ0mPsscT4VSYQG7SJbn 1272943111979892738-s9yUN5E1WON1JALm7y6EZd670CFnV5 sxfSsQVolMcRdgMJOfKbEnECQLxw8l1CMtrTDAPdIn197 offers shopping offers shoes
```
output file: filtered_tweets.txt

### Spark streaming

* To consume from Kafka topic offers with Spark Streaming execute:

```
spark-submit Predict_Category_Spark_Streaming.py localhost:9092 offers
```
output file: program_output_tweets_prediction.txt
