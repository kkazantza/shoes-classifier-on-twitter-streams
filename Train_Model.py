from pyspark.sql import Row
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import Tokenizer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.tuning import ParamGridBuilder
from crossValidatorVerbose import CrossValidatorVerbose

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
sc = SparkContext()
spark = SparkSession(sc)


def train_model(stages, paramGrid):
    pipeline = Pipeline(stages = stages)
    crossval = CrossValidatorVerbose(estimator=pipeline,
                              estimatorParamMaps=paramGrid,
                              evaluator=MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy"),
                              numFolds=3) 

    (train, test) = shoes_df.randomSplit([0.8, 0.2])
    cvModel = crossval.fit(train)
    prediction = cvModel.transform(test)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(prediction)
    print("Test Error = %g " % (1.0 - accuracy))
    #get the best model
    best_model = cvModel.bestModel
    return best_model


shoes_df = spark.read.parquet("hdfs:///data/products/shoes.parquet")
shoes_df.show(10)


stages = []

label_stringIdx = StringIndexer(inputCol = 'category_name', outputCol = 'label')
label_stringIdx.setHandleInvalid("skip")
stages += [label_stringIdx]

tokenizer = Tokenizer(inputCol="descr", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="tf_features")
idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="idf_features")
stages += [tokenizer, hashingTF, idf]

assemblerInputs = ['idf_features']
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

rf = RandomForestClassifier(labelCol="label", featuresCol="features")
paramGrid = ParamGridBuilder() \
        .addGrid(hashingTF.numFeatures, [10000]) \
        .addGrid(idf.minDocFreq, [10, 20] ) \
        .addGrid(rf.numTrees, [100] ) \
        .addGrid(rf.maxDepth, [10, 20]) \
        .build()
best_rf = train_model(stages+[rf], paramGrid)
best_rf.save("hdfs:///models/randomForestModel")

lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
paramGrid = ParamGridBuilder() \
            .addGrid(hashingTF.numFeatures, [10000, 100]) \
            .addGrid(idf.minDocFreq, [10, 20] ) \
            .addGrid(lr.elasticNetParam,[0.0])\
            .addGrid(lr.fitIntercept,[False, True])\
            .addGrid(lr.maxIter,[10, 100])\
            .addGrid(lr.regParam,[0.01])\
            .build()
best_lr = train_model(stages+[lr], paramGrid)
best_lr.save("hdfs:///models/logisticRegressionModel")

