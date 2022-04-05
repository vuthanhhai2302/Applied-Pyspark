#!/usr/bin/env python
# coding: utf-8

# In[1]:


# đặt biến môi trường 
get_ipython().run_line_magic('env', 'SPARK_LOCAL_HOSTNAME=localhost')
#khởi động spark
import findspark
findspark.init()


# In[2]:


from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
import scipy
from pyspark.python.pyspark.shell import spark


# In[5]:


data = spark.read.load("C:/Users/Admin/Documents/Absenteeism_at_work.csv", format="csv", header=True, delimiter=";")
data = data.withColumn("MOA", data["Month of absence"] - 0).withColumn("label", data['Height'] - 0).     withColumn("ROA", data["Reason for absence"] - 0).     withColumn("distance", data["Distance from Residence to Work"] - 0).     withColumn("BMI", data["Body mass index"] - 0)
#data.show()

assem = VectorAssembler(inputCols=["label", "distance"], outputCol='features')
data = assem.transform(data)


labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)

featureIndexer =\VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)


(trainingData, testData) = data.randomSplit([0.7, 0.3])


dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")


pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])


model = pipeline.fit(trainingData)

predictions = model.transform(testData)

predictions.select("prediction", "indexedLabel", "features").show(5)

evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")

accuracy = evaluator.evaluate(predictions)

y_true = data.select("BMI").rdd.flatMap(lambda x: x).collect()
y_pred = data.select("ROA").rdd.flatMap(lambda x: x).collect()

confusionmatrix = confusion_matrix(y_true, y_pred)

precision = precision_score(y_true, y_pred, average='micro')

recall = recall_score(y_true, y_pred, average='micro')

treeModel = model.stages[2]

print(treeModel)
print("Decision Tree - Test Accuracy = %g" % (accuracy))
print("Decision Tree - Test Error = %g" % (1.0 - accuracy))

print("The Confusion Matrix for Decision Tree Model is :\n" + str(confusionmatrix))

print("The precision score for Decision Tree Model is: " + str(precision))

print("The recall score for Decision Tree Model is: " + str(recall))


# In[ ]:




