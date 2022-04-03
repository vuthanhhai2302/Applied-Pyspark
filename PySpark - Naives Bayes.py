#!/usr/bin/env python
# coding: utf-8

# In[1]:


# đặt biến môi trường 
get_ipython().run_line_magic('env', 'SPARK_LOCAL_HOSTNAME=localhost')


# In[2]:


#khởi động spark
import findspark
findspark.init()


# In[7]:


# thêm các thư viện spark để thực hiện model Naives Bayes

import os
import numpy as np
from pyspark.ml.feature import VectorAssembler
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
import scipy
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import SparseVector
from pyspark.sql import SparkSession
from pyspark.python.pyspark.shell import spark


# In[9]:


spark = SparkSession.builder.getOrCreate()
data = spark.read.load("C:/Users/Admin/Documents/Absenteeism_at_work.csv", format="csv", header=True, delimiter=";")
data = data.withColumn("MOA", data["Month of absence"] - 0).withColumn("label", data['Seasons'] - 0).     withColumn("ROA", data["Reason for absence"] - 0).     withColumn("distance", data["Distance from Residence to Work"] - 0).     withColumn("BMI", data["Body mass index"] - 0)

assem = VectorAssembler(inputCols=["label", "MOA"], outputCol='features')

data = assem.transform(data)

# chia tập dữ liệu ra để test và train. 30 cho test 70 cho train
splits = data.randomSplit([0.7, 0.3], 1000)
train = splits[0]
test = splits[1]

# cài đặt thuật toán
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

# train model
model = nb.fit(train)

predictions = model.transform(test)

# tính toán độ chính xác bộ dữ liệu test
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")

y_true = data.select("BMI").rdd.flatMap(lambda x: x).collect()
y_pred = data.select("ROA").rdd.flatMap(lambda x: x).collect()

# tính toán các thuộc tính và lập confusion matrix
accuracy = evaluator.evaluate(predictions)

confusionMatrix = confusion_matrix(y_true, y_pred)

precision = precision_score(y_true, y_pred, average='micro')

recall = recall_score(y_true, y_pred, average='micro')


print("độ chính xác của bộ dữ liệu Test Naive Bayes:" + str(accuracy))

print("Ma trận Confusion Matrix của Naive Bayes Model:\n" + str(confusionMatrix))

print("Độ chính xác của model Naive Bayes là: " + str(precision))

print("Recall của model Naive Bayes là: " + str(recall))


# In[ ]:




