#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Evaluating nDCG@k and mAP@k for popularity baseline using Spark library.
The parquet files used in the code have been created in the iPython notebooks in the directory. 
'''

import getpass
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.sql import SparkSession

import pickle
def main(spark):
    '''Main
    Parameters
    ----------
    spark : SparkSession object
    '''
    top_20 = spark.read.parquet('hdfs:/user/sr6172/top_20_weighted.parquet')
    test = spark.read.parquet('hdfs:/user/sr6172/df_test.parquet')
    

    test.createOrReplaceTempView('test')
    top_20.createOrReplaceTempView('top_20')
    
    interacted = test.groupBy("tracing_id").agg(F.collect_list("zpid_encoded").alias("propertyids_interacted"))
    interacted.createOrReplaceTempView("interacted")

    top_20 = top_20.select('zpid_encoded').rdd.flatMap(lambda x: x).collect()
   
    eval_list = []
    for row in interacted.rdd.collect():
        
        eval_list.append((top_20, row.propertyids_interacted))


    sc =  SparkContext.getOrCreate()
     
    predictionAndLabels = sc.parallelize(eval_list)
    metrics = RankingMetrics(predictionAndLabels)
    
    print(metrics.meanAveragePrecisionAt(20))
    print(metrics.meanAveragePrecisionAt(10))
    print(metrics.meanAveragePrecisionAt(5))
    print(metrics.meanAveragePrecisionAt(2))
    print(metrics.meanAveragePrecisionAt(1))

    print(metrics.ndcgAt(20))
    print(metrics.ndcgAt(10))
    print(metrics.ndcgAt(5))
    print(metrics.ndcgAt(2))
    print(metrics.ndcgAt(1))
    


if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    netID = getpass.getuser()

    # Call our main routine
    main(spark)
    
