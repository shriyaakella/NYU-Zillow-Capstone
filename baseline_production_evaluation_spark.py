#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Evaluating nDCG@k and mAP@k for production baseline using Spark library.
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

    interacted= spark.read.parquet('hdfs:/user/sr6172/df_test_weighted.parquet') 
    test = spark.read.parquet('hdfs:/user/sr6172/df_test.parquet')

    test.createOrReplaceTempView('test')
    interacted.createOrReplaceTempView('interacted')

    test_final = test.groupBy("tracing_id").agg(F.collect_list("zpid_encoded").alias("propertyids"))
    interacted_final = interacted.groupBy("tracing_id").agg(F.collect_list("zpid_encoded").alias("propertyids_interacted"))

    test_final.createOrReplaceTempView("test_final")
    interacted_final.createOrReplaceTempView("interacted_final")
    

    merged = spark.sql("SELECT t.propertyids,  c.propertyids_interacted, t.tracing_id FROM test_final t INNER JOIN interacted_final c ON t.tracing_id = c.tracing_id")
    merged.createOrReplaceTempView("merged")
            
    eval_list = []
    for row in merged.rdd.collect():
        eval_list.append((row.propertyids, row.propertyids_interacted))
    
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
    
