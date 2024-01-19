#!/usr/bin/env python
# -*- coding: utf-8 -*-
import getpass
from pyspark.sql import SparkSession

def main(spark):
    '''
    Parameters
    ----------
    spark : SparkSession object
    '''
    data = spark.read.parquet('hdfs:/user/sa6523/merged_data.parquet')
    data.show()
    data.createOrReplaceTempView('data')
    data = data.sort("tracing_id")
    print(data.count())
    partition = data.select('tracing_id').distinct()
    print(partition.count())
    train_ratio = 0.7
    split_point = int(partition.count() * train_ratio)
    train_users = partition.limit(split_point)
    test_users =  partition.subtract(train_users)
    train = data.join(train_users, how="inner", on="tracing_id")
    test = data.join(test_users, how="inner", on="tracing_id")
    print(test.count(), train.count())

    train.write.parquet('hdfs:/user/sa6523/zillow_train_data.parquet')
    test.write.parquet('hdfs:/user/sa6523/zillow_test_data.parquet')
if __name__ == "__main__":

    spark = SparkSession.builder.appName('partitioning').getOrCreate()
    main(spark)
