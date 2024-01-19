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
    user_session = spark.read.parquet('hdfs:/user/sa6523/final_user.parquet')
    search_session = spark.read.parquet('hdfs:/user/sa6523/final_search_session.parquet')
    user_session.show()
    search_session.show()
    user_session.createOrReplaceTempView('user_session')
    search_session.createOrReplaceTempView('search_session')
    merged=spark.sql('SELECT user_session.tracing_id, user_session.zpid_hash, \
    user_session.feature1, user_session.feature2, user_session.feature3, user_session.feature4, \
    user_session.feature5, user_session.feature6, user_session.feature7, user_session.feature8,\
    search_session.user_id_hash, search_session.timestamp_session, search_session.user_session_id,\
    search_session.order, search_session.submit, search_session.fav, search_session.click FROM user_session \
    INNER JOIN search_session ON user_session.tracing_id = search_session.tracing_id AND user_session.zpid_hash = search_session.zpid_hash')
    merged.show()
    merged.write.parquet('hdfs:/user/sa6523/merged_data.parquet')
    
if __name__ == "__main__":

    spark = SparkSession.builder.appName('merging').getOrCreate()
    main(spark)
