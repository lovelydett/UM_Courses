# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 10:20:19 2020
"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sc = spark.sparkContext


def logParse(log):
    log = log.replace(' -- ', ', ')
    log = log.replace('.rb: ', ', ')
    log = log.replace(', ghtorrent-', ', ')
    return log.split(', ', 4)

def loadRDD(filename):
    textFile = sc.textFile(filename) # In case you test the program locally
    parsedRDD = textFile.map(logParse)
    return parsedRDD

def hw1():
    # You can load the log using the following script
    rowrdd = loadRDD("data/ghtorrent-logs.txt").cache()
    print(rowrdd.take(17))

    # Exercise 1: report num of ERROR and WARN
    def report_err_and_warn():
        res = rowrdd.map(
            lambda x : 1 if x[0] == "ERROR" or x[0] == "WARN" else 0).sum()
        print(res)
    # report_err_and_warn()

    # Exercise 2: count successful request for retrieval stage “api_client”
    def count_successful_request():
        res = rowrdd.map(
            lambda x : 1 if len(x) > 4 and x[3] == "api_client" and x[4].find("Successful request") >= 0 else 0).sum()
        print(res)
    # count_successful_request()

    #Exercise 3: for all failed request of “api_client”, report the failed count of each repository. (using groupby)
    def count_failed_repo():
        def pick(x):
            if len(x) < 4 or x[3] != "api_client" or x[4].find("Successful request") >= 0:
                return
            url = x[4]
            url = url.replace("URL: ", ",")
            url = url.split(',')
            if len(url) < 2:
                return
            return url[1]
        res = rowrdd.map(pick).collect()
        print(res[1])
    count_failed_repo()


if __name__ == "__main__":
    hw1()
