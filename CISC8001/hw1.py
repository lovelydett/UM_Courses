"""
PySpark HW1
Author: Yuting Xie
2022.2.16
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType

spark = SparkSession.builder.appName('LocalTest').getOrCreate()
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
            if len(x) < 4 or x[3] != "api_client" or x[4].find("Failed request") < 0:
                return False
            url = x[4].replace("URL: ", ",").split(',')[1]
            repo = url.replace("//", "/").split('/')
            return len(repo) > 4
        def get_repo(x):
            url = x[4]
            url = url.replace("URL: ", ",")
            url = url.split(',')
            url = url[1]
            repo = url.replace("//", "/")
            repo = repo.replace("?", "/")
            repo = repo.split('/')
            repo = repo[3] + '/' + repo[4]
            return [repo]

        res = rowrdd.filter(pick).map(get_repo) # .groupBy(lambda x : x).countByKey()
        schema = StructType([
            StructField("Repo", StringType(), True)
        ])
        df = spark.createDataFrame(res, schema=schema)
        res = df.groupBy("Repo").count()
        print(res.toPandas())
    # count_failed_repo()

    # Exercise 4: top-5 active repository
    def top5_active_repo():
        def pick(x):
            if len(x) < 4 or x[4].find("Successful request") < 0:
                return False
            url = x[4]
            url = url.replace("URL: ", ",")
            url = url.split(',')
            url = url[1]
            repo = url.replace("//", "/")
            repo = repo.split('/')
            return len(repo) > 4
        def get_repo(x):
            url = x[4]
            url = url.replace("URL: ", ",")
            url = url.split(',')
            url = url[1]
            repo = url.replace("//", "/")
            repo = repo.replace("?", "/")
            repo = repo.split('/')
            repo = "https://api.github.com/repos/" + repo[3] + '/' + repo[4]
            return [repo]

        res = rowrdd.filter(pick).map(get_repo) # .groupBy(lambda x : x).countByKey()
        schema = StructType([
            StructField("Repo", StringType(), True)
        ])
        df = spark.createDataFrame(res, schema=schema)
        res = df.groupBy("Repo").count().sort(["count"], ascending=[False])
        print(res.take(5))
    # top5_active_repo()

    # Exercise 5:
    def join_on_repo():
        textfile = sc.textFile("data/interesting-repos.csv")
        interestingRDD = textfile.map(lambda line: [line.split(",")[1]])
        schema = StructType(
            [StructField("url", StringType(), True)]
        )
        df_left = interestingRDD.toDF(schema=schema)

        def pick(x):
            return len(x) >= 4 and len(x[4].replace("//", "/").replace("?", "/").split('/')) > 4
        def get_repo(x):
            repo = x[4].replace("//", "/").replace("?", "/").split('/')
            repo = "https://api.github.com/repos/" + repo[3] + '/' + repo[4]
            return [repo]
        df_right = rowrdd.filter(pick).map(get_repo).toDF(schema=schema)

        res = df_left.join(df_right, ["url"])
        print(res.count())
        print(res.head())
    # join_on_repo()

    # Exercise 6:
    def top5_failed_interesting_repo():
        textfile = sc.textFile("data/interesting-repos.csv")
        interestingRDD = textfile.map(lambda line: [line.split(",")[1]])
        schema = StructType(
            [StructField("url", StringType(), True)]
        )
        df_left = interestingRDD.toDF(schema=schema)

        def pick(x):
            if len(x) < 4 or x[4].find("Failed request") < 0:
                return False
            url = x[4].replace("URL: ", ",").split(',')[1]
            repo = url.replace("//", "/").split('/')
            return len(repo) > 4

        def get_repo(x):
            repo = x[4].replace("//", "/").replace("?", "/").split('/')
            repo = "https://api.github.com/repos/" + repo[3] + '/' + repo[4]
            return [repo]
        df_right = rowrdd.filter(pick).map(get_repo).toDF(schema=schema)
        df_right = df_right.groupBy("url").count()
        res = df_left.join(df_right, ["url"])
        res = res.sort(["count"], ascending=[False])
        print(res.take(5))
    # top5_failed_interesting_repo()


if __name__ == "__main__":
    hw1()
