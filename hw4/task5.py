import sys
from urlparse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as sqlfn


def task5(input_path, output_path):
    spark = SparkSession.builder \
        .config("spark.submit.deployMode", "client") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    get_path = sqlfn.udf(lambda url: urlparse(url).path.rstrip("/"))
    get_host = sqlfn.udf(lambda url: urlparse(url).hostname)

    schema = "uid STRING, ts STRING, url STRING"
    df = spark.read.csv(input_path, sep="\t", schema=schema) \
        .withColumn("path", get_path("url")) \
        .withColumn("host", get_host("url")) \
        .select("host", "path") \
        .distinct()

    df.groupby("path") \
        .agg(sqlfn.count("path").alias("count")) \
        .orderBy(sqlfn.desc("count")) \
        .write.csv(output_path, sep="\t")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) < 2:
        print "usage: python task5.py input_path output_path"
    print "input path:", args[0]
    print "output path:", args[1]
    task5(args[0], args[1])
