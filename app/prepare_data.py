from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
import re

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()
    


df = spark.read.parquet("/a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)
df = df.withColumn('title', regexp_replace(col('title'), r'\s+', ' '))


# def create_doc(row):
#     filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
#     with open(filename, "w") as f:
#         f.write(row['text'])


def create_doc(row):
    filename = sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    filename = "data/" + re.sub(r'[^a-zA-Z0-9.]', '', filename)
    with open(filename, "w") as f:
        f.write(row['text'])

df.foreach(create_doc)
df.write.option("sep", "\t").mode("overwrite").csv("/index/data")