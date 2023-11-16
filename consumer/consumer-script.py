import findspark
findspark.init()

import logging
#from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,DoubleType,BooleanType,IntegerType,ArrayType
from pyspark.sql.functions import from_json, col, lit, concat,to_date

from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())


def create_spark_configuration():
    spark_config = None

    try:
        spark_config = (SparkSession.builder
            .appName("KafkaElasticsearchSparkIntegration")
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-20_2.12:7.17.14,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4")
            .getOrCreate())
        
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark_config


def connect_to_kafka(spark_config):
    spark_df = None
    try:
        spark_df = spark_config.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "movies_info") \
            .load()
        
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("adult", BooleanType(), False),
        StructField("backdrop_path", StringType(), False),
        StructField("genre_ids", ArrayType(IntegerType()), False),
        StructField("original_language", StringType(), False),
        StructField("original_title", StringType(), False),
        StructField("overview", StringType(), False),
        StructField("popularity", DoubleType(), False),
        StructField("poster_path", StringType(), False),
        StructField("release_date", StringType(), False),
        StructField("title", StringType(), False),
        StructField("video", BooleanType(), False),
        StructField("vote_average", DoubleType(), False),
        StructField("vote_count", IntegerType(), False)
    ])

    sel = (spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*") \
        .withColumn("description", concat(col('title'),lit(' : '),col('overview'))) \
        .withColumn('popularity',col("popularity").cast('double')) \
        .withColumn('vote_average',col("vote_average").cast('double')) \
        .withColumn('release_date',to_date(col('release_date'),"yyyy-MM-dd")) \
        .select('id','adult','genre_ids','original_language','original_title','description','popularity','release_date','video','vote_average','vote_count','backdrop_path','poster_path'))

    return sel


spark_config = create_spark_configuration()

# Function to write data to Elasticsearch
def write_to_elasticsearch(df):
    df.persist()
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.resource", "movies-index/_doc") \
        .save(mode="append")
    df.unpersist()

if spark_config != None:
    spark_df = connect_to_kafka(spark_config)
    selection = create_selection_df_from_kafka(spark_df)

    # streaming_es_query = selection.writeStream.outputMode("append").format("console").option("format", "json").start()

    # streaming_es_query.awaitTermination()

    # Write data to Elasticsearch
    write_query = selection.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/elasticsearch_l/") \
    .foreachBatch(lambda batch_df, batch_id: write_to_elasticsearch(batch_df)) \
    .start()

    write_query.awaitTermination()


