// Databricks notebook source
// DBTITLE 1,Libraries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import java.text.SimpleDateFormat
import java.util.Locale
import java.sql.Timestamp 
import org.apache.spark.sql.streaming.Trigger

val HOST = "localhost"
var PORT = 9094
val spark = SparkSession
  .builder
  .appName("twitter_geolocation")
  .getOrCreate()

// COMMAND ----------

// MAGIC %run "/Users/hector.mejia@nodel.co/geolocation/consumer/consumer_utils"

// COMMAND ----------

// DBTITLE 1,Data pipe Ops
val tweetDataSchema = consumerUtils.getTweetStruct
val tweetUserSchema = consumerUtils.getUserStruct
val tweetPlaceSchema = consumerUtils.getPlaceStruct

val tweet_df = spark.readStream
                    .format("socket")
                    .option("host", HOST)
                    .option("port", PORT).load()
                    .select(from_json(col("value").cast("string"),tweetDataSchema).as("data")).select("data.*")
                    .select(col("id").as("tweet_id"),
                            from_json(col("user").cast("string"),tweetUserSchema).as("user_data"),
                            col("created_at").as("time_str"),
                            col("text"),
                            col("coordinates"),
                            col("lang"),
                            col("geo"),
                            from_json(col("place").cast("string"),tweetPlaceSchema).as("place_data"),
                            col("retweeted"),
                            col("favorited"))
                    .where("LENGTH(text) > 3")
                    .withColumn("time", consumerUtils.udfs.toTimestamp(col("time_str")))
                    .select(col("tweet_id"),
                            col("user_data.id_str").as("author_id"),
                            col("place_data.id").as("place_id"),
                            col("time"),
                            col("coordinates"),
                            col("text"),
                            col("lang"),
                            col("geo"),
                            col("place_data.country_code").as("country"),
                            col("retweeted").cast("boolean"),
                            col("favorited").cast("boolean"))
                    .withColumn("keywords", consumerUtils.udfs.keywordParams())

val finalDF = tweet_df.withWatermark("time", "10 seconds").dropDuplicates(Array("tweet_id"))

// COMMAND ----------

// DBTITLE 1,Memory Sink (debugging)
finalDF.writeStream
    .outputMode("append") 
    .format("memory")
    .queryName("tweetquery")
    .start()

// COMMAND ----------

// DBTITLE 1,Delta Lake Sink (Persistent Storage, original table only)
finalDF.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/delta/geolocation_model/ckpt_tweets")
    .table("geolocation_model.tweets")

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from geolocation_model.tweets

// COMMAND ----------

// MAGIC %sh
// MAGIC rm -r -f "/delta/geolocation_model/ckpt_tweets"

// COMMAND ----------


