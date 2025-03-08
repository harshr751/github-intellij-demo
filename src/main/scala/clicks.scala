package org.com.wmt

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object clicks {

  def preProcessedData(df: DataFrame, userAgentFilter: Option[String]): DataFrame = {
    val dfIntermediate = df
      .withColumn("creation_time", (col("transaction_header.creation_time") / 1000).cast("timestamp"))
      .withColumn("user_agent", col("device_settings.user_agent"))

    userAgentFilter match {
      case Some(str) => dfIntermediate.filter(col("user_agent") === str)
      case None => dfIntermediate
    }
  }

  //TASK 1 - countEvents: Computes the count of impressions or clicks per hour.
  def countEvents(df: DataFrame, eventType: String, userAgentFilter: Option[String], spark: SparkSession): DataFrame = {

    val dfFiltered = preProcessedData(df,userAgentFilter)

    val hoursDF = spark.range(0, 24).toDF("hour")
    val datesDF = dfFiltered.select(to_date(col("creation_time")).alias("date")).distinct()
    val userAgentsDF = dfFiltered.select(col("user_agent")).distinct()
    val fullHoursDF = datesDF.crossJoin(userAgentsDF).crossJoin(hoursDF)

    val dfAggregated = dfFiltered
      .withColumn("date", to_date(col("creation_time")))
      .withColumn("hour", hour(col("creation_time")))
      .groupBy("date", "hour", "user_agent")
      .agg(count("*").alias(eventType))

    fullHoursDF.join(dfAggregated, Seq("date", "hour", "user_agent"), "left_outer")
      .na.fill(0, Seq(eventType))
  }

  //Task 2 - avgTimeBetweenEvents: Computes the average time between subsequent events.
  def avgTimeBetweenEvents(df: DataFrame, eventType: String, userAgentFilter: Option[String]): DataFrame = {

    val dfFiltered = preProcessedData(df,userAgentFilter)

    val dfProcessed = dfFiltered
      .withColumn("date", to_date(col("creation_time")))
      .withColumn("hour", hour(col("creation_time")))
      .withColumn("prev_creation_time", lag("creation_time", 1)
        .over(Window.partitionBy("date", "hour", "user_agent").orderBy("creation_time")))
      .withColumn("time_diff", unix_timestamp(col("creation_time"))  - unix_timestamp(col("prev_creation_time")))
      .groupBy("date", "hour", "user_agent")
      .agg(
        avg(s"time_diff").alias(s"avg_time_between_${eventType}")
      )
      .na.fill(0, Seq(s"avg_time_between_${eventType}"))

    dfProcessed
  }

  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder().appName("Clicks/Impression").master("local").getOrCreate()
    val parquetDirectory = "data"
    val clickDf=spark.read.parquet(s"$parquetDirectory/clicks_processed_dk_*.parquet")
    val impressionDf=spark.read.parquet(s"$parquetDirectory/impressions_processed_dk_*.parquet")

    clickDf.show(100,truncate = false)
    impressionDf.show(100,truncate = false)

    val countClicks = countEvents(clickDf, "clicks", Some("some user agent"), spark)
    val countImpressions = countEvents(impressionDf, "impressions", Some("some user agent"), spark)

    countClicks.show(50,truncate = false)
    countImpressions.show(50,truncate = false)

    val processedClicks = avgTimeBetweenEvents(clickDf, "clicks", Some("some user agent"))
    val processedImpressions = avgTimeBetweenEvents(impressionDf, "impressions", Some("some user agent"))

    processedClicks.show(10,truncate = false)
    processedImpressions.show(10,truncate = false)
  }


}
