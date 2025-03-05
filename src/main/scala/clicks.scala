package org.com.wmt

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, from_unixtime, hour, lag, to_date, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

object clicks {

  def processEvents(df: DataFrame, eventType: String, userAgentFilter: Option[String]): DataFrame = {

    val dfIntermediate = df
      .withColumn("creation_time", col("transaction_header.creation_time"))
      .withColumn("user_agent", col("device_settings.user_agent"))

    val dfFiltered = userAgentFilter match {
      case Some(str) => dfIntermediate.filter(col("user_agent") === str)
      case None => dfIntermediate
    }

    val dfProcessed = dfFiltered
      .withColumn("date", to_date(from_unixtime(col("creation_time") / 1000).cast("timestamp")))
      .withColumn("hour", hour(from_unixtime(col("creation_time") / 1000).cast("timestamp")))
      .withColumn("prev_creation_time", lag("creation_time", 1)
        .over(Window.partitionBy("date", "hour", "user_agent").orderBy("creation_time")))
      .withColumn("time_diff", from_unixtime(col("creation_time") / 1000).cast("timestamp") - from_unixtime(col("prev_creation_time") / 1000).cast("timestamp"))
      .groupBy("date", "hour", "user_agent")
      .agg(
        count("*").alias("clicks"),
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

    clickDf.show(100,false)
    impressionDf.show(100,false)

    val processedClicks = processEvents(clickDf, "clicks", Some("some user agent"))
    val processedImpressions = processEvents(impressionDf, "impressions", Some("some user agent"))

    processedClicks.show(10,false)
    processedImpressions.show(10,false)
  }


}
