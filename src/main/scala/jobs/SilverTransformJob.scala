package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scopt.OParser
import utils.{IO, SparkSessionBuilder}

case class SilverArgs(
  bronzePath: String = "warehouse/bronze/security_events",
  silverPath: String = "warehouse/silver/security_events"
)

object SilverTransformJob {

  private def normalize(df: DataFrame): DataFrame = {
    df
      .withColumn("event_ts_ts", to_timestamp(col("event_ts")))
      .drop("event_ts")
      .withColumnRenamed("event_ts_ts", "event_ts")
      .withColumn("event_type", lower(trim(col("event_type"))))
      .withColumn("action", lower(trim(col("action"))))
      .withColumn("ip", trim(col("ip")))
      .withColumn("user_id", trim(col("user_id")))
      .withColumn("device_id", trim(col("device_id")))
      // basic “validity” flag for governance-minded downstream filtering
      .withColumn("is_valid",
        col("event_ts").isNotNull && col("event_type").isNotNull && col("event_date").isNotNull
      )
      // drop obvious duplicates (you can replace with event_id later)
      .dropDuplicates("event_date", "event_type", "action", "user_id", "device_id", "ip", "resource", "status")
  }

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[SilverArgs]
    val parser = {
      import builder._
      OParser.sequence(
        programName("SilverTransformJob"),
        opt[String]("bronzePath").optional().action((x, c) => c.copy(bronzePath = x)),
        opt[String]("silverPath").optional().action((x, c) => c.copy(silverPath = x))
      )
    }

    OParser.parse(parser, args, SilverArgs()) match {
      case Some(conf) =>
        val spark = SparkSessionBuilder.build("cyber-lakehouse-silver")
        implicit val s: SparkSession = spark

        val bronze = spark.read.parquet(conf.bronzePath)
        val silver = normalize(bronze).filter(col("is_valid") === true)

        IO.overwriteParquet(silver, conf.silverPath, partitionCols = Seq("event_date"))

        spark.stop()

      case _ => sys.exit(1)
    }
  }
}
