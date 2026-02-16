package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scopt.OParser
import utils.{IO, SparkSessionBuilder}

case class GoldArgs(
  silverPath: String = "warehouse/silver/security_events",
  goldUserDailyRiskPath: String = "warehouse/gold/user_daily_risk",
  goldFailedLoginWindowsPath: String = "warehouse/gold/failed_login_windows"
)

object GoldSignalsJob {

  private def failedLoginWindows(silver: DataFrame): DataFrame = {
    val loginFails = silver
      .filter(col("event_type") === "login" && col("action") === "login_fail" && col("user_id").isNotNull)
      .select("event_ts", "event_date", "user_id", "ip", "device_id")

    // rolling 15-minute fail count per user
    val w = Window.partitionBy(col("user_id"))
      .orderBy(col("event_ts").cast("long"))
      .rangeBetween(-15 * 60, 0)

    loginFails
      .withColumn("fail_15m", count(lit(1)).over(w))
      .withColumn("is_bruteforce_suspected", col("fail_15m") >= 10)
  }

  private def userDailyRisk(silver: DataFrame): DataFrame = {
    val daily = silver
      .filter(col("user_id").isNotNull)
      .groupBy(col("event_date"), col("user_id"))
      .agg(
        sum(when(col("event_type") === "login" && col("action") === "login_fail", 1).otherwise(0)).as("failed_logins"),
        countDistinct(when(col("ip").isNotNull, col("ip"))).as("distinct_ips"),
        countDistinct(when(col("device_id").isNotNull, col("device_id"))).as("distinct_devices"),
        count(lit(1)).as("total_events")
      )
      // simple risk score (replace with ML later)
      .withColumn(
        "risk_score",
        col("failed_logins") * lit(2) + (col("distinct_ips") - lit(1)) * lit(3) + (col("distinct_devices") - lit(1)) * lit(2)
      )
      .withColumn("risk_score", greatest(col("risk_score"), lit(0)))

    daily
  }

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[GoldArgs]
    val parser = {
      import builder._
      OParser.sequence(
        programName("GoldSignalsJob"),
        opt[String]("silverPath").optional().action((x, c) => c.copy(silverPath = x)),
        opt[String]("goldUserDailyRiskPath").optional().action((x, c) => c.copy(goldUserDailyRiskPath = x)),
        opt[String]("goldFailedLoginWindowsPath").optional().action((x, c) => c.copy(goldFailedLoginWindowsPath = x))
      )
    }

    OParser.parse(parser, args, GoldArgs()) match {
      case Some(conf) =>
        val spark = SparkSessionBuilder.build("cyber-lakehouse-gold")
        implicit val s: SparkSession = spark

        val silver = spark.read.parquet(conf.silverPath).cache()

        val windows = failedLoginWindows(silver)
        IO.overwriteParquet(windows, conf.goldFailedLoginWindowsPath, partitionCols = Seq("event_date"))

        val risk = userDailyRisk(silver)
        IO.overwriteParquet(risk, conf.goldUserDailyRiskPath, partitionCols = Seq("event_date"))

        spark.stop()

      case _ => sys.exit(1)
    }
  }
}
