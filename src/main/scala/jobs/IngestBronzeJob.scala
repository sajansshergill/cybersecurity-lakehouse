package jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OParser
import utils.{IO, SparkSessionBuilder}

case class IngestArgs(
  input: String = "",
  inputFormat: String = "json",  // json or csv
  output: String = "warehouse",
  bronzePath: String = "warehouse/bronze/security_events"
)

object IngestBronzeJob {

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[IngestArgs]
    val parser = {
      import builder._
      OParser.sequence(
        programName("IngestBronzeJob"),
        opt[String]("input").required().action((x, c) => c.copy(input = x)),
        opt[String]("inputFormat").optional().action((x, c) => c.copy(inputFormat = x)),
        opt[String]("output").optional().action((x, c) => c.copy(output = x)),
        opt[String]("bronzePath").optional().action((x, c) => c.copy(bronzePath = x))
      )
    }

    OParser.parse(parser, args, IngestArgs()) match {
      case Some(conf) =>
        val spark = SparkSessionBuilder.build("cyber-lakehouse-bronze")
        implicit val s: SparkSession = spark

        val raw: DataFrame =
          conf.inputFormat.toLowerCase match {
            case "csv"  => IO.readCsv(conf.input)
            case _      => IO.readJson(conf.input)
          }

        // Bronze: minimal transformation, add ingestion metadata + partition key
        val bronze = raw
          .withColumn("ingested_at", current_timestamp())
          .withColumn("event_date", to_date(to_timestamp(col("event_ts"))))

        IO.writeParquet(bronze, conf.bronzePath, partitionCols = Seq("event_date"))

        spark.stop()

      case _ => sys.exit(1)
    }
  }
}
