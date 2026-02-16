package utils

import org.apache.spark.sql.{DataFrame, SaveMode}

object IO {

    def readJson(sparkPath: String)(implicit spark: org.apache.spark.sql.SparkSession): DataFrame = 
        spark.read.json(sparkPath)

        def readCsv(sparkPath: String, header: Boolean = true)(implicit spark: org.apache.spark.sql.SparkSession): DataFrame = 
            spark.read.option("header", header.totring).csv(sparkPath)

        def writeParquet(df: DataFrame, path: String, partitionCols: Seq[String] = Seq.empty): Unit = {
            val writer = df.write.mode(Savemode.Overwrite).format("parquet)
            if (partitionCols.nonEmpty) write.partitionBy(partitionCols: _*).save(path)
            else writer.save(path)
        }
}