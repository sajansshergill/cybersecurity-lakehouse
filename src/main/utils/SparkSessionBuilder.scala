package utils

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
    def build(appName: String): SparkSession ={
        SparkSession.builder()
        .appName(appName)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    }
}