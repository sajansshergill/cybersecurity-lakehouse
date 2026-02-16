package model

import org.apache.spark.sql.types._

object Schemas {

  // Minimal “security event” schema (raw-ish)
  val rawEventSchema: StructType = StructType(Seq(
    StructField("event_ts", StringType, nullable = false),     // ISO string in input
    StructField("event_type", StringType, nullable = false),   // login, api, network
    StructField("action", StringType, nullable = true),        // login_fail, login_success, ...
    StructField("user_id", StringType, nullable = true),
    StructField("device_id", StringType, nullable = true),
    StructField("ip", StringType, nullable = true),
    StructField("resource", StringType, nullable = true),
    StructField("status", StringType, nullable = true)         // keep string for bronze
  ))
}
