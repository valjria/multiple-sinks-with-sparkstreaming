import findspark

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("yarn").appName("Create Delta Table")\
        .config("spark.jars.packages","io.delta:delta-core_2.12:0.7.0")\
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .enableHiveSupport() \
        .getOrCreate()

iot_schema = "row_id int, event_ts double, device string, co float, humidity float, light boolean, " \
             "lpg float, motion boolean, smoke float, temp float, generate_ts timestamp"

jdbc_url = "jdbc:postgresql://localhost/traindb?user=train&password=Ankara06"
delta_path = "hdfs://localhost:9000/user/train/deltaPathSensor4d"

lines=(
    spark.readStream.format("csv")\
    .option("header",False)\
    .schema(iot_schema)
    .option("maxFilesPerTrigger",1)
    .load("file:///home/train/data-generator/output")
)


def write_to_multiple_sinks (df,batchId):
    df.persist()
    df.show()

    df.filter("device == '00:0f:00:70:91:0a'").write.jdbc(url=jdbc_url,table="sensor_0a",mode="append",properties={"driver":'org.postgresql.Driver'})

    df.filter("device == 'b8:27:eb:bf:9d:51'").write.format("parquet").mode("append").saveAsTable("test1.sensor51")

    df.filter("device == '1c:bf:ce:15:ec:4d'").write.format("delta") \
    .mode("append")\
    .save(delta_path)

    df.unpersist()

checkpointDir = "file:///home/train/checkpoint/iot_multiple_sink_foreach_batch"

streamingQuery = (lines.writeStream
                .foreachBatch(write_to_multiple_sinks)
                .option("checkpointLocation",checkpointDir)
                .start()
                              )

streamingQuery.awaitTermination()








