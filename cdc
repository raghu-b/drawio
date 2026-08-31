from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (SparkSession.builder
    .appName("cdc-parquet-to-iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.type", "hive")        # or glue / hadoop / rest, depending on your catalog
    .config("spark.sql.catalog.my_catalog.warehouse", "s3://my-bucket/warehouse")
    .getOrCreate())

source_path = "s3://landing/cdc/"
checkpoint_path = "s3://checkpoints/cdc_to_iceberg/"
target_table = "my_catalog.db.target_table"
schema = spark.read.parquet(source_path).schema   # or define explicitly for streaming

raw_stream = (spark.readStream
    .schema(schema)
    .format("parquet")
    .load(source_path))

def upsert_to_iceberg(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    # collapse to the latest change per key within this micro-batch
    w = Window.partitionBy("pk").orderBy(F.col("cdc_ts").desc())
    latest = (batch_df.withColumn("rn", F.row_number().over(w))
                       .filter("rn = 1").drop("rn"))
    latest.createOrReplaceTempView("cdc_batch")

    spark.sql(f"""
        MERGE INTO {target_table} t
        USING cdc_batch s
        ON t.pk = s.pk
        WHEN MATCHED AND s.op = 'D' THEN DELETE
        WHEN MATCHED AND s.op IN ('U','I') AND s.cdc_ts > t.cdc_ts THEN UPDATE SET *
        WHEN NOT MATCHED AND s.op IN ('I','U') THEN INSERT *
    """)

query = (raw_stream.writeStream
    .foreachBatch(upsert_to_iceberg)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)   # drains what's currently there, then stops
    .start())
query.awaitTermination()
