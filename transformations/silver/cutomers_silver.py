import dlt
from pyspark.sql.functions import *

@dlt.view(
    name="customer_silver_view"
)
def customer_silver_view():
    df = spark.readStream.table("customers_bronze")
    df = df.withColumn("processDate", current_timestamp())
    df = df.withColumn("domain", split(col("email"), "@").getItem(1))
    df = df.withColumn("name", upper(col("name")))
    return df

dlt.create_streaming_table(name="customer_silver")

dlt.create_auto_cdc_flow(
    target="customer_silver",
    source="customer_silver_view",
    keys=["customer_id"],
    sequence_by=col("processDate"),
    stored_as_scd_type=1
)