import dlt
from pyspark.sql.functions import *

@dlt.view(
    name = "customers_gold_view"
)
def products_gold_view():
  df = spark.readStream.table("customer_silver_view")
  return df

dlt.create_streaming_table(name ="customers_gold")

dlt.create_auto_cdc_flow(
    target="customers_gold",
    source="customers_gold_view",
    keys=["customer_id"],
    sequence_by=col("processDate"),
    stored_as_scd_type=2,
    except_column_list=['processDate']
)