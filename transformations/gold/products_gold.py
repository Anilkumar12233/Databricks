import dlt
from pyspark.sql.functions import *

@dlt.view(
    name = "products_gold_view"
)
def products_gold_view():
  df = spark.readStream.table("products_silver_view")
  return df

dlt.create_streaming_table(name ="products_gold")

dlt.create_auto_cdc_flow(
    target="products_gold",
    source="products_gold_view",
    keys=["product_id"],
    sequence_by=col("processDate"),
    stored_as_scd_type=2,
    except_column_list=["processDate"]
)