import dlt

expectations_customers={
  "rule1": "customer_id IS NOT NULL" 
}
expectations_products={
  "rule1": "product_id IS NOT NULL" 
}
@dlt.table(
    name="customers_bronze"
)
@dlt.expect_all_or_drop(expectations_customers)
def customers_bronze():
  df = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").load("/Volumes/dlt/landingzone/raw_data/cutomers_data/")
  return df

@dlt.table(
    name="products_bronze"
)
@dlt.expect_all_or_drop(expectations_products)
def products_bronze():
  df = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").load("/Volumes/dlt/landingzone/raw_data/products_data/")
  return df