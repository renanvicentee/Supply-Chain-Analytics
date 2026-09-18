# Databricks notebook source
products_path = "/Volumes/supply_chain/raw/files/products/products.csv"

df_products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(products_path)
)

display(df_products)

# COMMAND ----------

df_products.printSchema()

# COMMAND ----------

df_products.count()

# COMMAND ----------

suppliers_path = "/Volumes/supply_chain/raw/files/suppliers/suppliers.csv"

df_suppliers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(suppliers_path)
)

display(df_suppliers)

# COMMAND ----------

df_suppliers.printSchema()
df_suppliers.count()

# COMMAND ----------

warehouses_path = "/Volumes/supply_chain/raw/files/warehouses/warehouses.csv"

df_warehouses = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(warehouses_path)
)

display(df_warehouses)

# COMMAND ----------

df_warehouses.printSchema()
df_warehouses.count()

# COMMAND ----------

df_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.bronze.products")

# COMMAND ----------

df_suppliers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.bronze.suppliers")

# COMMAND ----------

df_warehouses.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.bronze.warehouses")

# COMMAND ----------

spark.sql("SHOW TABLES IN supply_chain.bronze").show()

# COMMAND ----------

df_bronze_products = spark.table("supply_chain.bronze.products")

display(df_bronze_products)

# COMMAND ----------

purchase_orders_path = "/Volumes/supply_chain/raw/files/purchase_orders/"

df_purchase_orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(purchase_orders_path)
)

display(df_purchase_orders)

# COMMAND ----------

df_purchase_orders.printSchema()

# COMMAND ----------

df_purchase_orders.count()

# COMMAND ----------

df_purchase_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.bronze.purchase_orders")

# COMMAND ----------

spark.sql("SHOW TABLES IN supply_chain.bronze").show()

# COMMAND ----------

inventory_path = "/Volumes/supply_chain/raw/files/inventory/"

df_inventory = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inventory_path)
)

display(df_inventory)

# COMMAND ----------

df_inventory.printSchema()
df_inventory.count()

# COMMAND ----------

df_inventory.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.bronze.inventory")

# COMMAND ----------

orders_path = "/Volumes/supply_chain/raw/files/orders/"

df_orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(orders_path)
)

display(df_orders)

# COMMAND ----------

df_orders.printSchema()
df_orders.count()

# COMMAND ----------

df_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.bronze.orders")

# COMMAND ----------

purchase_orders_path = "/Volumes/supply_chain/raw/files/purchase_orders/"

df_purchase_orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(purchase_orders_path)
)

display(df_purchase_orders)

# COMMAND ----------

shipments_path = "/Volumes/supply_chain/raw/files/shipments/"

df_shipments = (
    spark.read
    .option("multiLine", "false")
    .json(shipments_path)
)

display(df_shipments)

# COMMAND ----------

df_shipments.printSchema()
df_shipments.count()

# COMMAND ----------

df_shipments.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.bronze.shipments")

# COMMAND ----------

spark.sql("SHOW TABLES IN supply_chain.bronze").show()

# COMMAND ----------

tables = [
    "products",
    "suppliers",
    "warehouses",
    "inventory",
    "orders",
    "purchase_orders",
    "shipments"
]

for table in tables:
    count = spark.table(f"supply_chain.bronze.{table}").count()
    print(f"{table}: {count:,}")