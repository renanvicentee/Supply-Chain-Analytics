# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

df_products = spark.table("supply_chain.bronze.products")
df_suppliers = spark.table("supply_chain.bronze.suppliers")
df_warehouses = spark.table("supply_chain.bronze.warehouses")

# COMMAND ----------

display(df_products)

# COMMAND ----------

df_products.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_products.columns
]).show()

# COMMAND ----------

print("Total: ", df_products.count())
print("Duplicados por product_id: ",
      df_products.count() - df_products.dropDuplicates(['product_id']).count())

# COMMAND ----------

df_products.select(
    F.min("unit_cost").alias("min_unit_cost"),
    F.max("unit_cost").alias("max_unit_cost"),
    F.min("unit_price").alias("min_unit_price"),
    F.max("unit_price").alias("max_unit_price"),
    F.min("weight_kg").alias("min_weight"),
    F.max("weight_kg").alias("max_weight")
).show()

# COMMAND ----------

median_weight = df_products.approxQuantile(
    "weight_kg",
    [0.5],
    0.01
)[0]

print("Mediana do peso:", median_weight)

# COMMAND ----------

df_products_silver = (
    df_products

    # Remove duplicados pela chave
    .dropDuplicates(["product_id"])

    # Remove registros sem chave primária
    .filter(F.col("product_id").isNotNull())

    # Padroniza textos
    .withColumn("sku", F.trim(F.col("sku")))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("subcategory", F.trim(F.col("subcategory")))

    # Preenche peso ausente com a mediana
    .fillna({"weight_kg": median_weight})

    # Regras básicas de qualidade
    .filter(F.col("unit_cost") > 0)
    .filter(F.col("unit_price") > 0)
    .filter(F.col("weight_kg") > 0)
    .filter(F.col("lead_time_days") >= 0)
    .filter(F.col("reorder_point") >= 0)
    .filter(F.col("safety_stock") >= 0)
)

# COMMAND ----------

print("Bronze:", df_products.count())
print("Silver:", df_products_silver.count())

df_products_silver.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_products_silver.columns
]).show()

# COMMAND ----------

df_products_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.products")

# COMMAND ----------

spark.table("supply_chain.silver.products").count()

# COMMAND ----------

df_suppliers = spark.table("supply_chain.bronze.suppliers")

display(df_suppliers)

# COMMAND ----------

df_suppliers.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_suppliers.columns
]).show()

# COMMAND ----------

print("Total:", df_suppliers.count())
print(
    "Duplicados por supplier_id:",
    df_suppliers.count() - df_suppliers.dropDuplicates(["supplier_id"]).count()
)

# COMMAND ----------

df_suppliers.select(
    F.min("rating").alias("min_rating"),
    F.max("rating").alias("max_rating"),
    F.min("avg_lead_time_days").alias("min_lead_time"),
    F.max("avg_lead_time_days").alias("max_lead_time"),
    F.min("on_time_delivery_target").alias("min_target"),
    F.max("on_time_delivery_target").alias("max_target")
).show()

# COMMAND ----------

df_suppliers.select("country").distinct().show(50, False)
df_suppliers.select("supplier_category").distinct().show(50, False)

# COMMAND ----------

df_suppliers.groupBy("active").count().show()

# COMMAND ----------

df_suppliers_silver = (
    df_suppliers

    .dropDuplicates(["supplier_id"])

    .filter(F.col("supplier_id").isNotNull())

    .withColumn("supplier_name", F.trim(F.col("supplier_name")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("supplier_category", F.trim(F.col("supplier_category")))
    .withColumn("supplier_profile", F.trim(F.col("supplier_profile")))

    .filter((F.col("rating") >= 1) & (F.col("rating") <= 5))
    .filter(F.col("avg_lead_time_days") >= 0)
    .filter(
        (F.col("on_time_delivery_target") >= 0) &
        (F.col("on_time_delivery_target") <= 100)
    )
    .filter(
        (F.col("delay_probability") >= 0) &
        (F.col("delay_probability") <= 1)
    )
    .filter(
        (F.col("partial_probability") >= 0) &
        (F.col("partial_probability") <= 1)
    )
)

# COMMAND ----------

print("Bronze:", df_suppliers.count())
print("Silver:", df_suppliers_silver.count())

# COMMAND ----------

df_suppliers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.suppliers")

# COMMAND ----------

df_warehouses = spark.table("supply_chain.bronze.warehouses")

display(df_warehouses)

# COMMAND ----------

df_warehouses.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_warehouses.columns
]).show()

# COMMAND ----------

print("Total:", df_warehouses.count())
print(
    "Duplicados por warehouse_id:",
    df_warehouses.count() - df_warehouses.dropDuplicates(["warehouse_id"]).count()
)

# COMMAND ----------

df_warehouses.select(
    F.min("capacity_units").alias("min_capacity"),
    F.max("capacity_units").alias("max_capacity"),
    F.min("monthly_operating_cost").alias("min_cost"),
    F.max("monthly_operating_cost").alias("max_cost")
).show()

# COMMAND ----------

df_warehouses.select("region").distinct().show(50, False)
df_warehouses.select("active").distinct().show()

# COMMAND ----------

df_warehouses_silver = (
    df_warehouses

    .dropDuplicates(["warehouse_id"])

    .filter(F.col("warehouse_id").isNotNull())

    .withColumn("warehouse_name", F.trim(F.col("warehouse_name")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("region", F.trim(F.col("region")))

    .filter(F.col("capacity_units") > 0)
    .filter(F.col("monthly_operating_cost") > 0)
)

# COMMAND ----------

print("Bronze:", df_warehouses.count())
print("Silver:", df_warehouses_silver.count())

# COMMAND ----------

df_warehouses_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.warehouses")

# COMMAND ----------

spark.table("supply_chain.silver.warehouses").count()

# COMMAND ----------

df_purchase_orders = spark.table("supply_chain.bronze.purchase_orders")

display(df_purchase_orders)

# COMMAND ----------

df_purchase_orders.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_purchase_orders.columns
]).show()

# COMMAND ----------

print("Total:", df_purchase_orders.count())

print(
    "Duplicados por purchase_order_id:",
    df_purchase_orders.count()
    - df_purchase_orders.dropDuplicates(["purchase_order_id"]).count()
)

# COMMAND ----------

df_purchase_orders.select(
    F.min("quantity_ordered").alias("min_quantity_ordered"),
    F.max("quantity_ordered").alias("max_quantity_ordered"),
    F.min("quantity_received").alias("min_quantity_received"),
    F.max("quantity_received").alias("max_quantity_received"),
    F.min("unit_cost").alias("min_unit_cost"),
    F.max("unit_cost").alias("max_unit_cost")
).show()

# COMMAND ----------

df_purchase_orders.select(
    F.sum(
        (F.col("expected_delivery_date") < F.col("order_date"))
        .cast("int")
    ).alias("expected_before_order"),

    F.sum(
        (
            F.col("actual_delivery_date").isNotNull()
            &
            (F.col("actual_delivery_date") < F.col("order_date"))
        ).cast("int")
    ).alias("actual_before_order")
).show()

# COMMAND ----------

df_purchase_orders.groupBy("purchase_status").count().show()

# COMMAND ----------

invalid_suppliers = (
    df_purchase_orders
    .join(
        spark.table("supply_chain.silver.suppliers")
        .select("supplier_id"),
        on="supplier_id",
        how="left_anti"
    )
)

print("Supplier IDs inválidos:", invalid_suppliers.count())

# COMMAND ----------

invalid_products = (
    df_purchase_orders
    .join(
        spark.table("supply_chain.silver.products")
        .select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

print("Product IDs inválidos:", invalid_products.count())

# COMMAND ----------

invalid_warehouses = (
    df_purchase_orders
    .join(
        spark.table("supply_chain.silver.warehouses")
        .select("warehouse_id"),
        on="warehouse_id",
        how="left_anti"
    )
)

print("Warehouse IDs inválidos:", invalid_warehouses.count())

# COMMAND ----------

df_purchase_orders \
    .filter(F.col("actual_delivery_date").isNull()) \
    .groupBy("purchase_status") \
    .count() \
    .show()

# COMMAND ----------

df_purchase_orders.select(
    F.sum(
        (
            F.col("quantity_received") >
            F.col("quantity_ordered")
        ).cast("int")
    ).alias("received_greater_than_ordered")
).show()

# COMMAND ----------

df_purchase_orders.filter(
    F.col("quantity_ordered") <= 0
).count()

# COMMAND ----------

df_purchase_orders.filter(
    F.col("unit_cost") <= 0
).count()

# COMMAND ----------

df_purchase_orders.select(
    F.upper(F.trim(F.col("purchase_status"))).alias("status_padronizado")
).distinct().show()

# COMMAND ----------

df_purchase_orders_silver = (
    df_purchase_orders

    # Remove duplicados pela chave da ordem de compra
    .dropDuplicates(["purchase_order_id"])

    # Remove registros sem supplier_id
    .filter(F.col("supplier_id").isNotNull())

    # Padroniza o status
    .withColumn(
        "purchase_status",
        F.upper(F.trim(F.col("purchase_status")))
    )

    # Remove quantidades inválidas
    .filter(F.col("quantity_ordered") > 0)

    # Não permite receber mais do que foi pedido
    .filter(
        F.col("quantity_received") <= F.col("quantity_ordered")
    )

    # Remove datas de entrega prevista impossíveis
    .filter(
        F.col("expected_delivery_date") >= F.col("order_date")
    )
)

# COMMAND ----------

print("Bronze:", df_purchase_orders.count())
print("Silver candidata:", df_purchase_orders_silver.count())

# COMMAND ----------

invalid_suppliers_after_clean = (
    df_purchase_orders_silver
    .join(
        spark.table("supply_chain.silver.suppliers")
        .select("supplier_id"),
        on="supplier_id",
        how="left_anti"
    )
)

invalid_suppliers_after_clean.count()

# COMMAND ----------

df_purchase_orders_silver.filter(
    F.col("quantity_received") < 0
).count()

# COMMAND ----------

df_purchase_orders_silver.filter(
    (F.col("purchase_status") == "RECEIVED") &
    (F.col("actual_delivery_date").isNull())
).count()

# COMMAND ----------

df_purchase_orders.groupBy("purchase_status").count().show()

# COMMAND ----------

df_purchase_orders_silver.groupBy("purchase_status").count().show()

# COMMAND ----------

df_purchase_orders_silver.select(
    F.min("quantity_ordered").alias("min_quantity_ordered"),
    F.max("quantity_ordered").alias("max_quantity_ordered")
).show()

# COMMAND ----------

df_purchase_orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.purchase_orders")

# COMMAND ----------

spark.table("supply_chain.silver.purchase_orders").count()

# COMMAND ----------

df_inventory = spark.table("supply_chain.bronze.inventory")

# COMMAND ----------

df_inventory.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_inventory.columns
]).show()

# COMMAND ----------

df_inventory = spark.table("supply_chain.bronze.inventory")

# COMMAND ----------

df_inventory.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_inventory.columns
]).show()

# COMMAND ----------

print("Total:", df_inventory.count())

print(
    "Duplicados por inventory_id:",
    df_inventory.count()
    - df_inventory.dropDuplicates(["inventory_id"]).count()
)

# COMMAND ----------

df_inventory.select(
    F.min("on_hand_qty").alias("min_on_hand"),
    F.max("on_hand_qty").alias("max_on_hand"),
    F.min("reserved_qty").alias("min_reserved"),
    F.max("reserved_qty").alias("max_reserved"),
    F.min("available_qty").alias("min_available"),
    F.max("available_qty").alias("max_available"),
    F.min("incoming_qty").alias("min_incoming"),
    F.max("incoming_qty").alias("max_incoming"),
    F.min("damaged_qty").alias("min_damaged"),
    F.max("damaged_qty").alias("max_damaged"),
    F.min("stock_value").alias("min_stock_value"),
    F.max("stock_value").alias("max_stock_value")
).show()

# COMMAND ----------

df_inventory.filter(
    F.col("on_hand_qty") < 0
).count()

# COMMAND ----------

df_inventory.filter(
    F.col("available_qty") !=
    (F.col("on_hand_qty") - F.col("reserved_qty"))
).count()

# COMMAND ----------

df_inventory.filter(
    (F.col("on_hand_qty") >= 0) &
    (
        F.col("available_qty") !=
        (F.col("on_hand_qty") - F.col("reserved_qty"))
    )
).count()

# COMMAND ----------

df_inventory.filter(
    F.col("reserved_qty") > F.col("on_hand_qty")
).count()

# COMMAND ----------

invalid_inventory_products = (
    df_inventory
    .join(
        spark.table("supply_chain.silver.products")
        .select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

invalid_inventory_products.count()

# COMMAND ----------

invalid_inventory_warehouses = (
    df_inventory
    .join(
        spark.table("supply_chain.silver.warehouses")
        .select("warehouse_id"),
        on="warehouse_id",
        how="left_anti"
    )
)

invalid_inventory_warehouses.count()

# COMMAND ----------

df_inventory.filter(
    F.col("stock_value") < 0
).count()

# COMMAND ----------

df_inventory_silver = (
    df_inventory

    # Remove duplicados pela chave
    .dropDuplicates(["inventory_id"])

    # Remove registros sem warehouse
    .filter(F.col("warehouse_id").isNotNull())

    # Remove estoques físicos inválidos
    .filter(F.col("on_hand_qty") >= 0)

    # Mantém apenas produtos válidos
    .join(
        spark.table("supply_chain.silver.products")
        .select("product_id"),
        on="product_id",
        how="inner"
    )

    # Mantém apenas warehouses válidos
    .join(
        spark.table("supply_chain.silver.warehouses")
        .select("warehouse_id"),
        on="warehouse_id",
        how="inner"
    )

    # Recalcula a disponibilidade corretamente
    .withColumn(
        "available_qty",
        F.col("on_hand_qty") - F.col("reserved_qty")
    )
)

# COMMAND ----------

print("Bronze:", df_inventory.count())
print("Silver candidata:", df_inventory_silver.count())


# COMMAND ----------

df_inventory_silver.filter(
    F.col("available_qty") !=
    (F.col("on_hand_qty") - F.col("reserved_qty"))
).count()

# COMMAND ----------

invalid_products_after_clean = (
    df_inventory_silver
    .join(
        spark.table("supply_chain.silver.products")
        .select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

invalid_products_after_clean.count()

# COMMAND ----------

invalid_warehouses_after_clean = (
    df_inventory_silver
    .join(
        spark.table("supply_chain.silver.warehouses")
        .select("warehouse_id"),
        on="warehouse_id",
        how="left_anti"
    )
)

invalid_warehouses_after_clean.count()

# COMMAND ----------

df_inventory_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.inventory")

# COMMAND ----------

spark.table("supply_chain.silver.inventory").count()

# COMMAND ----------

df_orders = spark.table("supply_chain.bronze.orders")

# COMMAND ----------

df_orders.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_orders.columns
]).show()

# COMMAND ----------

print("Total:", df_orders.count())

print(
    "Duplicados por order_id:",
    df_orders.count()
    - df_orders.dropDuplicates(["order_id"]).count()
)

# COMMAND ----------

df_orders.select(
    F.min("quantity").alias("min_quantity"),
    F.max("quantity").alias("max_quantity"),
    F.min("unit_price").alias("min_unit_price"),
    F.max("unit_price").alias("max_unit_price"),
    F.min("discount_pct").alias("min_discount_pct"),
    F.max("discount_pct").alias("max_discount_pct")
).show()

# COMMAND ----------

df_orders.filter(
    F.col("quantity") <= 0
).count()

# COMMAND ----------

df_orders.filter(
    F.col("unit_price") <= 0
).count()

# COMMAND ----------

df_orders.filter(
    (F.col("discount_pct") < 0) |
    (F.col("discount_pct") > 100)
).count()

# COMMAND ----------

invalid_order_products = (
    df_orders
    .join(
        spark.table("supply_chain.silver.products")
        .select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

invalid_order_products.count()

# COMMAND ----------

invalid_order_warehouses = (
    df_orders
    .join(
        spark.table("supply_chain.silver.warehouses")
        .select("warehouse_id"),
        on="warehouse_id",
        how="left_anti"
    )
)

invalid_order_warehouses.count()

# COMMAND ----------

df_orders.select(
    F.upper(F.trim(F.col("order_status"))).alias("status_padronizado")
).distinct().show()

# COMMAND ----------

df_orders.filter(
    F.col("promised_delivery_date") < F.to_date(F.col("order_date"))
).count()

# COMMAND ----------

df_orders_silver = (
    df_orders

    # Remove duplicados pela chave
    .dropDuplicates(["order_id"])

    # Padroniza status
    .withColumn(
        "order_status",
        F.upper(F.trim(F.col("order_status")))
    )

    # Preenche região ausente
    .fillna({"customer_region": "UNKNOWN"})

    # Remove registros numericamente inválidos
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") > 0)
    .filter(
        (F.col("discount_pct") >= 0) &
        (F.col("discount_pct") <= 100)
    )

    # Mantém somente produtos válidos
    .join(
        spark.table("supply_chain.silver.products")
        .select("product_id"),
        on="product_id",
        how="inner"
    )

    # Mantém somente warehouses válidos
    .join(
        spark.table("supply_chain.silver.warehouses")
        .select("warehouse_id"),
        on="warehouse_id",
        how="inner"
    )
)

# COMMAND ----------

print("Bronze:", df_orders.count())
print("Silver candidata:", df_orders_silver.count())

# COMMAND ----------

invalid_products_after_clean = (
    df_orders_silver
    .join(
        spark.table("supply_chain.silver.products")
        .select("product_id"),
        on="product_id",
        how="left_anti"
    )
)

invalid_products_after_clean.count()

# COMMAND ----------

invalid_warehouses_after_clean = (
    df_orders_silver
    .join(
        spark.table("supply_chain.silver.warehouses")
        .select("warehouse_id"),
        on="warehouse_id",
        how="left_anti"
    )
)

invalid_warehouses_after_clean.count()

# COMMAND ----------

df_orders_silver.filter(
    F.col("customer_region").isNull()
).count()

# COMMAND ----------

df_orders_silver.count() - df_orders_silver.dropDuplicates(["order_id"]).count()

# COMMAND ----------

df_orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.orders")

# COMMAND ----------

spark.table("supply_chain.silver.orders").count()

# COMMAND ----------

df_shipments = spark.table("supply_chain.bronze.shipments")

# COMMAND ----------

df_shipments.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df_shipments.columns
]).show()

# COMMAND ----------

df_shipments \
    .filter(F.col("delivered_at").isNull()) \
    .groupBy("shipment_status") \
    .count() \
    .show()

# COMMAND ----------

print("Total:", df_shipments.count())

print(
    "Duplicados por shipment_id:",
    df_shipments.count()
    - df_shipments.dropDuplicates(["shipment_id"]).count()
)

# COMMAND ----------

df_shipments.select(
    F.min("shipping_cost").alias("min_shipping_cost"),
    F.max("shipping_cost").alias("max_shipping_cost"),
    F.min("distance_km").alias("min_distance_km"),
    F.max("distance_km").alias("max_distance_km")
).show()

# COMMAND ----------

df_shipments.filter(
    F.col("shipping_cost") <= 0
).count()

# COMMAND ----------

invalid_shipment_orders = (
    df_shipments
    .join(
        spark.table("supply_chain.silver.orders")
        .select("order_id"),
        on="order_id",
        how="left_anti"
    )
)

invalid_shipment_orders.count()

# COMMAND ----------

invalid_shipment_warehouses = (
    df_shipments
    .join(
        spark.table("supply_chain.silver.warehouses")
        .select("warehouse_id"),
        on="warehouse_id",
        how="left_anti"
    )
)

invalid_shipment_warehouses.count()

# COMMAND ----------

df_shipments.select(
    F.upper(F.trim(F.col("shipment_status"))).alias("status_padronizado")
).distinct().show()

# COMMAND ----------

df_shipments_silver = (
    df_shipments

    # Remove duplicados pela chave
    .dropDuplicates(["shipment_id"])

    # Padroniza o status
    .withColumn(
        "shipment_status",
        F.upper(F.trim(F.col("shipment_status")))
    )

    # Remove custos de frete inválidos
    .filter(F.col("shipping_cost") > 0)

    # Mantém apenas pedidos válidos
    .join(
        spark.table("supply_chain.silver.orders")
        .select("order_id"),
        on="order_id",
        how="inner"
    )

    # Mantém apenas warehouses válidos
    .join(
        spark.table("supply_chain.silver.warehouses")
        .select("warehouse_id"),
        on="warehouse_id",
        how="inner"
    )
)

# COMMAND ----------

print("Bronze:", df_shipments.count())
print("Silver candidata:", df_shipments_silver.count())

# COMMAND ----------

invalid_orders_after_clean = (
    df_shipments_silver
    .join(
        spark.table("supply_chain.silver.orders")
        .select("order_id"),
        on="order_id",
        how="left_anti"
    )
)

invalid_orders_after_clean.count()

# COMMAND ----------

invalid_warehouses_after_clean = (
    df_shipments_silver
    .join(
        spark.table("supply_chain.silver.warehouses")
        .select("warehouse_id"),
        on="warehouse_id",
        how="left_anti"
    )
)

invalid_warehouses_after_clean.count()

# COMMAND ----------

df_shipments_silver.count() - df_shipments_silver.dropDuplicates(["shipment_id"]).count()

# COMMAND ----------

df_shipments_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.silver.shipments")

# COMMAND ----------

spark.table("supply_chain.silver.shipments").count()