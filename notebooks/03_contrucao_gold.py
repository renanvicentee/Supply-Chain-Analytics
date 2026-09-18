# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

df_inventory = spark.table("supply_chain.silver.inventory")
df_products = spark.table("supply_chain.silver.products")
df_warehouses = spark.table("supply_chain.silver.warehouses")

# COMMAND ----------

df_inventory_gold = (
    df_inventory

    .join(
        df_products.select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "unit_cost",
            "reorder_point",
            "safety_stock"
        ),
        on="product_id",
        how="inner"
    )

    .join(
        df_warehouses.select(
            "warehouse_id",
            "warehouse_name",
            "city",
            "state",
            "region"
        ),
        on="warehouse_id",
        how="inner"
    )
)

# COMMAND ----------

display(df_inventory_gold)

# COMMAND ----------

df_inventory_gold = (
    df_inventory_gold
    .withColumn(
        "stockout_flag",
        F.when(F.col("available_qty") <= 0, 1).otherwise(0)
    )
)

# COMMAND ----------

df_inventory_gold.groupBy("stockout_flag").count().show()

# COMMAND ----------

df_inventory_gold.select(
    (
        F.sum("stockout_flag") / F.count("*") * 100
    ).alias("stockout_rate_pct")
).show()

# COMMAND ----------

df_inventory_gold = (
    df_inventory_gold
    .withColumn(
        "low_stock_flag",
        F.when(
            (F.col("available_qty") > 0) &
            (F.col("available_qty") <= F.col("safety_stock")),
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

df_inventory_gold.groupBy("low_stock_flag").count().show()

# COMMAND ----------

df_inventory_gold.select(
    (
        F.sum("low_stock_flag") / F.count("*") * 100
    ).alias("low_stock_rate_pct")
).show()

# COMMAND ----------

df_inventory_gold = (
    df_inventory_gold
    .withColumn(
        "overstock_flag",
        F.when(
            F.col("available_qty") > (F.col("safety_stock") * 3),
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

df_inventory_gold.groupBy("overstock_flag").count().show()

# COMMAND ----------

df_inventory_gold.select(
    (
        F.sum("overstock_flag") / F.count("*") * 100
    ).alias("overstock_rate_pct")
).show()

# COMMAND ----------

df_inventory_gold.filter(
    F.col("overstock_flag") == 1
).select(
    F.sum("stock_value").alias("overstock_stock_value")
).show()

# COMMAND ----------

df_inventory_gold.filter(
    F.col("overstock_flag") == 1
).select(
    F.round(
        F.sum("stock_value"),
        2
    ).alias("overstock_stock_value")
).show(truncate=False)

# COMMAND ----------

df_inventory_gold.select(
    F.round(
        F.sum("stock_value"),
        2
    ).alias("total_stock_value")
).show(truncate=False)

# COMMAND ----------

df_inventory_gold.select(
    (
        F.sum(
            F.when(
                F.col("overstock_flag") == 1,
                F.col("stock_value")
            ).otherwise(0)
        )
        /
        F.sum("stock_value")
        * 100
    ).alias("overstock_value_pct")
).show()

# COMMAND ----------

df_inventory_gold = (
    df_inventory_gold
    .withColumn(
        "slow_moving_flag",
        F.when(
            F.datediff(
                F.col("snapshot_date"),
                F.to_date(F.col("last_movement_at"))
            ) > 60,
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

df_inventory_gold.groupBy("slow_moving_flag").count().show()

# COMMAND ----------

df_inventory_gold.select(
    F.min(
        F.datediff(
            F.col("snapshot_date"),
            F.to_date(F.col("last_movement_at"))
        )
    ).alias("min_days_without_movement"),

    F.max(
        F.datediff(
            F.col("snapshot_date"),
            F.to_date(F.col("last_movement_at"))
        )
    ).alias("max_days_without_movement")
).show()

# COMMAND ----------

df_inventory_gold = (
    df_inventory_gold
    .withColumn(
        "slow_moving_flag",
        F.when(
            F.datediff(
                F.col("snapshot_date"),
                F.to_date(F.col("last_movement_at"))
            ) > 20,
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

df_inventory_gold.groupBy("slow_moving_flag").count().show()

# COMMAND ----------

df_inventory_gold.filter(
    F.col("slow_moving_flag") == 1
).select(
    F.round(
        F.sum("stock_value"),
        2
    ).alias("slow_moving_stock_value")
).show(truncate=False)

# COMMAND ----------

df_inventory_gold.select(
    "stockout_flag",
    "low_stock_flag",
    "overstock_flag",
    "slow_moving_flag"
).distinct().show()

# COMMAND ----------

df_inventory_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.gold.inventory_analysis")

# COMMAND ----------

spark.table("supply_chain.gold.inventory_analysis").count()

# COMMAND ----------

df_purchase_orders = spark.table("supply_chain.silver.purchase_orders")
df_suppliers = spark.table("supply_chain.silver.suppliers")

# COMMAND ----------

df_supplier_gold = (
    df_purchase_orders

    .join(
        df_suppliers.select(
            "supplier_id",
            "supplier_name",
            "country",
            "state",
            "city",
            "supplier_category",
            "rating",
            "supplier_profile"
        ),
        on="supplier_id",
        how="inner"
    )
)

# COMMAND ----------

df_supplier_gold = (
    df_supplier_gold
    .withColumn(
        "delay_days",
        F.when(
            F.col("actual_delivery_date").isNotNull(),
            F.greatest(
                F.datediff(
                    F.col("actual_delivery_date"),
                    F.col("expected_delivery_date")
                ),
                F.lit(0)
            )
        ).otherwise(None)
    )
)

# COMMAND ----------

df_supplier_gold.select(
    F.min("delay_days").alias("min_delay"),
    F.max("delay_days").alias("max_delay"),
    F.avg("delay_days").alias("avg_delay")
).show()

# COMMAND ----------

df_supplier_gold = (
    df_supplier_gold
    .withColumn(
        "on_time_flag",
        F.when(
            F.col("actual_delivery_date").isNull(),
            None
        ).when(
            F.col("actual_delivery_date") <= F.col("expected_delivery_date"),
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

df_supplier_gold.groupBy("on_time_flag").count().show()

# COMMAND ----------

df_supplier_gold.filter(
    F.col("on_time_flag").isNotNull()
).select(
    (
        F.sum("on_time_flag") / F.count("*") * 100
    ).alias("on_time_delivery_rate_pct")
).show()

# COMMAND ----------

df_supplier_gold = (
    df_supplier_gold
    .withColumn(
        "fill_rate",
        F.col("quantity_received") / F.col("quantity_ordered")
    )
)

# COMMAND ----------

df_supplier_gold.select(
    (
        F.sum("quantity_received") /
        F.sum("quantity_ordered") * 100
    ).alias("overall_fill_rate_pct")
).show()

# COMMAND ----------

df_supplier_performance = (
    df_supplier_gold
    .groupBy(
        "supplier_id",
        "supplier_name",
        "country",
        "state",
        "city",
        "supplier_category",
        "rating",
        "supplier_profile"
    )
    .agg(
        F.count("*").alias("purchase_orders_count"),

        F.round(
            F.avg("delay_days"),
            2
        ).alias("avg_delay_days"),

        F.round(
            F.avg("on_time_flag") * 100,
            2
        ).alias("on_time_delivery_rate_pct"),

        F.round(
            F.sum("quantity_received") /
            F.sum("quantity_ordered") * 100,
            2
        ).alias("fill_rate_pct"),

        F.sum("quantity_ordered").alias("total_quantity_ordered"),

        F.sum("quantity_received").alias("total_quantity_received")
    )
)

# COMMAND ----------

display(df_supplier_performance)

# COMMAND ----------

df_supplier_performance = (
    df_supplier_performance
    .withColumn(
        "performance_class",
        F.when(
            (F.col("on_time_delivery_rate_pct") >= 90) &
            (F.col("fill_rate_pct") >= 98) &
            (F.col("avg_delay_days") <= 1),
            "EXCELLENT"
        )
        .when(
            (F.col("on_time_delivery_rate_pct") >= 75) &
            (F.col("fill_rate_pct") >= 95) &
            (F.col("avg_delay_days") <= 3),
            "GOOD"
        )
        .when(
            (F.col("on_time_delivery_rate_pct") >= 60) &
            (F.col("fill_rate_pct") >= 90),
            "REGULAR"
        )
        .otherwise("PROBLEMATIC")
    )
)

# COMMAND ----------

df_supplier_performance.groupBy("performance_class").count().show()

# COMMAND ----------

df_supplier_performance.select(
    "supplier_id",
    "supplier_name",
    "avg_delay_days",
    "on_time_delivery_rate_pct",
    "fill_rate_pct",
    "performance_class"
).orderBy(
    F.col("on_time_delivery_rate_pct").desc(),
    F.col("fill_rate_pct").desc(),
    F.col("avg_delay_days").asc()
).show(10, False)

# COMMAND ----------

df_supplier_performance.select(
    F.expr(
        "percentile_approx(on_time_delivery_rate_pct, array(0.25, 0.50, 0.75, 0.90))"
    ).alias("on_time_percentiles"),

    F.expr(
        "percentile_approx(fill_rate_pct, array(0.25, 0.50, 0.75, 0.90))"
    ).alias("fill_rate_percentiles"),

    F.expr(
        "percentile_approx(avg_delay_days, array(0.25, 0.50, 0.75, 0.90))"
    ).alias("delay_percentiles")
).show(truncate=False)

# COMMAND ----------

df_supplier_performance = (
    df_supplier_performance
    .withColumn(
        "performance_class",
        F.when(
            (F.col("on_time_delivery_rate_pct") >= 76) &
            (F.col("fill_rate_pct") >= 98) &
            (F.col("avg_delay_days") <= 1.2),
            "EXCELLENT"
        )
        .when(
            (F.col("on_time_delivery_rate_pct") >= 73) &
            (F.col("fill_rate_pct") >= 97.5) &
            (F.col("avg_delay_days") <= 1.5),
            "GOOD"
        )
        .when(
            (F.col("on_time_delivery_rate_pct") >= 60) &
            (F.col("fill_rate_pct") >= 96) &
            (F.col("avg_delay_days") <= 3),
            "REGULAR"
        )
        .otherwise("PROBLEMATIC")
    )
)

# COMMAND ----------

df_supplier_performance.groupBy("performance_class").count().show()

# COMMAND ----------

df_supplier_performance.count()

# COMMAND ----------

df_supplier_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.gold.supplier_performance")

# COMMAND ----------

spark.table("supply_chain.gold.supplier_performance").count()

# COMMAND ----------

df_shipments = spark.table("supply_chain.silver.shipments")
df_orders = spark.table("supply_chain.silver.orders")
df_warehouses = spark.table("supply_chain.silver.warehouses")

# COMMAND ----------

df_delivery_gold = (
    df_shipments

    .join(
        df_orders.select(
            "order_id",
            "order_date",
            "customer_region",
            "priority",
            "promised_delivery_date"
        ),
        on="order_id",
        how="inner"
    )

    .join(
        df_warehouses.select(
            "warehouse_id",
            "warehouse_name",
            "city",
            "state",
            "region"
        ),
        on="warehouse_id",
        how="inner"
    )
)

# COMMAND ----------

display(df_delivery_gold)

# COMMAND ----------

df_delivery_gold = (
    df_delivery_gold
    .withColumn(
        "delivery_delay_days",
        F.when(
            F.col("delivered_at").isNotNull(),
            F.greatest(
                F.datediff(
                    F.to_date(F.col("delivered_at")),
                    F.to_date(F.col("estimated_delivery_at"))
                ),
                F.lit(0)
            )
        ).otherwise(None)
    )
)

# COMMAND ----------

df_delivery_gold.select(
    F.min("delivery_delay_days").alias("min_delay"),
    F.max("delivery_delay_days").alias("max_delay"),
    F.avg("delivery_delay_days").alias("avg_delay")
).show()

# COMMAND ----------

df_delivery_gold = (
    df_delivery_gold
    .withColumn(
        "on_time_delivery_flag",
        F.when(
            F.col("delivered_at").isNull(),
            None
        ).when(
            F.col("delivered_at") <= F.col("estimated_delivery_at"),
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

df_delivery_gold.groupBy("on_time_delivery_flag").count().show()

# COMMAND ----------

df_delivery_gold.filter(
    F.col("on_time_delivery_flag").isNotNull()
).select(
    (
        F.sum("on_time_delivery_flag") /
        F.count("*") * 100
    ).alias("on_time_delivery_rate_pct")
).show()

# COMMAND ----------

df_delivery_gold = (
    df_delivery_gold
    .withColumn(
        "shipping_cost_per_km",
        F.col("shipping_cost") / F.col("distance_km")
    )
)

# COMMAND ----------

df_delivery_gold.select(
    F.min("shipping_cost_per_km").alias("min_cost_per_km"),
    F.max("shipping_cost_per_km").alias("max_cost_per_km"),
    F.avg("shipping_cost_per_km").alias("avg_cost_per_km")
).show()

# COMMAND ----------

df_carrier_performance = (
    df_delivery_gold
    .groupBy("carrier")
    .agg(
        F.count("*").alias("shipments_count"),

        F.round(
            F.avg("delivery_delay_days"),
            2
        ).alias("avg_delay_days"),

        F.round(
            F.avg("on_time_delivery_flag") * 100,
            2
        ).alias("on_time_delivery_rate_pct"),

        F.round(
            F.avg("shipping_cost_per_km"),
            3
        ).alias("avg_shipping_cost_per_km"),

        F.round(
            F.avg(F.col("damaged_flag").cast("int")) * 100,
            2
        ).alias("damaged_rate_pct")
    )
)

# COMMAND ----------

display(df_carrier_performance)

# COMMAND ----------

df_carrier_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.gold.carrier_performance")

# COMMAND ----------

spark.table("supply_chain.gold.carrier_performance").count()

# COMMAND ----------

df_delivery_gold.count()

# COMMAND ----------

df_delivery_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.gold.delivery_performance")

# COMMAND ----------

spark.table("supply_chain.gold.delivery_performance").count()

# COMMAND ----------

df_inventory_analysis = spark.table("supply_chain.gold.inventory_analysis")
df_delivery_performance = spark.table("supply_chain.gold.delivery_performance")

# COMMAND ----------

df_warehouse_inventory = (
    df_inventory_analysis
    .groupBy(
        "warehouse_id",
        "warehouse_name",
        "city",
        "state",
        "region"
    )
    .agg(
        F.count("*").alias("inventory_records"),

        F.round(
            F.avg("stockout_flag") * 100,
            2
        ).alias("stockout_rate_pct"),

        F.round(
            F.avg("low_stock_flag") * 100,
            2
        ).alias("low_stock_rate_pct"),

        F.round(
            F.avg("overstock_flag") * 100,
            2
        ).alias("overstock_rate_pct"),

        F.round(
            F.avg("slow_moving_flag") * 100,
            2
        ).alias("slow_moving_rate_pct"),

        F.round(
            F.sum("stock_value"),
            2
        ).alias("total_stock_value")
    )
)

# COMMAND ----------

display(df_warehouse_inventory)

# COMMAND ----------

df_warehouse_delivery = (
    df_delivery_performance
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("shipments_count"),

        F.round(
            F.avg("delivery_delay_days"),
            2
        ).alias("avg_delivery_delay_days"),

        F.round(
            F.avg("on_time_delivery_flag") * 100,
            2
        ).alias("on_time_delivery_rate_pct"),

        F.round(
            F.avg("shipping_cost_per_km"),
            3
        ).alias("avg_shipping_cost_per_km"),

        F.round(
            F.avg(F.col("damaged_flag").cast("int")) * 100,
            2
        ).alias("damaged_rate_pct")
    )
)

# COMMAND ----------

display(df_warehouse_delivery)

# COMMAND ----------

df_warehouse_performance = (
    df_warehouse_inventory
    .join(
        df_warehouse_delivery,
        on="warehouse_id",
        how="inner"
    )
)

# COMMAND ----------

display(df_warehouse_performance)

# COMMAND ----------

df_warehouse_performance = (
    df_warehouse_performance
    .withColumn(
        "performance_class",
        F.when(
            (F.col("stockout_rate_pct") < 33) &
            (F.col("on_time_delivery_rate_pct") >= 42.6) &
            (F.col("damaged_rate_pct") <= 1.13),
            "GOOD"
        )
        .when(
            (F.col("stockout_rate_pct") <= 34) &
            (F.col("on_time_delivery_rate_pct") >= 42.4),
            "REGULAR"
        )
        .otherwise("PROBLEMATIC")
    )
)

# COMMAND ----------

df_warehouse_performance.groupBy("performance_class").count().show()

# COMMAND ----------

df_warehouse_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("supply_chain.gold.warehouse_performance")

# COMMAND ----------

spark.table("supply_chain.gold.warehouse_performance").count()

# COMMAND ----------

spark.sql("SHOW TABLES IN supply_chain.gold").show()

# COMMAND ----------

gold_tables = [
    "inventory_analysis",
    "supplier_performance",
    "carrier_performance",
    "delivery_performance",
    "warehouse_performance"
]

for table in gold_tables:
    count = spark.table(f"supply_chain.gold.{table}").count()
    print(f"{table}: {count:,}")

# COMMAND ----------

inventory_gold = spark.table("supply_chain.gold.inventory_analysis")
supplier_gold = spark.table("supply_chain.gold.supplier_performance")
delivery_gold = spark.table("supply_chain.gold.delivery_performance")

print("=== INVENTORY ===")

inventory_gold.select(
    F.round(F.avg("stockout_flag") * 100, 2).alias("stockout_rate_pct"),
    F.round(F.avg("low_stock_flag") * 100, 2).alias("low_stock_rate_pct"),
    F.round(F.avg("overstock_flag") * 100, 2).alias("overstock_rate_pct"),
    F.round(F.avg("slow_moving_flag") * 100, 2).alias("slow_moving_rate_pct")
).show()

print("=== SUPPLIERS ===")

supplier_gold.select(
    F.round(F.avg("on_time_delivery_rate_pct"), 2).alias("avg_supplier_on_time_pct"),
    F.round(F.avg("fill_rate_pct"), 2).alias("avg_supplier_fill_rate_pct"),
    F.round(F.avg("avg_delay_days"), 2).alias("avg_supplier_delay_days")
).show()

print("=== DELIVERIES ===")

delivery_gold.filter(
    F.col("on_time_delivery_flag").isNotNull()
).select(
    F.round(F.avg("on_time_delivery_flag") * 100, 2).alias("on_time_delivery_rate_pct"),
    F.round(F.avg("delivery_delay_days"), 2).alias("avg_delivery_delay_days"),
    F.round(F.avg("shipping_cost_per_km"), 3).alias("avg_shipping_cost_per_km")
).show()