from pyspark.sql.functions import *

def apply_transformations(df):
    """Apply business logic transformations"""
    
    # Add derived columns
    df = df.withColumn("hour", hour(col("timestamp")))
    df = df.withColumn("day_of_week", dayofweek(col("timestamp")))
    
    # Filter out invalid events
    df = df.filter(col("value") > 0)
    
    # Add event category
    df = df.withColumn(
        "event_category",
        when(col("event_type") == "purchase", "conversion")
        .when(col("event_type").isin(["add_to_cart", "page_view"]), "engagement")
        .otherwise("other")
    )
    
    return df
