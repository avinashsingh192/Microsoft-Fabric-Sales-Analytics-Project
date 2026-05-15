from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, quarter
from pyspark.sql.types import *

# Initialize Spark Session (Required for local VS Code environments)
# If running in Fabric/Synapse, the 'spark' object is usually pre-defined.
spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

def configure_spark():
    """Apply performance optimizations for Delta Lake."""
    spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
    spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

def transform_fact_sales():
    """Load raw sales data, engineer date features, and save as partitioned Delta table."""
    print("--> Processing Fact Sales...")
    
    # Load raw data
    source_path = 'Files/imported_files/WideWorldImportersDW/parquet/full/fact_sale_1y_full'
    fact_sale_raw_df = spark.read.format("parquet").load(source_path)

    # Feature Engineering: Add Year, Quarter, and Month
    fact_sale_transformed_df = fact_sale_raw_df.withColumn("SaleYear", year(col("InvoiceDateKey"))) \
                                               .withColumn("SaleQuarter", quarter(col("InvoiceDateKey"))) \
                                               .withColumn("SaleMonth", month(col("InvoiceDateKey")))

    # Save to Delta Table with partitioning
    output_table_name = "fact_sale"
    fact_sale_transformed_df.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .partitionBy("SaleYear", "SaleQuarter") \
        .save(f"Tables/{output_table_name}")
    
    print(f"Successfully saved partitioned table: {output_table_name}")

def process_and_load_dimension(table_name: str) -> None:
    """Reads raw parquet dimension, removes 'Photo' column, and saves to Delta."""
    print(f"--> Starting processing for dimension: '{table_name}'")
    
    source_path = f"Files/imported_files/WideWorldImportersDW/parquet/full/{table_name}"
    target_table = f"Tables/{table_name}"
    
    dimension_df = spark.read.format("parquet").load(source_path)
    
    # Remove 'Photo' column if it exists
    cleaned_df = dimension_df.select([c for c in dimension_df.columns if c != 'Photo'])
    
    cleaned_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .format("delta") \
        .save(target_table)
        
    print(f"--- Successfully saved table: '{table_name}'")

def create_business_aggregates():
    """Creates sales aggregates by City/Date (PySpark) and Employee/Date (SQL)."""
    
    # 1. Sales by City and Date (PySpark Approach)
    print("--> Creating City Sales Aggregates...")
    fact_sale_df = spark.read.table("medium1.fact_sale")
    dim_date_df = spark.read.table("medium1.dimension_date")
    dim_city_df = spark.read.table("medium1.dimension_city")

    joined_df = fact_sale_df.alias("sales") \
        .join(dim_date_df.alias("date"), col("sales.InvoiceDateKey") == col("date.Date"), "inner") \
        .join(dim_city_df.alias("city"), col("sales.CityKey") == col("city.CityKey"), "inner")

    city_sales_agg_df = joined_df \
        .groupBy("date.Date", "date.CalendarMonthLabel", "city.City", "city.StateProvince") \
        .sum("sales.TotalIncludingTax", "sales.Profit") \
        .withColumnRenamed("sum(TotalIncludingTax)", "TotalSales") \
        .withColumnRenamed("sum(Profit)", "TotalProfit") \
        .orderBy("date.Date")

    city_sales_agg_df.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .save("Tables/aggregate_sale_by_date_city")

    # 2. Sales by Employee (Spark SQL Approach)
    print("--> Creating Employee Sales Aggregates via Spark SQL...")
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW view_employee_sales_by_date AS
        SELECT 
            DD.Date, DD.CalendarMonthLabel, DE.Employee, DE.PreferredName,
            SUM(FS.TotalIncludingTax) AS TotalSales,
            SUM(FS.Profit) AS TotalProfit
        FROM medium1.fact_sale AS FS
        INNER JOIN medium1.dimension_date AS DD ON FS.InvoiceDateKey = DD.Date
        INNER JOIN medium1.dimension_employee AS DE ON FS.SalespersonKey = DE.EmployeeKey
        GROUP BY DD.Date, DD.CalendarMonthLabel, DE.Employee, DE.PreferredName
    """)

    employee_sales_agg_df = spark.sql("SELECT * FROM view_employee_sales_by_date ORDER BY Date, PreferredName")
    
    employee_sales_agg_df.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .save("Tables/aggregate_sale_by_date_employee")

if __name__ == "__main__":
    # Configure Spark
    configure_spark()

    # Step 1: Transform Fact Table
    transform_fact_sales()

    # Step 2: Process Dimensions
    DIMENSIONS = [
        'dimension_city', 'dimension_customer', 'dimension_date', 
        'dimension_employee', 'dimension_stock_item'
    ]
    for dim in DIMENSIONS:
        process_and_load_dimension(dim)

    # Step 3: Create Aggregates
    create_business_aggregates()

    print("\n[FINISH] All transformations and aggregates completed successfully.")
    