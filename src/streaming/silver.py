# Silver Layer: Processed Asset Management Customer KYC Data

import dlt
import sys
sys.path.append(spark.conf.get("bundle.sourcePath", "."))
from pyspark.sql.functions import expr, col, when, hour, dayofweek, month, year, current_timestamp, lit

# Silver table: processed customer KYC data with quality checks
@dlt.table(
    comment="Processed customer KYC data with quality expectations and derived fields",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_age", "age >= 18 AND age <= 100")
@dlt.expect_or_drop("valid_income", "annual_income > 0")
@dlt.expect_or_drop("valid_net_worth", "net_worth > 0")
@dlt.expect_or_drop("valid_experience", "investment_experience_years >= 0")
@dlt.expect("non_null_customer_id", "customer_id IS NOT NULL")
@dlt.expect("non_null_email", "email_address IS NOT NULL")
@dlt.expect("non_null_name", "full_name IS NOT NULL")
@dlt.expect("valid_kyc_status", "kyc_status_code IN (1, 2, 3)")
def customer_kyc_silver():
    df = dlt.readStream("customer_kyc_raw")
    
    # Add derived fields for analytics
    processed_df = df \
        .withColumn("age_group", 
                   when(col("age") < 25, "18-24")
                   .when(col("age") < 35, "25-34")
                   .when(col("age") < 45, "35-44")
                   .when(col("age") < 55, "45-54")
                   .when(col("age") < 65, "55-64")
                   .otherwise("65+")) \
        .withColumn("income_tier",
                   when(col("annual_income") < 50000, "Low")
                   .when(col("annual_income") < 100000, "Medium")
                   .when(col("annual_income") < 200000, "High")
                   .otherwise("Very High")) \
        .withColumn("net_worth_tier",
                   when(col("net_worth") < 100000, "Low")
                   .when(col("net_worth") < 500000, "Medium")
                   .when(col("net_worth") < 1000000, "High")
                   .otherwise("Very High")) \
        .withColumn("experience_level",
                   when(col("investment_experience_years") < 2, "Beginner")
                   .when(col("investment_experience_years") < 5, "Intermediate")
                   .when(col("investment_experience_years") < 10, "Advanced")
                   .otherwise("Expert")) \
        .withColumn("kyc_status",
                   when(col("kyc_status_code") == 1, "Pending")
                   .when(col("kyc_status_code") == 2, "Approved")
                   .otherwise("Rejected")) \
        .withColumn("processing_date", expr("date(processing_timestamp)")) \
        .withColumn("processing_hour", hour(col("processing_timestamp"))) \
        .withColumn("processing_day_of_week", dayofweek(col("processing_timestamp"))) \
        .withColumn("processing_month", month(col("processing_timestamp"))) \
        .withColumn("processing_year", year(col("processing_timestamp")))
    
    return processed_df.filter(
        col("customer_id").isNotNull() & 
        col("email_address").isNotNull() & 
        col("full_name").isNotNull()
    )
