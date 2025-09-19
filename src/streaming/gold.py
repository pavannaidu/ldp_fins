# Gold Layer: Asset Management Customer Analytics Aggregations

import dlt
import sys
sys.path.append(spark.conf.get("bundle.sourcePath", "."))
from pyspark.sql.functions import expr, col, count, avg, sum, countDistinct, round, max, min

# Gold table: customer demographics by age group and location
@dlt.table(
    comment="Customer demographics aggregated by age group and location"
)
def customer_demographics_gold():
    df = dlt.read("customer_kyc_silver")
    return (
        df.groupBy("age_group", "country", "city")
          .agg(
              count("*").alias("customer_count"),
              round(avg("age"), 1).alias("avg_age"),
              round(avg("annual_income"), 2).alias("avg_annual_income"),
              round(avg("net_worth"), 2).alias("avg_net_worth"),
              round(avg("investment_experience_years"), 1).alias("avg_experience_years"),
              countDistinct("risk_profile").alias("risk_profiles_count")
          )
          .orderBy("country", "city", "age_group")
    )

# Gold table: risk profile and investment experience analytics
@dlt.table(
    comment="Risk profile and investment experience analytics"
)
def risk_experience_analytics_gold():
    df = dlt.read("customer_kyc_silver")
    return (
        df.groupBy("risk_profile", "experience_level")
          .agg(
              count("*").alias("customer_count"),
              round(avg("annual_income"), 2).alias("avg_annual_income"),
              round(avg("net_worth"), 2).alias("avg_net_worth"),
              round(avg("investment_experience_years"), 1).alias("avg_experience_years"),
              countDistinct("customer_id").alias("unique_customers"),
              countDistinct("account_type").alias("account_types_count")
          )
          .orderBy("risk_profile", "experience_level")
    )

# Gold table: income and net worth tier analytics
@dlt.table(
    comment="Income and net worth tier analytics"
)
def wealth_analytics_gold():
    df = dlt.read("customer_kyc_silver")
    return (
        df.groupBy("income_tier", "net_worth_tier")
          .agg(
              count("*").alias("customer_count"),
              round(avg("annual_income"), 2).alias("avg_annual_income"),
              round(avg("net_worth"), 2).alias("avg_net_worth"),
              round(avg("investment_experience_years"), 1).alias("avg_experience_years"),
              countDistinct("customer_id").alias("unique_customers"),
              countDistinct("risk_profile").alias("risk_profiles_count")
          )
          .orderBy("income_tier", "net_worth_tier")
    )

# Gold table: KYC status and account type analytics
@dlt.table(
    comment="KYC status and account type analytics"
)
def kyc_account_analytics_gold():
    df = dlt.read("customer_kyc_silver")
    return (
        df.groupBy("kyc_status", "account_type")
          .agg(
              count("*").alias("customer_count"),
              round(avg("annual_income"), 2).alias("avg_annual_income"),
              round(avg("net_worth"), 2).alias("avg_net_worth"),
              round(avg("investment_experience_years"), 1).alias("avg_experience_years"),
              countDistinct("customer_id").alias("unique_customers"),
              countDistinct("employment_status").alias("employment_statuses_count")
          )
          .orderBy("kyc_status", "account_type")
    )

# Gold table: employment status analytics
@dlt.table(
    comment="Employment status analytics"
)
def employment_analytics_gold():
    df = dlt.read("customer_kyc_silver")
    return (
        df.groupBy("employment_status")
          .agg(
              count("*").alias("customer_count"),
              round(avg("annual_income"), 2).alias("avg_annual_income"),
              round(avg("net_worth"), 2).alias("avg_net_worth"),
              round(avg("investment_experience_years"), 1).alias("avg_experience_years"),
              countDistinct("customer_id").alias("unique_customers"),
              countDistinct("risk_profile").alias("risk_profiles_count"),
              countDistinct("account_type").alias("account_types_count")
          )
          .orderBy("employment_status")
    )

# Gold table: real-time processing metrics
@dlt.table(
    comment="Real-time KYC processing metrics by hour and date"
)
def realtime_kyc_metrics_gold():
    df = dlt.read("customer_kyc_silver")
    return (
        df.groupBy("processing_date", "processing_hour")
          .agg(
              count("*").alias("kyc_records_processed"),
              countDistinct("customer_id").alias("unique_customers"),
              countDistinct("country").alias("countries_count"),
              countDistinct("risk_profile").alias("risk_profiles_count"),
              countDistinct("account_type").alias("account_types_count"),
              countDistinct("employment_status").alias("employment_statuses_count"),
              round(avg("annual_income"), 2).alias("avg_annual_income"),
              round(avg("net_worth"), 2).alias("avg_net_worth"),
              round(avg("investment_experience_years"), 1).alias("avg_experience_years"),
              countDistinct("kyc_status").alias("kyc_statuses_count")
          )
          .orderBy("processing_date", "processing_hour")
    )

# Gold table: monthly KYC trends
@dlt.table(
    comment="Monthly KYC processing trends and statistics"
)
def monthly_kyc_trends_gold():
    df = dlt.read("customer_kyc_silver")
    return (
        df.groupBy("processing_year", "processing_month")
          .agg(
              count("*").alias("kyc_records_processed"),
              countDistinct("customer_id").alias("unique_customers"),
              countDistinct("country").alias("countries_count"),
              countDistinct("risk_profile").alias("risk_profiles_count"),
              countDistinct("account_type").alias("account_types_count"),
              countDistinct("employment_status").alias("employment_statuses_count"),
              round(avg("annual_income"), 2).alias("avg_annual_income"),
              round(avg("net_worth"), 2).alias("avg_net_worth"),
              round(avg("investment_experience_years"), 1).alias("avg_experience_years"),
              round(max("annual_income"), 2).alias("max_annual_income"),
              round(min("annual_income"), 2).alias("min_annual_income"),
              round(max("net_worth"), 2).alias("max_net_worth"),
              round(min("net_worth"), 2).alias("min_net_worth")
          )
          .orderBy("processing_year", "processing_month")
    )
