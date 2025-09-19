# Silver Layer: Processed Wealth Asset Management Data

import dlt
import sys
sys.path.append(spark.conf.get("bundle.sourcePath", "."))
from pyspark.sql.functions import expr, col, when, hour, dayofweek, month, year, current_timestamp, lit, date_format, datediff, round, coalesce, upper, lower, trim, sum as spark_sum
from pyspark.sql.window import Window

# Silver table: processed portfolio holdings with quality checks
@dlt.table(
    comment="Processed portfolio holdings data with quality expectations and derived fields",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_market_value", "market_value > 0")
@dlt.expect_or_drop("valid_cost_basis", "cost_basis > 0")
@dlt.expect("non_null_holding_id", "holding_id IS NOT NULL")
@dlt.expect("non_null_client_name", "client_name IS NOT NULL")
@dlt.expect("non_null_asset_name", "asset_name IS NOT NULL")
@dlt.expect("valid_risk_rating", "risk_rating IN (1, 2, 3)")
@dlt.expect("valid_account_id", "account_id >= 1000 AND account_id <= 1999")
@dlt.expect("valid_portfolio_id", "portfolio_id >= 500 AND portfolio_id <= 999")
def portfolio_holdings_silver():
    df = dlt.read("portfolio_holdings_raw")
    
    # Add derived fields for analytics
    processed_df = df \
        .withColumn("client_name_clean", trim(upper(col("client_name")))) \
        .withColumn("asset_name_clean", trim(upper(col("asset_name")))) \
        .withColumn("unrealized_gain_loss", col("market_value") - col("cost_basis")) \
        .withColumn("gain_loss_percentage", 
                   when(col("cost_basis") > 0, 
                        round((col("market_value") - col("cost_basis")) / col("cost_basis") * 100, 2))
                   .otherwise(0)) \
        .withColumn("position_size_percentage", 
                   round(col("market_value") / spark_sum(col("market_value")).over(Window.partitionBy()), 4)) \
        .withColumn("risk_level",
                   when(col("risk_rating") == 1, "Low")
                   .when(col("risk_rating") == 2, "Medium")
                   .otherwise("High")) \
        .withColumn("valuation_date_clean", date_format(col("valuation_date"), "yyyy-MM-dd")) \
        .withColumn("days_held", 
                   when(col("valuation_date").isNotNull(), 
                        datediff(current_timestamp(), col("valuation_date")))
                   .otherwise(0)) \
        .withColumn("processing_date", date_format(col("processing_timestamp"), "yyyy-MM-dd")) \
        .withColumn("processing_hour", hour(col("processing_timestamp"))) \
        .withColumn("processing_day_of_week", dayofweek(col("processing_timestamp"))) \
        .withColumn("processing_month", month(col("processing_timestamp"))) \
        .withColumn("processing_year", year(col("processing_timestamp")))
    
    return processed_df.filter(
        (col("holding_id").isNotNull()) & 
        (col("client_name").isNotNull()) & 
        (col("asset_name").isNotNull()) &
        (col("quantity") > 0) &
        (col("market_value") > 0) &
        (col("cost_basis") > 0)
    )

# Silver table: processed market data with quality checks
@dlt.table(
    comment="Processed market data with quality expectations and derived fields",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_prices", "open_price > 0 AND high_price > 0 AND low_price > 0 AND close_price > 0")
@dlt.expect_or_drop("valid_volume", "volume > 0")
@dlt.expect_or_drop("valid_market_cap", "market_cap > 0")
@dlt.expect_or_drop("valid_pe_ratio", "pe_ratio > 0")
@dlt.expect("non_null_symbol", "symbol IS NOT NULL")
@dlt.expect("non_null_asset_name", "asset_name IS NOT NULL")
@dlt.expect("valid_exchange", "exchange IN ('NYSE', 'NASDAQ', 'LSE', 'TSE')")
@dlt.expect("price_consistency", "high_price >= low_price AND high_price >= open_price AND high_price >= close_price")
def market_data_silver():
    df = dlt.read("market_data_raw")
    
    # Add derived fields for analytics
    processed_df = df \
        .withColumn("symbol_clean", trim(upper(col("symbol")))) \
        .withColumn("asset_name_clean", trim(upper(col("asset_name")))) \
        .withColumn("daily_return", 
                   when(col("open_price") > 0, 
                        round((col("close_price") - col("open_price")) / col("open_price") * 100, 4))
                   .otherwise(0)) \
        .withColumn("price_range", col("high_price") - col("low_price")) \
        .withColumn("price_range_percentage", 
                   when(col("low_price") > 0, 
                        round((col("high_price") - col("low_price")) / col("low_price") * 100, 2))
                   .otherwise(0)) \
        .withColumn("volume_weighted_price", 
                   round((col("high_price") + col("low_price") + col("close_price")) / 3, 2)) \
        .withColumn("market_cap_tier",
                   when(col("market_cap") < 1000000000, "Small Cap")
                   .when(col("market_cap") < 10000000000, "Mid Cap")
                   .when(col("market_cap") < 100000000000, "Large Cap")
                   .otherwise("Mega Cap")) \
        .withColumn("pe_ratio_tier",
                   when(col("pe_ratio") < 10, "Low PE")
                   .when(col("pe_ratio") < 20, "Medium PE")
                   .when(col("pe_ratio") < 30, "High PE")
                   .otherwise("Very High PE")) \
        .withColumn("dividend_yield_tier",
                   when(col("dividend_yield") < 1, "Low Yield")
                   .when(col("dividend_yield") < 3, "Medium Yield")
                   .when(col("dividend_yield") < 5, "High Yield")
                   .otherwise("Very High Yield")) \
        .withColumn("trade_date_clean", date_format(col("trade_date"), "yyyy-MM-dd")) \
        .withColumn("processing_date", date_format(col("processing_timestamp"), "yyyy-MM-dd")) \
        .withColumn("processing_hour", hour(col("processing_timestamp"))) \
        .withColumn("processing_day_of_week", dayofweek(col("processing_timestamp"))) \
        .withColumn("processing_month", month(col("processing_timestamp"))) \
        .withColumn("processing_year", year(col("processing_timestamp")))
    
    return processed_df.filter(
        (col("symbol").isNotNull()) & 
        (col("asset_name").isNotNull()) &
        (col("open_price") > 0) &
        (col("high_price") > 0) &
        (col("low_price") > 0) &
        (col("close_price") > 0) &
        (col("volume") > 0) &
        (col("market_cap") > 0) &
        (col("pe_ratio") > 0)
    )

# Silver table: processed client transactions with quality checks
@dlt.table(
    comment="Processed client transaction data with quality expectations and derived fields",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("valid_price_per_share", "price_per_share > 0")
@dlt.expect_or_drop("valid_total_amount", "total_amount > 0")
@dlt.expect_or_drop("valid_fees", "fees >= 0")
@dlt.expect("non_null_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect("non_null_client_name", "client_name IS NOT NULL")
@dlt.expect("non_null_asset_symbol", "asset_symbol IS NOT NULL")
@dlt.expect("valid_transaction_type", "transaction_type IN ('BUY', 'SELL', 'DIVIDEND', 'REINVEST')")
@dlt.expect("valid_account_id", "account_id >= 1000 AND account_id <= 1999")
@dlt.expect("valid_portfolio_id", "portfolio_id >= 500 AND portfolio_id <= 999")
def client_transactions_silver():
    df = dlt.read("client_transactions_raw")
    
    # Add derived fields for analytics
    processed_df = df \
        .withColumn("client_name_clean", trim(upper(col("client_name")))) \
        .withColumn("asset_symbol_clean", trim(upper(col("asset_symbol")))) \
        .withColumn("advisor_name_clean", trim(upper(col("advisor_name")))) \
        .withColumn("transaction_amount_after_fees", col("total_amount") - col("fees")) \
        .withColumn("fee_percentage", 
                   when(col("total_amount") > 0, 
                        round(col("fees") / col("total_amount") * 100, 4))
                   .otherwise(0)) \
        .withColumn("transaction_category",
                   when(col("transaction_type") == "BUY", "Purchase")
                   .when(col("transaction_type") == "SELL", "Sale")
                   .when(col("transaction_type") == "DIVIDEND", "Income")
                   .otherwise("Reinvestment")) \
        .withColumn("transaction_size_tier",
                   when(col("total_amount") < 1000, "Small")
                   .when(col("total_amount") < 10000, "Medium")
                   .when(col("total_amount") < 100000, "Large")
                   .otherwise("Very Large")) \
        .withColumn("transaction_date_clean", date_format(col("transaction_date"), "yyyy-MM-dd")) \
        .withColumn("transaction_month", month(col("transaction_date"))) \
        .withColumn("transaction_year", year(col("transaction_date"))) \
        .withColumn("transaction_day_of_week", dayofweek(col("transaction_date"))) \
        .withColumn("processing_date", date_format(col("processing_timestamp"), "yyyy-MM-dd")) \
        .withColumn("processing_hour", hour(col("processing_timestamp"))) \
        .withColumn("processing_day_of_week", dayofweek(col("processing_timestamp"))) \
        .withColumn("processing_month", month(col("processing_timestamp"))) \
        .withColumn("processing_year", year(col("processing_timestamp")))
    
    return processed_df.filter(
        (col("transaction_id").isNotNull()) & 
        (col("client_name").isNotNull()) & 
        (col("asset_symbol").isNotNull()) &
        (col("quantity") > 0) &
        (col("price_per_share") > 0) &
        (col("total_amount") > 0) &
        (col("fees") >= 0)
    )

# Silver table: processed client profiles with quality checks
@dlt.table(
    comment="Processed client profile data with quality expectations and derived fields",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_age", "age >= 18 AND age <= 100")
@dlt.expect_or_drop("valid_annual_income", "annual_income > 0")
@dlt.expect_or_drop("valid_net_worth", "net_worth > 0")
@dlt.expect_or_drop("valid_portfolio_value", "total_portfolio_value > 0")
@dlt.expect_or_drop("valid_experience", "investment_experience_years >= 0")
@dlt.expect("non_null_client_id", "client_id IS NOT NULL")
@dlt.expect("non_null_client_name", "client_name IS NOT NULL")
@dlt.expect("non_null_email", "email_address IS NOT NULL")
@dlt.expect("valid_account_type", "account_type_code IN (1, 2, 3)")
@dlt.expect("valid_risk_tolerance", "risk_tolerance IN ('Conservative', 'Moderate', 'Aggressive', 'Balanced')")
def client_profiles_silver():
    df = dlt.read("client_profiles_raw")
    
    # Add derived fields for analytics
    processed_df = df \
        .withColumn("client_name_clean", trim(upper(col("client_name")))) \
        .withColumn("email_address_clean", trim(lower(col("email_address")))) \
        .withColumn("advisor_name_clean", trim(upper(col("advisor_name")))) \
        .withColumn("age_group", 
                   when(col("age") < 30, "18-29")
                   .when(col("age") < 40, "30-39")
                   .when(col("age") < 50, "40-49")
                   .when(col("age") < 60, "50-59")
                   .when(col("age") < 70, "60-69")
                   .otherwise("70+")) \
        .withColumn("income_tier",
                   when(col("annual_income") < 50000, "Low Income")
                   .when(col("annual_income") < 100000, "Medium Income")
                   .when(col("annual_income") < 200000, "High Income")
                   .when(col("annual_income") < 500000, "Very High Income")
                   .otherwise("Ultra High Income")) \
        .withColumn("net_worth_tier",
                   when(col("net_worth") < 100000, "Low Net Worth")
                   .when(col("net_worth") < 500000, "Medium Net Worth")
                   .when(col("net_worth") < 1000000, "High Net Worth")
                   .when(col("net_worth") < 5000000, "Very High Net Worth")
                   .otherwise("Ultra High Net Worth")) \
        .withColumn("portfolio_value_tier",
                   when(col("total_portfolio_value") < 100000, "Small Portfolio")
                   .when(col("total_portfolio_value") < 500000, "Medium Portfolio")
                   .when(col("total_portfolio_value") < 1000000, "Large Portfolio")
                   .when(col("total_portfolio_value") < 5000000, "Very Large Portfolio")
                   .otherwise("Ultra Large Portfolio")) \
        .withColumn("experience_level",
                   when(col("investment_experience_years") < 2, "Beginner")
                   .when(col("investment_experience_years") < 5, "Intermediate")
                   .when(col("investment_experience_years") < 10, "Advanced")
                   .when(col("investment_experience_years") < 20, "Expert")
                   .otherwise("Veteran")) \
        .withColumn("account_type",
                   when(col("account_type_code") == 1, "Individual")
                   .when(col("account_type_code") == 2, "Joint")
                   .otherwise("Corporate")) \
        .withColumn("account_age_days", 
                   when(col("account_open_date").isNotNull(), 
                        datediff(current_timestamp(), col("account_open_date")))
                   .otherwise(0)) \
        .withColumn("account_age_years", 
                   round(col("account_age_days") / 365.25, 1)) \
        .withColumn("account_open_date_clean", date_format(col("account_open_date"), "yyyy-MM-dd")) \
        .withColumn("processing_date", date_format(col("processing_timestamp"), "yyyy-MM-dd")) \
        .withColumn("processing_hour", hour(col("processing_timestamp"))) \
        .withColumn("processing_day_of_week", dayofweek(col("processing_timestamp"))) \
        .withColumn("processing_month", month(col("processing_timestamp"))) \
        .withColumn("processing_year", year(col("processing_timestamp")))
    
    return processed_df.filter(
        (col("client_id").isNotNull()) & 
        (col("client_name").isNotNull()) & 
        (col("email_address").isNotNull()) &
        (col("age") >= 18) &
        (col("age") <= 100) &
        (col("annual_income") > 0) &
        (col("net_worth") > 0) &
        (col("total_portfolio_value") > 0) &
        (col("investment_experience_years") >= 0)
    )
