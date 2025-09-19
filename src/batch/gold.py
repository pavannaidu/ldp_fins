# Gold Layer: Wealth Asset Management Analytics Aggregations

import dlt
import sys
sys.path.append(spark.conf.get("bundle.sourcePath", "."))
from pyspark.sql.functions import expr, col, count, avg, sum, countDistinct, round, max, min, stddev, variance, first, last, lit

# Gold table: client portfolio performance analytics
@dlt.table(
    comment="Client portfolio performance analytics aggregated by client demographics"
)
def client_portfolio_performance_gold():
    holdings_df = dlt.read("portfolio_holdings_silver")
    profiles_df = dlt.read("client_profiles_silver")
    
    # Join holdings with client profiles
    joined_df = holdings_df.alias("h").join(profiles_df.alias("p"), 
                               col("h.client_name_clean") == col("p.client_name_clean"), 
                               "inner")
    
    return (
        joined_df.groupBy(col("p.age_group"), col("p.income_tier"), col("p.net_worth_tier"), col("p.risk_tolerance"), col("p.experience_level"))
          .agg(
              count("*").alias("total_positions"),
              countDistinct(col("p.client_id")).alias("unique_clients"),
              countDistinct(col("h.portfolio_id")).alias("unique_portfolios"),
              round(avg(col("h.market_value")), 2).alias("avg_position_value"),
              round(sum(col("h.market_value")), 2).alias("total_portfolio_value"),
              round(avg(col("h.unrealized_gain_loss")), 2).alias("avg_unrealized_gain_loss"),
              round(avg(col("h.gain_loss_percentage")), 2).alias("avg_gain_loss_percentage"),
              round(max(col("h.market_value")), 2).alias("max_position_value"),
              round(min(col("h.market_value")), 2).alias("min_position_value"),
              round(stddev(col("h.market_value")), 2).alias("position_value_stddev"),
              countDistinct(col("h.asset_type")).alias("asset_types_count"),
              countDistinct(col("h.sector")).alias("sectors_count"),
              countDistinct(col("h.region")).alias("regions_count")
          )
          .orderBy("age_group", "income_tier", "net_worth_tier", "risk_tolerance")
    )

# Gold table: asset allocation analytics
@dlt.table(
    comment="Asset allocation analytics by asset type, sector, and region"
)
def asset_allocation_analytics_gold():
    holdings_df = dlt.read("portfolio_holdings_silver")
    
    return (
        holdings_df.groupBy("asset_type", "sector", "region", "risk_level")
          .agg(
              count("*").alias("position_count"),
              countDistinct("client_name_clean").alias("unique_clients"),
              countDistinct("portfolio_id").alias("unique_portfolios"),
              round(sum("market_value"), 2).alias("total_market_value"),
              round(avg("market_value"), 2).alias("avg_position_value"),
              round(sum("cost_basis"), 2).alias("total_cost_basis"),
              round(sum("unrealized_gain_loss"), 2).alias("total_unrealized_gain_loss"),
              round(avg("gain_loss_percentage"), 2).alias("avg_gain_loss_percentage"),
              round(max("market_value"), 2).alias("max_position_value"),
              round(min("market_value"), 2).alias("min_position_value"),
              round(stddev("market_value"), 2).alias("position_value_stddev"),
              round(sum("quantity"), 2).alias("total_quantity"),
              round(avg("days_held"), 1).alias("avg_days_held")
          )
          .orderBy("asset_type", "sector", "region", "risk_level")
    )

# Gold table: market data performance analytics
@dlt.table(
    comment="Market data performance analytics by sector, region, and market cap"
)
def market_performance_analytics_gold():
    market_df = dlt.read("market_data_silver")
    
    return (
        market_df.groupBy("sector", "region", "market_cap_tier", "pe_ratio_tier", "dividend_yield_tier")
          .agg(
              count("*").alias("trading_days_count"),
              countDistinct("symbol_clean").alias("unique_symbols"),
              round(avg("daily_return"), 4).alias("avg_daily_return"),
              round(max("daily_return"), 4).alias("max_daily_return"),
              round(min("daily_return"), 4).alias("min_daily_return"),
              round(stddev("daily_return"), 4).alias("daily_return_stddev"),
              round(avg("price_range_percentage"), 2).alias("avg_price_range_percentage"),
              round(avg("volume"), 0).alias("avg_volume"),
              round(sum("volume"), 0).alias("total_volume"),
              round(avg("market_cap"), 0).alias("avg_market_cap"),
              round(avg("pe_ratio"), 2).alias("avg_pe_ratio"),
              round(avg("dividend_yield"), 2).alias("avg_dividend_yield"),
              round(max("close_price"), 2).alias("max_close_price"),
              round(min("close_price"), 2).alias("min_close_price")
          )
          .orderBy("sector", "region", "market_cap_tier")
    )

# Gold table: client transaction analytics
@dlt.table(
    comment="Client transaction analytics by transaction type, size, and demographics"
)
def client_transaction_analytics_gold():
    transactions_df = dlt.read("client_transactions_silver")
    profiles_df = dlt.read("client_profiles_silver")
    
    # Join transactions with client profiles
    joined_df = transactions_df.alias("t").join(profiles_df.alias("p"), 
                                   col("t.client_name_clean") == col("p.client_name_clean"), 
                                   "inner")
    
    return (
        joined_df.groupBy(col("t.transaction_type"), col("t.transaction_category"), col("t.transaction_size_tier"), 
                         col("p.age_group"), col("p.income_tier"), col("p.risk_tolerance"))
          .agg(
              count("*").alias("transaction_count"),
              countDistinct(col("p.client_id")).alias("unique_clients"),
              countDistinct(col("t.portfolio_id")).alias("unique_portfolios"),
              round(sum(col("t.total_amount")), 2).alias("total_transaction_amount"),
              round(avg(col("t.total_amount")), 2).alias("avg_transaction_amount"),
              round(sum(col("t.fees")), 2).alias("total_fees"),
              round(avg(col("t.fees")), 2).alias("avg_fees"),
              round(avg(col("t.fee_percentage")), 4).alias("avg_fee_percentage"),
              round(sum(col("t.transaction_amount_after_fees")), 2).alias("total_amount_after_fees"),
              round(max(col("t.total_amount")), 2).alias("max_transaction_amount"),
              round(min(col("t.total_amount")), 2).alias("min_transaction_amount"),
              round(stddev(col("t.total_amount")), 2).alias("transaction_amount_stddev"),
              countDistinct(col("t.asset_symbol_clean")).alias("unique_assets"),
              countDistinct(col("t.advisor_name_clean")).alias("unique_advisors"),
              countDistinct(col("t.sector")).alias("sectors_count")
          )
          .orderBy("transaction_type", "transaction_category", "transaction_size_tier")
    )

# Gold table: advisor performance analytics
@dlt.table(
    comment="Advisor performance analytics by advisor and client demographics"
)
def advisor_performance_analytics_gold():
    transactions_df = dlt.read("client_transactions_silver")
    profiles_df = dlt.read("client_profiles_silver")
    
    # Join transactions with client profiles
    joined_df = transactions_df.alias("t").join(profiles_df.alias("p"), 
                                   col("t.client_name_clean") == col("p.client_name_clean"), 
                                   "inner")
    
    return (
        joined_df.groupBy(col("t.advisor_name_clean"), col("p.age_group"), col("p.income_tier"), col("p.net_worth_tier"))
          .agg(
              count("*").alias("total_transactions"),
              countDistinct(col("p.client_id")).alias("unique_clients"),
              countDistinct(col("t.portfolio_id")).alias("unique_portfolios"),
              round(sum(col("t.total_amount")), 2).alias("total_transaction_volume"),
              round(avg(col("t.total_amount")), 2).alias("avg_transaction_amount"),
              round(sum(col("t.fees")), 2).alias("total_fees_generated"),
              round(avg(col("t.fees")), 2).alias("avg_fees_per_transaction"),
              round(avg(col("t.fee_percentage")), 4).alias("avg_fee_percentage"),
              countDistinct(col("t.asset_symbol_clean")).alias("unique_assets_traded"),
              countDistinct(col("t.transaction_type")).alias("transaction_types_count"),
              countDistinct(col("t.sector")).alias("sectors_traded_count"),
              round(max(col("t.total_amount")), 2).alias("max_transaction_amount"),
              round(min(col("t.total_amount")), 2).alias("min_transaction_amount")
          )
          .orderBy("advisor_name_clean", "age_group", "income_tier")
    )

# Gold table: risk analytics by client segments
@dlt.table(
    comment="Risk analytics by client segments and portfolio characteristics"
)
def risk_analytics_gold():
    holdings_df = dlt.read("portfolio_holdings_silver")
    profiles_df = dlt.read("client_profiles_silver")
    
    # Join holdings with client profiles
    joined_df = holdings_df.alias("h").join(profiles_df.alias("p"), 
                               col("h.client_name_clean") == col("p.client_name_clean"), 
                               "inner")
    
    return (
        joined_df.groupBy(col("p.risk_tolerance"), col("h.risk_level"), col("p.age_group"), col("p.experience_level"))
          .agg(
              count("*").alias("position_count"),
              countDistinct(col("p.client_id")).alias("unique_clients"),
              countDistinct(col("h.portfolio_id")).alias("unique_portfolios"),
              round(sum(col("h.market_value")), 2).alias("total_portfolio_value"),
              round(avg(col("h.market_value")), 2).alias("avg_position_value"),
              round(sum(col("h.unrealized_gain_loss")), 2).alias("total_unrealized_gain_loss"),
              round(avg(col("h.gain_loss_percentage")), 2).alias("avg_gain_loss_percentage"),
              round(max(col("h.gain_loss_percentage")), 2).alias("max_gain_percentage"),
              round(min(col("h.gain_loss_percentage")), 2).alias("min_gain_percentage"),
              round(stddev(col("h.gain_loss_percentage")), 2).alias("gain_loss_volatility"),
              countDistinct(col("h.asset_type")).alias("asset_types_count"),
              countDistinct(col("h.sector")).alias("sectors_count"),
              countDistinct(col("h.region")).alias("regions_count"),
              round(avg(col("h.days_held")), 1).alias("avg_days_held")
          )
          .orderBy("risk_tolerance", "risk_level", "age_group")
    )

# Gold table: monthly portfolio performance trends
@dlt.table(
    comment="Monthly portfolio performance trends and statistics"
)
def monthly_portfolio_trends_gold():
    holdings_df = dlt.read("portfolio_holdings_silver")
    
    return (
        holdings_df.groupBy("processing_year", "processing_month", "asset_type", "sector")
          .agg(
              count("*").alias("position_count"),
              countDistinct("client_name_clean").alias("unique_clients"),
              countDistinct("portfolio_id").alias("unique_portfolios"),
              round(sum("market_value"), 2).alias("total_market_value"),
              round(avg("market_value"), 2).alias("avg_position_value"),
              round(sum("unrealized_gain_loss"), 2).alias("total_unrealized_gain_loss"),
              round(avg("gain_loss_percentage"), 2).alias("avg_gain_loss_percentage"),
              round(max("market_value"), 2).alias("max_position_value"),
              round(min("market_value"), 2).alias("min_position_value"),
              round(stddev("market_value"), 2).alias("position_value_stddev"),
              round(sum("quantity"), 2).alias("total_quantity"),
              round(avg("days_held"), 1).alias("avg_days_held")
          )
          .orderBy("processing_year", "processing_month", "asset_type", "sector")
    )

# Gold table: client acquisition and retention analytics
@dlt.table(
    comment="Client acquisition and retention analytics by demographics"
)
def client_acquisition_retention_gold():
    profiles_df = dlt.read("client_profiles_silver")
    
    return (
        profiles_df.groupBy("age_group", "income_tier", "net_worth_tier", "employment_status", "experience_level")
          .agg(
              count("*").alias("total_clients"),
              countDistinct("advisor_name_clean").alias("unique_advisors"),
              round(avg("total_portfolio_value"), 2).alias("avg_portfolio_value"),
              round(sum("total_portfolio_value"), 2).alias("total_portfolio_value"),
              round(avg("annual_income"), 2).alias("avg_annual_income"),
              round(sum("annual_income"), 2).alias("total_annual_income"),
              round(avg("net_worth"), 2).alias("avg_net_worth"),
              round(sum("net_worth"), 2).alias("total_net_worth"),
              round(avg("investment_experience_years"), 1).alias("avg_experience_years"),
              round(avg("account_age_years"), 1).alias("avg_account_age_years"),
              round(max("account_age_years"), 1).alias("max_account_age_years"),
              round(min("account_age_years"), 1).alias("min_account_age_years"),
              countDistinct("account_type").alias("account_types_count"),
              countDistinct("risk_tolerance").alias("risk_tolerances_count")
          )
          .orderBy("age_group", "income_tier", "net_worth_tier")
    )

# Gold table: sector and region performance comparison
@dlt.table(
    comment="Sector and region performance comparison analytics"
)
def sector_region_performance_gold():
    holdings_df = dlt.read("portfolio_holdings_silver")
    market_df = dlt.read("market_data_silver")
    
    # Join holdings with market data
    joined_df = holdings_df.alias("h").join(market_df.alias("m"), 
                               (col("h.asset_name_clean") == col("m.asset_name_clean")) &
                               (col("h.sector") == col("m.sector")) &
                               (col("h.region") == col("m.region")), 
                               "inner")
    
    return (
        joined_df.groupBy(col("h.sector"), col("h.region"), col("m.market_cap_tier"))
          .agg(
              count("*").alias("position_count"),
              countDistinct(col("h.client_name_clean")).alias("unique_clients"),
              countDistinct(col("h.portfolio_id")).alias("unique_portfolios"),
              round(sum(col("h.market_value")), 2).alias("total_market_value"),
              round(avg(col("h.market_value")), 2).alias("avg_position_value"),
              round(sum(col("h.unrealized_gain_loss")), 2).alias("total_unrealized_gain_loss"),
              round(avg(col("h.gain_loss_percentage")), 2).alias("avg_gain_loss_percentage"),
              round(avg(col("m.daily_return")), 4).alias("avg_daily_return"),
              round(stddev(col("m.daily_return")), 4).alias("daily_return_volatility"),
              round(avg(col("m.pe_ratio")), 2).alias("avg_pe_ratio"),
              round(avg(col("m.dividend_yield")), 2).alias("avg_dividend_yield"),
              round(avg(col("m.market_cap")), 0).alias("avg_market_cap"),
              round(avg(col("m.volume")), 0).alias("avg_volume"),
              round(avg(col("h.days_held")), 1).alias("avg_days_held")
          )
          .orderBy("sector", "region", "market_cap_tier")
    )

# Gold table: processing metrics and data quality
@dlt.table(
    comment="Processing metrics and data quality statistics"
)
def processing_metrics_gold():
    # Combine all silver tables for processing metrics
    holdings_df = dlt.read("portfolio_holdings_silver")
    market_df = dlt.read("market_data_silver")
    transactions_df = dlt.read("client_transactions_silver")
    profiles_df = dlt.read("client_profiles_silver")
    
    # Add source table identifier
    holdings_with_source = holdings_df.withColumn("source_table", lit("portfolio_holdings"))
    market_with_source = market_df.withColumn("source_table", lit("market_data"))
    transactions_with_source = transactions_df.withColumn("source_table", lit("client_transactions"))
    profiles_with_source = profiles_df.withColumn("source_table", lit("client_profiles"))
    
    # Union all tables
    all_data = holdings_with_source.unionByName(market_with_source, allowMissingColumns=True) \
                                  .unionByName(transactions_with_source, allowMissingColumns=True) \
                                  .unionByName(profiles_with_source, allowMissingColumns=True)
    
    return (
        all_data.groupBy("source_table", "processing_date", "processing_hour")
          .agg(
              count("*").alias("records_processed"),
              countDistinct("processing_timestamp").alias("processing_batches"),
              max("processing_timestamp").alias("latest_processing_timestamp"),
              min("processing_timestamp").alias("earliest_processing_timestamp")
          )
          .orderBy("source_table", "processing_date", "processing_hour")
    )
