# Bronze Layer: Wealth Asset Management Batch Data Pipeline

import dlt
import sys
sys.path.append(spark.conf.get("bundle.sourcePath", "."))
from pyspark.sql.functions import expr, col, to_timestamp, current_timestamp, when, abs, lit, rand, monotonically_increasing_id, date_format, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, DateType
from typing import List
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    InputPartition,
)

# Embedded FakeDataSource implementation for batch processing
def _validate_faker_schema(schema):
    try:
        from faker import Faker
    except ImportError:
        raise Exception("You need to install `faker` to use the fake datasource.")

    fake = Faker()
    for field in schema.fields:
        try:
            getattr(fake, field.name)()
        except AttributeError:
            raise Exception(
                f"Unable to find a method called `{field.name}` in faker. "
                f"Please check Faker's documentation to see supported methods."
            )
        if field.dataType != StringType():
            raise Exception(
                f"Field `{field.name}` is not a StringType. "
                f"Only StringType is supported in the fake datasource."
            )


class FakeDataSource(DataSource):
    """
    A fake data source for PySpark to generate synthetic data using the `faker` library.
    """

    @classmethod
    def name(cls):
        return "fake"

    def schema(self):
        return "name string, date string, zipcode string, state string"

    def reader(self, schema: StructType) -> "FakeDataSourceReader":
        _validate_faker_schema(schema)
        return FakeDataSourceReader(schema, self.options)

    def streamReader(self, schema) -> "FakeDataSourceStreamReader":
        _validate_faker_schema(schema)
        return FakeDataSourceStreamReader(schema, self.options)


class FakeDataSourceReader(DataSourceReader):
    def __init__(self, schema, options) -> None:
        self.schema: StructType = schema
        self.options = options

    def read(self, partition):
        from faker import Faker

        fake = Faker()
        num_rows = int(self.options.get("numRows", 100))
        for _ in range(num_rows):
            row = []
            for field in self.schema.fields:
                value = getattr(fake, field.name)()
                row.append(value)
            yield tuple(row)


class FakeDataSourceStreamReader(DataSourceStreamReader):
    def __init__(self, schema, options) -> None:
        self.schema: StructType = schema
        self.rows_per_microbatch = int(options.get("numRows", 100))
        self.options = options
        self.offset = 0

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def latestOffset(self) -> dict:
        self.offset += self.rows_per_microbatch
        return {"offset": self.offset}

    def partitions(self, start, end) -> List[InputPartition]:
        return [InputPartition(end["offset"] - start["offset"])]

    def read(self, partition):
        from faker import Faker

        fake = Faker()
        for _ in range(partition.value):
            row = []
            for field in self.schema.fields:
                value = getattr(fake, field.name)()
                row.append(value)
            yield tuple(row)

# Register the embedded FakeDataSource
try:
    spark.dataSource.register(FakeDataSource)
    print("Successfully registered embedded FakeDataSource")
except Exception as e:
    print(f"Warning: Could not register FakeDataSource: {e}")

# Define schema for portfolio holdings data
portfolio_holdings_schema = StructType([
    StructField("uuid4", StringType(), True),      # Will be mapped to holding_id
    StructField("name", StringType(), True),       # Will be mapped to client_name
    StructField("company", StringType(), True),    # Will be mapped to asset_name
    StructField("pyfloat", StringType(), True),    # Will be converted to quantity
    StructField("pydecimal", StringType(), True),  # Will be converted to market_value
    StructField("date_time", StringType(), True),  # Will be converted to valuation_date
    StructField("word", StringType(), True),       # Will be mapped to asset_type
    StructField("random_int", StringType(), True), # Will be converted to account_id
    StructField("random_number", StringType(), True), # Will be converted to portfolio_id
    StructField("city", StringType(), True),       # Will be mapped to sector
    StructField("country", StringType(), True),    # Will be mapped to region
    StructField("random_digit", StringType(), True), # Will be converted to risk_rating
    StructField("email", StringType(), True),      # Will be mapped to currency
    StructField("job", StringType(), True),        # Will be mapped to investment_strategy
    StructField("latitude", StringType(), True)    # Will be converted to cost_basis
])

# Define schema for market data
market_data_schema = StructType([
    StructField("company", StringType(), True),    # Will be mapped to symbol
    StructField("name", StringType(), True),      # Will be mapped to asset_name
    StructField("pyfloat", StringType(), True),   # Will be converted to open_price
    StructField("pydecimal", StringType(), True), # Will be converted to high_price
    StructField("latitude", StringType(), True),  # Will be converted to low_price
    StructField("longitude", StringType(), True), # Will be converted to close_price
    StructField("random_int", StringType(), True), # Will be converted to volume
    StructField("date_time", StringType(), True), # Will be converted to trade_date
    StructField("word", StringType(), True),       # Will be mapped to exchange
    StructField("city", StringType(), True),       # Will be mapped to sector
    StructField("country", StringType(), True),    # Will be mapped to region
    StructField("random_number", StringType(), True), # Will be converted to market_cap
    StructField("random_digit", StringType(), True), # Will be converted to pe_ratio
    StructField("building_number", StringType(), True), # Will be converted to dividend_yield
    StructField("email", StringType(), True)       # Will be mapped to currency
])

# Define schema for client transactions
client_transactions_schema = StructType([
    StructField("uuid4", StringType(), True),      # Will be mapped to transaction_id
    StructField("name", StringType(), True),      # Will be mapped to client_name
    StructField("random_int", StringType(), True), # Will be converted to account_id
    StructField("company", StringType(), True),    # Will be mapped to asset_symbol
    StructField("word", StringType(), True),       # Will be mapped to transaction_type
    StructField("pyfloat", StringType(), True),    # Will be converted to quantity
    StructField("pydecimal", StringType(), True), # Will be converted to price_per_share
    StructField("latitude", StringType(), True),   # Will be converted to total_amount
    StructField("date_time", StringType(), True), # Will be converted to transaction_date
    StructField("random_number", StringType(), True), # Will be converted to portfolio_id
    StructField("city", StringType(), True),       # Will be mapped to sector
    StructField("country", StringType(), True),    # Will be mapped to region
    StructField("random_digit", StringType(), True), # Will be converted to fees
    StructField("email", StringType(), True),     # Will be mapped to currency
    StructField("job", StringType(), True)        # Will be mapped to advisor_name
])

# Define schema for client profiles
client_profiles_schema = StructType([
    StructField("uuid4", StringType(), True),      # Will be mapped to client_id
    StructField("name", StringType(), True),       # Will be mapped to client_name
    StructField("email", StringType(), True),      # Will be mapped to email_address
    StructField("random_int", StringType(), True), # Will be converted to age
    StructField("city", StringType(), True),       # Will be mapped to city
    StructField("country", StringType(), True),    # Will be mapped to country
    StructField("pyfloat", StringType(), True),    # Will be converted to annual_income
    StructField("pydecimal", StringType(), True), # Will be converted to net_worth
    StructField("word", StringType(), True),       # Will be mapped to risk_tolerance
    StructField("date_time", StringType(), True),  # Will be converted to account_open_date
    StructField("random_number", StringType(), True), # Will be converted to investment_experience_years
    StructField("job", StringType(), True),        # Will be mapped to employment_status
    StructField("random_digit", StringType(), True), # Will be converted to account_type_code
    StructField("company", StringType(), True),    # Will be mapped to advisor_name
    StructField("latitude", StringType(), True)    # Will be converted to total_portfolio_value
])

# Portfolio Holdings Bronze Table
@dlt.table(
    comment="Raw portfolio holdings data from wealth management system"
)
def portfolio_holdings_raw():
    df_faker = spark.read \
        .format("fake") \
        .schema(portfolio_holdings_schema) \
        .option("numRows", 500) \
        .load()
    
    df = df_faker \
        .withColumn("holding_id", 
                   when(expr("rand() < 0.02"), lit(None))  # 2% null holding IDs
                   .otherwise(col("uuid4"))) \
        .withColumn("client_name", 
                   when(expr("rand() < 0.01"), lit(None))  # 1% null client names
                   .otherwise(col("name"))) \
        .withColumn("asset_name", col("company")) \
        .withColumn("quantity", 
                   when(expr("rand() < 0.05"), expr("cast(rand() * 100 - 50 as double)"))  # 5% negative quantities
                   .otherwise(abs(col("pyfloat").cast("double")) * 1000 + 10)) \
        .withColumn("market_value", 
                   when(expr("rand() < 0.03"), expr("cast(rand() * 100000 - 50000 as double)"))  # 3% negative values
                   .otherwise(abs(col("pydecimal").cast("double")) * 1000000 + 10000)) \
        .withColumn("valuation_date", to_timestamp(col("date_time"))) \
        .withColumn("asset_type", 
                   when(col("word").rlike("^[aeiou]"), "Equity")
                   .when(col("word").rlike("^[bcdfg]"), "Bond")
                   .when(col("word").rlike("^[hjklm]"), "ETF")
                   .when(col("word").rlike("^[npqr]"), "Mutual Fund")
                   .otherwise("Alternative Investment")) \
        .withColumn("account_id", (abs(col("random_int").cast("int")) % 1000) + 1000) \
        .withColumn("portfolio_id", (abs(col("random_number").cast("int")) % 500) + 500) \
        .withColumn("sector", 
                   when(col("city").rlike(".*[Tt]ech.*"), "Technology")
                   .when(col("city").rlike(".*[Ff]inance.*"), "Financial Services")
                   .when(col("city").rlike(".*[Hh]ealth.*"), "Healthcare")
                   .when(col("city").rlike(".*[Ee]nergy.*"), "Energy")
                   .otherwise("Consumer Goods")) \
        .withColumn("region", 
                   when(col("country").rlike(".*[Uu]nited.*"), "North America")
                   .when(col("country").rlike(".*[Gg]ermany.*"), "Europe")
                   .when(col("country").rlike(".*[Jj]apan.*"), "Asia")
                   .otherwise("Other")) \
        .withColumn("risk_rating", 
                   when(expr("rand() < 0.02"), expr("cast(rand() * 3 + 4 as int)"))  # 2% invalid risk ratings (4,5,6)
                   .otherwise((abs(col("random_digit").cast("int")) % 3) + 1)) \
        .withColumn("currency", 
                   when(col("email").rlike(".*@.*"), "USD")
                   .otherwise("EUR")) \
        .withColumn("investment_strategy", 
                   when(col("job").rlike(".*[Gg]rowth.*"), "Growth")
                   .when(col("job").rlike(".*[Vv]alue.*"), "Value")
                   .when(col("job").rlike(".*[Ii]ncome.*"), "Income")
                   .otherwise("Balanced")) \
        .withColumn("cost_basis", 
                   when(expr("rand() < 0.04"), expr("cast(rand() * 50000 - 25000 as double)"))  # 4% negative cost basis
                   .otherwise(abs(col("latitude").cast("double")) * 800000 + 5000)) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .select("holding_id", "client_name", "asset_name", "quantity", "market_value", 
                "valuation_date", "asset_type", "account_id", "portfolio_id", "sector", 
                "region", "risk_rating", "currency", "investment_strategy", "cost_basis", 
                "processing_timestamp")
    
    return df

# Market Data Bronze Table
@dlt.table(
    comment="Raw market data from financial data providers"
)
def market_data_raw():
    df_faker = spark.read \
        .format("fake") \
        .schema(market_data_schema) \
        .option("numRows", 1000) \
        .load()
    
    df = df_faker \
        .withColumn("symbol", 
                   when(expr("rand() < 0.01"), lit(None))  # 1% null symbols
                   .otherwise(col("company"))) \
        .withColumn("asset_name", col("name")) \
        .withColumn("open_price", 
                   when(expr("rand() < 0.03"), expr("cast(rand() * 200 - 100 as double)"))  # 3% negative prices
                   .otherwise(abs(col("pyfloat").cast("double")) * 500 + 10)) \
        .withColumn("high_price", 
                   when(expr("rand() < 0.02"), expr("cast(rand() * 300 - 150 as double)"))  # 2% negative prices
                   .otherwise(abs(col("pydecimal").cast("double")) * 600 + 15)) \
        .withColumn("low_price", 
                   when(expr("rand() < 0.02"), expr("cast(rand() * 100 - 50 as double)"))  # 2% negative prices
                   .otherwise(abs(col("latitude").cast("double")) * 400 + 5)) \
        .withColumn("close_price", 
                   when(expr("rand() < 0.02"), expr("cast(rand() * 250 - 125 as double)"))  # 2% negative prices
                   .otherwise(abs(col("longitude").cast("double")) * 550 + 12)) \
        .withColumn("volume", (abs(col("random_int").cast("int")) % 10000000) + 100000) \
        .withColumn("trade_date", to_timestamp(col("date_time"))) \
        .withColumn("exchange", 
                   when(col("word").rlike("^[aeiou]"), "NYSE")
                   .when(col("word").rlike("^[bcdfg]"), "NASDAQ")
                   .when(col("word").rlike("^[hjklm]"), "LSE")
                   .otherwise("TSE")) \
        .withColumn("sector", 
                   when(col("city").rlike(".*[Tt]ech.*"), "Technology")
                   .when(col("city").rlike(".*[Ff]inance.*"), "Financial Services")
                   .when(col("city").rlike(".*[Hh]ealth.*"), "Healthcare")
                   .when(col("city").rlike(".*[Ee]nergy.*"), "Energy")
                   .otherwise("Consumer Goods")) \
        .withColumn("region", 
                   when(col("country").rlike(".*[Uu]nited.*"), "North America")
                   .when(col("country").rlike(".*[Gg]ermany.*"), "Europe")
                   .when(col("country").rlike(".*[Jj]apan.*"), "Asia")
                   .otherwise("Other")) \
        .withColumn("market_cap", (abs(col("random_number").cast("int")) % 1000000000000) + 1000000000) \
        .withColumn("pe_ratio", 
                   when(expr("rand() < 0.05"), expr("cast(rand() * 50 - 25 as double)"))  # 5% negative PE ratios
                   .otherwise(abs(col("random_digit").cast("double")) * 30 + 5)) \
        .withColumn("dividend_yield", 
                   when(expr("rand() < 0.03"), expr("cast(rand() * 10 - 5 as double)"))  # 3% negative yields
                   .otherwise(abs(col("building_number").cast("double")) * 5)) \
        .withColumn("currency", 
                   when(col("email").rlike(".*@.*"), "USD")
                   .otherwise("EUR")) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .select("symbol", "asset_name", "open_price", "high_price", "low_price", 
                "close_price", "volume", "trade_date", "exchange", "sector", "region", 
                "market_cap", "pe_ratio", "dividend_yield", "currency", "processing_timestamp")
    
    return df

# Client Transactions Bronze Table
@dlt.table(
    comment="Raw client transaction data from trading system"
)
def client_transactions_raw():
    df_faker = spark.read \
        .format("fake") \
        .schema(client_transactions_schema) \
        .option("numRows", 2000) \
        .load()
    
    df = df_faker \
        .withColumn("transaction_id", 
                   when(expr("rand() < 0.01"), lit(None))  # 1% null transaction IDs
                   .otherwise(col("uuid4"))) \
        .withColumn("client_name", 
                   when(expr("rand() < 0.02"), lit(None))  # 2% null client names
                   .otherwise(col("name"))) \
        .withColumn("account_id", (abs(col("random_int").cast("int")) % 1000) + 1000) \
        .withColumn("asset_symbol", col("company")) \
        .withColumn("transaction_type", 
                   when(col("word").rlike("^[aeiou]"), "BUY")
                   .when(col("word").rlike("^[bcdfg]"), "SELL")
                   .when(col("word").rlike("^[hjklm]"), "DIVIDEND")
                   .otherwise("REINVEST")) \
        .withColumn("quantity", 
                   when(expr("rand() < 0.04"), expr("cast(rand() * 1000 - 500 as double)"))  # 4% negative quantities
                   .otherwise(abs(col("pyfloat").cast("double")) * 500 + 1)) \
        .withColumn("price_per_share", 
                   when(expr("rand() < 0.03"), expr("cast(rand() * 200 - 100 as double)"))  # 3% negative prices
                   .otherwise(abs(col("pydecimal").cast("double")) * 300 + 10)) \
        .withColumn("total_amount", 
                   when(expr("rand() < 0.05"), expr("cast(rand() * 100000 - 50000 as double)"))  # 5% negative amounts
                   .otherwise(abs(col("latitude").cast("double")) * 50000 + 1000)) \
        .withColumn("transaction_date", to_timestamp(col("date_time"))) \
        .withColumn("portfolio_id", (abs(col("random_number").cast("int")) % 500) + 500) \
        .withColumn("sector", 
                   when(col("city").rlike(".*[Tt]ech.*"), "Technology")
                   .when(col("city").rlike(".*[Ff]inance.*"), "Financial Services")
                   .when(col("city").rlike(".*[Hh]ealth.*"), "Healthcare")
                   .when(col("city").rlike(".*[Ee]nergy.*"), "Energy")
                   .otherwise("Consumer Goods")) \
        .withColumn("region", 
                   when(col("country").rlike(".*[Uu]nited.*"), "North America")
                   .when(col("country").rlike(".*[Gg]ermany.*"), "Europe")
                   .when(col("country").rlike(".*[Jj]apan.*"), "Asia")
                   .otherwise("Other")) \
        .withColumn("fees", 
                   when(expr("rand() < 0.06"), expr("cast(rand() * 100 - 50 as double)"))  # 6% negative fees
                   .otherwise(abs(col("random_digit").cast("double")) * 50 + 5)) \
        .withColumn("currency", 
                   when(col("email").rlike(".*@.*"), "USD")
                   .otherwise("EUR")) \
        .withColumn("advisor_name", col("job")) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .select("transaction_id", "client_name", "account_id", "asset_symbol", 
                "transaction_type", "quantity", "price_per_share", "total_amount", 
                "transaction_date", "portfolio_id", "sector", "region", "fees", 
                "currency", "advisor_name", "processing_timestamp")
    
    return df

# Client Profiles Bronze Table
@dlt.table(
    comment="Raw client profile data from CRM system"
)
def client_profiles_raw():
    df_faker = spark.read \
        .format("fake") \
        .schema(client_profiles_schema) \
        .option("numRows", 300) \
        .load()
    
    df = df_faker \
        .withColumn("client_id", 
                   when(expr("rand() < 0.01"), lit(None))  # 1% null client IDs
                   .otherwise(col("uuid4"))) \
        .withColumn("client_name", 
                   when(expr("rand() < 0.02"), lit(None))  # 2% null client names
                   .otherwise(col("name"))) \
        .withColumn("email_address", 
                   when(expr("rand() < 0.03"), lit(None))  # 3% null emails
                   .otherwise(col("email"))) \
        .withColumn("age", 
                   when(expr("rand() < 0.05"), expr("cast(rand() * 20 - 10 as int)"))  # 5% invalid ages
                   .otherwise((abs(col("random_int").cast("int")) % 50) + 25)) \
        .withColumn("city", col("city")) \
        .withColumn("country", col("country")) \
        .withColumn("annual_income", 
                   when(expr("rand() < 0.04"), expr("cast(rand() * 200000 - 100000 as double)"))  # 4% negative income
                   .otherwise(abs(col("pyfloat").cast("double")) * 500000 + 50000)) \
        .withColumn("net_worth", 
                   when(expr("rand() < 0.03"), expr("cast(rand() * 2000000 - 1000000 as double)"))  # 3% negative net worth
                   .otherwise(abs(col("pydecimal").cast("double")) * 5000000 + 100000)) \
        .withColumn("risk_tolerance", 
                   when(col("word").rlike("^[aeiou]"), "Conservative")
                   .when(col("word").rlike("^[bcdfg]"), "Moderate")
                   .when(col("word").rlike("^[hjklm]"), "Aggressive")
                   .otherwise("Balanced")) \
        .withColumn("account_open_date", to_timestamp(col("date_time"))) \
        .withColumn("investment_experience_years", 
                   when(expr("rand() < 0.06"), expr("cast(rand() * 15 - 7 as int)"))  # 6% negative experience
                   .otherwise((abs(col("random_number").cast("int")) % 30) + 1)) \
        .withColumn("employment_status", 
                   when(col("job").rlike(".*[Mm]anager.*"), "Employed")
                   .when(col("job").rlike(".*[Cc]onsultant.*"), "Self-Employed")
                   .when(col("job").rlike(".*[Rr]etired.*"), "Retired")
                   .otherwise("Unemployed")) \
        .withColumn("account_type_code", 
                   when(expr("rand() < 0.03"), expr("cast(rand() * 3 + 4 as int)"))  # 3% invalid account type codes
                   .otherwise((abs(col("random_digit").cast("int")) % 3) + 1)) \
        .withColumn("advisor_name", col("company")) \
        .withColumn("total_portfolio_value", 
                   when(expr("rand() < 0.04"), expr("cast(rand() * 1000000 - 500000 as double)"))  # 4% negative portfolio value
                   .otherwise(abs(col("latitude").cast("double")) * 2000000 + 50000)) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .select("client_id", "client_name", "email_address", "age", "city", "country", 
                "annual_income", "net_worth", "risk_tolerance", "account_open_date", 
                "investment_experience_years", "employment_status", "account_type_code", 
                "advisor_name", "total_portfolio_value", "processing_timestamp")
    
    return df
