# Bronze Layer: Asset Management Customer KYC Data Pipeline

import dlt
import sys
sys.path.append(spark.conf.get("bundle.sourcePath", "."))
from pyspark.sql.functions import expr, col, to_timestamp, current_timestamp, when, abs, lit, rand, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from typing import List
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    InputPartition,
)

# Embedded FakeDataSource implementation
def _validate_faker_schema(schema):
    # Verify the library is installed correctly.
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
        # Note: every value in this `self.options` dictionary is a string.
        num_rows = int(self.options.get("numRows", 3))
        for _ in range(num_rows):
            row = []
            for field in self.schema.fields:
                value = getattr(fake, field.name)()
                row.append(value)
            yield tuple(row)


class FakeDataSourceStreamReader(DataSourceStreamReader):
    def __init__(self, schema, options) -> None:
        self.schema: StructType = schema
        self.rows_per_microbatch = int(options.get("numRows", 3))
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
    print("FakeDataSource registration failed - this may cause issues with fake data source")

# Define schema for fake KYC data using valid Faker method names
# Note: All fields must be StringType as required by FakeDataSource
faker_kyc_schema = StructType([
    StructField("uuid4", StringType(), True),      # Will be mapped to customer_id
    StructField("name", StringType(), True),       # Will be mapped to full_name
    StructField("email", StringType(), True),      # Will be mapped to email_address
    StructField("random_int", StringType(), True), # Will be converted to age
    StructField("city", StringType(), True),       # Valid Faker method
    StructField("country", StringType(), True),    # Valid Faker method
    StructField("pyfloat", StringType(), True),    # Will be converted to annual_income
    StructField("word", StringType(), True),       # Will be mapped to risk_profile
    StructField("date_time", StringType(), True),  # Will be converted to kyc_timestamp
    StructField("random_number", StringType(), True), # Will be converted to investment_experience_years
    StructField("job", StringType(), True),        # Will be mapped to employment_status
    StructField("pydecimal", StringType(), True),  # Will be converted to net_worth
    StructField("company", StringType(), True),    # Will be mapped to account_type
    StructField("random_digit", StringType(), True) # Will be converted to kyc_status_code
])

# Create streaming data source using FakeDataSource with KYC data generation
@dlt.table(
    comment="Streaming asset management customer KYC data generated using FakeDataSource"
)
def customer_kyc_raw():
    # Create streaming DataFrame using fake data source with valid Faker schema
    df_faker = spark.readStream \
        .format("fake") \
        .schema(faker_kyc_schema) \
        .option("numRows", 2) \
        .load()
    
    # Transform Faker data to match our desired KYC schema with some intentionally bad data
    df = df_faker \
        .withColumn("customer_id", 
                   when(expr("rand() < 0.05"), lit(None))  # 5% null customer IDs
                   .otherwise(col("uuid4"))) \
        .withColumn("full_name", 
                   when(expr("rand() < 0.03"), lit(None))  # 3% null names
                   .otherwise(col("name"))) \
        .withColumn("email_address", 
                   when(expr("rand() < 0.04"), lit(None))  # 4% null emails
                   .otherwise(col("email"))) \
        .withColumn("age", 
                   when(expr("rand() < 0.08"), expr("cast(rand() * 20 - 10 as int)"))  # 8% invalid ages (< 18 or > 100)
                   .otherwise((abs(col("random_int").cast("int")) % 63) + 18)) \
        .withColumn("annual_income", 
                   when(expr("rand() < 0.06"), expr("cast(rand() * 50000 - 10000 as double)"))  # 6% negative/zero income
                   .otherwise(abs(col("pyfloat").cast("double")) * 200000 + 30000)) \
        .withColumn("risk_profile", 
                   when(col("word").rlike("^[aeiou]"), "Conservative")
                   .when(col("word").rlike("^[bcdfg]"), "Moderate")
                   .when(col("word").rlike("^[hjklm]"), "Aggressive")
                   .otherwise("Balanced")) \
        .withColumn("kyc_timestamp", to_timestamp(col("date_time"))) \
        .withColumn("investment_experience_years", 
                   when(expr("rand() < 0.07"), expr("cast(rand() * 10 - 5 as int)"))  # 7% negative experience
                   .otherwise((abs(col("random_number").cast("int")) % 20) + 1)) \
        .withColumn("employment_status", 
                   when(col("job").rlike(".*[Mm]anager.*"), "Employed")
                   .when(col("job").rlike(".*[Cc]onsultant.*"), "Self-Employed")
                   .when(col("job").rlike(".*[Rr]etired.*"), "Retired")
                   .otherwise("Unemployed")) \
        .withColumn("net_worth", 
                   when(expr("rand() < 0.05"), expr("cast(rand() * 100000 - 50000 as double)"))  # 5% negative/zero net worth
                   .otherwise(abs(col("pydecimal").cast("double")) * 1000000 + 50000)) \
        .withColumn("account_type", 
                   when(col("company").rlike(".*[Ii]ndividual.*"), "Individual")
                   .when(col("company").rlike(".*[Jj]oint.*"), "Joint")
                   .when(col("company").rlike(".*[Cc]orporate.*"), "Corporate")
                   .otherwise("Trust")) \
        .withColumn("kyc_status_code", 
                   when(expr("rand() < 0.04"), expr("cast(rand() * 3 + 4 as int)"))  # 4% invalid KYC status codes (4,5,6)
                   .otherwise((abs(col("random_digit").cast("int")) % 3) + 1)) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .select("customer_id", "full_name", "email_address", "age", "city", "country", 
                "annual_income", "risk_profile", "kyc_timestamp", "investment_experience_years",
                "employment_status", "net_worth", "account_type", "kyc_status_code", "processing_timestamp")
    
    return df