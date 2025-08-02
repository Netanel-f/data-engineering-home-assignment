from pyspark.context import SparkContext
from pyspark.sql import SparkSession, types as T, functions as F
from pyspark.sql import Window

from awsglue.context import GlueContext
from awsglue.job import Job

NON_NEGATIVE_AND_NULL_COLS = ["open", "high", "low", "volume"]
NON_NEGATIVE_COLS = ['close']

BUCKET_BASE_PATH = "s3://data-engineer-assignment-netanel/"
INPUT_PATH = f"{BUCKET_BASE_PATH}data/"
OUTPUT_BASE_PATH = f"{BUCKET_BASE_PATH}outputs/"

AVG_DAILY_RETURN_PATH = f"{OUTPUT_BASE_PATH}avg_daily_return/"
HIGHEST_WORTH_PATH = f"{OUTPUT_BASE_PATH}highest_worth/"
MOST_VOLATILE_PATH = f"{OUTPUT_BASE_PATH}most_volatile/"
TOP_THREE_30_DAYS_PATH = f"{OUTPUT_BASE_PATH}top_three_30_days/"


def load_stock_data(spark):
    """
    Load stock data from a CSV file into a Spark DataFrame with a predefined schema.
    """
    stocks_schema = T.StructType([
        T.StructField("Date", T.DateType(), True),
        T.StructField("open", T.DoubleType(), True),
        T.StructField("high", T.DoubleType(), True),
        T.StructField("low", T.DoubleType(), True),
        T.StructField("close", T.DoubleType(), True),
        T.StructField("volume", T.IntegerType(), True),
        T.StructField("ticker", T.StringType(), True)
    ])

    raw_stocks_df = (spark.read.csv(f"{INPUT_PATH}stocks_data.csv",
                                   header=True,
                                   schema=stocks_schema)
                     .withColumnRenamed("Date", "date"))  # just to keep all cols lower-case
    return raw_stocks_df


def filter_invalid_values(df,
                          non_negative_and_nulls_cols=NON_NEGATIVE_AND_NULL_COLS,
                          non_negative_cols=NON_NEGATIVE_COLS):
    """
    Filters out invalid rows from the DataFrame
    """

    # filter out rows with negative values or nulls
    for col in non_negative_and_nulls_cols:
        df = df.filter(F.col(col) >= 0)
    
    # filter out rows with negative values, allow nulls
    for col in non_negative_cols:
        df = df.filter((F.col(col).isNotNull()) & (F.col(col) >= 0))

    return df

def filter_out_of_range(df):
    """
    Filters out rows where price ranges are invalid:
    """
    filtered_df = df.filter("low <= open AND open <= high")
    return filtered_df

def sanitize_stocks(df):
    df = filter_invalid_values(df)
    df = filter_out_of_range(df)
    return df

def append_previous_record_close_price(df, days_ago=1):
    """
    Fill days_ago close_orice if missing
    """
    window = Window.partitionBy("ticker").orderBy("date")
    df_with_prev_close = df.withColumn("prev_close", F.lag(col="close", offset=days_ago).over(window))
    return df_with_prev_close


def compute_return(df, days_ago=1):
    """
    Compute daily or interval return based on previous close price.
    """
    df_with_prev_close = append_previous_record_close_price(df, days_ago)
    
    output_col_name = "daily_return" if days_ago == 1 else f"interval_return"
    df_with_return = df_with_prev_close.withColumn(output_col_name,
                                                (F.col("close") - F.col("prev_close")) / F.col("prev_close"))
    return df_with_return


def compute_avg_daily_return(df):
    """
    objective 1: Compute the average daily return of all stocks for every date
    """
    df_with_return = compute_return(df)
    
    avg_daily_return_df = (df_with_return
                         .groupBy("date")
                         .agg(F.avg("daily_return").alias("avg_daily_return")))
    
    return avg_daily_return_df


def calc_avg_daily_worth(df):
    """
    Calculate the avg daily worth of each stock.
    """
    daily_worth_df = df.withColumn("daily_worth",
                                   F.when(F.col("close").isNotNull() & F.col("volume").isNotNull(),
                                          F.col("close") * F.col("volume"))
                                   .otherwise(0))

    avg_worth_df = (daily_worth_df
                    .groupBy("ticker")
                    .agg(F.avg("daily_worth").alias("avg_daily_worth")))
    return avg_worth_df

def highest_avg_worth_ticker(df):
    """
    objective 2: Calculate the average daily worth of each stock.
    """
    avg_worth_df = calc_avg_daily_worth(df)
    highest_avg_worth_ticker_df = avg_worth_df.orderBy(F.desc("avg_daily_worth")).limit(1)

    return highest_avg_worth_ticker_df

def calc_volatility(df):
    """
    Calculate the annualized volatility of each stock.
    """
    TOTAL_TRADING_DAYS = 252
    ANNUALIZED_VOLATILITY_FACTOR = TOTAL_TRADING_DAYS ** 0.5

    avg_daily_return_df = compute_avg_daily_return(df)

    volatile_df = (avg_daily_return_df
                   .groupBy("ticker")
                   .agg(F.stddev("daily_return").alias("stddev_daily_return"))
                   .withColumn("annual_volatility", F.col("stddev_daily_return") * ANNUALIZED_VOLATILITY_FACTOR)
                   .drop("stddev_daily_return")
                   )
    return volatile_df

def get_most_volatile_ticker(df):
    """
    objective 3: Get the most volatile stock based on annualized volatility.
    """
    volatile_df = calc_volatility(df)
    most_volatile_ticker_df = volatile_df.orderBy(F.desc("annual_volatility")).limit(1)
    return most_volatile_ticker_df


def find_top_three_30_days_ago_return(df):
    """
    objective 4: Find the top three stocks with the highest return from 30 days ago.
    """
    df_with_return = compute_return(df, 30).filter("interval_return is not null")

    # Calc top three
    top_three_df = (df_with_return
                    .orderBy(F.desc("interval_return"))
                    .limit(3)
                    .select("ticker", "date")
                    )
    return top_three_df


def stock_analysis(spark):
    # load data
    raw_stocks_df = load_stock_data(spark)
    stocks_df = sanitize_stocks(raw_stocks_df)
    
    # Obj. 1
    avg_daily_return_df = compute_avg_daily_return(stocks_df)
    avg_daily_return_df.write.mode("overwrite").parquet(AVG_DAILY_RETURN_PATH)

    # Obj. 2
    highest_avg_worth_ticker_df = highest_avg_worth_ticker(stocks_df)
    highest_avg_worth_ticker_df.write.mode("overwrite").parquet(HIGHEST_WORTH_PATH)

    # Obj. 3
    most_volatile_ticker_df = get_most_volatile_ticker(stocks_df)
    most_volatile_ticker_df.write.mode("overwrite").parquet(MOST_VOLATILE_PATH)

    # Obj. 4
    top_three_df = find_top_three_30_days_ago_return(stocks_df)
    top_three_df.write.mode("overwrite").parquet(TOP_THREE_30_DAYS_PATH)


if __name__ == "__main__":
    # Initialize Glue&Spark Context and create SparkSession
    sc = SparkContext.getOrCreate()
    gc = GlueContext(sc)

    spark_session = gc.spark_session

    # Job initialization
    job = Job(gc)
    job.init('stock_analysis_spark_job')

    stock_analysis(spark_session)
