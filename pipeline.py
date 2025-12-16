"""Optional helper script.
"""

def main():
    print("Run the Databricks notebook: DS2002_Project2_Capstone_Lakehouse_FIXED.ipynb")

if __name__ == "__main__":
    main()

# pipeline.py
"""
DS2002 Project 2 – Lakehouse Pipeline (Bronze/Silver/Gold)

What this script does:
1) Loads insurance.csv (Bronze)
2) Builds dimensions: dim_date + dim_member + dim_region + dim_risk_profile + dim_plan
3) Builds fact table: fact_insurance_silver
4) Builds a couple of Gold aggregates

Run in Databricks (How I ran it):
"""

import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# CONFIG
DBFS_BASE = os.getenv("DBFS_BASE", "dbfs:/Volumes/workspace/ds2002_project2/ds2002")
CSV_PATH  = os.getenv("CSV_PATH", f"{DBFS_BASE}/insurance.csv")


# If you want to read DimDate from Azure SQL, set these env vars
SQL_HOST = os.getenv("SQL_HOST")   # "yourserver.database.windows.net"
SQL_DB   = os.getenv("SQL_DB")     #  "ds2002_project2"
SQL_USER = os.getenv("SQL_USER")   #  "ds2002admin"
SQL_PWD  = os.getenv("SQL_PWD")     

# Table names (Delta)
T_DIM_DATE         = "dim_date"
T_DIM_MEMBER       = "dim_member"
T_DIM_REGION       = "dim_region"
T_DIM_RISK_PROFILE = "dim_risk_profile"
T_DIM_PLAN         = "dim_plan"
T_FACT_SILVER      = "fact_insurance_silver"

# Gold tables
T_GOLD_REGION_SMOKER  = "gold_region_smoker"
T_GOLD_PROVIDER_MONTH = "gold_provider_month"


# Helpers
def write_delta_table(df, table_name: str, mode: str = "overwrite"):
    df.write.format("delta").mode(mode).saveAsTable(table_name)


def read_insurance_csv():
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(CSV_PATH)
    )


def add_synthetic_event_date(df, start_date="2019-01-01", num_days=1461):
    """
    insurance.csv usually has no date column.
    This creates a deterministic synthetic event_date so the Date dimension can be used.
    """
    return df.withColumn(
        "event_date",
        F.date_add(F.to_date(F.lit(start_date)), (F.monotonically_increasing_id() % F.lit(num_days)).cast("int"))
    )


def build_dim_plan():
    plan_rows = [
        (1, "Bronze",   6000, 60, "Basic"),
        (2, "Silver",   3000, 40, "Standard"),
        (3, "Gold",     1000, 20, "Premium"),
        (4, "Platinum",  500, 10, "Premium+"),
    ]
    return spark.createDataFrame(
        plan_rows,
        ["plan_key", "plan_name", "deductible_usd", "copay_usd", "coverage_tier"]
    )


def build_dim_region(df):
    return (
        df.select(F.col("region").cast("string").alias("region"))
        .dropDuplicates()
        .na.drop(subset=["region"])
        .withColumn("region_key", F.row_number().over(Window.orderBy("region")))
        .select("region_key", "region")
    )


def build_dim_member(df, dim_region):
    """
    Member dimension from the source file.
    Adds a member_id (surrogate-ish) and region_key lookup.
    """
    members_raw = (
        df.withColumn("member_id", F.monotonically_increasing_id().cast("long"))
        .select(
            "member_id",
            F.expr("try_cast(age as int)").alias("age"),
            F.col("sex").cast("string").alias("sex"),
            F.expr("try_cast(bmi as double)").alias("bmi"),
            F.expr("try_cast(children as int)").alias("children"),
            F.col("smoker").cast("string").alias("smoker"),
            F.col("region").cast("string").alias("region"),
        )
    )

    return (
        members_raw.join(dim_region, on="region", how="left")
        .drop("region")
        .select("member_id", "age", "sex", "bmi", "children", "smoker", "region_key")
    )


def build_dim_risk_profile(dim_member):
    """
    An additional dimension based on member attributes.
    """
    return (
        dim_member.select("sex", "smoker", "region_key")
        .dropDuplicates()
        .withColumn("risk_profile_key", F.row_number().over(Window.orderBy("sex", "smoker", "region_key")))
        .select("risk_profile_key", "sex", "smoker", "region_key")
    )


def build_dim_date_from_sql():
    """
    Optional: Read dbo.DimDate from Azure SQL.
    Expects you to provide env vars: SQL_HOST, SQL_DB, SQL_USER, SQL_PWD.
    """
    if not all([SQL_HOST, SQL_DB, SQL_USER, SQL_PWD]):
        return None

    jdbc_url = (
        f"jdbc:sqlserver://{SQL_HOST}:1433;"
        f"database={SQL_DB};"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;"
        "loginTimeout=30;"
    )

    dim_date_sql = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("dbtable", "dbo.DimDate")
        .option("user", SQL_USER)
        .option("password", SQL_PWD)
        .load()
    )

    # Normalize to columns we will use: date_key (int yyyymmdd), full_date (date)
    # Many DimDate tables already have DateKey and FullDateAlternateKey; handle common cases.
    cols = [c.lower() for c in dim_date_sql.columns]
    if "datekey" in cols:
        date_key_col = dim_date_sql.columns[cols.index("datekey")]
    else:
        date_key_col = None

    if "fulldatealternatekey" in cols:
        full_date_col = dim_date_sql.columns[cols.index("fulldatealternatekey")]
    elif "date" in cols:
        full_date_col = dim_date_sql.columns[cols.index("date")]
    else:
        full_date_col = None

    if not date_key_col or not full_date_col:
        # If your SQL DimDate has different names, adjust here.
        return None

    return (
        dim_date_sql
        .select(
            F.col(date_key_col).cast("int").alias("date_key"),
            F.to_date(F.col(full_date_col)).alias("full_date")
        )
        .dropDuplicates(["date_key"])
    )


def build_dim_date_from_fact(fact_df):
    """
    Fallback: build dim_date directly from the synthetic event_date.
    date_key is yyyymmdd integer.
    """
    return (
        fact_df.select(F.to_date("event_date").alias("full_date"))
        .dropDuplicates()
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .select("date_key", "full_date")
        .orderBy("full_date")
    )


def build_fact_insurance_silver(df, dim_member, dim_plan, dim_date):
    """
    Creates the fact table with foreign keys:
    - member_id
    - risk_profile_key
    - plan_key
    - date_key
    Measures:
    - charge_amount
    """
    # Add a simple plan assignment (deterministic) so plan_key is used in the model.
    df2 = (
        df.withColumn("member_id", F.monotonically_increasing_id().cast("long"))
          .withColumn("charge_amount", F.expr("try_cast(charges as double)"))
          .withColumn("plan_key", ((F.col("member_id") % F.lit(4)) + F.lit(1)).cast("int"))
    )

    # Bring in member attributes needed for risk profile
    m = dim_member.select("member_id", "sex", "smoker", "region_key")

    fact = (
        df2.join(m, on="member_id", how="left")
           .withColumn("date_key", F.date_format(F.to_date("event_date"), "yyyyMMdd").cast("int"))
           .join(dim_date.select("date_key"), on="date_key", how="left")
           .join(dim_plan.select("plan_key"), on="plan_key", how="left")
           .select(
               "date_key",
               "member_id",
               "sex",
               "smoker",
               "region_key",
               "plan_key",
               F.col("charge_amount").alias("charge_amount"),
               F.to_date("event_date").alias("event_date"),
           )
    )

    # Create risk_profile_key by matching to dim_risk_profile logic
    dim_risk_profile = build_dim_risk_profile(dim_member)
    fact = (
        fact.join(
            dim_risk_profile.select("risk_profile_key", "sex", "smoker", "region_key"),
            on=["sex", "smoker", "region_key"],
            how="left"
        )
        .drop("sex", "smoker")  # keep region_key + risk_profile_key
    )

    return fact


def build_gold_tables():
    # Region + smoker aggregates
    gold_region_smoker = spark.sql(f"""
        SELECT
          r.region,
          rp.smoker,
          COUNT(*) AS num_events,
          ROUND(SUM(f.charge_amount), 2) AS total_charges,
          ROUND(AVG(f.charge_amount), 2) AS avg_charges
        FROM {T_FACT_SILVER} f
        LEFT JOIN {T_DIM_REGION} r ON f.region_key = r.region_key
        LEFT JOIN {T_DIM_RISK_PROFILE} rp ON f.risk_profile_key = rp.risk_profile_key
        GROUP BY r.region, rp.smoker
        ORDER BY r.region, rp.smoker
    """)
    write_delta_table(gold_region_smoker, T_GOLD_REGION_SMOKER)

    # Example provider-month table (synthetic provider/network)
    # If you already have provider fields, replace this with your real ones.
    provider_month = spark.sql(f"""
        SELECT
          date_format(to_date(event_date), 'yyyy-MM') AS event_month,
          COUNT(*) AS num_events,
          ROUND(SUM(charge_amount), 2) AS total_charges,
          ROUND(AVG(charge_amount), 2) AS avg_charges
        FROM {T_FACT_SILVER}
        GROUP BY date_format(to_date(event_date), 'yyyy-MM')
        ORDER BY event_month
    """)
    write_delta_table(provider_month, T_GOLD_PROVIDER_MONTH)


# Main pipeline
def main():
    # 1) Bronze load
    insurance = read_insurance_csv()
    insurance = add_synthetic_event_date(insurance)

    # 2) Dimensions
    dim_plan = build_dim_plan()
    dim_region = build_dim_region(insurance)
    dim_member = build_dim_member(insurance, dim_region)
    dim_risk_profile = build_dim_risk_profile(dim_member)

    dim_date = build_dim_date_from_sql()
    if dim_date is None:
        dim_date = build_dim_date_from_fact(insurance)

    # Write dims
    write_delta_table(dim_date, T_DIM_DATE)
    write_delta_table(dim_member, T_DIM_MEMBER)
    write_delta_table(dim_region, T_DIM_REGION)
    write_delta_table(dim_risk_profile, T_DIM_RISK_PROFILE)
    write_delta_table(dim_plan, T_DIM_PLAN)

    # 3) Fact (Silver)
    fact_silver = build_fact_insurance_silver(insurance, dim_member, dim_plan, dim_date)
    write_delta_table(fact_silver, T_FACT_SILVER)

    # 4) Gold
    build_gold_tables()

    print("Pipeline complete! :)")
    print("Tables created:")
    print(T_DIM_DATE, T_DIM_MEMBER, T_DIM_REGION, T_DIM_RISK_PROFILE, T_DIM_PLAN, T_FACT_SILVER)
    print(T_GOLD_REGION_SMOKER, T_GOLD_PROVIDER_MONTH)


if __name__ == "__main__":
    main()
