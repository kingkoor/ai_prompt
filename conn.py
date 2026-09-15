import pyspark.sql.functions as f

def null_report(table_name: str):
    df = spark.table(table_name)
    total = df.count()
    counts = df.select([
        f.sum(f.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]).first().asDict()
    print(f"\n=== {table_name} ({total} rows) ===")
    for col, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        if n:
            print(f"  {col:40s} {n:>8d}  ({n/total:.1%} null)")

for t in [
    "bbdatawarehouse_dev.silver.chargebee_sat_customers_restricted_sitedocs_us",
    "bbdatawarehouse_dev.silver.chargebee_sat_subscriptions_restricted_sitedocs_us",
    "bbdatawarehouse_dev.silver.chargebee_sat_invoices_restricted_sitedocs_us",
]:
    null_report(t)
