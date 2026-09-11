from pyspark.sql import functions as F

cat, sch = "bbdatawarehouse_dev", "gold"
rows = []
for t in [r.tableName for r in spark.sql(f"SHOW TABLES IN {cat}.{sch}").collect()
          if r.tableName.startswith("salesforce_dim_")]:
    df = spark.table(f"{cat}.{sch}.{t}").where("ETL_Brand = 'Bluebeam'")
    cols = [c.name for c in df.schema.fields if c.dataType.simpleString() == "string"]
    agg = df.select([F.sum((F.col(c).isNull() | (F.trim(F.col(c)) == "")).cast("int")).alias(c)
                     for c in cols]).first().asDict()
    rows += [(t, c, n) for c, n in agg.items() if n]
display(spark.createDataFrame(rows, "table string, column string, empty_rows long"))
