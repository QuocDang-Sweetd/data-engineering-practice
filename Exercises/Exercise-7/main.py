from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import zipfile
import os

zip_path = os.path.join('data', 'hard-drive-2022-01-01-failures.csv.zip')


# Thư mục giải nén
extract_dir = r"D:\data-engineering-practice\Exercises\Exercise-7\data\unzipped"

if not os.path.exists(extract_dir):
    os.makedirs(extract_dir)

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

unzipped_file_path = os.path.join(extract_dir, "hard-drive-2022-01-01-failures.csv")


def process_data(spark):
    df = spark.read.option("header", True).csv(unzipped_file_path)

    df = df.withColumn("source_file", F.input_file_name())

    df = df.withColumn(
        "file_date",
        F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1), "yyyy-MM-dd")
    )

    df = df.withColumn(
        "brand",
        F.when(
            F.instr(F.col("model"), " ") > 0,
            F.split("model", " ")[0]
        ).otherwise(F.lit("unknown"))
    )

    window_spec = Window.partitionBy("model").orderBy(F.col("capacity_bytes").cast("bigint").desc())
    df = df.withColumn("storage_ranking", F.dense_rank().over(window_spec))

    key_columns = ["date", "serial_number", "model", "capacity_bytes"]
    df = df.withColumn("primary_key", F.sha2(F.concat_ws("||", *key_columns), 256))

    df.show(5, truncate=False)

    return df


def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    process_data(spark)


if __name__ == "__main__":
    main()
