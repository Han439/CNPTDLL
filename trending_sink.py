"""
trending_sink.py  —  Cầu nối giữa Thành viên 2 (Spark) và Thành viên 3 (Dashboard)
==================================================================================
Tài liệu mục 13 cho thấy pipeline của Thành viên 2 hiện chỉ ghi kết quả ra CONSOLE
(format="console"). Console thì dashboard KHÔNG đọc được. File này bổ sung một sink
ghi kết quả streaming xuống SQLite (trending.db) để dashboard.py đọc và trực quan hóa.

CÁCH 1 — Dùng nhanh (khuyến nghị): chỉ cần SỬA 3 DÒNG trong trending_products.py
--------------------------------------------------------------------------------
Thay đoạn writeStream ra console (mục 13) bằng:

    from trending_sink import write_to_sqlite, init_db
    init_db()                                  # tạo bảng 1 lần

    query = top_products.writeStream \\
        .outputMode("complete") \\
        .foreachBatch(write_to_sqlite) \\      # <-- thay cho .format("console")
        .start()
    query.awaitTermination()

CÁCH 2 — Chạy trọn gói: `spark-submit trending_sink.py`
--------------------------------------------------------
File này cũng chứa LẠI toàn bộ pipeline của Thành viên 2 (đọc Kafka -> event-time ->
watermark -> window 1h -> count) và ghi ra ĐỒNG THỜI console + SQLite, nên có thể chạy
thay cho trending_products.py mà không mất hành vi cũ.

Lưu ý: outputMode="complete" nghĩa là mỗi micro-batch chứa TOÀN BỘ trạng thái tổng hợp
hiện tại, nên ta ghi đè (replace) bảng mỗi batch — khớp với cách dashboard mong đợi.
"""

import os
import sqlite3
from datetime import datetime, timezone

DB_PATH = os.environ.get("TRENDING_DB", "trending.db")


def init_db(db_path: str = DB_PATH) -> None:
    """Tạo bảng kết quả (idempotent)."""
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS trending_products (
            window_start TEXT NOT NULL,
            window_end   TEXT NOT NULL,
            product_id   TEXT NOT NULL,
            view_count   INTEGER NOT NULL,
            updated_at   TEXT NOT NULL,
            PRIMARY KEY (window_start, product_id)
        );
        """
    )
    conn.commit()
    conn.close()


def write_to_sqlite(batch_df, batch_id: int, db_path: str = DB_PATH) -> None:
    """
    Hàm foreachBatch: nhận DataFrame của 1 micro-batch (đã là kết quả trending đầy đủ),
    làm phẳng cột window struct -> ghi đè vào SQLite.

    batch_df có schema: window {start, end}, product_id, count
    """
    from pyspark.sql.functions import col

    # Làm phẳng struct window thành 2 cột thời gian dạng chuỗi để dễ lưu/đọc.
    flat = (
        batch_df.select(
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("product_id").cast("string").alias("product_id"),
            col("count").cast("long").alias("view_count"),
        )
    )

    # Batch trending đã được tổng hợp nên nhỏ -> đưa về pandas trên driver là an toàn.
    pdf = flat.toPandas()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    conn = sqlite3.connect(db_path)
    try:
        # complete mode: thay toàn bộ bằng trạng thái mới nhất.
        conn.execute("DELETE FROM trending_products")
        conn.executemany(
            """INSERT OR REPLACE INTO trending_products
               (window_start, window_end, product_id, view_count, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            [
                (r.window_start, r.window_end, r.product_id, int(r.view_count), now)
                for r in pdf.itertuples(index=False)
            ],
        )
        conn.commit()
    finally:
        conn.close()

    print(f"[sink] batch {batch_id}: đã ghi {len(pdf)} dòng trending vào {db_path}")


# ============================================================================
# CÁCH 2 — Pipeline trọn gói (tái hiện lại phần của Thành viên 2 + thêm sink)
# Chạy: spark-submit trending_sink.py
# ============================================================================
def _run_full_pipeline() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, to_timestamp, window
    from pyspark.sql.types import StructType, StructField, StringType

    spark = (
        SparkSession.builder.appName("TrendingProducts-WithSink")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- (mục 4) Đọc Kafka ---
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "ecommerce-events")
        .option("startingOffsets", "latest")
        .load()
    )

    # --- (mục 5) binary -> string ---
    json_df = raw_df.selectExpr("CAST(value AS STRING)")

    # --- (mục 6) schema ---
    schema = StructType(
        [
            StructField("event_time", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("user_id", StringType(), True),
        ]
    )

    # --- (mục 7) JSON -> DataFrame ---
    parsed_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    # --- (mục 8) event-time ---
    df = parsed_df.withColumn(
        "event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'")
    )

    # --- (mục 9) chỉ giữ event "view" ---
    df = df.filter(col("event_type") == "view")

    # --- (mục 10) watermark 10 phút ---
    df = df.withWatermark("event_time", "10 minutes")

    # --- (mục 11) window 1 giờ + count ---
    result = df.groupBy(window(col("event_time"), "1 hour"), col("product_id")).count()

    # --- (mục 12) sắp xếp trending ---
    top_products = result.orderBy(col("count").desc())

    # --- (mục 13 + sink mới) ghi ra console VÀ SQLite ---
    init_db()
    query = (
        top_products.writeStream.outputMode("complete")
        .foreachBatch(write_to_sqlite)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    _run_full_pipeline()
