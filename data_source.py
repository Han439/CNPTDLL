"""
data_source.py
==============
Lớp truy xuất dữ liệu cho Dashboard

Đọc kết quả "trending products" (thông qua sink ghi vào SQLite — xem trending_sink.py).

Output dataframe có dạng:
    window_start | window_end | product_id | view_count
"""

import os
import sqlite3
from datetime import datetime

import pandas as pd

DEFAULT_DB = os.environ.get("TRENDING_DB", "trending.db")


def db_exists(db_path: str = DEFAULT_DB) -> bool:
    return os.path.exists(db_path)


def load_trending(db_path: str = DEFAULT_DB) -> pd.DataFrame:
    """
    Đọc toàn bộ kết quả trending hiện tại.

    Trả về DataFrame các cột:
        window_label, window_start, window_end, product_id,
        product_name, view_count, updated_at
    """
    if not db_exists(db_path):
        return _empty_df()

    conn = sqlite3.connect(db_path)
    try:
        # LEFT JOIN catalog để có tên sản phẩm nếu có; pipeline thật chỉ cần product_id.
        has_catalog = (
            conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='product_catalog'"
            ).fetchone()
            is not None
        )
        if has_catalog:
            query = """
                SELECT t.window_start, t.window_end, t.product_id,
                       COALESCE(c.product_name, t.product_id) AS product_name,
                       t.view_count, t.updated_at
                FROM trending_products t
                LEFT JOIN product_catalog c ON t.product_id = c.product_id
            """
        else:
            query = """
                SELECT window_start, window_end, product_id,
                       product_id AS product_name, view_count, updated_at
                FROM trending_products
            """
        df = pd.read_sql(query, conn)
    except Exception:
        return _empty_df()
    finally:
        conn.close()

    if df.empty:
        return _empty_df()

    df["window_start"] = pd.to_datetime(df["window_start"])
    df["window_end"] = pd.to_datetime(df["window_end"])
    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce").fillna(0).astype(int)
    df["window_label"] = (
        df["window_start"].dt.strftime("%H:%M") + " - " + df["window_end"].dt.strftime("%H:%M")
    )
    return df.sort_values(["window_start", "view_count"], ascending=[True, False])


def last_updated(df: pd.DataFrame) -> str:
    if df.empty or "updated_at" not in df:
        return "—"
    try:
        return str(df["updated_at"].max())
    except Exception:
        return "—"


def kpis(df: pd.DataFrame) -> dict:
    """Các chỉ số tổng quan cho hàng KPI trên dashboard."""
    if df.empty:
        return {
            "total_views": 0,
            "num_products": 0,
            "num_windows": 0,
            "top_product": "—",
            "top_product_views": 0,
        }
    agg = df.groupby(["product_id", "product_name"], as_index=False)["view_count"].sum()
    top = agg.sort_values("view_count", ascending=False).iloc[0]
    return {
        "total_views": int(df["view_count"].sum()),
        "num_products": int(df["product_id"].nunique()),
        "num_windows": int(df["window_start"].nunique()),
        "top_product": str(top["product_name"]),
        "top_product_views": int(top["view_count"]),
    }


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "window_start",
            "window_end",
            "product_id",
            "product_name",
            "view_count",
            "updated_at",
            "window_label",
        ]
    )
