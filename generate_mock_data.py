"""
generate_mock_data.py
======================
Tạo dữ liệu mẫu (mock data) giống hệt output của file `trending_products.py`
(Spark Structured Streaming) để Thành viên 3 có thể phát triển và demo Dashboard
mà KHÔNG cần phải chạy toàn bộ hệ thống Kafka + Spark.

Dữ liệu được ghi vào SQLite (cùng schema mà sink thật sẽ dùng) -> dashboard.py
đọc từ đây. Khi hệ thống thật chạy, chỉ cần đổi sang DB do `trending_sink.py` ghi ra.

Cách dùng:
    python generate_mock_data.py                 # tạo 1 lần
    python generate_mock_data.py --live          # giả lập streaming: cập nhật liên tục
"""

import argparse
import random
import sqlite3
import time
from datetime import datetime, timedelta, timezone

DB_PATH = "trending.db"

# Một vài product_id mẫu (lấy theo phong cách dataset eCommerce REES46 mà đề bài dùng)
# product_name chỉ để hiển thị cho đẹp trên dashboard; pipeline thật chỉ có product_id.
PRODUCTS = [
    ("1001", "Smartphone Galaxy A"),
    ("1002", "Laptop UltraBook 14"),
    ("2001", "Tai nghe Bluetooth Pro"),
    ("2002", "Bàn phím cơ RGB"),
    ("3001", "Đồng hồ thông minh"),
    ("3002", "Sạc nhanh 65W"),
    ("4001", "Camera an ninh WiFi"),
    ("4002", "Loa di động chống nước"),
]


def init_db(conn: sqlite3.Connection) -> None:
    """Tạo bảng đúng schema mà sink thật (trending_sink.py) sẽ ghi ra."""
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

        -- Bảng tra cứu tên sản phẩm (tuỳ chọn, để dashboard hiển thị tên thay vì id)
        CREATE TABLE IF NOT EXISTS product_catalog (
            product_id   TEXT PRIMARY KEY,
            product_name TEXT
        );
        """
    )
    conn.executemany(
        "INSERT OR REPLACE INTO product_catalog(product_id, product_name) VALUES (?, ?)",
        PRODUCTS,
    )
    conn.commit()


def generate_windows(num_windows: int = 8):
    """Tạo các cửa sổ 1 giờ liên tiếp, mô phỏng window() trong Spark."""
    # Bắt đầu từ đầu ngày của dataset gốc cho dễ liên hệ với tài liệu Thành viên 2.
    base = datetime(2019, 10, 1, 0, 0, 0)
    windows = []
    for i in range(num_windows):
        start = base + timedelta(hours=i)
        end = start + timedelta(hours=1)
        windows.append((start, end))
    return windows


def build_rows(num_windows: int = 8):
    """Sinh số lượt xem cho mỗi (window, product) với xu hướng tăng/giảm thực tế."""
    rows = []
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # Mỗi sản phẩm có một "độ phổ biến" cơ bản khác nhau
    base_popularity = {pid: random.randint(5, 40) for pid, _ in PRODUCTS}

    for (start, end) in generate_windows(num_windows):
        for pid, _ in PRODUCTS:
            # Dao động quanh độ phổ biến cơ bản + nhiễu ngẫu nhiên -> tạo xu hướng
            base = base_popularity[pid]
            count = max(0, int(random.gauss(base, base * 0.4)))
            if count == 0:
                continue  # sản phẩm không có lượt xem trong cửa sổ này
            rows.append(
                (
                    start.strftime("%Y-%m-%d %H:%M:%S"),
                    end.strftime("%Y-%m-%d %H:%M:%S"),
                    pid,
                    count,
                    now,
                )
            )
    return rows


def write_rows(conn: sqlite3.Connection, rows) -> None:
    """Ghi đè dữ liệu (giống outputMode='complete' của Spark: luôn là trạng thái đầy đủ)."""
    conn.execute("DELETE FROM trending_products")
    conn.executemany(
        """INSERT OR REPLACE INTO trending_products
           (window_start, window_end, product_id, view_count, updated_at)
           VALUES (?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()


def run_once(num_windows: int = 8) -> None:
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    rows = build_rows(num_windows)
    write_rows(conn, rows)
    print(f"[OK] Đã ghi {len(rows)} dòng vào {DB_PATH} ({num_windows} cửa sổ 1 giờ).")
    conn.close()


def run_live(interval: int = 5) -> None:
    """Giả lập streaming: cứ mỗi `interval` giây cập nhật lại dữ liệu (như Spark micro-batch)."""
    print(f"[LIVE] Cập nhật dữ liệu mỗi {interval}s. Nhấn Ctrl+C để dừng.")
    n = 1
    try:
        while True:
            # Số cửa sổ tăng dần để mô phỏng thời gian trôi đi
            run_once(num_windows=min(2 + n, 12))
            n += 1
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[STOP] Đã dừng giả lập streaming.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sinh dữ liệu mock cho dashboard Thành viên 3")
    parser.add_argument("--live", action="store_true", help="Giả lập streaming realtime")
    parser.add_argument("--interval", type=int, default=5, help="Khoảng cập nhật (giây) ở chế độ --live")
    parser.add_argument("--windows", type=int, default=8, help="Số cửa sổ 1 giờ ở chế độ chạy 1 lần")
    args = parser.parse_args()

    if args.live:
        run_live(args.interval)
    else:
        run_once(args.windows)
