"""
dashboard.py - Trending Products Dashboard
==========================================================
Trực quan hóa kết quả "sản phẩm trending" do pipeline Spark Structured Streaming
của Thành viên 2 sinh ra (Kafka -> Spark -> window 1h -> count).

Chạy:
    streamlit run dashboard.py

Nguồn dữ liệu: SQLite (trending.db) do trending_sink.py ghi ra (hoặc generate_mock_data.py
khi demo). Có thể đổi đường dẫn DB ở thanh bên trái.
"""

import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import data_source as ds

# ----------------------------------------------------------------------------
# Cấu hình trang
# ----------------------------------------------------------------------------
st.set_page_config(
    page_title="Trending Products Dashboard",
    page_icon="",
    layout="wide",
)

PALETTE = px.colors.qualitative.Set2
ACCENT = "#2563eb"

# CSS nhỏ cho gọn gàng
st.markdown(
    """
    <style>
      .block-container {padding-top: 1.6rem;}
      [data-testid="stMetricValue"] {font-size: 1.6rem;}
      h1 {font-size: 1.7rem !important;}
    </style>
    """,
    unsafe_allow_html=True,
)


# ----------------------------------------------------------------------------
# Thanh bên: nguồn dữ liệu + tuỳ chọn
# ----------------------------------------------------------------------------
with st.sidebar:
    st.header("Cấu hình")
    db_path = st.text_input("Đường dẫn database (SQLite)", value=ds.DEFAULT_DB)
    top_n = st.slider("Số sản phẩm Top hiển thị", min_value=3, max_value=15, value=8)

    st.divider()
    auto_refresh = st.toggle("Làm mới tự động (realtime)", value=False)
    refresh_secs = st.slider("Chu kỳ làm mới (đơn vị: giây)", 2, 30, 5, disabled=not auto_refresh)

    st.divider()


# ----------------------------------------------------------------------------
# Nạp dữ liệu
# ----------------------------------------------------------------------------
df = ds.load_trending(db_path)

st.title("Xu hướng sản phẩm theo thời gian thực")
st.caption(
    "Phân tích lượt xem sản phẩm theo cửa sổ thời gian 1 giờ"
)

if df.empty:
    st.warning(
        "Chưa có dữ liệu. Hãy chạy pipeline Spark (`spark-submit trending_products.py` + "
        "`trending_sink.py`) hoặc sinh dữ liệu demo bằng `python generate_mock_data.py`."
    )
    st.stop()


# ----------------------------------------------------------------------------
# Bộ lọc cửa sổ thời gian
# ----------------------------------------------------------------------------
windows = (
    df[["window_start", "window_label"]]
    .drop_duplicates()
    .sort_values("window_start")
)
window_options = ["Tất cả (tổng hợp)"] + windows["window_label"].tolist()
selected_window = st.selectbox("Cửa sổ thời gian", window_options, index=0)

if selected_window == "Tất cả (tổng hợp)":
    view_df = df.copy()
    scope_label = "tất cả cửa sổ"
else:
    view_df = df[df["window_label"] == selected_window].copy()
    scope_label = f"cửa sổ {selected_window}"


# ----------------------------------------------------------------------------
# Hàng KPI
# ----------------------------------------------------------------------------
k = ds.kpis(view_df)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Tổng lượt xem", f"{k['total_views']:,}")
c2.metric("Số sản phẩm", k["num_products"])
c3.metric("Số cửa sổ 1h", k["num_windows"])
c4.metric("Top đầu sản phẩm", k["top_product"], f"{k['top_product_views']:,} lượt")

st.divider()


# ----------------------------------------------------------------------------
# 1) Bảng xếp hạng Top sản phẩm (bar chart ngang)
# ----------------------------------------------------------------------------
left, right = st.columns([3, 2])

with left:
    st.subheader(f"Top {top_n} sản phẩm trending — {scope_label}")
    ranking = (
        view_df.groupby(["product_id", "product_name"], as_index=False)["view_count"]
        .sum()
        .sort_values("view_count", ascending=False)
        .head(top_n)
    )
    fig_bar = px.bar(
        ranking.sort_values("view_count"),
        x="view_count",
        y="product_name",
        orientation="h",
        text="view_count",
        labels={"view_count": "Lượt xem", "product_name": "Sản phẩm"},
        color="view_count",
        color_continuous_scale="Blues",
    )
    fig_bar.update_traces(textposition="outside", cliponaxis=False)
    fig_bar.update_layout(
        coloraxis_showscale=False,
        height=420,
        margin=dict(l=10, r=30, t=10, b=10),
        yaxis_title=None,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with right:
    st.subheader("Tỷ trọng lượt xem")
    fig_pie = px.pie(
        ranking,
        names="product_name",
        values="view_count",
        hole=0.45,
        color_discrete_sequence=PALETTE,
    )
    fig_pie.update_traces(textposition="inside", textinfo="percent")
    fig_pie.update_layout(height=420, margin=dict(l=10, r=10, t=10, b=10),
                          legend=dict(orientation="h", y=-0.1))
    st.plotly_chart(fig_pie, use_container_width=True)


# ----------------------------------------------------------------------------
# 2) Xu hướng theo thời gian (multi-line) — chỉ ý nghĩa khi xem nhiều cửa sổ
# ----------------------------------------------------------------------------
st.subheader("Xu hướng lượt xem theo từng cửa sổ 1 giờ")

if df["window_start"].nunique() <= 1:
    st.info("Hiện chỉ có 1 cửa sổ thời gian — biểu đồ xu hướng sẽ rõ hơn khi có nhiều cửa sổ.")
else:
    # Mặc định chọn các sản phẩm trong top để biểu đồ đỡ rối
    top_products = (
        df.groupby("product_name")["view_count"].sum().sort_values(ascending=False).head(top_n).index.tolist()
    )
    chosen = st.multiselect(
        "Chọn sản phẩm để so sánh xu hướng",
        options=sorted(df["product_name"].unique()),
        default=top_products,
    )
    trend = df[df["product_name"].isin(chosen)].sort_values("window_start")
    fig_line = px.line(
        trend,
        x="window_label",
        y="view_count",
        color="product_name",
        markers=True,
        labels={"window_label": "Cửa sổ thời gian", "view_count": "Lượt xem", "product_name": "Sản phẩm"},
        color_discrete_sequence=PALETTE,
    )
    fig_line.update_layout(height=420, margin=dict(l=10, r=10, t=10, b=10),
                           legend_title=None, xaxis_title=None)
    st.plotly_chart(fig_line, use_container_width=True)

    # Heatmap product x window — nhìn nhanh "điểm nóng"
    st.subheader("Heatmap mật độ lượt xem")
    pivot = df.pivot_table(
        index="product_name", columns="window_label", values="view_count", aggfunc="sum", fill_value=0
    )
    # giữ thứ tự cột theo thời gian
    ordered_cols = windows["window_label"].tolist()
    pivot = pivot.reindex(columns=[c for c in ordered_cols if c in pivot.columns])
    fig_heat = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale="Blues",
            text=pivot.values,
            texttemplate="%{text}",
            colorbar=dict(title="Lượt xem"),
        )
    )
    fig_heat.update_layout(height=380, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_heat, use_container_width=True)


# ----------------------------------------------------------------------------
# 3) Bảng dữ liệu thô (đúng output của Thành viên 2) + tải xuống
# ----------------------------------------------------------------------------
st.subheader("Dữ liệu chi tiết")
table = view_df[["window_label", "product_id", "product_name", "view_count"]].rename(
    columns={
        "window_label": "Cửa sổ (window)",
        "product_id": "product_id",
        "product_name": "Tên sản phẩm",
        "view_count": "Lượt xem (count)",
    }
).reset_index(drop=True)
st.dataframe(table, use_container_width=True, hide_index=True)

st.download_button(
    "Tải báo cáo CSV",
    data=table.to_csv(index=False).encode("utf-8-sig"),
    file_name="trending_report.csv",
    mime="text/csv",
)

st.caption(f"Cập nhật lần cuối: {ds.last_updated(df)}")


# ----------------------------------------------------------------------------
# Tự động làm mới (mô phỏng realtime)
# ----------------------------------------------------------------------------
if auto_refresh:
    time.sleep(refresh_secs)
    st.rerun()
