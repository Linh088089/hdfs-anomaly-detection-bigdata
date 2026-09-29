import os
import re
from collections import Counter

import altair as alt
import pandas as pd
import streamlit as st
from deltalake import DeltaTable


# =========================================================
# CẤU HÌNH
# =========================================================

st.set_page_config(
    page_title="HDFS Anomaly Detection Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

DELTA_PATH = "delta/hdfs_predictions"
TEMPLATE_PATH = "data/raw/HDFS.log_templates.csv"

REFRESH_SECONDS = 5


# =========================================================
# CSS
# =========================================================

st.markdown(
    """
    <style>

    /* =====================================================
       TOÀN TRANG
       ===================================================== */

    .block-container {
        padding-top: 3.5rem !important;
        padding-bottom: 2rem !important;
    }


    /* =====================================================
       MENU ☰
       Đẩy xuống dưới để không bị header Streamlit che
       ===================================================== */

    div[data-testid="stPopover"] {
        position: relative !important;
        z-index: 99999 !important;

        width: 52px !important;
        min-width: 52px !important;
        max-width: 52px !important;

        margin-top: 24px !important;
        margin-bottom: 24px !important;
        margin-left: 0 !important;
    }


    div[data-testid="stPopover"] > button {

        width: 52px !important;
        min-width: 52px !important;
        max-width: 52px !important;

        height: 46px !important;
        min-height: 46px !important;

        padding: 0 !important;

        border: 1px solid #b7b7b7 !important;
        border-radius: 10px !important;

        background: white !important;
        color: #111111 !important;

        font-size: 24px !important;
        font-weight: 700 !important;

        display: flex !important;
        align-items: center !important;
        justify-content: center !important;

        box-shadow: 0 2px 7px rgba(0, 0, 0, 0.08) !important;
    }


    div[data-testid="stPopover"] > button:hover {
        border-color: #333333 !important;
        background: #f7f7f7 !important;
    }


    div[data-testid="stPopover"] > button svg {
        display: none !important;
    }


    div[data-testid="stPopover"] > button p {
        margin: 0 !important;
        padding: 0 !important;

        font-size: 24px !important;
        line-height: 1 !important;

        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
    }


    /* =====================================================
       TRẠNG THÁI HỆ THỐNG
       ===================================================== */

    .system-running {
        padding: 12px 18px;
        border-radius: 8px;
        border: 1px solid #9bd59b;
        background: #eef9ee;
        font-weight: 700;
        margin-bottom: 18px;
    }


    /* =====================================================
       BLOCK ID ĐANG XỬ LÝ
       ===================================================== */

    .block-card {
        border: 1px solid #d8d8d8;
        border-radius: 10px;
        padding: 20px;
        margin-top: 5px;
        margin-bottom: 15px;
    }

    </style>
    """,
    unsafe_allow_html=True
)


# =========================================================
# SESSION STATE
# =========================================================

if "page" not in st.session_state:
    st.session_state.page = "Tổng quan"


# =========================================================
# ĐỌC DELTA LAKE
# =========================================================

def load_delta_data():

    try:

        if not os.path.exists(DELTA_PATH):
            return pd.DataFrame()

        delta_table = DeltaTable(
            DELTA_PATH
        )

        df = (
            delta_table
            .to_pyarrow_table()
            .to_pandas()
        )

        if df.empty:
            return df


        if "KafkaOffset" in df.columns:

            df["KafkaOffset"] = pd.to_numeric(
                df["KafkaOffset"],
                errors="coerce"
            )


        if "KafkaTimestamp" in df.columns:

            df["KafkaTimestamp"] = pd.to_datetime(
                df["KafkaTimestamp"],
                errors="coerce"
            )


        if "ProcessedAt" in df.columns:

            df["ProcessedAt"] = pd.to_datetime(
                df["ProcessedAt"],
                errors="coerce"
            )


        if "KafkaOffset" in df.columns:

            df = (
                df
                .sort_values(
                    "KafkaOffset",
                    ascending=True,
                    na_position="first"
                )
                .reset_index(
                    drop=True
                )
            )

        elif "ProcessedAt" in df.columns:

            df = (
                df
                .sort_values(
                    "ProcessedAt",
                    ascending=True
                )
                .reset_index(
                    drop=True
                )
            )

        return df


    except Exception as error:

        st.error(
            f"Không thể đọc Delta Lake: {error}"
        )

        return pd.DataFrame()


# =========================================================
# ĐỌC TEMPLATE EVENT
# =========================================================

@st.cache_data(show_spinner=False)
def load_templates():

    try:

        return pd.read_csv(
            TEMPLATE_PATH
        )

    except Exception as error:

        st.error(
            f"Không thể đọc HDFS.log_templates.csv: {error}"
        )

        return pd.DataFrame()


# =========================================================
# XÁC ĐỊNH CỘT TEMPLATE
# =========================================================

def template_columns(template_df):

    event_col = None
    template_col = None

    for column in template_df.columns:

        name = column.lower()

        if name in [
            "eventid",
            "event_id",
            "event"
        ]:
            event_col = column

        if "template" in name:
            template_col = column


    if event_col is None:
        event_col = template_df.columns[0]


    if template_col is None:
        template_col = template_df.columns[-1]


    return event_col, template_col


# =========================================================
# LẤY TEMPLATE CỦA EVENT
# =========================================================

def get_template(
    event_id,
    template_df
):

    if template_df.empty:
        return ""


    event_col, template_col = (
        template_columns(
            template_df
        )
    )


    matched = template_df[
        template_df[event_col].astype(str)
        == str(event_id)
    ]


    if matched.empty:
        return ""


    return str(
        matched.iloc[0][template_col]
    )


# =========================================================
# TÁCH EVENT
# =========================================================

def parse_events(features):

    if pd.isna(features):
        return []


    return re.findall(
        r"E\d+",
        str(features)
    )


# =========================================================
# CHUỖI EVENT
# =========================================================

def event_text(features):

    events = parse_events(
        features
    )


    if not events:
        return "-"


    return " → ".join(
        events
    )


# =========================================================
# SỐ EVENT
# =========================================================

def event_number(event_id):

    match = re.search(
        r"\d+",
        str(event_id)
    )


    if match:
        return int(
            match.group()
        )


    return 999


# =========================================================
# TEXT AN TOÀN
# =========================================================

def safe_text(value):

    if pd.isna(value):
        return "-"


    text = str(
        value
    )


    if text.lower() == "nan":
        return "-"


    return text


# =========================================================
# GỢI Ý KIỂM TRA
# =========================================================

def recommendation_from_template(template):

    text = str(
        template
    ).lower()


    if (
        "exception" in text
        or "error" in text
    ):

        return (
            "Kiểm tra exception tương ứng trong log "
            "DataNode/NameNode và các Event trước/sau."
        )


    if (
        "timeout" in text
        or "timed out" in text
    ):

        return (
            "Kiểm tra kết nối NameNode/DataNode, "
            "trạng thái DataNode và cơ chế replication."
        )


    if (
        "replicat" in text
        or "replica" in text
    ):

        return (
            "Kiểm tra replication factor, số replica "
            "và DataNode đang giữ block."
        )


    if (
        "write" in text
        or "receiv" in text
    ):

        return (
            "Kiểm tra quá trình ghi/nhận block, "
            "kết nối DataNode và dung lượng lưu trữ."
        )


    if (
        "delete" in text
        or "deleting" in text
    ):

        return (
            "Kiểm tra thao tác xóa block "
            "và trạng thái replica."
        )


    return (
        "Kiểm tra log NameNode/DataNode liên quan "
        "đến Event này và chuỗi Event trước/sau."
    )


# =========================================================
# LOAD TEMPLATE
# =========================================================

template_df = load_templates()


# =========================================================
# MENU ☰
# =========================================================

with st.popover(
    "☰"
):

    st.markdown(
        "### Điều hướng"
    )


    if st.button(
        "🏠 Tổng quan",
        use_container_width=True
    ):

        st.session_state.page = "Tổng quan"

        st.rerun()


    if st.button(
        "📡 Log Streaming",
        use_container_width=True
    ):

        st.session_state.page = "Log Streaming"

        st.rerun()


    if st.button(
        "🚨 Block ID bất thường",
        use_container_width=True
    ):

        st.session_state.page = "Block ID bất thường"

        st.rerun()


    if st.button(
        "📊 Thống kê Event",
        use_container_width=True
    ):

        st.session_state.page = "Thống kê Event"

        st.rerun()


    if st.button(
        "📖 Event E1–E29",
        use_container_width=True
    ):

        st.session_state.page = "Event E1–E29"

        st.rerun()


# =========================================================
# TRANG 1 - TỔNG QUAN
# =========================================================

if st.session_state.page == "Tổng quan":

    st.title(
        "HDFS Anomaly Detection Dashboard"
    )


    st.caption(
        "Giám sát và phát hiện bất thường "
        "log HDFS theo thời gian thực"
    )


    st.markdown(
        """
        <div class="system-running">
        🟢 Hệ thống đang giám sát —
        Producer gửi 1 Block ID / 5 giây
        </div>
        """,
        unsafe_allow_html=True
    )


    @st.fragment(
        run_every=REFRESH_SECONDS
    )
    def overview_kpi():

        df = load_delta_data()


        if df.empty:

            c1, c2, c3, c4 = st.columns(
                4
            )


            c1.metric(
                "Tổng Block ID",
                0
            )


            c2.metric(
                "Normal",
                0
            )


            c3.metric(
                "Anomaly",
                0
            )


            c4.metric(
                "Tỷ lệ bất thường",
                "0.00%"
            )


            return


        total = len(
            df
        )


        normal = (
            df["PredictionLabel"]
            .eq("Normal")
            .sum()
        )


        anomaly = (
            df["PredictionLabel"]
            .eq("Anomaly")
            .sum()
        )


        anomaly_rate = (
            anomaly
            / total
            * 100
            if total > 0
            else 0
        )


        c1, c2, c3, c4 = st.columns(
            4
        )


        c1.metric(
            "Tổng Block ID",
            f"{total:,}"
        )


        c2.metric(
            "Normal",
            f"{normal:,}"
        )


        c3.metric(
            "Anomaly",
            f"{anomaly:,}"
        )


        c4.metric(
            "Tỷ lệ bất thường",
            f"{anomaly_rate:.2f}%"
        )


    overview_kpi()


    st.divider()


    # =====================================================
    # BLOCK ID ĐANG XỬ LÝ
    # =====================================================

    @st.fragment(
        run_every=REFRESH_SECONDS
    )
    def current_block():

        st.subheader(
            "Block ID đang xử lý"
        )


        df = load_delta_data()


        if df.empty:

            st.info(
                "Đang chờ Block ID..."
            )

            return


        latest = df.iloc[
            -1
        ]


        st.markdown(
            f"""
            <div class="block-card">

            <b>Block ID:</b>
            {safe_text(latest["BlockId"])}

            <br><br>

            <b>Events:</b>
            {event_text(latest["Features"])}

            <br><br>

            <b>Nhãn gốc:</b>
            {safe_text(latest["Label"])}

            <br><br>

            <b>Kết quả dự đoán:</b>
            {safe_text(latest["PredictionLabel"])}

            </div>
            """,
            unsafe_allow_html=True
        )


    current_block()


    # =====================================================
    # BIỂU ĐỒ NORMAL / ANOMALY
    # =====================================================

    @st.fragment(
        run_every=REFRESH_SECONDS
    )
    def prediction_chart():

        st.subheader(
            "Phân bố Normal / Anomaly"
        )


        df = load_delta_data()


        if df.empty:

            st.info(
                "Chưa có dữ liệu."
            )

            return


        distribution = (
            df["PredictionLabel"]
            .value_counts()
            .rename_axis(
                "Trạng thái"
            )
            .reset_index(
                name="Số lượng"
            )
        )


        st.bar_chart(
            distribution
            .set_index(
                "Trạng thái"
            )
        )


    prediction_chart()


# =========================================================
# TRANG 2 - LOG STREAMING
# =========================================================

elif st.session_state.page == "Log Streaming":

    st.title(
        "Log Streaming"
    )


    st.caption(
        "Các Block ID đã được Spark Structured "
        "Streaming xử lý. Block mới nhất nằm trên cùng."
    )


    @st.fragment(
        run_every=REFRESH_SECONDS
    )
    def log_streaming_fragment():

        df = load_delta_data()


        if df.empty:

            st.info(
                "Đang chờ dữ liệu từ "
                "Spark Structured Streaming..."
            )

            return


        st.metric(
            "Tổng bản ghi đã xử lý",
            len(df)
        )


        stream_df = (
            df
            .sort_values(
                "KafkaOffset",
                ascending=False
            )
            .head(
                100
            )
            .copy()
        )


        stream_df[
            "Events"
        ] = stream_df[
            "Features"
        ].apply(
            event_text
        )


        st.dataframe(
            stream_df[
                [
                    "BlockId",
                    "Events",
                    "Label",
                    "PredictionLabel",
                    "KafkaOffset"
                ]
            ],
            use_container_width=True,
            height=650,
            hide_index=True
        )


    log_streaming_fragment()


# =========================================================
# TRANG 3 - BLOCK ID BẤT THƯỜNG
# =========================================================

elif st.session_state.page == "Block ID bất thường":

    st.title(
        "Block ID bất thường"
    )


    st.caption(
        "Danh sách các Block ID được mô hình "
        "Random Forest phát hiện là Anomaly."
    )


    @st.fragment(
        run_every=REFRESH_SECONDS
    )
    def anomaly_page():

        df = load_delta_data()


        if df.empty:

            st.info(
                "Chưa có dữ liệu."
            )

            return


        anomaly_df = (
            df[
                df["PredictionLabel"]
                == "Anomaly"
            ]
            .sort_values(
                "KafkaOffset",
                ascending=False
            )
            .copy()
        )


        anomaly_count = len(
            anomaly_df
        )


        total_count = len(
            df
        )


        anomaly_rate = (
            anomaly_count
            / total_count
            * 100
            if total_count > 0
            else 0
        )


        c1, c2 = st.columns(
            2
        )


        c1.metric(
            "Tổng Block ID bất thường",
            anomaly_count
        )


        c2.metric(
            "Tỷ lệ bất thường",
            f"{anomaly_rate:.2f}%"
        )


        if anomaly_df.empty:

            st.success(
                "Chưa phát hiện Block ID bất thường."
            )

            return


        anomaly_df[
            "Events"
        ] = anomaly_df[
            "Features"
        ].apply(
            event_text
        )


        st.subheader(
            "Danh sách Block ID bất thường"
        )


        st.dataframe(
            anomaly_df[
                [
                    "BlockId",
                    "Events",
                    "Label",
                    "PredictionLabel"
                ]
            ],
            use_container_width=True,
            height=650,
            hide_index=True
        )


    anomaly_page()


# =========================================================
# TRANG 4 - THỐNG KÊ EVENT
# =========================================================

elif st.session_state.page == "Thống kê Event":

    st.title(
        "Thống kê Event theo Block ID"
    )


    st.caption(
        "Chọn một Block ID để xem các Event "
        "xuất hiện trong Block đó."
    )


    df = load_delta_data()


    if df.empty:

        st.warning(
            "Chưa có Block ID."
        )


    else:

        block_options = (
            df
            .sort_values(
                "KafkaOffset",
                ascending=False
            )[
                "BlockId"
            ]
            .astype(str)
            .drop_duplicates()
            .tolist()
        )


        selected_block = st.selectbox(
            "Chọn Block ID",
            options=block_options,
            key="selected_event_block"
        )


        selected_df = (
            df[
                df["BlockId"]
                .astype(str)
                == selected_block
            ]
            .sort_values(
                "KafkaOffset",
                ascending=False
            )
        )


        selected_row = (
            selected_df.iloc[
                0
            ]
        )


        events = parse_events(
            selected_row[
                "Features"
            ]
        )


        counts = Counter(
            events
        )


        c1, c2, c3, c4 = st.columns(
            4
        )


        c1.metric(
            "Kết quả",
            safe_text(
                selected_row[
                    "PredictionLabel"
                ]
            )
        )


        c2.metric(
            "Nhãn gốc",
            safe_text(
                selected_row[
                    "Label"
                ]
            )
        )


        c3.metric(
            "Số loại Event",
            len(counts)
        )


        c4.metric(
            "Tổng Event",
            len(events)
        )


        st.subheader(
            "Trình tự Event"
        )


        st.code(
            " → ".join(
                events
            )
            if events
            else "Không có Event"
        )


        st.subheader(
            "Tần suất Event trong Block ID"
        )


        unique_events = list(
            dict.fromkeys(
                events
            )
        )


        frequency_rows = []


        for event_id in unique_events:

            frequency_rows.append(
                {
                    "Event":
                        event_id,

                    "Số lần xuất hiện":
                        counts[
                            event_id
                        ],

                    "Mô tả Event":
                        get_template(
                            event_id,
                            template_df
                        )
                }
            )


        frequency_df = pd.DataFrame(
            frequency_rows
        )


        if not frequency_df.empty:

            frequency_df[
                "_EventNumber"
            ] = frequency_df[
                "Event"
            ].apply(
                event_number
            )


            frequency_df = (
                frequency_df
                .sort_values(
                    "_EventNumber"
                )
                .drop(
                    columns=[
                        "_EventNumber"
                    ]
                )
                .reset_index(
                    drop=True
                )
            )


            block_event_order = (
                frequency_df[
                    "Event"
                ]
                .tolist()
            )


            block_event_chart = (
                alt.Chart(
                    frequency_df
                )
                .mark_bar()
                .encode(

                    x=alt.X(
                        "Event:N",
                        sort=block_event_order,
                        title="Event",
                        axis=alt.Axis(
                            labelAngle=0
                        )
                    ),

                    y=alt.Y(
                        "Số lần xuất hiện:Q",
                        title="Số lần xuất hiện"
                    ),

                    tooltip=[
                        alt.Tooltip(
                            "Event:N",
                            title="Event"
                        ),

                        alt.Tooltip(
                            "Số lần xuất hiện:Q",
                            title="Số lần xuất hiện"
                        )
                    ]
                )
                .properties(
                    height=350
                )
            )


            st.altair_chart(
                block_event_chart,
                use_container_width=True
            )


            st.dataframe(
                frequency_df,
                use_container_width=True,
                hide_index=True
            )


        if (
            selected_row[
                "PredictionLabel"
            ]
            == "Anomaly"
        ):

            st.subheader(
                "Gợi ý kiểm tra bất thường"
            )


            st.warning(
                "Block ID này được mô hình "
                "phát hiện là Anomaly."
            )


            anomaly_events = sorted(
                unique_events,
                key=event_number
            )


            for event_id in anomaly_events:

                template = get_template(
                    event_id,
                    template_df
                )


                count = counts[
                    event_id
                ]


                with st.expander(
                    f"{event_id} — "
                    f"xuất hiện {count} lần"
                ):

                    st.write(
                        "**Mẫu log:**"
                    )


                    st.write(
                        template
                    )


                    st.write(
                        "**Gợi ý kiểm tra:**"
                    )


                    st.write(
                        recommendation_from_template(
                            template
                        )
                    )


# =========================================================
# TRANG 5 - EVENT E1-E29
# =========================================================

elif st.session_state.page == "Event E1–E29":

    st.title(
        "Danh mục Event E1–E29"
    )


    st.caption(
        "Mô tả Event và số lần xuất hiện "
        "trong luồng dữ liệu realtime."
    )


    @st.fragment(
        run_every=REFRESH_SECONDS
    )
    def realtime_event_catalog():

        df = load_delta_data()


        if template_df.empty:

            st.warning(
                "Không đọc được "
                "HDFS.log_templates.csv."
            )

            return


        counter = Counter()


        if not df.empty:

            for features in df[
                "Features"
            ]:

                counter.update(
                    parse_events(
                        features
                    )
                )


        event_col, template_col = (
            template_columns(
                template_df
            )
        )


        display_df = template_df[
            [
                event_col,
                template_col
            ]
        ].copy()


        display_df.columns = [
            "Event",
            "Mô tả Event"
        ]


        display_df[
            "Số lần xuất hiện realtime"
        ] = (
            display_df[
                "Event"
            ]
            .map(
                counter
            )
            .fillna(
                0
            )
            .astype(
                int
            )
        )


        display_df[
            "_EventNumber"
        ] = (
            display_df[
                "Event"
            ]
            .str.extract(
                r"(\d+)"
            )[0]
            .astype(
                int
            )
        )


        display_df = (
            display_df
            .sort_values(
                "_EventNumber",
                ascending=True
            )
            .drop(
                columns=[
                    "_EventNumber"
                ]
            )
            .reset_index(
                drop=True
            )
        )


        total_events = int(
            display_df[
                "Số lần xuất hiện realtime"
            ]
            .sum()
        )


        c1, c2, c3 = st.columns(
            3
        )


        c1.metric(
            "Tổng số loại Event",
            len(
                display_df
            )
        )


        c2.metric(
            "Block ID đã xử lý",
            len(
                df
            )
        )


        c3.metric(
            "Tổng Event đã xuất hiện",
            total_events
        )


        st.subheader(
            "Tần suất Event theo thời gian thực"
        )


        event_order = [
            f"E{i}"
            for i in range(
                1,
                30
            )
        ]


        event_chart = (
            alt.Chart(
                display_df
            )
            .mark_bar()
            .encode(

                x=alt.X(
                    "Event:N",
                    sort=event_order,
                    title="Event",

                    axis=alt.Axis(
                        labelAngle=0,
                        values=event_order
                    )
                ),

                y=alt.Y(
                    "Số lần xuất hiện realtime:Q",
                    title="Số lần xuất hiện"
                ),

                tooltip=[
                    alt.Tooltip(
                        "Event:N",
                        title="Event"
                    ),

                    alt.Tooltip(
                        "Số lần xuất hiện realtime:Q",
                        title="Số lần xuất hiện"
                    )
                ]
            )
            .properties(
                height=420
            )
        )


        st.altair_chart(
            event_chart,
            use_container_width=True
        )


        st.subheader(
            "Chi tiết E1–E29"
        )


        st.dataframe(
            display_df,
            use_container_width=True,
            height=700,
            hide_index=True
        )


    realtime_event_catalog()
