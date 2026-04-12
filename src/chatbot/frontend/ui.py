import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import json

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Lakehouse AI Analyst",
    page_icon="🏔️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    /* ── Global text — đảm bảo text luôn sáng trên nền dark ── */
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] li,
    [data-testid="stMarkdownContainer"] ul li,
    [data-testid="stMarkdownContainer"] ol li {
        color: #cbd5e1 !important;
        line-height: 1.75;
    }
    [data-testid="stMarkdownContainer"] h1,
    [data-testid="stMarkdownContainer"] h2,
    [data-testid="stMarkdownContainer"] h3,
    [data-testid="stMarkdownContainer"] h4 {
        color: #c7d2fe !important;
        margin-top: 8px;
        margin-bottom: 4px;
    }
    [data-testid="stMarkdownContainer"] strong,
    [data-testid="stMarkdownContainer"] b {
        color: #a78bfa !important;
    }

    /* ── Chat header ── */
    .chat-header {
        background: linear-gradient(135deg, #1a1f36 0%, #0d1b2a 100%);
        border: 1px solid rgba(99,102,241,0.3);
        border-radius: 16px;
        padding: 20px 28px;
        margin-bottom: 20px;
    }
    .chat-header h1 {
        color: #e2e8f0 !important;
        font-size: 1.6rem; margin: 0; font-weight: 700;
    }
    .chat-header p {
        color: #94a3b8 !important;
        margin: 4px 0 0 0; font-size: 0.9rem;
    }

    /* ── Report section label ── */
    .report-label {
        font-size: 0.72rem;
        color: #6366f1 !important;
        font-weight: 700;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        margin: 12px 0 2px 2px;
    }

    /* ── KPI card ── */
    .kpi-card {
        background: linear-gradient(135deg, #312e81 0%, #1e1b4b 100%);
        border: 1px solid rgba(167,139,250,0.4);
        border-radius: 16px;
        padding: 28px;
        text-align: center;
        margin: 8px 0;
    }
    .kpi-value {
        font-size: 2.8rem; font-weight: 700;
        color: #a78bfa !important; line-height: 1.2;
    }
    .kpi-label {
        color: #94a3b8 !important;
        font-size: 0.9rem; margin-top: 8px;
    }

    /* ── Meta pills ── */
    .meta-pill {
        display: inline-block;
        background: rgba(99,102,241,0.15);
        border: 1px solid rgba(99,102,241,0.3);
        color: #818cf8 !important;
        padding: 3px 10px;
        border-radius: 20px;
        font-size: 0.78rem;
        margin: 2px 3px;
    }

    /* ── Input box ── */
    .stTextInput > div > div > input {
        background: #1e293b !important;
        border: 1px solid rgba(99,102,241,0.4) !important;
        border-radius: 12px !important;
        color: #e2e8f0 !important;
    }

    /* ── Sidebar text ── */
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] p {
        color: #cbd5e1 !important;
    }

    /* ── Expander ── */
    [data-testid="stExpander"] summary {
        color: #94a3b8 !important;
    }
</style>
""", unsafe_allow_html=True)

API_URL = "http://localhost:8000"

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding: 16px 0 8px 0;'>
        <span style='font-size: 2.5rem;'>🏔️</span>
        <h2 style='color: #e2e8f0; margin: 8px 0 4px 0; font-size: 1.2rem;'>Lakehouse AI Analyst</h2>
        <p style='color: #64748b; font-size: 0.8rem; margin: 0;'>Powered by Agent + RAG</p>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    # Backend status
    try:
        resp = requests.get(f"{API_URL}/health", timeout=3)
        status_ok = resp.ok and resp.json().get("status") == "ok"
    except Exception:
        status_ok = False

    if status_ok:
        st.success("🟢 Backend Online")
    else:
        st.error("🔴 Backend Offline")

    st.divider()

    st.markdown("**💡 Câu hỏi mẫu:**")
    sample_questions = [
        "Doanh thu theo từng tháng năm 2018?",
        "Top 10 danh mục sản phẩm bán chạy nhất?",
        "Tỷ lệ các phương thức thanh toán?",
        "Trạng thái đơn hàng phân phối như thế nào?",
        "Tổng doanh thu của toàn bộ hệ thống?",
        "Seller nào có doanh thu cao nhất?",
        "Thời gian giao hàng trung bình theo tháng?",
    ]
    for q in sample_questions:
        if st.button(q, use_container_width=True, key=f"sample_{q[:20]}"):
            st.session_state["pending_question"] = q

    st.divider()

    if st.button("🔄 Re-index Schema", use_container_width=True):
        with st.spinner("Đang re-index..."):
            try:
                r = requests.post(f"{API_URL}/index-schema?force_rebuild=true", timeout=60)
                if r.json().get("success"):
                    st.success("Schema đã được re-index!")
                else:
                    st.error(r.json().get("error", "Lỗi"))
            except Exception as e:
                st.error(f"Lỗi: {e}")

    show_debug = st.toggle("🔍 Hiện Debug Panel", value=False)

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class='chat-header'>
    <h1>🏔️ Lakehouse AI Analyst</h1>
    <p>Đặt câu hỏi bằng tiếng Việt — AI sẽ phân tích và trực quan hóa dữ liệu cho bạn</p>
</div>
""", unsafe_allow_html=True)

# ── Chat history ───────────────────────────────────────────────────────────────
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


# ── Render chart ──────────────────────────────────────────────────────────────
def render_chart(chart_config: dict, df: pd.DataFrame):
    """Render Plotly chart dựa trên chart_config từ backend"""
    if not chart_config or df.empty:
        return

    chart_type = chart_config.get("chart_type", "table")
    x          = chart_config.get("x")
    y          = chart_config.get("y")
    color      = chart_config.get("color")
    title      = chart_config.get("title", "")
    x_label    = chart_config.get("x_label") or x or ""
    y_label    = chart_config.get("y_label") or (y if isinstance(y, str) else "")

    plotly_theme = "plotly_dark"
    fig = None

    try:
        if chart_type == "line":
            fig = px.line(
                df, x=x, y=y, color=color, title=title,
                labels={x: x_label, (y if isinstance(y, str) else "_"): y_label},
                template=plotly_theme, markers=True,
                color_discrete_sequence=px.colors.qualitative.Vivid,
            )

        elif chart_type in ("bar", "bar_vertical"):
            fig = px.bar(
                df, x=x, y=y, color=color, title=title,
                labels={x: x_label, (y if isinstance(y, str) else "_"): y_label},
                template=plotly_theme,
                color_discrete_sequence=px.colors.qualitative.Vivid,
            )

        elif chart_type == "bar_horizontal":
            fig = px.bar(
                df, x=y, y=x, color=color, title=title,
                orientation="h", template=plotly_theme,
                color_discrete_sequence=px.colors.qualitative.Vivid,
            )

        elif chart_type == "pie":
            names_col  = x or (df.columns[0] if len(df.columns) > 0 else None)
            values_col = y or (df.columns[1] if len(df.columns) > 1 else None)
            if names_col and values_col:
                fig = px.pie(
                    df, names=names_col, values=values_col, title=title,
                    template=plotly_theme,
                    color_discrete_sequence=px.colors.qualitative.Vivid,
                    hole=0.35,
                )

        elif chart_type == "scatter":
            fig = px.scatter(
                df, x=x, y=y, color=color, title=title,
                template=plotly_theme,
                color_discrete_sequence=px.colors.qualitative.Vivid,
            )

        elif chart_type == "kpi":
            kpi_val = chart_config.get("kpi_value")
            if kpi_val is None and not df.empty:
                kpi_val = df.iloc[0, 0]
            try:
                num = float(kpi_val)
                if num >= 1_000_000:
                    display = f"{num / 1_000_000:,.2f}M"
                elif num >= 1_000:
                    display = f"{num:,.0f}"
                else:
                    display = f"{num:,.2f}"
            except Exception:
                display = str(kpi_val)

            st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-value'>{display}</div>
                <div class='kpi-label'>{title}</div>
            </div>
            """, unsafe_allow_html=True)
            return

        if fig:
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(15,17,23,0.6)",
                font_color="#cbd5e1",
                title_font_size=14,
                font_family="Inter",
                margin=dict(l=20, r=20, t=40, b=20),
            )
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.warning(f"Không thể render chart: {e}")


# ── Render assistant message ──────────────────────────────────────────────────
def render_assistant_message(msg: dict):
    """Render 1 tin nhắn assistant từ history"""
    intent = msg.get("intent", "data_query")

    if intent != "data_query":
        st.markdown(msg.get("report", ""))
        return

    # ── Report / Analysis ─────────────────────────────────────────────────
    report = msg.get("report", "")
    if report:
        st.markdown("<div class='report-label'>📝 Phân tích &amp; Báo cáo</div>",
                    unsafe_allow_html=True)
        # Dùng st.container để tạo vùng và st.markdown để Streamlit xử lý markdown đúng
        with st.container():
            st.markdown(report)

    # ── Chart ─────────────────────────────────────────────────────────────
    chart_config = msg.get("chart_config")
    rows         = msg.get("rows", [])
    columns      = msg.get("columns", [])
    if rows and columns:
        df = pd.DataFrame(rows, columns=columns)
        df = df.dropna(how="all")

        if chart_config and chart_config.get("chart_type") != "table":
            st.markdown("**📊 Biểu đồ**")
            render_chart(chart_config, df)

        with st.expander("📋 Xem bảng dữ liệu", expanded=False):
            st.dataframe(df, use_container_width=True, height=250)

    # ── Metadata pills ─────────────────────────────────────────────────────
    schemas    = msg.get("schemas_used", [])
    pruned     = msg.get("columns_pruned", 0)
    row_count  = msg.get("row_count", 0)
    if schemas:
        pills_html = "".join(
            f"<span class='meta-pill'>🗃️ {s.split('.')[-1]}</span>" for s in schemas
        )
        if pruned:
            pills_html += f"<span class='meta-pill'>✂️ {pruned} cột đã prune</span>"
        if row_count:
            pills_html += f"<span class='meta-pill'>📊 {row_count} dòng</span>"
        st.markdown(pills_html, unsafe_allow_html=True)


# ── Debug panel ───────────────────────────────────────────────────────────────
def render_debug_panel(msg: dict):
    sql = msg.get("sql")
    log = msg.get("execution_log", [])
    if sql:
        st.markdown("**🔍 SQL được sinh**")
        st.code(sql, language="sql")
    if log:
        with st.expander("📜 Execution Log", expanded=False):
            for entry in log:
                st.text(entry)


# ── Render lịch sử ────────────────────────────────────────────────────────────
for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        if message["role"] == "assistant":
            render_assistant_message(message)
            if show_debug:
                render_debug_panel(message)
        else:
            st.markdown(message["content"])

# ── Sample question từ sidebar ────────────────────────────────────────────────
if "pending_question" in st.session_state:
    user_input = st.session_state.pop("pending_question")
else:
    user_input = st.chat_input("Nhập câu hỏi phân tích dữ liệu...")

# ── Xử lý input ───────────────────────────────────────────────────────────────
if user_input:
    st.session_state.chat_history.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("🤔 Đang phân tích..."):
            try:
                resp = requests.post(
                    f"{API_URL}/chat",
                    json={"query": user_input},
                    timeout=300,
                )
                resp.raise_for_status()
                data = resp.json()

                msg = {"role": "assistant", **data}
                render_assistant_message(msg)
                if show_debug:
                    render_debug_panel(msg)

                st.session_state.chat_history.append(msg)

            except requests.exceptions.ConnectionError:
                err = "❌ Không kết nối được backend. Kiểm tra uvicorn đang chạy trên port 8000."
                st.error(err)
                st.session_state.chat_history.append({
                    "role": "assistant", "report": err, "intent": "out_of_scope",
                })
            except Exception as e:
                err = f"❌ Lỗi: {str(e)}"
                st.error(err)
                st.session_state.chat_history.append({
                    "role": "assistant", "report": err, "intent": "out_of_scope",
                })
