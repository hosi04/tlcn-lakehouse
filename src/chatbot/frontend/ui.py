import streamlit as st
import requests
import pandas as pd
import json

st.set_page_config(
    page_title="Lakehouse AI Analyst",
    page_icon="🏔️",
    layout="wide",
    initial_sidebar_state="expanded",
)

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

with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding: 16px 0 8px 0;'>
        <span style='font-size: 2.5rem;'>🏔️</span>
        <h2 style='color: #e2e8f0; margin: 8px 0 4px 0; font-size: 1.2rem;'>Lakehouse AI Analyst</h2>
        <p style='color: #64748b; font-size: 0.8rem; margin: 0;'>Powered by Agent + RAG</p>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

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
        "Tổng doanh thu của toàn bộ hệ thống?",
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

st.markdown("""
<div class='chat-header'>
    <h1>🏔️ Lakehouse AI Analyst</h1>
    <p>Đặt câu hỏi bằng tiếng Việt — AI sẽ phân tích và trực quan hóa dữ liệu cho bạn</p>
</div>
""", unsafe_allow_html=True)

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []



def render_assistant_message(msg: dict, msg_idx: int = 0):
    intent = msg.get("intent", "data_query")

    if intent != "data_query":
        st.markdown(msg.get("direct_answer", msg.get("error_msg", "Tôi không thể xử lý câu hỏi này.")))
        return

    sql = msg.get("sql")
    if sql:
        st.markdown("**💻 SQL Query:**")
        st.code(sql, language="sql")

    rows    = msg.get("rows", [])
    columns = msg.get("columns", [])
    if rows and columns:
        df = pd.DataFrame(rows, columns=columns)
        df = df.dropna(how="all")
        with st.expander("📋 Xem bảng dữ liệu", expanded=False):
            st.dataframe(df, use_container_width=True, height=250)

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

for i, message in enumerate(st.session_state.chat_history):
    with st.chat_message(message["role"]):
        if message["role"] == "assistant":
            render_assistant_message(message, msg_idx=i)
            if show_debug:
                render_debug_panel(message)
        else:
            st.markdown(message["content"])

if "pending_question" in st.session_state:
    user_input = st.session_state.pop("pending_question")
else:
    user_input = st.chat_input("Nhập câu hỏi phân tích dữ liệu...")

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
                render_assistant_message(msg, msg_idx=len(st.session_state.chat_history))
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
