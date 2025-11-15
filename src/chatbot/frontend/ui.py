import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="SQL AI Chat", layout="wide")

st.sidebar.markdown(
    """
    <div style="padding:10px; background-color:#f5f5f5; border-radius:10px; text-align:center;">
        <h2 style="color:#4B8BBE;">🧠 SQL AI Assistant</h2>
        <hr style="border:1px solid #ccc;">
        <h4>Hướng dẫn sử dụng</h4>
        <ol style="padding-left:20px;">
            <li>Nhập câu hỏi phân tích dữ liệu vào ô dưới.</li>
            <li>Nhấn Enter để gửi câu hỏi.</li>
            <li>Lịch sử chat được lưu tạm thời trong phiên làm việc.</li>
        </ol>
    </div>
    """,
    unsafe_allow_html=True
)

API_URL = "http://localhost:5000/chat"

try:
    test = requests.get(API_URL.replace("/chat", "/health"), timeout=2)
    backend_status = "🟢 Online" if test.ok else "🔴 Offline"
except:
    backend_status = "🔴 Offline"

status_color = "#28a745" if backend_status == "🟢 Online" else "#dc3545"
st.sidebar.markdown(
    f"<div style='padding:8px; background-color:{status_color}; color:white; border-radius:5px; text-align:center;'>"
    f"Trạng thái backend: {backend_status}</div>",
    unsafe_allow_html=True
)

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        if message["role"] == "assistant":
            sql_code = message.get("sql", "")
            rows = message.get("rows", [])
            if sql_code:
                st.code(sql_code, language="sql")
            if rows:
                df = pd.DataFrame(rows)
                df = df.dropna(how="all")
                st.dataframe(df, height=200)
            reply_text = message.get("reply_text", "")
            if reply_text:
                st.markdown(reply_text)
        else:
            st.markdown(message["content"])

if user_input := st.chat_input("Nhập câu hỏi phân tích dữ liệu..."):
    st.session_state.chat_history.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Đang trả lời..."):
            try:
                payload = {"query": user_input}
                response = requests.post(API_URL, json=payload, timeout=60)
                response.raise_for_status()
                data = response.json()

                sql_code = data.get("sql", "")
                rows = data.get("rows", [])
                reply_text = ""

                if sql_code:
                    st.code(sql_code, language="sql")

                if rows:
                    df = pd.DataFrame(rows)
                    st.dataframe(df, height=200)

            except Exception as e:
                reply_text = f"Lỗi kết nối backend: {str(e)}"
                st.markdown(reply_text)

            st.session_state.chat_history.append({
                "role": "assistant",
                "sql": sql_code,
                "rows": rows,
                "reply_text": reply_text
            })
