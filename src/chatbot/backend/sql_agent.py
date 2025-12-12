from src.chatbot.backend.gemini_llm import get_llm
from src.chatbot.backend.prompt import SQL_SYSTEM_PROMPT
from src.chatbot.backend.trino_connector import trino_query

from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser

sql_prompt = PromptTemplate(
    input_variables=["question"],
    template=SQL_SYSTEM_PROMPT + "\nCâu hỏi người dùng: {question}\nSQL:"
)

llm = get_llm()
sql_chain = sql_prompt | llm | StrOutputParser()

def generate_sql(question: str) -> str:
    sql = sql_chain.invoke({"question": question})
    sql = sql.replace("```sql", "").replace("```", "").replace(";", "").strip()
    print(sql)
    if not sql.lower().strip().startswith("select"):
        raise ValueError("LLM sinh SQL không hợp lệ.")
    return sql.strip()

def chatbot_sql(question: str):
    sql = generate_sql(question)
    df = trino_query(sql)

    return {
        "sql": sql,
        "rows": df.to_dict(orient="records"),
        "columns": df.columns.tolist()
    }