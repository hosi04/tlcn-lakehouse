from src.chatbot.backend.vertex_llm import get_vertex_llm
from src.chatbot.backend.prompt import SQL_SYSTEM_PROMPT
from src.chatbot.backend.trino_connector import trino_query

from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate

sql_prompt = PromptTemplate(
    input_variables=["question"],
    template=SQL_SYSTEM_PROMPT + "\nCâu hỏi người dùng: {question}\nSQL:"
)

llm = get_vertex_llm()
sql_chain = LLMChain(llm=llm, prompt=sql_prompt)

def generate_sql(question: str) -> str:
    sql = sql_chain.run({"question": question})
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
