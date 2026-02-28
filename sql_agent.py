from langchain_classic.chains.sql_database.query import create_sql_query_chain
from langchain_community.utilities import SQLDatabase
from langchain.chat_models import init_chat_model
from langchain_core.prompts import PromptTemplate
from config import settings


class CustomSQLDatabase(SQLDatabase):
    def get_table_info(self, table_names=None):
        original_info = super().get_table_info(table_names)
        fk_notes = """
        -- Table Relationship Description：
        -- order_info.sd_id → kehu.id
        -- order_info.ck_id → cangku.id
        -- order_info.fhck_id → cangku.id
        """
        return '\n'.join([original_info, fk_notes])



def extract_sql_simple(text: str) -> str:
    start = text.find("```")
    if start == -1:
        return text.strip()
    # 找到第一个 ``` 之后的内容
    rest = text[start + 3:]
    # 移除可能的 "sql"
    if rest.lstrip().lower().startswith("sql"):
        rest = rest.lstrip()[3:].lstrip()
    # 找到结束 ```
    end = rest.find("```")
    if end != -1:
        sql = rest[:end]
    else:
        sql = rest  # 没有结束符，取全部
    return sql.strip()


def generate_sql(user_prompt: str):
    db = CustomSQLDatabase.from_uri(f"mysql+mysqlconnector://{settings.DB_USERNAME}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DATABASE}")
    model = init_chat_model("deepseek-chat")

    # user_prompt = "查询一下2026年2月份所有商品的销售额和销量，标头为商品货号，商品名，销售额，销量"

    custom_prompt = PromptTemplate.from_template(
        """
    You are a SQL expert. Based on the table schema below, write a correct and executable {dialect} query to answer the question.
    - ONLY output the SQL query. Do NOT include any explanations, markdown, or extra text.
    - NEVER add "LIMIT" clause unless explicitly requested by the user--IGNORE the top_k value: {top_k}.
    - Select ONLY the columns that are relevant to the question.
    - Always check tables first and use query_checker before running.
    - NO DML (INSERT/UPDATE/DELETE). Rewrite on errors.

    Schema:
    {table_info}

    Question: {input}
    """
    )
    sql_chain = create_sql_query_chain(
        llm=model,
        db=db,
        prompt=custom_prompt,
    )

    sql_query = sql_chain.invoke({"question": user_prompt})
    sql_query = extract_sql_simple(sql_query)

    return sql_query
