from flask import Flask, request, send_file
from sqlalchemy import create_engine
import pandas as pd
from io import BytesIO
from sql_agent import generate_sql
from config import settings
import uuid
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__, static_folder='static')

limiter = Limiter(
    app,
    default_limits=["5 per minute"]
)

@app.route('/')
def index():
    return app.send_static_file('index.html')


@limiter.limit("5 per minute", key_func=get_remote_address)
@app.route('/query', methods=['POST'])
def query():
    pass
    request_data = request.json
    question = request_data.get('question', '').strip()
    token = request_data.get('token', '').strip()
    if not question:
        return None
    bio = BytesIO()
    if token != settings.AUTH_SECRET:
        message = {"message": ["Token错误"]}
        data = pd.DataFrame(message)
    else:
        conn = create_engine(
            f"mysql+mysqlconnector://{settings.DB_USERNAME}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DATABASE}")
        sql = generate_sql(question)
        filename = f"{uuid.uuid4().hex}.xlsx"
        data = pd.read_sql(sql, conn)
    data.to_excel(bio, index=False)
    bio.seek(0)
    return send_file(bio, download_name=filename)


if __name__ == '__main__':
    app.run(debug=True,port=5000)
