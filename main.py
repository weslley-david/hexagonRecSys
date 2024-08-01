from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Recuperar a URL do banco de dados do ambiente
DATABASE_URL = os.getenv("DATABASE_URL")

# Criar o engine do SQLAlchemy
engine = create_engine(DATABASE_URL)

app = FastAPI()

class QueryParams(BaseModel):
    client: int
    avaliationid: int

def fetch_answers(client, avaliationid):
    query_answers = """
    WITH answers AS (
        SELECT
            avaliation.id AS avaliationid,
            avaliation.client,
            question.id AS questionid,
            item.score,
            avaliation.created_at AS timestamp
        FROM avaliation
        INNER JOIN answer ON avaliation.id = answer.avaliation
        INNER JOIN item ON item.id = answer.item
        INNER JOIN question ON question.id = answer.question
    )
    SELECT * FROM answers
    WHERE client != :client OR avaliationid = :avaliationid;
    """
    params = {'client': client, 'avaliationid': avaliationid}
    with engine.connect() as connection:
        result = connection.execute(text(query_answers), params)
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def fetch_questions():
    query_questions = """
    SELECT question.id AS questionid, question.number, question.content AS content, question.area
    FROM question;
    """
    with engine.connect() as connection:
        result = connection.execute(text(query_questions))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def fetch_evaluation_details(avaliation_id, client):
    query = """
    SELECT
        avaliation.id AS avaliationid,
        avaliation.client,
        question.id AS questionid,
        item.score,
        avaliation.created_at AS timestamp
    FROM avaliation
    INNER JOIN answer ON avaliation.id = answer.avaliation
    INNER JOIN item ON item.id = answer.item
    INNER JOIN question ON question.id = answer.question
    WHERE avaliation.id > :avaliation_id AND avaliation.client = :client
    LIMIT 77;
    """
    params = {'client': client, 'avaliation_id': avaliation_id}
    with engine.connect() as connection:
        result = connection.execute(text(query), params)
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def query_relation(client, avaliationid):
    query_relation = """
    SELECT * FROM avaliation WHERE avaliation.client = :client AND avaliation.id = :avaliationid;
    """
    params = {'client': client, 'avaliationid': avaliationid}
    with engine.connect() as connection:
        result = connection.execute(text(query_relation), params)
        return pd.DataFrame(result.fetchall(), columns=result.keys())

@app.get("/atec/recommend")
async def recommend_questions_route(avaliation: int, client: int):
    
    avaliationid = avaliation
    
    avaliation = query_relation(client, avaliationid)
    if avaliation.empty:
        raise HTTPException(status_code=404, detail="Avaliação não encontrada")

    df_primary_answers = fetch_answers(client=client, avaliationid=avaliationid)
    df_questions = fetch_questions()

    pivot_table = df_primary_answers.pivot_table(index='avaliationid', columns='questionid', values='score', fill_value=0)
    matrix = pivot_table.values
    similarity_matrix = cosine_similarity(matrix)
    similarity_df = pd.DataFrame(similarity_matrix, index=pivot_table.index, columns=pivot_table.index)

    similarity_scores = similarity_df.loc[avaliationid]
    top_similarities = similarity_scores.sort_values(ascending=False).head(5)
    top_similarities = top_similarities.drop(avaliationid, errors='ignore')
    similar_ids = top_similarities.index.tolist()

    clients_df = df_primary_answers[['avaliationid', 'client']].drop_duplicates()
    clients_df = clients_df.set_index('avaliationid')
    similar_clients = clients_df.loc[similar_ids]

    results_list = []
    for similar_client in similar_clients.itertuples():
        evaluation_details = fetch_evaluation_details(similar_client.Index, similar_client.client)
        if not evaluation_details.empty:
            results_list.append(evaluation_details)

    if results_list:
        combined_results = pd.concat(results_list, ignore_index=True)
        pivot_table_2 = combined_results.pivot_table(index='avaliationid', columns='questionid', values='score', fill_value=0)
        mean_scores = pivot_table_2.mean()
        evaluation_of_interest = df_primary_answers[df_primary_answers['avaliationid'] == avaliationid]
        pivot_avaliation_of_interest = evaluation_of_interest.pivot_table(index='avaliationid', columns='questionid', values='score', fill_value=0)
        evaluation_series = pivot_avaliation_of_interest.loc[avaliationid].squeeze()
        differences = mean_scores - evaluation_series
        filtered_differences = differences[differences < 0]
        sorted_filtered_differences = filtered_differences.sort_values(ascending=True)
        filtered_questions = df_questions[df_questions['questionid'].isin(sorted_filtered_differences.index)]
        return {"filtered_questions": filtered_questions.to_dict(orient='records')}
    else:
        return {"message": "Nenhum dado retornado para as avaliações similares."}
