from typing import List
from fastapi import FastAPI, HTTPException, Depends
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import pairwise_distances
import psycopg2
from dotenv import load_dotenv
import os

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Inicializa FastAPI
app = FastAPI()

# Função para conectar ao banco de dados e executar a consulta SQL
def fetch_data(query, params=None):
    try:
        # Configurações de conexão utilizando variáveis de ambiente
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        # Executando a consulta
        if params:
            df = pd.read_sql_query(query, conn, params=params)
        else:
            df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()
    return df

# Queries SQL
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
WHERE client != %s OR avaliationid = %s;
"""

query_relation = """
    select * from avaliation where avaliation.client = %s and avaliation.id = %s
"""

query_questions = """
SELECT question.id AS questionid, question.content AS content, question.area 
FROM question;
"""

# Buscando os dados diretamente do banco de dados
def fetch_answers(client, avaliation):
    answers = fetch_data(query_answers, (client, avaliation))
    return answers

def fetch_relation(client, avaliation):
    relation = fetch_data(query_relation, (client, avaliation))
    if relation.empty:
        raise HTTPException(status_code=404, detail=f"No relation between {client} client and avaliation {avaliation}")
    return relation

questions = fetch_data(query_questions)

# Criando uma matriz de avaliações de usuários e filmes
def create_question_ratings(answers):
    test_question_ratings = answers.pivot(index='avaliationid', columns='questionid', values='score').fillna(0)
    return test_question_ratings

# Calculando a matriz de similaridade entre usuários
def calculate_similarity(test_question_ratings):
    user_similarity = 1 - pairwise_distances(test_question_ratings, metric='cosine')
    return user_similarity

# Função para recomendar questões
def recommend_questions(test_id, test_question_ratings, user_similarity, questions, num_recommendations=5):
    if test_id not in test_question_ratings.index:
        raise HTTPException(status_code=404, detail=f"Test ID {test_id} not found.")

    test_ratings = test_question_ratings.loc[test_id]
    similar_users = user_similarity[test_question_ratings.index.get_loc(test_id)]

    similar_users_ids = test_question_ratings.index[np.argsort(similar_users)[::-1][1:4]]

    unrated_questions = test_ratings[test_ratings == 0].index

    similar_test_ratings = test_question_ratings.loc[similar_users_ids, unrated_questions]
    recommendation_scores = similar_test_ratings.mean(axis=0)

    recommended_questions = recommendation_scores.sort_values()

    top_recommendations = recommended_questions.head(num_recommendations)
    recommended_questions_info = questions.loc[top_recommendations.index, ['questionid', 'content', 'area']]

    recommended_questions_list = []
    for index, row in recommended_questions_info.iterrows():
        question_info = {
            "id": row['questionid'],
            "content": row['content'],
            "area": row['area']
        }
        recommended_questions_list.append(question_info)

    return recommended_questions_list

# Rota FastAPI para recomendar questões
@app.get("/atec/recommend")
async def recommend_questions_route(avaliation: int, client: int):
    try:
        fetch_relation(client, avaliation)
        answers = fetch_answers(client, avaliation)
        test_question_ratings = create_question_ratings(answers)
        user_similarity = calculate_similarity(test_question_ratings)
        recommended_questions = recommend_questions(avaliation, test_question_ratings, user_similarity, questions)
        return {"recommended_questions": recommended_questions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
