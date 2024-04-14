import csv
import math
import time
from collections import defaultdict

user_id_to_idx = {}
book_title_to_idx = {}
book_titles = []
ratings_matrix = defaultdict(dict)


def loadData(arquivo):
    with open(arquivo, newline='') as csvfile:
        reader = csv.reader(csvfile)
        user_index = 0
        book_index = 0
        for row in reader:
            user_id, rating, book_title = row
            if user_id not in user_id_to_idx:
                user_id_to_idx[user_id] = user_index
                user_index += 1
            if book_title not in book_title_to_idx:
                book_title_to_idx[book_title] = book_index
                book_titles.append(book_title)
                book_index += 1
            user_idx = user_id_to_idx[user_id]
            book_idx = book_title_to_idx[book_title]
            ratings_matrix[user_idx][book_idx] = float(rating)


def recommend_books(user_id, k):
    user_idx = user_id_to_idx.get(user_id, -1)
    if user_idx == -1:
        print("Usuário não encontrado.")
        return []

    user_ratings = ratings_matrix.get(user_idx, {})

    similarities = {}
    for other_user_idx, other_user_ratings in ratings_matrix.items():
        if other_user_idx != user_idx:
            similarity = calcular_similaridade_cosseno(user_ratings, other_user_ratings)
            if similarity > 0:
                similarities[other_user_idx] = similarity

    similar_users = sorted(similarities, key=similarities.get, reverse=True)

    recommended_books = []
    for similar_user_idx in similar_users:
        similar_user_ratings = ratings_matrix[similar_user_idx]
        for book_idx, rating in similar_user_ratings.items():
            if book_idx not in user_ratings:
                recommended_books.append((book_titles[book_idx], rating))
                if len(recommended_books) == k:
                    return recommended_books

    return recommended_books


def calcular_similaridade_cosseno(vector1, vector2):
    dot_product = 0
    norm_vector1 = 0
    norm_vector2 = 0

    for book_idx, rating1 in vector1.items():
        rating2 = vector2.get(book_idx, 0.0)
        dot_product += rating1 * rating2
        norm_vector1 += rating1 * rating1
        norm_vector2 += rating2 * rating2

    if norm_vector1 == 0 or norm_vector2 == 0:
        return 0

    return dot_product / (math.sqrt(norm_vector1) * math.sqrt(norm_vector2))


if __name__ == "__main__":

    start_time = time.time()
    
    loadData("teste3.csv")

    end_time = time.time()
    print("Tempo de resposta: ", round((end_time - start_time)) * 1e3," microssegundos")

    user_id = "A3UH4UZ4RSVO82"
    k = 5

    start_time = time.time()

    recommended_books = recommend_books(user_id, k)
    print("Recomendações de livros para o usuário", user_id + ":")
    for i, (book_title, rating) in enumerate(recommended_books):
        print(i + 1, ".", book_title, " - Rating:", rating)

    end_time = time.time()
    print("Tempo de resposta: ", round((end_time - start_time)) * 1e3, " microssegundos")
