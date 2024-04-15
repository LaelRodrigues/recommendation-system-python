import csv
import math
import threading
import time
from collections import defaultdict

user_id_to_idx = {}
book_title_to_idx = {}
book_titles = []
ratings_matrix = defaultdict(dict)
lock = threading.Lock()  



def load_data_threaded(arquivo, start, end):
    print(f"Thread {threading.current_thread().name} iniciada")
    with open(arquivo, newline='') as csvfile:
        reader = csv.reader(csvfile)
        user_index = start
        book_index = start
        for _ in range(start):
            next(reader)  
        for _ in range(start, end):
            row = next(reader)
            user_id, rating, book_title = row
            with lock:
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
    print(f"Thread {threading.current_thread().name} terminada")

def load_data(arquivo):
    with open(arquivo, newline='') as csvfile:
        reader = csv.reader(csvfile)
        num_lines = sum(1 for _ in reader)

    chunk_size = (num_lines + 3) // 4
    threads = []
    for i in range(4):
        start = i * chunk_size
        end = min((i + 1) * chunk_size, num_lines)
        thread = threading.Thread(target=load_data_threaded, args=(arquivo, start, end))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
        
        
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

def processRecommendations(user_id, k):
    recommended_books = recommend_books(user_id, k)
    print("Recomendações de livros para o usuário", user_id + ":")
    for i, (book_title, rating) in enumerate(recommended_books):
        print(i + 1, ".", book_title, " - Rating:", rating)    

if __name__ == "__main__":

    start_time = time.time()
    
    load_data("teste3.csv")

    print(len(ratings_matrix))

    end_time = time.time()
    print("Tempo de resposta: ",  round((end_time - start_time) * 1e3)," milisegundos")    

    users = ['A3UH4UZ4RSVO82', 'A3UH4UZ4RSVO82', 'A3UH4UZ4RSVO82', 'A3UH4UZ4RSVO82']

    user_id = "A3UH4UZ4RSVO82"
    k = 5

    start_time = time.time()

    threads = []
    for user_id in users:
        thread = threading.Thread(target=processRecommendations, args=(user_id, k))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
  
    end_time = time.time()
    print("Tempo de reposta: ", round((end_time - start_time) * 1e3), "milisegundos")
