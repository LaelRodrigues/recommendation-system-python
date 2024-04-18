import time
import pytest
from RecommenderBook import loadData, recommend_books

@pytest.fixture
def load_data():
    loadData("teste3.csv")

@pytest.mark.parametrize("user_id", ["A3UH4UZ4RSVO82"])  
@pytest.mark.parametrize("k", [5])  
def test_recommend_books(benchmark, load_data, user_id, k):
    result = benchmark.pedantic(recommend_books, args=(user_id, k), rounds=5)
    assert result is not None

