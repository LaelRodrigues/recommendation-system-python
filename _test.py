import pytest
from RecommenderBook import load_data, recommend_books

@pytest.fixture
def loadData():
    load_data("teste.csv")

@pytest.mark.parametrize("user_id", ["A3UH4UZ4RSVO82"])  
@pytest.mark.parametrize("k", [5])  
def test_recommend_books(benchmark, loadData, user_id, k):
    result = benchmark.pedantic(recommend_books, args=(user_id, k), rounds=5)
    assert result is not None


