import pandas as pd

class DataIngestion:
    def __init__(self, book_path, rating_path, user_path):
        self.book_path = book_path
        self.rating_path = rating_path
        self.user_path = user_path

    def load_data(self):
        books = pd.read_csv(self.book_path, sep=";", encoding="latin-1", on_bad_lines='skip')
        ratings = pd.read_csv(self.rating_path, sep=";", encoding="latin-1", on_bad_lines='skip')
        users = pd.read_csv(self.user_path, sep=";", encoding="latin-1", on_bad_lines='skip')

        books.columns = books.columns.str.strip().str.replace('"', '')
        ratings.columns = ratings.columns.str.strip().str.replace('"', '')
        users.columns = users.columns.str.strip().str.replace('"', '')

        return books, ratings, users
