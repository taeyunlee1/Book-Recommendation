import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from logger import logger

class DataTransformation:
    @staticmethod
    def transform(books, ratings):
        logger.info("Filtering users and books...")

        # Keep only users who rated at least 10 books
        user_counts = ratings['User-ID'].value_counts()
        ratings = ratings[ratings['User-ID'].isin(user_counts[user_counts >= 10].index)]

        # Keep only books with at least 10 ratings
        book_counts = ratings['ISBN'].value_counts()
        ratings = ratings[ratings['ISBN'].isin(book_counts[book_counts >= 10].index)]

        logger.info(f"Filtered to {len(ratings)} ratings from active users and popular books.")

        # Merge titles into ratings
        ratings = ratings.merge(books[['ISBN', 'Book-Title']], on='ISBN')

        logger.info("Creating user-book matrix...")
        user_book_matrix = ratings.pivot_table(index='User-ID', columns='Book-Title', values='Book-Rating').fillna(0)

        logger.info("Computing cosine similarity matrix...")
        similarity = cosine_similarity(user_book_matrix.T)
        similarity_df = pd.DataFrame(similarity, index=user_book_matrix.columns, columns=user_book_matrix.columns)

        logger.info("Data transformation complete.")
        return similarity_df
