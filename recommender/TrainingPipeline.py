from recommender.Data_Ingenstion import DataIngestion
from recommender.Data_Validation import DataValidation
from recommender.DataTransformation import DataTransformation
from logger import logger

logger.info("Starting training pipeline...")

try:
    books, ratings, users = DataIngestion(
        "data/BX-Books.csv",
        "data/BX-Book-Ratings.csv",
        "data/BX-Users.csv"
    ).load_data()

    books, ratings, users = DataValidation.validate(books, ratings, users)
    similarity_df = DataTransformation.transform(books, ratings)

    logger.info("Training pipeline successfully completed.")
except Exception as e:
    logger.error(f"Pipeline execution failed: {e}")
    similarity_df = None

def get_recommendations(book_title, top_n=5):
    if similarity_df is None or book_title not in similarity_df:
        logger.warning(f"No recommendations found for '{book_title}'")
        return ["No recommendations found."]
    similar_books = similarity_df[book_title].sort_values(ascending=False)[1:top_n+1]
    return similar_books.index.tolist()
