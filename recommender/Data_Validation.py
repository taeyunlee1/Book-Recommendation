from logger import logger

class DataValidation:
    @staticmethod
    def validate(books, ratings, users):
        logger.info("Validating datasets...")
        try:
            assert not books.empty, "Books data is empty!"
            assert not ratings.empty, "Ratings data is empty!"
            assert not users.empty, "Users data is empty!"

            required_book_cols = {'ISBN', 'Book-Title'}
            required_rating_cols = {'User-ID', 'ISBN', 'Book-Rating'}
            required_user_cols = {'User-ID'}

            assert required_book_cols.issubset(books.columns), "Missing columns in books!"
            assert required_rating_cols.issubset(ratings.columns), "Missing columns in ratings!"
            assert required_user_cols.issubset(users.columns), "Missing columns in users!"

            ratings = ratings[ratings['Book-Rating'] > 0]
            logger.info("Validation successful.")
            return books, ratings, users
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise
