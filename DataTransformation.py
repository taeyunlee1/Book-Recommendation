import os
import pickle
import pandas as pd
from books_recommender.logger.log import logging
from books_recommender.config.configuration import AppConfiguration
from books_recommender.exception.exception_handler import AppException

class DataTransformation:
    def __init__(self, app_config=AppConfiguration()):
        try:
            self.data_transformation_config = app_config.get_data_transformation_config()
        except Exception as e:
            raise AppException(e, sys) from e

    def get_data_transformer(self):
        try:
            df = pd.read_csv(self.data_transformation_config.clean_data_file_path)
            book_pivot = df.pivot_table(columns='user_id', index='title', values='rating')
            book_pivot.fillna(0, inplace=True)
            
            os.makedirs(self.data_transformation_config.transformed_data_dir, exist_ok=True)
            pickle.dump(book_pivot, open(os.path.join(self.data_transformation_config.transformed_data_dir,"transformed_data.pkl"), 'wb'))
        except Exception as e:
            raise AppException(e, sys) from e

    def initiate_data_transformation(self):
        try:
            self.get_data_transformer()
        except Exception as e:
            raise AppException(e, sys) from e