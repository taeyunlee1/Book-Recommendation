from Data_ingestion import DataIngestion
from Data_validation import DataValidation
from Data_transformation import DataTransformation
from books_recommender.components.stage_03_model_trainer import ModelTrainer

class TrainingPipeline:
    def __init__(self):
        self.data_ingestion = DataIngestion()
        self.data_validation = DataValidation()
        self.data_transformation = DataTransformation()
        self.model_trainer = ModelTrainer()

    def start_pipeline(self):
        """
        Runs the full training pipeline in a step-by-step manner.
        """
        self.data_ingestion.run_data_ingestion()
        self.data_validation.run_data_validation()
        self.data_transformation.run_data_transformation()
        self.model_trainer.run_model_training()