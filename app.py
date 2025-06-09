from flask import Flask, render_template, request, jsonify, json
import pandas as pd
from recommender.TrainingPipeline import get_recommendations, similarity_df
from logger import logger

app = Flask(__name__)

# Load book metadata
try:
    books = pd.read_csv("data/BX-Books.csv", sep=";", encoding="latin-1", on_bad_lines='skip', low_memory=False)
    books.columns = books.columns.str.strip().str.replace('"', '')
    book_meta = books[['Book-Title', 'Book-Author', 'Image-URL-M']].dropna().drop_duplicates(subset='Book-Title')

    # Filter metadata to only include books in similarity_df
    book_titles_in_model = similarity_df.columns.tolist()
    filtered_books = book_meta[book_meta['Book-Title'].isin(book_titles_in_model)]

    logger.info(f"Loaded {len(filtered_books)} books for frontend search.")
except Exception as e:
    logger.error(f"Failed to load book metadata: {e}")
    filtered_books = pd.DataFrame()

@app.route('/')
def home():
    return render_template('index.html', books=filtered_books.to_dict(orient='records'))

@app.route('/recommend', methods=['POST'])
def recommend():
    try:
        title = request.json['title']
        logger.info(f"Recommendation requested for: {title}")
        recs = get_recommendations(title)

        # Load ratings
        ratings = pd.read_csv("data/BX-Book-Ratings.csv", sep=";", encoding="latin-1", on_bad_lines='skip', low_memory=False)
        ratings.columns = ratings.columns.str.strip().str.replace('"', '')

        # Compute average ratings
        avg_ratings = ratings[ratings['ISBN'].notna() & ratings['Book-Rating'] > 0]
        avg_ratings = avg_ratings.groupby('ISBN')['Book-Rating'].agg(['mean', 'count']).reset_index()
        avg_ratings.columns = ['ISBN', 'Avg-Rating', 'Num-Ratings']

        # Merge metadata
        merged = book_meta[book_meta['Book-Title'].isin(recs)].merge(
            books[['ISBN', 'Book-Title']], on='Book-Title'
        )

        # Drop conflicting columns before merge
        for col in ['Avg-Rating', 'Num-Ratings']:
            if col in merged.columns:
                merged.drop(columns=[col], inplace=True)

        merged = merged.merge(avg_ratings, on='ISBN', how='left')

        logger.info(f"Columns in merged: {merged.columns.tolist()}")
        logger.info(f"Sample merged data: {merged.head(1).to_dict()}")

        if 'Avg-Rating' in merged.columns:
            merged['Avg-Rating'] = merged['Avg-Rating'].round(1)

        merged = merged.where(pd.notnull(merged), None)
        result = json.loads(merged.to_json(orient='records'))

        return jsonify(result)
    except Exception as e:
        logger.error(f"Error generating recommendations: {e}")
        return jsonify([])

if __name__ == "__main__":
    logger.info("Starting Flask app...")
    app.run(debug=True)
