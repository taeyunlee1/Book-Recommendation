# Book Recommender System

A modern web-based book recommendation engine using collaborative filtering and a clean responsive UI built with Flask and vanilla HTML/CSS/JS.

---

## Features

- User-based collaborative filtering with cosine similarity
- Real-time book recommendations
- Autocomplete book search
- Dark mode toggle
-  Book cards with cover, author, and average rating + review count
---

## Project Structure

```
book-recommender/
├── app.py                     # Flask backend
├── recommender/              # Data pipeline modules
│   ├── Data_Ingenstion.py
│   ├── Data_Validation.py
│   ├── DataTransformation.py
│   └── TrainingPipeline.py
├── templates/
│   └── index.html            # Frontend HTML
├── static/
│   └── style.css             # Frontend CSS
├── data/                     # CSVs (Book-Crossing dataset)
├── logger.py                 # Logging setup
├── download_dataset.py       # Script to fetch dataset
├── requirements.txt
└── README.md
```

---

## 🛠Setup Instructions

```bash
# 1. Clone the repo
$ git clone https://github.com/taeyunlee1/Book-Recommendation.git
$ cd Book-Recommendation

# 2. (Optional) Create a virtual environment
$ python -m venv env
$ source env/bin/activate  # or env\Scripts\activate

# 3. Install requirements
$ pip install -r requirements.txt

# 4. Run the app
$ python app.py
```

Visit [http://127.0.0.1:5000](http://127.0.0.1:5000)

---

## Dataset

Book-Crossing dataset (from [Kaggle](https://www.kaggle.com/datasets/ruchi798/bookcrossing-dataset))

### Option A: Already included in `/data/`
If cloned from this repo, CSVs are preloaded.

## Dataset Files

- `BX-Books.csv`: book metadata (title, author, image URL)
- `BX-Book-Ratings.csv`: user ratings (0–10 scale)
- `BX-Users.csv`: user info (age, location — not used)

---

## Future Ideas

- Content-based recommendations (TF-IDF, genres, authors)
- Hybrid recommender
- User accounts + favorites
- Docker deployment / Render hosting

---

Pull requests and stars welcome!
