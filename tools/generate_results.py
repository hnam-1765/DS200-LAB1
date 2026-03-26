import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RESULTS_DIR = BASE_DIR / "results"
sys.path.insert(0, str(BASE_DIR))

from python_solution.common import exercise_1, exercise_2, exercise_3, exercise_4, load_movies, load_ratings, load_users


def write_text_output(filename, lines):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / filename).write_text("\n".join(lines) + "\n", encoding="utf-8")
def main():
    movies = load_movies()
    users = load_users()
    ratings = load_ratings()

    outputs = {
        "exercise1_average_ratings.txt": ("Bai 1 - Average Ratings", exercise_1(movies, ratings)),
        "exercise2_genre_analysis.txt": ("Bai 2 - Genre Analysis", exercise_2(movies, ratings)),
        "exercise3_gender_analysis.txt": ("Bai 3 - Gender Analysis", exercise_3(movies, users, ratings)),
        "exercise4_age_group_analysis.txt": ("Bai 4 - Age Group Analysis", exercise_4(movies, users, ratings)),
    }

    for text_name, (title, lines) in outputs.items():
        write_text_output(text_name, lines)


if __name__ == "__main__":
    main()
