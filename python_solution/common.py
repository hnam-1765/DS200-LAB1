from collections import defaultdict
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]


def load_movies():
    movies = {}
    for line in (BASE_DIR / "movies.txt").read_text(encoding="utf-8").splitlines():
        movie_id, title, genres = [part.strip() for part in line.split(",", 2)]
        movies[movie_id] = {"title": title, "genres": genres.split("|")}
    return movies


def load_users():
    users = {}
    for line in (BASE_DIR / "users.txt").read_text(encoding="utf-8").splitlines():
        user_id, gender, age, occupation, zipcode = [part.strip() for part in line.split(",", 4)]
        users[user_id] = {
            "gender": gender,
            "age": int(age),
            "occupation": occupation,
            "zipcode": zipcode,
        }
    return users


def load_ratings():
    ratings = []
    for name in ("ratings_1.txt", "ratings_2.txt"):
        for line in (BASE_DIR / name).read_text(encoding="utf-8").splitlines():
            user_id, movie_id, rating, timestamp = [part.strip() for part in line.split(",", 3)]
            ratings.append(
                {
                    "user_id": user_id,
                    "movie_id": movie_id,
                    "rating": float(rating),
                    "timestamp": timestamp,
                }
            )
    return ratings


def age_group(age):
    if age < 18:
        return "0-18"
    if age < 35:
        return "18-35"
    if age < 50:
        return "35-50"
    return "50+"


def exercise_1(movies, ratings):
    stats = defaultdict(lambda: [0.0, 0])
    for rating in ratings:
        stats[rating["movie_id"]][0] += rating["rating"]
        stats[rating["movie_id"]][1] += 1

    lines = []
    best_movie = None
    best_avg = float("-inf")
    best_count = 0

    for movie_id, values in sorted(stats.items(), key=lambda item: movies[item[0]]["title"]):
        total, count = values
        avg = total / count
        lines.append(f'{movies[movie_id]["title"]} AverageRating: {avg:.2f} (TotalRatings: {count})')
        if count >= 5 and avg > best_avg:
            best_movie = movies[movie_id]["title"]
            best_avg = avg
            best_count = count

    if best_movie is None:
        lines.append("No movie has at least 5 ratings in the provided dataset.")
    else:
        lines.append(
            f"{best_movie} is the highest rated movie with an average rating of {best_avg:.2f} "
            f"among movies with at least 5 ratings ({best_count} ratings)."
        )

    return lines


def exercise_2(movies, ratings):
    stats = defaultdict(lambda: [0.0, 0])
    for rating in ratings:
        for genre in movies[rating["movie_id"]]["genres"]:
            stats[genre][0] += rating["rating"]
            stats[genre][1] += 1

    return [
        f"{genre}: {values[0] / values[1]:.2f} (TotalRatings: {values[1]})"
        for genre, values in sorted(stats.items())
    ]


def exercise_3(movies, users, ratings):
    stats = defaultdict(lambda: {"M": [0.0, 0], "F": [0.0, 0]})
    for rating in ratings:
        gender = users[rating["user_id"]]["gender"]
        stats[rating["movie_id"]][gender][0] += rating["rating"]
        stats[rating["movie_id"]][gender][1] += 1

    lines = []
    for movie_id, values in sorted(stats.items(), key=lambda item: movies[item[0]]["title"]):
        male_sum, male_count = values["M"]
        female_sum, female_count = values["F"]
        male_avg = "N/A" if male_count == 0 else f"{male_sum / male_count:.2f}"
        female_avg = "N/A" if female_count == 0 else f"{female_sum / female_count:.2f}"
        lines.append(f'{movies[movie_id]["title"]}: Male_Avg={male_avg}, Female_Avg={female_avg}')
    return lines


def exercise_4(movies, users, ratings):
    ordered_groups = ["0-18", "18-35", "35-50", "50+"]
    stats = defaultdict(lambda: {group: [0.0, 0] for group in ordered_groups})

    for rating in ratings:
        group = age_group(users[rating["user_id"]]["age"])
        stats[rating["movie_id"]][group][0] += rating["rating"]
        stats[rating["movie_id"]][group][1] += 1

    lines = []
    for movie_id, values in sorted(stats.items(), key=lambda item: movies[item[0]]["title"]):
        parts = []
        for group in ordered_groups:
            total, count = values[group]
            avg = "N/A" if count == 0 else f"{total / count:.2f}"
            parts.append(f"{group}: {avg}")
        lines.append(f'{movies[movie_id]["title"]}: [{", ".join(parts)}]')
    return lines
