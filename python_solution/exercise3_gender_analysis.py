from common import exercise_3, load_movies, load_ratings, load_users


def main():
    movies = load_movies()
    users = load_users()
    ratings = load_ratings()
    for line in exercise_3(movies, users, ratings):
        print(line)


if __name__ == "__main__":
    main()
