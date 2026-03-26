from common import exercise_1, exercise_2, exercise_3, exercise_4, load_movies, load_ratings, load_users


def print_section(title, lines):
    print(title)
    print("-" * len(title))
    for line in lines:
        print(line)
    print()


def main():
    movies = load_movies()
    users = load_users()
    ratings = load_ratings()

    print_section("Bai 1", exercise_1(movies, ratings))
    print_section("Bai 2", exercise_2(movies, ratings))
    print_section("Bai 3", exercise_3(movies, users, ratings))
    print_section("Bai 4", exercise_4(movies, users, ratings))


if __name__ == "__main__":
    main()
