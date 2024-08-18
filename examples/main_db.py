import dotenv

dotenv.load_dotenv()


if __name__ == "__main__":
    from examples.db import get_db

    db = get_db().cli()
    db()
