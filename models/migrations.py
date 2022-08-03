import os
from models.tables import *
from models.views import *
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv


def migrate():
    load_dotenv()

    engine = create_engine(os.getenv("DENYLIST_DB_CONNECTION_STRING"))

    # create tables
    Base.metadata.create_all(engine)

    # create views
    engine.execute(users_view_sql)
