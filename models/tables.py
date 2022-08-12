from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Text, MetaData, Integer, Boolean, ForeignKey, Enum, TIMESTAMP, ARRAY
from sqlalchemy.dialects.postgresql import JSON
import enum
import os


Base = declarative_base(bind=os.getenv("POSTGRES_CONNECTION_STR"), metadata=MetaData(schema=os.getenv("DENYLIST_DB_SCHEMA")))


class issue_type(enum.Enum):
    addition = 1
    removal = 2
    other = 3


class state_type(enum.Enum):
    open = 1
    closed = 2


class entry_status_type(enum.Enum):
    not_reviewed = 1
    valid = 2
    invalid = 3
    unknown = 4


class Entries(Base):
    __tablename__ = "entries"

    address = Column(Text, primary_key=True, nullable=False)
    issue_number = Column(Integer, ForeignKey("issues.number"), primary_key=True, nullable=False)
    reports_generated = Column(Boolean)
    review_status = Column(Enum(entry_status_type), default=entry_status_type.not_reviewed)
    name = Column(Text)
    location = Column(Text)
    owner = Column(Text)
    payer = Column(Text)
    maker = Column(Text)
    long_country = Column(Text)
    long_state = Column(Text)
    long_city = Column(Text)
    first_block = Column(Integer)


class Issues(Base):
    __tablename__ = "issues"

    number = Column(Integer, primary_key=True, nullable=False)
    title = Column(Text)
    user = Column(Text)
    labels = Column(ARRAY(Text))
    issue_type = Column(Enum(issue_type))
    state = Column(Enum(state_type))
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)
    closed_at = Column(TIMESTAMP)
    comments = Column(Integer)
    body = Column(Text)
    reactions = Column(JSON)
    reports_generated = Column(Boolean, nullable=True)


class Pulls(Base):
    __tablename__ = "pulls"

    number = Column(Integer, primary_key=True)
    title = Column(Text)
    user = Column(Text)
    state = Column(Enum(state_type))
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)
    closed_at = Column(TIMESTAMP)
    body = Column(Text)


class PullIssues(Base):
    __tablename__ = "pull_issues"

    pull = Column(Integer, ForeignKey("pulls.number"), primary_key=True)
    issue = Column(Integer, ForeignKey("issues.number"), primary_key=True)
