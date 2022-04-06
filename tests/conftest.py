import pytest

from sqlalchemy import create_engine, Column, Integer, String, Table
from sqlalchemy.exc import InternalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

Base = declarative_base()
engine = create_engine('postgresql://dsqla-user:@localhost:5432/dynamic-sqlalchemy-test')
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)


class Account(Base):
    __tablename__ = 'account'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False)
    email = Column(String(128), nullable=True, index=True)
    age = Column(Integer)

@pytest.fixture(scope='session', autouse=True)
def db():
    """session wide test database"""
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)

@pytest.fixture(scope='function')
def test_session(db):
    """creates a new db session for a test"""
    try:
        session = Session()
        yield session
    finally:
        session.rollback()
        reset_tables()
        session.close()
        Session.remove()

def reset_tables():
    """Clears all tables between tests.

    This allows tests to be isolated from each other
    preventing cross-contamination of test state while
    still allowing the tests to run fast
    """
    # with engine.connect() as conn:
    session = Session()
    meta = Base.metadata
    # drop our test view
    session.execute("DROP VIEW IF EXISTS my_cool_view")
    # get dynamically generated tables in meta.sorted_tables
    meta.reflect(engine)
    for table in meta.sorted_tables:
        session.execute(table.delete())
    session.commit()
