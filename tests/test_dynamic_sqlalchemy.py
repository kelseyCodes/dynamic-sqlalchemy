import pytest

from sqlalchemy import Column, Integer, String
from sqlalchemy.exc import ProgrammingError
from dynamic_sqlalchemy.dynamic_sqlalchemy import DynamicSqlalchemy
from tests.conftest import Account

class TestDynamicSqlalchemy:

    def test_create_dynamic_table(self, test_session):
        result_class = DynamicSqlalchemy.create_dynamic_table('my_cool_table', columns={
            'id': Column(Integer, primary_key=True, autoincrement=True),
            'name': Column(String),
            'age': Column(Integer)
        })

        assert result_class.__tablename__ == 'my_cool_table'
        assert [c.name for c in result_class.__table__.c] == ['id', 'name', 'age']

    def test_create_dynamic_view(self, test_session):
        result_class = DynamicSqlalchemy.create_dynamic_view('my_cool_view', Account, ['name', 'age'])

        assert result_class.__tablename__ == 'my_cool_view'
        assert [c.name for c in result_class.__table__.c] == ['name', 'age', 'id']

    def test_add_column(self, test_session):
        result_class = DynamicSqlalchemy.add_column(Account, 'phone', Column(String))

        assert [c.name for c in result_class.__table__.c] == ['id', 'name', 'email', 'age', 'phone']

    def test_alter_column_name(self, test_session):
        DynamicSqlalchemy.alter_column_name(Account, 'name', 'Name')

        columns = [c.name for c in  Account.__table__.c]
        assert 'Name' in columns
        assert 'name' not in columns
        # rename for ease of resetting tables in drop_all in conftest
        DynamicSqlalchemy.alter_column_name(Account, 'Name', 'name')

    def test_hard_delete_column(self, test_session):
        DynamicSqlalchemy.hard_delete_column(Account, 'age')

        with pytest.raises(ProgrammingError) as e:
            test_session.add(Account(name='Henry', age=1))
            test_session.commit()
        assert 'column "age" of relation "account" does not exist' in str(e.value)

    def test_update_mapper_column(self, test_session):
        result = DynamicSqlalchemy.update_mapper_column(Account, 'name', 'Name')

        assert result.name == 'Name'
        assert 'Name' in [c.name for c in Account.__table__.c]
        assert 'name' not in [c.name for c in Account.__table__.c]

