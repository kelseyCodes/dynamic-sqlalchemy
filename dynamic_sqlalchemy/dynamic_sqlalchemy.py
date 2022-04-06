from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, create_engine, Integer, select, Table, text
from sqlalchemy.ext.declarative import declarative_base

from dynamic_sqlalchemy import get_class_from_tablename, snake_case_to_camel_case

Base = declarative_base()
POSTGRES_URI = 'postgresql://dsqla-user:@localhost:5432/dynamic-sqlalchemy-test'


class DynamicSqlalchemy:
    """Base class for all dynamic models providing convenience methods."""
    __abstract__ = True

    engine = create_engine(
        POSTGRES_URI
    )

    @classmethod
    def create_dynamic_table(cls, tablename, columns={}, base_class=Base, attr_dict={}, auto_create=True):
        """
        Either creates or reloads an existing database table into the sqlalchemy mapper such
        that a model class is generated for  the database table.

        args:
            base_class (Class): the class that your new model class should inherit from. At the most
                                basic level, this can be the base declarative class (declarative_base())
            tablename (str): the name of the table that should be created or already exists in the
                             database. This should be a snake_cased name and the corresponding model class
                             will be the camel case version of the tablename
            columns (dict): a dictionary of columns that should be added to the database. Required when
                            creating the database table for the first time. Keys (str) should be column
                            names, values should be Sqlalchemy column definitions
                                ex. {'vin': Column(String, nullable=False)}
            attr_dict (dict): a dictionary of attributes that should exist on the table
                                ex. {'__tablename__': 'my_new_table',
                                     '__table_args__': {'extend_existing': True}
                                    }
            auto_create (bool): defaulting to True, this flag indicates whether the tables should be created
                                or not. There are cases, like with Postgres views, for instance, that it is
                                beneficial to create a collection of tables at once. In that case, setting
                                auto_create to False will add the new model class to the mapper but not create
                                the database table
        """
        classname = snake_case_to_camel_case(tablename)
        if not attr_dict:
            attr_dict = {'__tablename__': tablename}
        if columns:
            attr_dict = {**attr_dict, **columns}

        if tablename in base_class.metadata.tables and '__table__' not in attr_dict:
            __table_args__ = {'extend_existing': True, 'autoload_with': cls.engine}
            attr_dict['__table_args__'] = __table_args__

        class_ = type(classname, (base_class,), attr_dict)
        if auto_create:
            base_class.metadata.create_all(cls.engine)

        return class_

    @classmethod
    def create_dynamic_view(cls, view_name, model_class, column_list, auto_create=True, deleted_at=None):
        """
        Either creates or reloads an existing database view into the sqlalchemy mapper such
        that a model class is generated for  the database view.

        args:
            view_name (str): the name of the view  that should be created or already exists in the
                             database. This should be a snake_cased name and the corresponding model class
                             will be the camel case version of the tablename
            model_class (Class): the class corresponding to the table in the database from which this view
                                 will be generated
            column_list (list): a list of columns that should be included in the database view. These columns must exist
                                on the model_class's corresponding table as well.
                                ex. {'vin': Column(String, nullable=False)}
            auto_create (bool): defaulting to True, this flag indicates whether the tables should be created
                                or not. If creating mulitple views, it's best practice to set thsi to False
                                and then call Base.metadata.create_all(engine) at the end of adding all views to the mapper.
            deleted_at (str): optionally specify the name of a column name containing deletion dates. If specified, the
                              view will exclude deleted rows from the model_class's table.
        """
        columns = ', '.join(column_list)
        view = select(text(columns)).\
               select_from(model_class.__table__)
        if deleted_at:
            view = view.where(getattr(model_class.__table__.c, deleted_at) == None)

        query = "CREATE OR REPLACE VIEW %s AS (%s)" % (view_name, view)
        with cls.engine.connect() as conn:
            conn.execute(query)

        class_ = cls.create_class_from_table(view_name, auto_create)

        return class_

    @classmethod
    def create_class_from_table(cls, tablename, auto_create):
        """
        args:
            tablename (str): the tablename of the postgres view
            auto_create (bool): indicates if the table should be created in the database
        """
        table = Table(
            tablename,
            Base.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            autoload_with=cls.engine,
            extend_existing=True
        )
        attr_dict = {'__table__': table,
                     '__tablename__': tablename,
                     'info': dict(is_view=True)
                     }

        return cls.create_dynamic_table(tablename, attr_dict=attr_dict, auto_create=auto_create)

    @classmethod
    def add_column(cls, class_, column_name, column_type):
        """
        args:
            class_ (Class): the class associated in the sqlalchemy mapper with the table
                            that should have a column added
            column_name (str): the column name that should be added
            column_type (sqlalchemy.Column): the sqlalchmey column definition
        """
        column_type.name = column_name
        with cls.engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            setattr(class_, column_name, column_type)

            op = Operations(ctx)
            op.add_column(class_.__tablename__, column_type)
        return class_

    @classmethod
    def alter_column_name(cls, class_, current_column, new_column):
        """
        args:
             class_ (Class): the class that should be updated to include the column name change
             current_column (str): the current column name
             new_column (str): the new column name
        """
        with cls.engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            op = Operations(ctx)
            op.alter_column(class_.__tablename__, current_column, new_column_name=new_column)

        return cls.update_mapper_column(class_, current_column, new_column)

    @classmethod
    def hard_delete_column(cls, class_, column):
        """
        args:
             class_ (Class): the class that should be updated
             column (str): the column that should be deleted
        """
        if column not in [c.name for c in class_.__table__.c]:
            return
        with cls.engine.connect() as conn:
            conn.execute(f'''ALTER TABLE {class_.__tablename__} DROP COLUMN IF EXISTS "{column}";''')

            return class_

    @classmethod
    def get_or_create_model_class(cls, tablename, columns=None):
        """
        Fetches or re-adds the model class to the mapper such that it can be used to interact with the
        table specified in tablename.

        Because these classes are dynamic, when a new session begins, the sqlalchemy mapper that maps
        model classes to database tables will not automatically include the dynamic table. As such,
        whenever referenceing the model class, re-add the database table and regenerate the model class
        definition to the mapper that stores these values.

        args:
             tablename (str): the name of the table whose model_class we are fetching
             columns (dict): the columns that should be included in the model class definition
        """
        class_ = get_class_from_tablename(Base, tablename)
        if not class_:
            class_ = cls.create_dynamic_table(tablename, columns)

        return class_

    @classmethod
    def update_mapper_column(cls, class_, old_column, new_column):
        """
        Here, because of how sqlalchemhy works, we both update and add the column name,
        meaning that initially the column name will show twice in the class column definition.
        However, as soon as a new connection (or engine) is established,
        the class will be read in normally with no extra columns. This function simply ensures that during
        the same connection, you can interact with the new column name normally.
        args:
             class_ (Class): a class whose mapper attributes should be updated
             old_column (str): the name of the old column
             new_columns (str): the name of the new column
        """
        column_arr = [c for c in class_.__table__.c if c.name == old_column]
        if not column_arr:
            if new_column in [c.name for c in class_.__table__.c]:
                return [c for c in class_.__table__.c if c.name == new_column]
            else:
                raise NotFoundException(f'Cannot update mapper column name from {old_column} '
                                        f'to {new_column} for class {class_}. Cannot find the '
                                        'old column in the class\'s table.')

        column = column_arr[0]
        column.name = new_column
        column.key = new_column
        setattr(class_, new_column, column)
        return column
