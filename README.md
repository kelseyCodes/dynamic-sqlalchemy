# Dynamic Sqlalchemy

## Summary

Dynamic-sqlalchemy is a library designed to provide basic functionality for creating, altering and interacting with dynamic tables using SQLAlchemy. To get started, simply install the package and follow the directions below.

## Install


1. Include the package in your requirements file. `-e git+ssh://git@github.com:kelseyCodes/dynamic-sqlalchemy.git#egg=dynamic-sqlalchemy`.

2. In your code, import DynamicSqalchemy `from dynamic-sqlalchemy import DynamicSqlalchemy`.

3. Set your database URI by setting it on the class `DynamicSqlalchemy.POSGRES_URI = <Your DB URI>`.

4. Use any of the methods located in `dynamic-sqlalchemy/dynamic_sqlalchemy.py`:
    - `create_dynamic_table`: generates a dynamic table
    - `create_dynamic_view`: generates a dynamic (postgres) view
    - `add_column`: adds a column to a table
    - `alter_column_name`: alters the  name of a column
    - `hard_delete_column`: deletes a column
    - `get_or_create_model_class`:  either fetches or newly adds the model class from the SQLAlchemy mapper (not automatically available on new session instantiations due to the dynamic class generation)



