def snake_case_to_camel_case(snake_str):
    """Takes a snake case string and converts it to camel case."""
    words = snake_str.split('_')
    return ''.join(word.title() for word in words)

def get_class_from_tablename(base_class, tablename):
    """Takes a tablename and finds the corresponding model class withing Sqlalchemy's mapper.
       Must specify a base_class to search through, usually declarative_base().
    """
    for class_ in base_class.registry.mappers:
        if hasattr(class_.class_, '__tablename__') and class_.class_.__tablename__ == tablename:
            return class_.class_