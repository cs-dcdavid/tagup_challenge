# Gets column names from a table name
# get_column_names(sqlalchemy engine, str table_name) -> (column0, column1, column2, ... columnN)
def get_column_names(engine, table_name):
    query = "PRAGMA table_info(%s)" % table_name
    column_names = tuple((column[1] for column in engine.execute(query).fetchall()))
    return column_names

# From a list, only get elements that have a certain prefix
# get_relevant_elements([str], str) -> [str]
def get_elements_with_prefix(elements, prefix):
    relevant_elements = [e for e in elements if e[:len(prefix)] == prefix]
    return relevant_elements