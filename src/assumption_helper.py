from natsort import natsorted
import operator
from sqlalchemy import inspect

from utility_helper import get_elements_with_prefix

# Checks the assumption that all the rows of the original data set 
# were sorted by timestamp and then sorted again by machine
# are_sorted_by_timestamp_and_machine(sqlalchemy engine) -> bool
def are_sorted_by_timestamp_and_machine(engine):
    # get relevant tables (i.e. not static_data)
    table_names = inspect(engine).get_table_names()
    relevant_table_names = get_elements_with_prefix(table_names, "feat_")
    for rtn in relevant_table_names:
        query = "SELECT * FROM %s;" % rtn
        rows = engine.execute(query).fetchall()

        # compare the above rows to rows sorted by timestamp, then sorted again by machine
        # TODO: We're sorting twice. Luckily, the list isn't too big, but this is not ideal
        sorted_by_timestamp = sorted(rows, key=operator.itemgetter(0))
        sorted_by_machine = natsorted(sorted_by_timestamp, key=lambda y : y[1])
        for i in range(len(rows)):
            if rows[i] != sorted_by_machine[i]:
                return False
    return True

# Checks the assumption that all the timestamps of the feat_n tables match row-by-row
# has_all_equal_timestamps(sqlalchemy engine) -> bool
def has_all_equal_timestamps(engine):
    timestamps_array = []

    # get a list of list of timestamps from relevant tables (i.e. not static_data)
    table_names = inspect(engine).get_table_names()
    relevant_table_names = get_elements_with_prefix(table_names, "feat_")
    for rtn in relevant_table_names:
        query = "SELECT * FROM %s;" % rtn
        rows = engine.execute(query).fetchall()
        timestamps = [r[0] for r in rows]
        timestamps_array.append(timestamps)

    # check if the lists in the l.o.l.o timestamps are of equal length
    are_equal_lengths = len(set(map(len, timestamps_array))) <= 1
    if not are_equal_lengths:
        print("Error: The tables do not have equal lengths")
        return None

    # check every row for differences in timestamp
    for i in range(len(timestamps_array[0])):
        compared_timestamps = [timestamps_array[j][i] for j in range(len(timestamps_array))]
        are_all_equal_timestamps = len(set(compared_timestamps)) <= 1
        if not are_all_equal_timestamps:
            return False
    return True