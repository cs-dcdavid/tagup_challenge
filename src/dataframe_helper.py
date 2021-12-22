from matplotlib.pyplot import legend, show
import pandas as pd
from os import path
from pickle import dump, load
from sqlalchemy import create_engine, inspect

from assumption_helper import are_sorted_by_timestamp_and_machine, has_all_equal_timestamps
from utility_helper import get_column_names, get_elements_with_prefix

# create_dataframes(sqlalchemy engine, str) -> pandas DataFrame=[[str]]}
def create_dataframes(engine, filename):
    dataframes = get_complex_EquipmentData(engine)

    # get the data of all machines
    columns = dataframes["machine_0"].columns.values
    filtered_data_frames = {key : None for key in dataframes}
    for machine, df in dataframes.items():
        # filter the data
        df_unfiltered = dataframes[machine]
        df = remove_outliers(df_unfiltered, columns)
        filtered_data_frames[machine] = df
    
    # save the dataframe for future use
    #filtered_data_frames.to_pickle(filename)
    with open(filename, 'wb') as handle:
        dump(filtered_data_frames, handle)
    return filtered_data_frames


# Wrapper for create_dataframes in case the dataframes already exist
# get_dataframes(str) -> {str: pandas DataFrame=[[str]]}
def get_dataframes(filename):
    if path.isfile(filename):
        with open(filename, 'rb') as handle:
            unpickled_dataframes = load(handle)
        return unpickled_dataframes
    else:
        engine = create_engine('sqlite:///../big_files/exampleco_db.db', echo=False)
        dataframes = create_dataframes(engine, filename)
        return dataframes

# Assumption 1: All the timestamps of the feat_n tables match row-by-row
# get_simple_EquipmentData(sqlalchemy engine) -> {str: pandas DataFrame=[[str]]}
def get_simple_EquipmentData(engine):
    assert has_all_equal_timestamps(engine), "Assertion Error: Not all the \
        timestamps of the feat_n tables match row-by-row"

    # convert [[(row)]] to {feat_n : [[row]]}
    table_names = inspect(engine).get_table_names()
    relevant_table_names = [tn for tn in table_names if tn[:len("feat_")] == "feat_"]
    data_dict = {rtn : [] for rtn in relevant_table_names}
    for i, tn in enumerate(relevant_table_names):
        sub_data = []
        column_names = get_column_names(engine, tn)
        query = "SELECT * FROM %s;" % tn
        rows = engine.execute(query).fetchall()
        for r in rows: sub_data.append(list(r))
        data_dict[relevant_table_names[i]] = sub_data
    
    # convert lists to data frames
    data_frames = {key : None for key in data_dict}
    for key, val in data_dict.items():
        data_frames[key] = pd.DataFrame(val, columns=column_names)
    return data_frames

# Assumption 1: All the timestamps of the feat_n tables match row-by-row
# Assumption 2: All rows in feat_n tables are sorted by timestamp and then by machine
# get_complex_EquipmentData(sqlalchemy engine) -> {str: pandas DataFrame=[[str]]}
def get_complex_EquipmentData(engine):
    assert has_all_equal_timestamps(engine), "Assertion Error: Not all the \
        timestamps of the feat_n tables match row-by-row"
    assert are_sorted_by_timestamp_and_machine(engine), "Assertion Error: Not \
        all rows in feat_n tables are sorted by timestamp and then by machine"

    # combine Value columns from each feat_n table, grouped by Timestamp and Machine #
    data = []
    relevant_counter = 0
    table_names = inspect(engine).get_table_names()
    relevant_table_names = get_elements_with_prefix(table_names, "feat_")
    for rtn in relevant_table_names:
        query = "SELECT * FROM %s;" % rtn
        rows = engine.execute(query).fetchall()
        if relevant_counter == 0: # can't just get i == 0 since the first table might not be relevant
            for r in rows: data.append([r[0], r[1], r[2]])
        else:
            for i in range(len(data)):
                data[i].append(rows[i][2])
        relevant_counter += 1

    # break down the array to data frames by Machine #
    data_dict = {r[1] : [] for r in data}
    for d in data:
        data_dict[d[1]].append([d[0],d[2],d[3],d[4],d[5]])
    
    # convert lists to data frames
    data_frames = {key : None for key in data_dict}
    for key, val in data_dict.items():
        data_frames[key] = pd.DataFrame(val, columns=["timestamp"]+relevant_table_names)
        #data_frames[key].set_index('timestamp', inplace=True)
    return data_frames

# remove_outliers(pandas DataFrame, [str]) -> pandas DataFrame
def remove_outliers(df_in, cols):
    # get the value at the first and the third quarter of our data set
    q1 = df_in[cols].quantile(0.25)
    q3 = df_in[cols].quantile(0.75)
    iqr = q3-q1

    # filter out the outliers
    df_out = df_in[~((df_in[cols]<(q1-1.5*iqr)) | (df_in[cols]>(q3+1.5*iqr)).any(axis=1))]
    return df_out

def scatterplot(machine, dataframe, columns):
    colors = ['r','g','b','y','c','m']

    # scatterplot for every column
    relevant_column_names = get_elements_with_prefix(columns, "feat_")
    for i, rcn in enumerate(relevant_column_names):
        if i == 0:
            ax = dataframe.plot(kind="scatter", x="timestamp", y=rcn, color=colors[i%len(colors)], label=rcn)
        else:
            dataframe.plot(kind="scatter", x="timestamp", y=rcn, color=colors[i%len(colors)], label=rcn, ax=ax)
    
    # label the plot
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Value")
    ax.set_title("(FILTERED) Time-series data on %s, value vs timestamp" % machine, \
        horizontalalignment='center', verticalalignment='top')
    legend(loc=2)

    # show the plot
    show()