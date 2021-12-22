from sys import argv

from dataframe_helper import get_dataframes, scatterplot

def main(argv, argc):
    # handle arguments, if any
    assert argc <= 2, "Assertion Error: main.py only takes in one optional argument.\n \
                      Usage: python main.py [num_dataframes_to_plot]\n \
                             (up to 20, default is 2)"
    if argc == 2:
        assert argv[1].isnumeric(), "Assertion Error: main.py only takes in one optional argument that must be numeric.\n \
                                     Usage: python main.py [num_dataframes_to_plot]\n \
                                            (1 to 20, default is 2)"
        counter = int(argv[1])
        assert 20 > counter >= 0, "Assertion Error: main.py only takes in one optional argument that must be between 0 and 20.\n \
                                  Usage: python main.py [num_dataframes_to_plot]\n \
                                         (0 to 20, default is 2)"
    else:
        counter = 2

    # get the dataframes
    dataframes = get_dataframes("../big_files/dataframes.pickle")
    columns = dataframes["machine_0"].columns.values
    for machine, df in dataframes.items():
        if not counter: return
        # make a scatterplot of all dataframes from i == 0 to counter-1
        scatterplot(machine, df, columns)
        counter -= 1

if __name__ == "__main__":
    main(argv, len(argv))