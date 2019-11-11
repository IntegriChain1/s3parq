import pandas as pd


def df_equal_by_set(df1: pd.DataFrame, df2: pd.DataFrame, cols: iter) -> bool:
    """ This checks over whether 2 dataframes are equal by comparing zips of
    the given columns

    Args:
        df1 (pd.DataFrame): The first dataframe to compare against
        df2 (pd.DataFrame): The second dataframe to compare against
        cols (iter): The column to compare on the dataframes

    Returns:
        bool of whether the dataframe columns are equal
    """
    set1, set2 = list(), list()
    for col in cols:
        set1.append(df1[col])
        set2.append(df2[col])
    zipped1 = set(zip(*set1))
    zipped2 = set(zip(*set2))

    return (zipped1 == zipped2)
