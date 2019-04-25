import pandas as pd

def df_equal_by_set(df1:pd.DataFrame, df2:pd.DataFrame, cols:iter)->bool:
    set1,set2 = list(), list()
    for col in cols:
        set1.append(df1[col])
        set2.append(df2[col])
    zipped1 = set(zip(*set1)) 
    zipped2 = set(zip(*set2))
    
    return (zipped1 == zipped2)
