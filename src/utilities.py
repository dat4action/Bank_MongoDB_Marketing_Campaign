import pandas as pd


def df_to_csv(df:pd.DataFrame,filename):
    """ export a clean data frame to a csv file into the "data/clean_data" folder

    Args:
        df (pd.DataFrame): dataframe to be exported as csv
        filename (str): name of the destination filename without the extension "csv"
    """
    path= r'data\clean_data'
    file= path + '\\' + filename + ".csv"
    df.to_csv(file,index=False)
    print(f' file {filename} has been created from the dataset {df.head()}')



