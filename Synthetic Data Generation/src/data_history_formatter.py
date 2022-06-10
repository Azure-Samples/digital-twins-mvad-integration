import pandas as pd
from datetime import datetime as dt

def data_history_formatter(df=None) -> pd.DataFrame:
    """
    Main function for synthetic data generation.

    Parameters
    ----------
    df: Synthetic data directly generated from main.py,
        pd.DataFrame

    Return
    ----------
    df_dh : Data formatted as the same as ADT Data History,
        pd.DataFrame
    """
    df_dh = df.rename(columns={'Timestamp': 'SourceTimeStamp'})
    df_dh['TimeStamp'] = dt.now()
    df_dh['ServiceId'] = 'temporary-adt.api.wcus.digitaltwins.azure.net'
    df_dh = df_dh.reindex(columns = ['TimeStamp', 'SourceTimeStamp', 'ServiceId', 'Id', \
                                     'ModelId', 'Key', 'Value', 'RelationshipTarget', 'RelationshipId'])
    return df_dh
