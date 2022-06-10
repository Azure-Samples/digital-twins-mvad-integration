#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from IPython.core.display import display

def get_topo_df():
    """
    Create topology table with columns: 'relationshipName', 'sourceId', 'targetId'.

    Return
    ----------
    topo_df : topology table created,
        pd.DataFrame
    """
    topo_df = pd.DataFrame(columns=['relationshipName', 'sourceId', 'targetId'])
    topo_df = topo_df.append({"relationshipName": "isParent", "sourceId": "B", 'targetId': 'E'}, ignore_index=True)
    topo_df = topo_df.append({"relationshipName": "isParent", "sourceId": "B", 'targetId': 'F'}, ignore_index=True)
    topo_df = topo_df.append({"relationshipName": "isParent", "sourceId": "B", 'targetId': 'G'}, ignore_index=True)
    topo_df = topo_df.append({"relationshipName": "isParent", "sourceId": "B", 'targetId': 'I'}, ignore_index=True)
    topo_df = topo_df.append({"relationshipName": "isParent", "sourceId": "D", 'targetId': 'H'}, ignore_index=True)
    topo_df = topo_df.append({"relationshipName": "isParent", "sourceId": "H", 'targetId': 'J'}, ignore_index=True)
    topo_df = topo_df.append({"relationshipName": "isParent", "sourceId": "A", 'targetId': 'C'}, ignore_index=True)
    topo_df = topo_df.append({"relationshipName": "isParent", "sourceId": "A", 'targetId': 'D'}, ignore_index=True)
    topo_df = topo_df.append({"relationshipName": "isParent", "sourceId": "A", 'targetId': 'E'}, ignore_index=True)
    topo_df = topo_df.append({"relationshipName": "isRedundant", "sourceId": "I", 'targetId': 'F'}, ignore_index=True)
    topo_df = topo_df.append({"relationshipName": "isRedundant", "sourceId": "F", 'targetId': 'I'}, ignore_index=True)
    display(topo_df)

    return topo_df
