import datetime
import pandas as pd
import pytz


def get_schema_into_dfs(list_models, df_inittwins, df_updatetwins):
    """ puts in the schema of the properties into df_inittwins and df_updatetwins
    Args:
        list_models: list of models' json, which has the property schemas
        df_inittwins: df with columns=["Id", "ModelId", "Key", "Timestamp", "Value"], with twin initialization data
        df_updatetwins: df with columns=["Id", "Key", "Timestamp", "Value"], with twin property time-series data
    Returns:
        df_inittwins: df with added 'Schema' column
        df_updatetwins: df with added 'Schema' column
    """
    # Transform list_models for future usage to access schema
    df_models = pd.json_normalize(list_models, "contents",["@id"])
    df_models= df_models.apply(func_get_prop, axis=1)
    df_models=df_models[["@id","Property","Schema"]]

    # Transform df_inittwins for future usage to access schema
    df_inittwins["Schema"] = df_inittwins.apply(lambda x: df_models[(df_models["Property"]==x["Key"]) &
                                                        (df_models["@id"]==x["ModelId"])]["Schema"].values[0] , axis=1)

    # Transform df_updatetwins to access schema
    df_updatetwins["Schema"] =  df_updatetwins.apply(lambda x: df_inittwins[(df_inittwins["Key"]==x["Key"]) & (df_inittwins["Id"]==x["Id"])]["Schema"].values[0] , axis=1)
    df_updatetwins.head()

    return(df_inittwins, df_updatetwins)

def func_get_prop(row):
    """Helper function to modify dataframe row to get property and schema in nested dict, of the form as defined in DTDL model
    """
    if row["@type"]=="Property":
        row["Property"]=row["name"]
        row["Schema"]=row["schema"]
    else:
        row["Property"]="NaN"
        row["Schema"]="NaN"
    return(row)

def transform_to_json(df_inittwins):
    """ transforms twin initialization df to list of dicts, format in which ADT package takes input to create each twin, according to the twins' DTDL model definitions
    Args:
        df_inittwins: df with columns=["Id", "ModelId", "Key", "Timestamp", "Value", "Schema"], with twin initialization data
    Returns:
        list_init_dicts: list of dictionaries, one for each twin to be initialized
        list_twin_ids: list of associated twin ids
    """
    list_init_dicts, list_twin_ids = [], []

    for index_i,gp_i in df_inittwins.groupby(["Id","ModelId"]):
        list_twin_ids.append(index_i[0])

        dict1={}
        dict1["$metadata"] = {"$model": index_i[1]}
        for i,row in gp_i.iterrows():
            try:
                sourcetimestamp_val = pytz.utc.localize(datetime.datetime.strptime(row["Timestamp"], '%Y-%m-%d %H:%M:%S.%f')).isoformat()
            except Exception:
                try:
                    sourcetimestamp_val = pytz.utc.localize(datetime.datetime.strptime(row["Timestamp"], '%Y-%m-%d %H:%M:%S')).isoformat()
                except Exception as e:
                    print("Check data, might be wrong timestamp format or data-type. Timestamp must be in the following acceptable formats: %Y-%m-%d %H:%M:%S.%f', %Y-%m-%d %H:%M:%S'")
                    print(e)
                    raise e

            dict1["$metadata"][row["Key"]] = {"sourceTime": sourcetimestamp_val}
            # dict1[row["Key"]] = float(row["Value"]) if row["Schema"]=="double" else row["Value"]
            if row["Schema"]=="double":
                dict1[row["Key"]] = float(row["Value"])
            elif row["Schema"]=="integer":
                dict1[row["Key"]] = int(row["Value"])
            else:
                dict1[row["Key"]] = row["Value"]

        list_init_dicts.append(dict1)

    return list_init_dicts, list_twin_ids

def  create_patch_update(df_row):
    """ Creates json patch for each line in the df of IoT telemetry time-series, according to the twins' DTDL model definitions
    Args:
        df_row: df row with columns=["Id", "Key", "Timestamp", "Value", "Schema"] for specific time-series signal (i.e. ADT twin identified by Id) and properties to update identified by Key
    Returns:
        df_row["Id"]: twin id
        df_row["Key"]: key or component to update
        patch: json patch for property update
    """
    try:
        sourcetimestamp_val = pytz.utc.localize(datetime.datetime.strptime(df_row["Timestamp"], '%Y-%m-%d %H:%M:%S.%f')).isoformat()
    except Exception:
        try:
            sourcetimestamp_val = pytz.utc.localize(datetime.datetime.strptime(df_row["Timestamp"], '%Y-%m-%d %H:%M:%S')).isoformat()
        except Exception as e:
            print("Check data, might be wrong timestamp format or data-type. Timestamp must be in the following acceptable formats: %Y-%m-%d %H:%M:%S.%f', %Y-%m-%d %H:%M:%S'")
            print(e)
            raise e

    val = df_row["Value"]
    if df_row["Schema"]=="double":
        val = float(df_row["Value"])
    elif df_row["Schema"]=="integer":
        val = int(df_row["Value"])
    patch = [
        {
            "op": "replace",
            "path": f'/{df_row["Key"]}',
            "value": val,
        },
        {
            "op": "replace",
            "path": f'/$metadata/{df_row["Key"]}/sourceTime',
            "value": sourcetimestamp_val,
        }
    ]
    return(df_row["Id"], df_row["Key"], patch)

def  batch_create_patch_update(df):
    """ Creates json patch for groups of lines in the df of IoT telemetry time-series (grouped by twin-id and component name), according to the twins' DTDL model definitions
    Args:
        df_row: df with columns=["Id", "Key", "Timestamp", "Value", "Schema"] for specific time-series signal (i.e. ADT twin identified by Id) and properties to update identified by Key
    Returns:
        list_twin_id: list of the twin ids, from that group of updates
        list_key: list of keys or components to update, from that group of updates
        list_patches: list of json patches for property update, from that group of updates
    """
    list_patches, list_twin_id, list_key = [], [], []
    for i, row in df.iterrows():

        try:
            sourcetimestamp_val = pytz.utc.localize(datetime.datetime.strptime(row["Timestamp"], '%Y-%m-%d %H:%M:%S.%f')).isoformat()
        except Exception:
            try:
                sourcetimestamp_val = pytz.utc.localize(datetime.datetime.strptime(row["Timestamp"], '%Y-%m-%d %H:%M:%S')).isoformat()
            except Exception as e:
                print("Check data, might be wrong timestamp format or data-type. Timestamp must be in the following acceptable formats: %Y-%m-%d %H:%M:%S.%f', %Y-%m-%d %H:%M:%S'")
                print(e)
                raise e

        val = row["Value"]
        if row["Schema"]=="double":
            val = float(row["Value"])
        elif row["Schema"]=="integer":
            val = int(row["Value"])
        patch = [
            {
                "op": "replace",
                "path": f'/{row["Key"]}',
                "value": val,
            },
            {
                "op": "replace",
                "path": f'/$metadata/{row["Key"]}/sourceTime',
                "value": sourcetimestamp_val,
            }
        ]

        list_twin_id.append(row["Id"])
        list_key.append(row["Key"])
        list_patches.append(patch)
    return(list_twin_id, list_key, list_patches)