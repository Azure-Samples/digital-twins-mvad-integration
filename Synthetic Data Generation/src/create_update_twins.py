from azure.core.exceptions import ResourceExistsError
from adt_sdk import ADTInstance
from utils.utils_adt import get_schema_into_dfs, transform_to_json, create_patch_update

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import pandas as pd
import traceback
import yaml

def main(args):

    AZURE_URL = "kaipkiun2DhAdtInstance.api.eus.digitaltwins.azure.net"

    ADTInstance1 = ADTInstance(args.adt_url, logging_enable=False)

    ###### Read data ######
    df_topology = pd.read_csv(args.topology_file)
    df_inittwins = pd.read_csv(args.inittwin_file)
    df_updatetwins = pd.read_csv(args.updatetiwn_file)

    ###### Create ADT model ######
    #Optional: ensure ADT models don't exist beofre creating, else error
    # model_id=""
    # ADTInstance1.delete_model(model_id)

    list_models=[]
    for i in os.listdir(args.models_folder):
        model_json = yaml.safe_load(open(os.path.join(args.models_folder,i)))
        list_models.append(model_json)
    try:
        ADTInstance1.create_models(list_models)
    except ResourceExistsError as e:
        print('ModelId might already exist, Error message: {}'.format(e.error.message))
    except Exception as e:
        raise e

    ###### Add schema data to dfs using model jsons ######
    # Add schema to df_inittwins and df_updatetwins
    df_inittwins, df_updatetwins = get_schema_into_dfs(list_models, df_inittwins, df_updatetwins)

    ###### Create ADT twin with properties ######
    #Optional: ensure ADT twins don't exist beofre creating, else error
    # list_twin_ids=[]
    # for twin_i in list_twin_ids:
    #     ADTInstance1.delete_digital_twin(twin_i)

    list_init_dicts, list_twin_ids = transform_to_json(df_inittwins)
    list_twins = []
    for i, twindict_i in enumerate(list_init_dicts):
        created_twin = ADTInstance1.upsert_digital_twin(list_twin_ids[i], twindict_i)
        list_twins.append(created_twin)
    print(f'##### {len(list_twins)} new twins created')

    ###### Add relationships to twins ######
    for i in df_topology.index:
        machine_relationship = {
            "$relationshipId": f"{df_topology.loc[i]['sourceId']}ownedBy{df_topology.loc[i]['targetId']}",
            "$sourceId": df_topology.loc[i]["sourceId"],
            "$relationshipName": df_topology.loc[i]["relationshipName"],
            "$targetId": df_topology.loc[i]["targetId"]
        }

        ADTInstance1.upsert_relationship(machine_relationship)

    ###### Example query using relationships ######
    query_expression = f"""
    SELECT * FROM digitaltwins t
    where IS_OF_MODEL('dtmi:syntheticfactory:sourcemachine;1')
    """
    query_result = ADTInstance1.query_twins(query_expression)
    print(query_result)

    query_expression = f"""
    SELECT t,ct FROM digitaltwins t
    JOIN ct RELATED t.isParent
    WHERE ct.$dtId = 'C'
    """
    query_result = ADTInstance1.query_twins(query_expression)
    print(query_result)


    ###### Run updates to twins ######
    def func_updatepatch(update_i,row):
        if update_i%args.rate_print_update==0:
                print(f"Running twin update:{update_i}")
        for try_i in range(args.update_retries):
            try:
                twin_id, key, patch = create_patch_update(row)
                ADTInstance1.update_component(twin_id, key, patch)
            except Exception as e:
                print(f'{type(e).__name__}: {e}')
                traceback.print_exc()
                continue
            else:
                break
        else:
            print(f'All {args.update_retries} retries failed for update {i}:{row}')

    def runner():
        threads= []
        with ThreadPoolExecutor(max_workers=args.arg_max_workers) as executor:
            for update_i in df_updatetwins.index.tolist() :
                threads.append(executor.submit(func_updatepatch, update_i, df_updatetwins.loc[update_i,:]))

            for task in as_completed(threads):
                if task.result() is not None:
                    print(task.result())




if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--adt_url', type=str, default='adt-synthetic-data.api.eus.digitaltwins.azure.net',
            help='Url of ADT instance')
    parser.add_argument('--models_folder', type=str, default='../data/models_json',
            help='folder containing json files of ADT models')
    parser.add_argument('--topology_file', type=str, default='../data/topology.csv',
            help='topology file keeping relationship data')
    parser.add_argument('--inittwin_file', type=str, default='../data/initial_twins.csv',
            help='file with initial values to initialize ADT twins')
    parser.add_argument('--updatetiwn_file', type=str, default='../data/update_stream.csv',
            help='telemetry data to update twin properties')
    parser.add_argument('--update_retries', type=int, default=5, help='Number of retries allowed in for loop for patch update')
    parser.add_argument('--arg_max_workers', type=int, default=20, help='Number of max_workers for multithreading')
    parser.add_argument('--rate_print_update', type=int, default=200, help='Number of updates after which to print out status')

    args = parser.parse_args()

    main(args)

