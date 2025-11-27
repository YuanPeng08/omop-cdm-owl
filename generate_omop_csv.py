import csv
import types
import datetime
import operator
import functools
import re
from collections import defaultdict
import psycopg2
from sqlalchemy  import create_engine, inspect

import pandas as pd

# Get the current OMOP CDM Stucture in csv
# conn = psycopg2.connect(
#     dbname="ohdsi",
#     user="ohdsi_admin_user",
#     password="bBMROrV2KGfXU571CkBS2Rm1w7hsbH8Hk292Ok4M",
#     host="localhost",
#     port="5433")

db_url="postgresql://ohdsi_admin_user:bBMROrV2KGfXU571CkBS2Rm1w7hsbH8Hk292Ok4M@172.16.7.2:5432/ohdsi"
#conn = psycopg2.connect(db_url)
engine=create_engine(db_url)

print("connected to db")

print("inspecting db")
inspector = inspect(engine)


table_structure = []
column_stucture = []

for table_name in inspector.get_table_names(schema='cds_cdm'):
    table_structure.append({
        "cdmTableName": table_name,
        "schema": "cds_cdm",
        "isRequired": "Yes" if table_name in ["person","observation_period"] else "No",
        "tableDescription": "NA",
        "userGuidance": "NA",
        "etlConventions": "NA"
    })

    # Primary Key
    pk_cols = inspector.get_pk_constraint(table_name,schema='cds_cdm').get('constrained_columns',[])
    # Foreign Keys
    fks=inspector.get_foreign_keys(table_name,schema='cds_cdm')
    #print(fks)

    for column in inspector.get_columns(table_name,schema='cds_cdm'):
        column_name = column['name']
        pk_boolean = "No" if column_name not in pk_cols else "Yes"
        fk_boolean = "No" if column_name not in [item for sublist in [fk['constrained_columns'] for fk in fks] for item in sublist] else "Yes"
        is_required = "No" if column['nullable'] else "Yes"

        fk_tab = []
        fk_col = []
        for fk in fks:
            if column['name'] in fk['constrained_columns']:
                fk_tab.append(fk['referred_table'])
                fk_col.append(fk['referred_columns'])
        #fk_tab = ",".join(fk_tab) if fk_tab else "NA"
        #fk_col = ",".join(fk_col) if fk_col else "NA"
        #print(fk_tab,fk_col)
        column_stucture.append({
            "cdmTableName": table_name,
            "cdmFieldName": column_name,
            "cdmDataType": str(column['type']).lower(),
            "userGuidance": "NA",
            "etlConventions": "NA",
            "isRequired": is_required,
            "isPrimaryKey": pk_boolean,
            "isForeignKey": fk_boolean,
            "fkTableName": str(fk_tab[0].upper()) if fk_tab else "NA",
            "fkFieldName": str(fk_col[0][0].upper()) if fk_col else "NA",
            "fkDomain": "NA",
            "fkClass": "NA",
            "unique DQ identifiers": "NA"
        })

table_df = pd.DataFrame(table_structure)
field_df = pd.DataFrame(column_stucture)
table_df.to_csv('omop_cdm_table_structure.csv', index=False)
field_df.to_csv('omop_cdm_field_structure.csv', index=False)


print("finished")