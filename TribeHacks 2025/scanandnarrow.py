import sqlite3
import pandas as pd

DATABASE_NAME = "hackathon_db.sqlite"


### THESE ARE THE WEIGHTS YOU CAN ADJUST ###
cert_weight = 1 # --> SAFETY SCORE
warn_weight = 1 # --> ENVIORNMENTAL SCORE
spec_weight = 1 # --> PHYSICAL SIMILARITY SCORE
### ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ###

warn_critical_weight = 2 # Weight for critical warnings. Higher than 1 makes it more significant.

pressure_weight = 1 # Weight for pressure difference in SPEC_SCORE calculation.
flow_weight = 1 # Weight for flow rate in SPEC_SCORE calculation.
mount_weight = 1 # Weight for mount type match in SPEC_SCORE calculation.
finish_weight = 1 # Weight for finish type match in SPEC_SCORE calculation.



def get_weight(column):
    weight = input(f"Enter weight for {column} (0.0-1.0): ")
    try:
        weight = float(weight)
        if 0.0 <= weight <= 1.0:
            return weight
        else:
            print("Weight must be between 0.0 and 1.0. Please try again.")
            return get_weight(column)
    except ValueError:
        print("Invalid input. Please enter a numeric value between 0.0 and 1.0.")
        return get_weight(column)

def clean(df, column):
    if column not in df.columns:
        print(f"Error: Column '{column}' not found in DataFrame.")
        return df
    try:
        # Convert to string, split, and take the first part
        df[column] = df[column].astype(str).str.split(' ', expand=True)[0]
        # Convert to numeric, coercing errors to NaN
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].fillna(0)
    except Exception as e:
        print(f"An error occurred during cleaning column '{column}': {e}")
    return df

def normalize(df, column):
    if column not in df.columns:
        print(f"Error: Column '{column}' not found in DataFrame.")
        return df
    try:
        min_val = df[column].min()
        max_val = df[column].max()
        if max_val == min_val:
            df[column] = 0
        else:
            df[column] = (df[column] - min_val) / (max_val - min_val)
    except Exception as e:
        print(f"An error occurred during normalizing column '{column}': {e}")
    return df

def yespoints(df, columns): 
    if not all(col in df.columns for col in columns):
        print(f"Error: One or more columns in {columns} not found in DataFrame.")
        return pd.Series([0]*len(df), index=df.index)
    try:
        return df[columns].apply(lambda row: sum(str(val).strip().lower() == 'yes' for val in row), axis=1)
    except Exception as e:
        print(f"An error occurred during processing columns '{columns}': {e}")
        return pd.Series([0]*len(df), index=df.index)

if __name__ == "__main__":
    ### GET USER PRODUCT ###
    product_id = input("Enter the product ID to search for: ")

    ### GRAB COLUMNS ###
    conn = sqlite3.connect('hackathon_db.sqlite')
    prod_hier = f"SELECT PROD_HIER FROM PRODUCTS WHERE PROD_ID = '{product_id}'"
    sql_query = f"""SELECT
        PROD_ID,
        PROD_NAME,
        PROD_HIER,
        PROD_FULL_PATH,
        PROD_DESC,
    
        CERT_ECOLOGO,
        CERT_ENERGY_STAR,
        CERT_GREEN_SEAL,
        CERT_WATER_SENSE,
        CERT_ADA,
        CERT_EPD,

        WARN_CA_YN,
        WARN_CA_TEXT,
        WARN_CRITICAL,

        SPEC_FLOW_RATE,
        SPEC_SHOWER_FLOW,
        SPEC_TUB_FLOW,
        SPEC_MAX_PRESSURE,
        SPEC_MIN_PRESSURE,
        SPEC_TEMP_RANGE,

        SPEC_APPLICATION,
        SPEC_FINISH_CAT,
        SPEC_FINISH_NAME,
        SPEC_MATERIAL,
        SPEC_STYLE_NAME,
        SPEC_STYLE_GENERIC,
        SPEC_MOUNT_TYPE,
        SPEC_INSTALL_TYPE,
        SPEC_VALVE_TYPE,

        SPEC_DRAIN_YN,
        SPEC_FAUCET_CENTER,
        SPEC_FAUCET_MOUNT,
        SPEC_FAUCET_TYPE_BATH,
        SPEC_FAUCET_TYPE_KITCHEN,
        SPEC_SPOUT_HEIGHT,
        SPEC_SPOUT_REACH,
        SPEC_NUM_HANDLES,
        SPEC_HANDLE_TYPE,

        SPEC_TOILET_TYPE,
        SPEC_DUAL_FLUSH,
        SPEC_FLUSH_TYPE,
        SPEC_BOWL_SHAPE,
        SPEC_TOILET_ROUGH_IN,
        SPEC_TANK_ONLY_YN
        FROM PRODUCTS
        WHERE PROD_HIER = (SELECT PROD_HIER FROM PRODUCTS WHERE PROD_ID = ?)"""
    df = pd.read_sql_query(sql_query, conn, params=(product_id,))
    conn.close()

    ### CERT TALLIES ###
    cert_columns = [
        'CERT_ECOLOGO', 'CERT_ENERGY_STAR', 'CERT_GREEN_SEAL',
        'CERT_WATER_SENSE', 'CERT_ADA'
    ]
    df['CERT_SCORE'] = yespoints(df, cert_columns)
    df['CERT_SCORE'] += df['CERT_EPD'].astype(str).str.strip().ne('').astype(int)
    sorted_df = df.sort_values(by='CERT_SCORE', ascending=False)
    print("\nCERT_SCORE assigned (1 point per 'Yes' certification):")
    print(sorted_df[['PROD_ID', 'PROD_DESC', 'CERT_SCORE']])

    ### WARNINGS ###
    df['WARN_SCORE'] = yespoints(df, ['WARN_CA_YN'])
    df['WARN_SCORE'] += warn_critical_weight*(df['WARN_CRITICAL'].fillna('').astype(str).str.strip().ne('').astype(int))
    sorted_df = df.sort_values(by='WARN_SCORE', ascending=False)
    print("\nWARN_SCORE assigned (1 point per 'Yes' warning and 2 points if there's warning text):")
    print(sorted_df[['PROD_ID', 'PROD_DESC', 'CERT_SCORE', 'WARN_SCORE']])

    ### FLOW RATE ###
    spec_columns = [
        'SPEC_FLOW_RATE', 'SPEC_SHOWER_FLOW', 'SPEC_TUB_FLOW', 
    ]
    for col in spec_columns:
        df = clean(df, col)
        df = normalize(df, col)
    df['FLOW_SCORE'] = df[spec_columns].sum(axis=1)


    ### PRESSURE ###
    df = clean (df, 'SPEC_MAX_PRESSURE')
    df = normalize(df, 'SPEC_MAX_PRESSURE')
    df = clean (df, 'SPEC_MIN_PRESSURE')
    df = normalize(df, 'SPEC_MIN_PRESSURE')
    df ['PRESSURE_SCORE'] = (df['SPEC_MAX_PRESSURE'] - df['SPEC_MIN_PRESSURE'])*pressure_weight

    ### MOUNT TYPE ###
    mount_target = df.loc[df['PROD_ID'] == product_id, 'SPEC_FAUCET_MOUNT'].iloc[0]
    df['MOUNT_TYPE_SCORE'] = (df['SPEC_FAUCET_MOUNT'] == mount_target).astype(int) * mount_weight
    
    sorted_df = df.sort_values(by='MOUNT_TYPE_SCORE', ascending=False)
    print(sorted_df[['PROD_ID', 'SPEC_FAUCET_MOUNT', 'MOUNT_TYPE_SCORE']])

    ### FINISH TYPE ###
    finish_target = df.loc[df['PROD_ID'] == product_id, 'SPEC_FINISH_CAT'].iloc[0]
    df['FINISH_TYPE_SCORE'] = (df['SPEC_FINISH_CAT'] == finish_target).astype(int) * finish_weight
    
    sorted_df = df.sort_values(by='FINISH_TYPE_SCORE', ascending=False)
    print(sorted_df[['PROD_ID', 'SPEC_FINISH_CAT', 'FINISH_TYPE_SCORE']])

#______________________________________________________________________________________________ Prod-2725559

    ### PUT SPEC ALL TOGETHER ###
    df['SPEC_SCORE'] = df['PRESSURE_SCORE']*pressure_weight + df['FLOW_SCORE']*flow_weight + df['MOUNT_TYPE_SCORE']*mount_weight + df['FINISH_TYPE_SCORE']*finish_weight

    sorted_df = df.sort_values(by='SPEC_SCORE', ascending=False)
    print(sorted_df[['PROD_ID', 'SPEC_SCORE']])

    ### OVERALL SCORE CALCULATION ###
    df['GREEN_SCORE'] = ((df['CERT_SCORE']*cert_weight) - (df['WARN_SCORE']*warn_weight)) * (df['SPEC_SCORE']*spec_weight)

    sorted_df = df.sort_values(by='GREEN_SCORE', ascending=False)
    print("\nOverall GREEN_SCORE assigned, top three products chosen:")
    print(sorted_df[['PROD_ID', 'GREEN_SCORE']].head(3))