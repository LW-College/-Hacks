import pandas as pd
import sqlite3

### LOAD AND READ THE CSV FILE ###
file_name = "fergusondat.csv"
df = pd.read_csv(file_name)

### CLEAN UP COLUMN NAMES ###
df.rename(columns={
    
    ### PROD IDENTIFICATION ###
    '<ID>': 'PROD_ID', # Unique ID number. "PROD-********"
    '<Name>': 'PROD_NAME', # Product name. Long as shit.
    'GLBL_PROD_HIER': 'PROD_HIER', # Product category. "*********"
    'GPH Full Path': 'PROD_FULL_PATH', # Product full hierarchical description.
    'PRODUCT_DESC': 'PROD_DESC',

    ### PROD CERTIFICATIONS ###
    'EcoLogo Certified - FEI': 'CERT_ECOLOGO', # Yes/No
    'Energy Star Compliant - FEI': 'CERT_ENERGY_STAR', # Yes/No
    'Green Seal Certified - FEI': 'CERT_GREEN_SEAL', # Yes/No
    'WaterSense Labeled - FEI': 'CERT_WATER_SENSE', # Yes/No
    'ADA Compliant': 'CERT_ADA', #Yes/No
    'EPDs URL': 'CERT_EPD', # URL. If there is a value in this column, this value is TRUE.

    ### PROD WARNINGS ###
    'CA_CHEM_PROP65_WARN_ELIG': 'WARN_CA_YN', # Whether or not the product has a California warning. Y/N
    'CA_CHEM_MFG_WARN_TXT': 'WARN_CA_TEXT', # The warning text if applicable.
    'DONT_FORGET': 'WARN_CRITICAL', # Warning message. If any, should ALWAYS print.

    ### PROD SPECIFICATIONS - FLOW & PERFORMANCE ###
    'Flow Rate - FEI': 'SPEC_FLOW_RATE', # Flow rate.
    'Showerhead Flow Rate': 'SPEC_SHOWER_FLOW', # Format: "*.* gpm"
    'Tub Spout Flow Rate': 'SPEC_TUB_FLOW',      # Literally no data points.
    'Pressure - Maximum - FEI': 'SPEC_MAX_PRESSURE',
    'Pressure - Minimum': 'SPEC_MIN_PRESSURE',
    'Temperature Range': 'SPEC_TEMP_RANGE',

    ### PROD SPECIFICATIONS - STYLE & PHYSICAL ###
    'Application': 'SPEC_APPLICATION', # Residential or Commercial. Formatted like "Residential;Commercial"
    'Color/Finish Category - FEI': 'SPEC_FINISH_CAT',
    'Color/Finish Name - FEI': 'SPEC_FINISH_NAME',
    'Material - FEI': 'SPEC_MATERIAL',
    'Style Name - FEI': 'SPEC_STYLE_NAME',
    'Style': 'SPEC_STYLE_GENERIC',
    'Mount Type': 'SPEC_MOUNT_TYPE',
    'Installation Type - FEI': 'SPEC_INSTALL_TYPE',
    'Valve Type': 'SPEC_VALVE_TYPE',

    ### PROD SPECIFICATIONS - FAUCET SPECIFIC ###
    'Drain Included': 'SPEC_DRAIN_YN', 
    'Faucet Center Size - FEI': 'SPEC_FAUCET_CENTER', 
    'Faucet Mount - FEI': 'SPEC_FAUCET_MOUNT', 
    'Bathroom Faucet Type': 'SPEC_FAUCET_TYPE_BATH',
    'Kitchen Faucet Type': 'SPEC_FAUCET_TYPE_KITCHEN',
    'Spout Height - FEI': 'SPEC_SPOUT_HEIGHT',
    'Spout Reach - FEI': 'SPEC_SPOUT_REACH',
    'Number of Handles - FEI': 'SPEC_NUM_HANDLES',
    'Handle Type - FEI': 'SPEC_HANDLE_TYPE',

    ### PROD SPECIFICATIONS - TOILET/URINAL SPECIFIC ###
    'Toilet Type - FEI': 'SPEC_TOILET_TYPE',
    'Dual Flush': 'SPEC_DUAL_FLUSH', # Yes/No
    'Flush Type - FEI': 'SPEC_FLUSH_TYPE', #
    'Bowl Shape': 'SPEC_BOWL_SHAPE',
    'Toilet Rough-In': 'SPEC_TOILET_ROUGH_IN',
    'Tank Only - FEI': 'SPEC_TANK_ONLY_YN'

}, inplace=True)

cert_cols = [
    'CERT_ECOLOGO', 'CERT_ENERGY_STAR', 'CERT_GREEN_SEAL',
    'CERT_WATER_SENSE', 'CERT_ADA', 'CERT_EPD'
    ]
df[cert_cols] = df[cert_cols].fillna("No").replace("", "No")

conn = sqlite3.connect('hackathon_db.sqlite')
df.to_sql('PRODUCTS', conn, if_exists='replace', index=False)
conn.close()

print("Data successfully loaded into hackathon_db.sqlite as table 'PRODUCTS'.")