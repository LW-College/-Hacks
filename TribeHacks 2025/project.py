import sqlite3
import pandas as pd
import json
from google import genai
import os
from textual.app import App
from textual.widgets import Input, Label, Button, Static
from textual.containers import Vertical

##requres q sqlite database to be connected
def get_row_mapping(table_name, match_value):
    cursor = conn.cursor()

    # Get the top row (first row by rowid)
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY rowid DESC LIMIT 1;")
    top_row = cursor.fetchone()
    
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    column_names = [col[1] for col in columns_info]
 
    # Find the row that starts with the given value
   # cursor.execute(f"SELECT * FROM {table_name} WHERE column_name LIKE ?", (f"{match_value}%",))
    cursor.execute(f"SELECT * FROM {table_name} WHERE PROD_ID = ?", (match_value,))
    matched_row = cursor.fetchone()

    if top_row and matched_row:
        # Create a hashmap using top_row values as keys and matched_row values as values
        return dict(zip(column_names, matched_row))
    else:
        return None

def prompt(t):
   return chat.send_message(t)

id="Prod-2964925"
SYSTEM=None
with open("prompt.txt", "r") as file:
   SYSTEM = file.read()+id

# The client gets the API key from the environment variable.
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

chat = client.chats.create(model="gemini-2.5-flash")

# 1. Load the data
file_name = "furgostat.csv"
df = pd.read_csv(file_name)


# 2. Clean up column names for easier SQL querying
df.rename(columns={
### PROD IDENTIFICATION ###
   '<ID>': 'PROD_ID', # Unique ID number. "PROD-********"
   '<Name>': 'PROD_NAME', # Product name. Long as shit.
   'GLBL_PROD_HIER': 'PROD_HIER', # Product category. "*********"
   'GPH Full Path': 'PROD_FULL_PATH', # Product full hierarchical description.
  
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
   'Pressure - Maximum - FEI': 'SPEC_MAX_PRESSURE', # ADDED: Operational limit
   'Pressure - Minimum': 'SPEC_MIN_PRESSURE',  # ADDED: Operational requirement
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

# 3. Connect to the SQLite database file
conn = sqlite3.connect('data.sqlite')




# 4. Write the DataFrame to a new SQL table called 'PRODUCTS'
#    'if_exists="replace"' means it will overwrite the table if it exists
#    'index=False' prevents Pandas from writing the DataFrame index as a column
df.to_sql('PRODUCTS', conn, if_exists='replace', index=False)



hashmap=get_row_mapping('PRODUCTS', id)


safety, appearance, enviormental = 0.0, 0.0, 0.0
end="based on the customer's inputs, give the float values in the order: safety, appearance, enviormental. they should go out to 2 decimal places and be seperated by spaces"
'''''
while True:
   message = input()
   if message[:4] == 'exit':
      break
   res = prompt(message)
   if 'exit' in res.text:
      break
   elif 'rqst' in res.text:
      a=''
      txt=res.text[res.text.find('rqst'):]
      txt=txt[txt.find(' ')+1:]
      while ':' in txt:
         a= a+' '+(str(hashmap[txt[:txt.find(':')]]))
         txt=txt[txt.find(':')+1:]
      a=a+' '+txt
      print(res.text)
      res=prompt("Here is the information: "+a)
   print(res.text)

values=prompt(end).text
try:
   safety, appearance, enviormental = map(float, values.split())
except ValueError:
   safety, appearance, enviormental = 0.0, 0.0, 0.0

print("Safety:", safety)
print("Appearance:", appearance)
print("Environmental:", enviormental)
'''''

class TextInputOutputApp(App):
    CSS_PATH = None  # You can define a CSS file if you want styling
    
    def compose(self):
        yield Vertical(
            Label("Enter your message below:"),
            Input(placeholder="Type something..."),
            Button("Submit"),
            Label(prompt(SYSTEM).text, id="output")
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        input_widget = self.query_one(Input)
        output_widget = self.query_one("#output", Static)
        ender=None
        ut = input_widget.value
        if ut[:4] == 'exit':
            ender=True
        res = prompt(ut)
        if 'exit' in res.text:
            ender=True
        elif 'rqst' in res.text:
            a=''
            txt=res.text[res.text.find('rqst'):]
            txt=txt[txt.find(' ')+1:]
            while ':' in txt:
               a= a+' '+(str(hashmap[txt[:txt.find(':')]]))
               txt=txt[txt.find(':')+1:]
               a=a+' '+txt
            res=prompt("Here is the information: "+a)
            output_widget.update(res.text)
        output_widget.update(res.text)
        input_widget.value=""
        if ender:
            values=prompt(end).text
            try:
               safety, appearance, enviormental = map(float, values.split())
            except ValueError:
               safety, appearance, enviormental = 0.0, 0.0, 0.0

            output_widget.update(output_widget.content+f"\nSafety: {safety}\nAppearance: {appearance}\nEnvironmental: {enviormental}")
            input_widget.disabled = True
            event.button.disabled = True
            sort()
    def out(self, temp):
         output_widget = self.query_one("#output", Static)
         output_widget.update(temp)



# 5. Close the connection
conn.close()


DATABASE_NAME = "hackathon_db.sqlite"


### THESE ARE THE WEIGHTS YOU CAN ADJUST ###
cert_weight = safety # --> SAFETY SCORE
warn_weight = enviormental # --> ENVIORNMENTAL SCORE
spec_weight = appearance # --> PHYSICAL SIMILARITY SCORE
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
def sort():  
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
    king.out(king,"\nOverall GREEN_SCORE assigned, top three products chosen:\n"+sorted_df[['PROD_ID', 'GREEN_SCORE']].head(3))



if __name__ == "__main__":
    king=TextInputOutputApp().run()
