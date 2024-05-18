import mysql.connector
from mysql.connector import Error
import pandas as pd
import json
import os
import git

# Establish database connection
try:
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='phonephe'
    )
    mycursor = mydb.cursor(buffered=True)
    print("Database connection successful.")
except Error as e:
    print(f"Error: {e}")
    print("DB connection failed.")

# Clone the GitHub repository
repo_url = 'https://github.com/PhonePe/pulse.git'
repo_path = 'C:/Projects/vscode/phonepe'  # your local path
if not os.path.exists(repo_path):
    git.Repo.clone_from(repo_url, repo_path)
else:
    print("Repository already cloned.")

# Function to correct state names
def correct_state_name(state_name):
    corrections = {
        'andaman-&-nicobar': 'Andaman & Nicobar',
        'andhra-pradesh': 'Andhra Pradesh',
        'arunachal-pradesh': 'Arunachal Pradesh',
        'assam': 'Assam',
        'bihar': 'Bihar',
        'chandigarh': 'Chandigarh',
        'chhattisgarh': 'Chhattisgarh',
        'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
        'delhi': 'Delhi',
        'goa': 'Goa',
        'gujarat': 'Gujarat',
        'haryana': 'Haryana',
        'himachal-pradesh': 'Himachal Pradesh',
        'jammu-&-kashmir': 'Jammu & Kashmir',
        'jharkhand': 'Jharkhand',
        'karnataka': 'Karnataka',
        'kerala': 'Kerala',
        'ladakh': 'Ladakh',
        'madhya-pradesh': 'Madhya Pradesh',
        'maharashtra': 'Maharashtra',
        'manipur': 'Manipur',
        'meghalaya': 'Meghalaya',
        'mizoram': 'Mizoram',
        'nagaland': 'Nagaland',
        'odisha': 'Odisha',
        'puducherry': 'Puducherry',
        'punjab': 'Punjab',
        'rajasthan': 'Rajasthan',
        'sikkim': 'Sikkim',
        'tamil-nadu': 'Tamil Nadu',
        'telangana': 'Telangana',
        'tripura': 'Tripura',
        'uttarakhand': 'Uttarakhand',
        'uttar-pradesh': 'Uttar Pradesh',
        'west-bengal': 'West Bengal',
        'lakshadweep': 'Lakshadweep'
    }
    return corrections.get(state_name.lower(), state_name)

# Path to the data directory
path = "C:/Projects/vscode/phonepe/data/aggregated/transaction/country/india/state/"
agg_state_list = os.listdir(path)

# Extract data and create DataFrame
columns = {
    'State': [], 'Year': [], 'Quater': [], 'Transaction_type': [], 
    'Transaction_count': [], 'Transaction_amount': []
}

for state in agg_state_list:
    state_path = os.path.join(path, state)
    agg_years = os.listdir(state_path)
    for year in agg_years:
        year_path = os.path.join(state_path, year)
        agg_quarters = os.listdir(year_path)
        for quarter_file in agg_quarters:
            quarter_path = os.path.join(year_path, quarter_file)
            with open(quarter_path, 'r') as data_file:
                data = json.load(data_file)
                for transaction in data['data']['transactionData']:
                    columns['State'].append(correct_state_name(state))
                    columns['Year'].append(year)
                    columns['Quater'].append(int(quarter_file.strip('.json')))
                    columns['Transaction_type'].append(transaction['name'])
                    columns['Transaction_count'].append(transaction['paymentInstruments'][0]['count'])
                    columns['Transaction_amount'].append(transaction['paymentInstruments'][0]['amount'])

# Create DataFrame
Agg_trans = pd.DataFrame(columns)

# Aggregate user

#This is to direct the path to get the data as states
path="C:/Projects/vscode/phonepe/data/aggregated/user/country/india/state/"
Agg_state_list=os.listdir(path)

#This is to extract the data's to create a dataframe
clm={'State':[], 'Year':[],'Quater':[],'Registered_Users':[], 'App_Opens':[]}

for i in Agg_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            with open(p_k,'r') as Data:
                D=json.load(Data)
                for z in D['data']['aggregated']:
                    reg_users = D['data']['aggregated']['registeredUsers']
                    app_opens = D['data']['aggregated']['appOpens']
                    clm['Registered_Users'].append(reg_users)
                    clm['App_Opens'].append(app_opens)
                    clm['State'].append(correct_state_name(i))
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))


#Successfully created a dataframe
Agg_User = pd.DataFrame(clm)


# Map hover transaction

path="C:/Projects/vscode/phonepe/data/map/transaction/hover/country/india/state/"
Agg_state_list=os.listdir(path)
Agg_state_list

#This is to extract the data's to create a dataframe
clm={'State':[], 'Year':[],'Quater':[],'District':[], 'Transaction_count':[],'Transaction_amount':[]}

for i in Agg_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            with open(p_k,'r') as Data:
                D=json.load(Data)
                for hovertran in D['data']['hoverDataList']:
                  Name=hovertran['name']
                  Count=hovertran['metric'][0]['count']
                  Amount=hovertran['metric'][0]['amount']
                  clm['District'].append(Name[:Name.find(' district')].capitalize())
                  clm['Transaction_count'].append(Count)
                  clm['Transaction_amount'].append(Amount)
                  clm['State'].append(correct_state_name(i))
                  clm['Year'].append(j)
                  clm['Quater'].append(int(k.strip('.json')))

#Successfully created a dataframe
map_tran = pd.DataFrame(clm)

# map hover user

#This is to direct the path to get the data as states
path="C:/Projects/vscode/phonepe/data/map/user/hover/country/india/state/"
Agg_state_list=os.listdir(path)


#This is to extract the data's to create a dataframe
clm={'State':[], 'Year':[],'Quater':[],'Registered_Users':[], 'App_Opens':[],'District':[]}

for i in Agg_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            with open(p_k,'r') as Data:
                D=json.load(Data)
                for z in D['data']['hoverData']:  
                #   print(z)  
                  user_registered=D['data']['hoverData'][z]['registeredUsers']
                  user_app_opens=D['data']['hoverData'][z]['appOpens']
                  clm['District'].append(z[:z.find(' district')].capitalize())
                  clm['Registered_Users'].append(user_registered)
                  clm['App_Opens'].append(user_app_opens)
                  clm['State'].append(correct_state_name(i))
                  clm['Year'].append(j)
                  clm['Quater'].append(int(k.strip('.json')))
                  


# #Successfully created a dataframe
map_user = pd.DataFrame(clm)


#top transaction
#This is to direct the path to get the data as states
path="C:/Projects/vscode/phonepe/data/top/transaction/country/india/state/"
Agg_state_list=os.listdir(path)

#This is to extract the data's to create a dataframe
clm={'State':[],'Year':[],'Quater':[],'pincode':[],'count' :[],'amount':[]}

states_list=[]
districts_list=[]
pincode_list=[]
for i in Agg_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            with open(p_k,'r') as Data:
                D=json.load(Data)
                for z in D['data']['pincode']:
                    EntityName=z['entityName']
                    Count=z['metric']['count']
                    Amount=z['metric']['amount']
                    clm['pincode'].append(EntityName)
                    clm['count'].append(Count)
                    clm['amount'].append(Amount)
                    clm['State'].append(correct_state_name(i))
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))

top_tran=pd.DataFrame(clm)


#top user 
import pandas as pd
import json
import os

#This is to direct the path to get the data as states
path="C:/Projects/vscode/phonepe/data/top/user/country/india/state/"
Agg_state_list=os.listdir(path)

#This is to extract the data's to create a dataframe
clm={'State':[],'Year':[],'Quater':[],'pincode':[],'Registered_user':[]}


for i in Agg_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            with open(p_k,'r') as Data:
                D=json.load(Data)
                for z in D['data']['pincode']:
                    Name=z['name']
                    Registereduser=z['registeredUsers']
                    clm['pincode'].append(Name)
                    clm['Registered_user'].append(Registereduser)
                    clm['State'].append(correct_state_name(i))
                    clm['Year'].append(j)
                    clm['Quater'].append(int(k.strip('.json')))

top_user=pd.DataFrame(clm)



# Mobile user
import pandas as pd
import json
import os

#This is to direct the path to get the data as states
path="C:/Projects/vscode/phonepe/data/aggregated/user/country/india/state/"
Agg_state_list=os.listdir(path)

#This is to extract the data's to create a dataframe
clm={'State':[], 'Year':[],'Quater':[],'Brand':[], 'Brand_count':[],'Brand_percentage':[]}

for i in Agg_state_list:
    p_i=path+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            with open(p_k,'r') as Data:
                D=json.load(Data)
                # Check if the JSON structure contains the expected keys
                
                if D['data']['usersByDevice'] is not None:

                    for z in D['data']['usersByDevice']:
                        brand = z.get('brand', None)
                        brandcount = z.get('count', None)
                        brandpercent = z.get('percentage', None)
                        clm['Brand'].append(brand)
                        clm['Brand_count'].append(brandcount)
                        clm['Brand_percentage'].append(brandpercent)
                        clm['State'].append(correct_state_name(i))
                        clm['Year'].append(j)
                        clm['Quater'].append(int(k.strip('.json')))
#Succesfully created a dataframe
Agg_user_mobile=pd.DataFrame(clm)



# insertion for Aggregate transaction

for index, row in Agg_trans.iterrows():
    mycursor.execute('''INSERT INTO Aggregate_transaction (state, year, Quater,transaction_type, transaction_count,transaction_amount) 
                                 VALUES (%s, %s, %s, %s, %s,%s)''', 
                                 (row['State'], row['Year'], row['Quater'], row['Transaction_type'],row['Transaction_count'], row['Transaction_amount']))
    mydb.commit()

# insertion for Aggregate user

for index, row in Agg_User.iterrows():
    mycursor.execute('''INSERT INTO Aggregate_User (State, Year, Quater, Registered_Users, App_Opens) 
                                 VALUES (%s, %s, %s, %s, %s)''', 
                                 (row['State'], row['Year'], row['Quater'], row['Registered_Users'],row['App_Opens']))
    mydb.commit()

# insertion for map tran

for index, row in map_tran.iterrows():
    mycursor.execute('''INSERT INTO map_tran (State, Year, Quater, District, Transaction_count,
       Transaction_amount) 
                                 VALUES (%s, %s, %s, %s, %s,%s)''', 
                                 (row['State'], row['Year'], row['Quater'], row['District'],row['Transaction_count'],row['Transaction_amount']))
    mydb.commit()

# insertion for map user

for index, row in map_user.iterrows():
    mycursor.execute('''INSERT INTO map_user (State, Year, Quater, Registered_Users, App_Opens,District) 
                                 VALUES (%s, %s, %s, %s, %s,%s)''', 
                                 (row['State'], row['Year'], row['Quater'], row['Registered_Users'],row['App_Opens'],row['District']))
    mydb.commit()

# insertion for top tran 

for index, row in top_tran.iterrows():
    mycursor.execute('''INSERT INTO top_tran (State, Year, Quater, pincode, count,amount) 
                                 VALUES (%s, %s, %s, %s, %s,%s)''', 
                                 (row['State'], row['Year'], row['Quater'], row['pincode'],row['count'],row['amount']))
    mydb.commit()

# insertion for top user

for index, row in top_user.iterrows():
    mycursor.execute('''INSERT INTO top_user (State, Year, Quater, pincode, Registered_user) 
                                 VALUES (%s, %s, %s, %s, %s)''', 
                                 (row['State'], row['Year'], row['Quater'], row['pincode'],row['Registered_user']))
    mydb.commit()

# insertion for agg_user_mobile

for index, row in Agg_user_mobile.iterrows():
    mycursor.execute('''INSERT INTO mobile_users (State, Year, Quater, brand, brand_count,brand_percentage) 
                                 VALUES (%s, %s, %s, %s, %s,%s)''', 
                                 (row['State'], row['Year'], row['Quater'], row['Brand'],row['Brand_count'],row['Brand_percentage']))
    mydb.commit()

# Close the cursor and connection
mycursor.close()
mydb.close()
