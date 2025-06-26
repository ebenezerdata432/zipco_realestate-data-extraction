import requests
import pandas as pd
import psycopg2
import json

url = "https://api.rentcast.io/v1/properties/random?limit=900"

headers = {
    "accept": "application/json",
    "X-Api-Key": "xxxxxxxxxxxxxxxxxxxx"
}

response = requests.get(url, headers=headers)

data = response.json()

file_name = 'real_estate.json'

with open(file_name, 'w') as file:
    json.dump(data,file, indent=4)
    
    
real_estate_df = pd.read_json('real_estate.json')
real_estate_df.head()


# Transformation Layer
# Replace missing values with appropriate replacements
real_estate_df.fillna({
    'bedrooms': 0.0,
    'bathrooms': 0.0,
    'squareFootage': 0.0,
    'lotSize': 0.0,
    'yearBuilt': 0.0,
    'features': 'Unknown',
    'assessorID': 'Unknown',
    'legalDescription': 'Unknown',
    'ownerOccupied': 0.0,
    'lastSaleDate': 'unknown',
    'propertyType': 'Unknown',
    'propertyTaxes': 'Unknown',
    'subdivision' : 'Unknown',
    'addressLine2': 'Unknown',
    'county': 'Unknown',
    'zoning': 'Unknown',
    'taxAssessments': 'Unknown',
    'hoa': 'Unknown'
      
}, inplace=True)

real_estate_df.info()

# creating the dimension tables
location_dim = real_estate_df[['formattedAddress', 'addressLine1', 'addressLine2', 'city', 'state', 'zipCode', 'county', 
                               'latitude', 'longitude']].copy().drop_duplicates().reset_index(drop=True)
location_dim.index.name= 'location_id'

location_dim = location_dim.reset_index()

location_dim


# sales dimension
sales_dim = real_estate_df[['lastSaleDate', 'lastSalePrice']].copy().drop_duplicates().reset_index(drop=True)
sales_dim.index.name= 'sales_id'

sales_dim = sales_dim.reset_index()

sales_dim



features_dim = features_dim.reset_index()

features_dim

real_estate_df['features'] = real_estate_df['features'].astype(str)


property_fact_table = real_estate_df.merge(sales_dim, on=['lastSaleDate', 'lastSalePrice'], how='left')\
                                    .merge(location_dim, on=['formattedAddress', 'addressLine1', 'addressLine2', 'city', 'state', 'zipCode', 'county', 'latitude', 'longitude'], how='left')\
                                    .merge(features_dim, on=['bedrooms', 'bathrooms', 'squareFootage', 'features','lotSize'], how='left')\
                                    [['id', 'location_id', 'sales_id', 'features_id', 'propertyType', 'yearBuilt', 'assessorID', 'legalDescription', 'subdivision', 'zoning', 'hoa', 'propertyTaxes','ownerOccupied']]
                                    
property_fact_table

property_fact_table.info()


# save into csv
location_dim.to_csv('location_dim.csv', index=False)
sales_dim.to_csv('sales_dim.csv', index=False)
features_dim.to_csv('features_dim.csv', index=False)
property_fact_table.to_csv('property_fact_table_dim.csv', index=False)



# Function to get the database connections
def get_db_connection():
    connection = psycopg2.connect(
        host = 'localhost',
        database='zipco_realestate',
        user='postgres',
        password='datapsl2025@'
    )
    return connection

conn = get_db_connection()


# create functions setting up schemas and tables
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''
                            CREATE SCHEMA IF NOT EXISTS zipco_realestate;

                            DROP TABLE IF EXISTS zipco_realestate.location_dim CASCADE;
                            DROP TABLE IF EXISTS zipco_realestate.sales_dim CASCADE;
                            DROP TABLE IF EXISTS zipco_realestate.features_dim CASCADE;
                            DROP TABLE IF EXISTS zipco_realestate.property_fact_table CASCADE;
                            
                            CREATE TABLE zipco_realestate.location_dim (
                                location_id INTEGER PRIMARY KEY,
                                formattedAddress VARCHAR(1000000),
                                addressLine1 VARCHAR(1000000),
                                addressLine2 VARCHAR(1000000),
                                city VARCHAR(100000),
                                state VARCHAR(100000),
                                zipCode INTEGER,
                                county VARCHAR(100000),
                                latitude FLOAT,
                                longitude FLOAT
                            );
                            
                            CREATE TABLE zipco_realestate.sales_dim (
                                sales_id INTEGER PRIMARY KEY,
                                lastSaleDate VARCHAR(1000000),
                                lastSalePrice FLOAT
                                
                            );
                            
                            CREATE TABLE zipco_realestate.features_dim (
                                features_id INTEGER PRIMARY KEY,
                                bedrooms FLOAT,
                                bathrooms FLOAT,
                                squareFootage FLOAT,
                                features VARCHAR(100000),
                                lotSize FLOAT
                                
                            );
                            
                            CREATE TABLE zipco_realestate.property_fact_table (
                                id VARCHAR(1000000) PRIMARY KEY,
                                location_id INTEGER,
                                sales_id INTEGER,
                                features_id INTEGER,
                                propertyType VARCHAR(100000),
                                yearBuilt FLOAT,
                                assessorID VARCHAR(100000),
                                legalDescription VARCHAR(100000),
                                subdivision VARCHAR(100000),
                                zoning VARCHAR(100000),
                                hoa VARCHAR(1000000),
                                propertyTaxes VARCHAR(1000000),
                                ownerOccupied FLOAT,
                                FOREIGN KEY(location_id) REFERENCES zipco_realestate.location_dim(location_id),
                                FOREIGN KEY(sales_id) REFERENCES zipco_realestate.sales_dim(sales_id),
                                FOREIGN KEY(features_id) REFERENCES zipco_realestate.features_dim(features_id)
                            );
    
                            '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    
    create_tables()
    
    
    ## loading the data into the database tables
conn = get_db_connection()
cursor = conn.cursor()
    
# Insert dataframe into the SQL tables
for _,row in location_dim.iterrows():
    cursor.execute(
    '''INSERT INTO zipco_realestate.location_dim (location_id, formattedAddress, addressLine1, addressLine2,
            city, state, zipCode, county, latitude, longitude)
          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
        (row['location_id'], row['formattedAddress'], row['addressLine1'], row['addressLine2'], row['city'], \
            row['state'], row['zipCode'], row['county'], row['latitude'], row['longitude'])

    )
    

for _,row in sales_dim.iterrows():
    cursor.execute(
    '''INSERT INTO zipco_realestate.sales_dim (sales_id, lastSaleDate, lastSalePrice)
          VALUES(%s, %s, %s)''',
        (row['sales_id'], row['lastSaleDate'], row['lastSalePrice'])
    
    )
    
    
    
for _,row in features_dim.iterrows():
    cursor.execute(
    '''INSERT INTO zipco_realestate.features_dim (features_id, bedrooms, bathrooms, squareFootage, features,
            lotSize)
          VALUES(%s, %s, %s, %s, %s, %s)''',
        (row['features_id'], row['bedrooms'], row['bathrooms'], row['squareFootage'], row['features'], \
              row['lotSize'])
    
    )
    
    
for _,row in property_fact_table.iterrows():
    cursor.execute(
    '''INSERT INTO zipco_realestate.property_fact_table(id, location_id, sales_id, features_id, propertyType,
            yearBuilt, assessorID, legalDescription, subdivision, zoning, 
            hoa, propertyTaxes, ownerOccupied)
          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
        (row['id'], row['location_id'], row['sales_id'], row['features_id'], row['propertyType'], \
            row['yearBuilt'], row['assessorID'], row['legalDescription'], row['subdivision'], row['zoning'], \
             row['hoa'], row['propertyTaxes'], row['ownerOccupied'])
    
    )
    
# commit changes
conn.commit()

#close connection
cursor.close()
conn.close()
