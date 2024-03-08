import spacy
nlp = spacy.load("en_core_web_lg")

import sqlite3
import pandas as pd
from datetime import datetime
import sys
import googlemaps

# Create table proc_log if it does not exist
def create_proc_log_table():
    # connecting to the FlyLeads_user.db . If not existent, creates an empty db
    conn = sqlite3.connect('FlyLeads_user.db')
    c = conn.cursor()

    # IF NOT EXISTS = only creates a table, if the proc_log table doesn't exist
    c.execute(
        "CREATE TABLE IF NOT EXISTS proc_log(Post_ID TEXT, proc_time DATE)")

    # Closing the connection
    c.close()
    conn.close()

    print("proc_log table created")

# Create table proc_posts if it does not exist
def create_proc_posts_table():

    # connecting to the FlyLeads_user.db. If not existent, creates an empty db
    conn = sqlite3.connect('FlyLeads_user.db')
    c = conn.cursor()

    # IF NOT EXISTS = only creates a table, if the proc_posts table doesn't exist
    c.execute(
        "CREATE TABLE IF NOT EXISTS proc_posts(Post_ID TEXT, URL TEXT, Destinations TEXT,\
         Latitude REAL, Longitude REAL, Country_Full_Name TEXT, Requirement TEXT)")

    # Closing the connection
    c.close()
    conn.close()
    print("proc_posts table created")


# insert in the control table the Post_IDs that are in the raw reddits table but not in the control table
def insert_not_in_log():
    conn = sqlite3.connect('FlyLeads_user.db')
    c = conn.cursor()

    sql_query = ('INSERT INTO proc_log(Post_ID, proc_time)\
     SELECT raw_reddits.Post_ID, NULL\
     FROM raw_reddits\
     WHERE NOT EXISTS( SELECT 1 FROM proc_log WHERE raw_reddits.Post_ID = proc_log.Post_ID);')

    c.execute(sql_query)
    conn.commit()

    c.close()
    conn.close()


# Get df with  UP TO 100 records that exist in the control table but have an empty time field
def query_empty_in_log():
    conn = sqlite3.connect('FlyLeads_user.db')
    c = conn.cursor()

    sql_query = "SELECT proc_log.Post_ID, Title, Body, URL\
    FROM proc_log\
    LEFT JOIN raw_reddits on proc_log.Post_ID = raw_reddits.Post_ID\
    WHERE proc_log.proc_time is NULL\
    LIMIT 100"

    posts_df = pd.read_sql_query(sql_query, conn)

    c.close()
    conn.close()

    return posts_df


# Adds a column with countries to the df
def extract_countries(row):
    post_title = row['Title']
    post_body = row['Body']

    # Process the title and body of the post with spaCy to extract countries
    doc_title = nlp(post_title)
    doc_body = nlp(post_body)
    countries = [ent.text for ent in doc_title.ents if ent.label_ == "GPE"] + [ent.text for ent in doc_body.ents if
                                                                               ent.label_ == "GPE"]

    row['Destinations'] = list(set([country.lower() for country in countries]))
    return row


def add_countries_column(df):
    processed_df = df.apply(extract_countries, axis=1)
    return processed_df

# Subset the processed df
def get_subset_df(df):
    subset_df = df[["Post_ID", "URL", "Destinations"]].copy()
    return subset_df


# Expand countries column in the subset df
def get_expanded_df(subset_df):
    expanded_rows = []

    # Iterate over each row in the original DataFrame
    for idx, row in subset_df.iterrows():
        post_id = row['Post_ID']
        url = row["URL"]
        destinations = row['Destinations']

        # If destinations is a list, expand it into separate rows
        if isinstance(destinations, list):
            for destination in destinations:
                expanded_rows.append({'Post_ID': post_id, 'URL': url, 'Destinations': destination})
        # If destinations is a string, add it as a single row
        else:
            expanded_rows.append({'Post_ID': post_id, 'URL': url, 'Destinations': destinations})

    # Create DataFrame from the list of expanded rows
    expanded_df = pd.DataFrame(expanded_rows)

    return expanded_df

# Aggregate all the process functions and returns processed df
def process_df(df):
    column_added_df = add_countries_column(df)
    subset_df = get_subset_df(column_added_df)
    expanded_df = get_expanded_df(subset_df)
    print("initial process done.")
    return expanded_df


# Function to geocode addresses and return latitude and longitude
def geocode_address(address, gmaps):

    try:
        # Geocode the address
        geocode_result = gmaps.geocode(address)
        # Extract latitude and longitude
        lat = geocode_result[0]['geometry']['location']['lat']
        lng = geocode_result[0]['geometry']['location']['lng']
        return lat, lng
    except:
        # If geocoding fails, return None
        return None, None

def reverse_geocode(lat, lng, gmaps):
    try:
        # Reverse geocode the coordinates
        reverse_geocode_result = gmaps.reverse_geocode((lat, lng))
        # Extract country from the result
        country = None
        for component in reverse_geocode_result[0]['address_components']:
            if 'country' in component['types']:
                country = component['long_name']
                break
        return country
    except:
        # If reverse geocoding fails, return None
        return None

# Adding lat and long columns to our df
def add_geo_columns(results_df, gmaps):
    results_df['Latitude'], results_df['Longitude'] = zip(*results_df['Destinations'].
                                                          apply(lambda x: geocode_address(x, gmaps)))
    print("lat, lon cols added")
    results_df['Country_Full_Name'] = results_df.apply(lambda row: reverse_geocode(row['Latitude'],
                                                                                   row['Longitude'], gmaps), axis=1)
    print("country_name cols added")
    return results_df


def get_requirements_p_country():
    # Evaluate if it makes sense to read this table from the database
    all_travel_requir_explan = pd.read_csv("all_travel_requirements_expl.csv")

    # Removing (Departure =) Passport column and Eliminating the duplicates
    shortlist_requirements = all_travel_requir_explan.drop(labels=["Passport"], axis=1).drop_duplicates().sort_values(
        "Destination")

    # Removing the -1 values becase it means they are from the same country
    shortlist_requirements.drop(shortlist_requirements[shortlist_requirements['Requirement'] == '-1'].index,
                                inplace=True)

    # Grouping the Requirements on a list
    grouped_requirements = shortlist_requirements.groupby('Destination')['Requirement'].agg(list).reset_index()
    grouped_requirements["Requirement"] = grouped_requirements["Requirement"].apply(lambda x: ', '.join(x))

    return grouped_requirements

def add_requirements_column(results_df, grouped_requirements):
    end_proc_df = results_df.merge(grouped_requirements, how="left", left_on="Country_Full_Name",
                                  right_on="Destination").drop(labels = ["Destination"], axis=1)

    return end_proc_df

# Append the df with new columns to proc_posts table (df to ddb)
def append_proc_messages(messages_df):
    # connecting to the db
    conn = sqlite3.connect('FlyLeads_user.db')
    c = conn.cursor()

    # Appends messages to the raw_messages table
    messages_df.to_sql("proc_posts", conn, if_exists="append", index=False)

    # closing connection
    c.close()
    conn.close()

# Update the NULL records of the column proc_time in proc_loc table with current timestamp
def update_proc_time():
    now = datetime.now()
    date_string = now.strftime("%d/%m/%Y %H:%M:%S")

    conn = sqlite3.connect('FlyLeads_user.db')
    c = conn.cursor()

    sql_query = 'UPDATE proc_log SET proc_time = ? WHERE proc_time IS NULL'

    c.execute(sql_query, (date_string,))
    conn.commit()

    c.close()
    conn.close()

if __name__ == '__main__':

    key = sys.argv[1]
    gmaps = googlemaps.Client(key=key)

    # Create proc_log table in db if not existent
    create_proc_log_table()
    # Create proc_posts table in db if not existent
    create_proc_posts_table()

    # Insert in the control table the POST_IDs that are in the raw_reddits table but not in the control table
    insert_not_in_log()

    # Read up to 100 posts that exist in the control table but have an empty time field
    posts_df = query_empty_in_log()

    # Process posts and calculate new fields
    results_df = process_df(posts_df)

    # Adding geo columns
    add_geo_columns(results_df, gmaps)

    # getting list of requirements per country
    grouped_requirements = get_requirements_p_country()

    # Adding requirements list
    end_proc_df = add_requirements_column(results_df, grouped_requirements)

    # Insert them in the processed table proc_messages
    append_proc_messages(end_proc_df)

    # Update the proc_time field in the proc_log table
    update_proc_time()




