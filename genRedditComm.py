import sys
import pandas as pd
import sqlite3
from flights_scapper import _ScrapeFlight
from hotel_scrapper import _ScrapeHotel


# Queries proc_posts by post_id
def query_proc_posts(post_id):
    conn = sqlite3.connect('FlyLeads_user.db')
    c = conn.cursor()

    sql_query = f"SELECT Post_ID, URL, Destinations, Requirement \
                  FROM proc_posts \
                  WHERE Post_ID='{post_id}' \
                  AND Destinations IS NOT NULL \
                  AND Requirement IS NOT NULL"

    proc_post_df = pd.read_sql_query(sql_query, conn)

    c.close()
    conn.close()

    return proc_post_df


# Adding flight info to our dataframe
def get_flight_price(row):
    origin = "Frankfurt"
    destination = row["Destinations"]

    scraper = _ScrapeFlight(origin, destination)
    round_trip_price = scraper.scrape()

    return round_trip_price


def add_flight_columns(df):
    df["Flight_Price"] = df.apply(lambda row: get_flight_price(row), axis=1)
    return df


# Adding Hotel info to our dataframe
def get_hotel_info(destination):
    scraper = _ScrapeHotel(destination)
    hotel_name, lowest_price = scraper.scrape()
    return hotel_name, lowest_price

def add_hotel_columns(df):
    df["Hotel_Name"], df["Hotel_Price"] = zip(*df["Destinations"].apply(get_hotel_info))
    return df

# Get values per row
def get_values(row):
    URL = row["URL"]
    destination = row["Destinations"]
    flight_price = row["Flight_Price"]
    hotel_name = row["Hotel_Name"]
    hotel_price = row["Hotel_Price"]
    travel_restrictions = row["Requirement"]
    return URL, destination, flight_price, hotel_name, hotel_price, travel_restrictions


# Generates a .txt file based on the inputs
def generate_comment_to_file(post_id, URL, destination, flight_price, hotel_name, hotel_price, travel_restrictions):
    file_name = f"Comments_dir/RedditComment_{post_id}_{destination}.txt"

    with open(file_name, 'w') as file:
        file.write(f"URL of the reddit post: {URL}\n")
        file.write(f"\n**Hi!! Do you want to travel to {destination}?**\n")
        file.write(f"Fly there for {flight_price} euros\n")
        file.write(f"Stay at {hotel_name} for {hotel_price} euros \n")

        if travel_restrictions:
            file.write("The **travel restrictions** can be the following:\n")
            restrictions_list = [restriction.strip() for restriction in travel_restrictions.split(',')]
            for restriction in restrictions_list:
                file.write(f"- {restriction}\n")
        else:
            file.write("There are no specific travel restrictions for this destination.\n")

        file.write("\nPlease check the exact restrictions applied to your country before planning your trip.\n")
        file.write(f"\n**TRAVEL HERE: [FlyLeads.com](https://www.flyleads.com/{destination})**")


def generate_files(df, post_id):
    for index, row in df.iterrows():
        if (row["Flight_Price"] is not None) and (row["Hotel_Name"] is not None) and (row["Hotel_Price"] is not None):
            URL, destination, flight_price, hotel_name, hotel_price, travel_restrictions = get_values(row)
            generate_comment_to_file(post_id, URL, destination, flight_price, hotel_name, hotel_price,
                                     travel_restrictions)
            print("Reddit comment File generated")
        else:
            print("Select another Post_ID")


if __name__ == '__main__':
    post_id = sys.argv[1]
    df = query_proc_posts(post_id)
    df = add_flight_columns(df)
    df = add_hotel_columns(df)
    generate_files(df, post_id)

