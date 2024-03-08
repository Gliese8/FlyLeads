import sys
import pandas as pd
import praw
import datetime
import sqlite3


# accessing parameters from CLI
def get_parameters():
    client_id = sys.argv[1]
    client_secret = sys.argv[2]
    user_agent = sys.argv[3]
    subreddit_name = sys.argv[4]

    return client_id, client_secret, user_agent, subreddit_name


# Gets 100 posts from a reddit subreddit
def get_100_posts(subreddit, client_id, client_secret, user_agent):
    reddit = praw.Reddit(client_id=client_id,
                         client_secret=client_secret,
                         user_agent=user_agent)

    subreddit_name = subreddit  # 'travel' 'TravelNoPics'
    subreddit = reddit.subreddit(subreddit_name)
    posts = subreddit.hot(limit=1)  # Number of Posts

    return posts


# Pass the data retrieved to a pandas DataFrame
def raw_data_to_df(posts):
    reddit_posts_list = []

    for post in posts:
        post_title = post.title
        post_body = post.selftext[:1000]  # Getting only the first 1000 characters
        author = post.author.name if post.author else "[deleted]"
        creation_date_utc = post.created_utc
        creation_date = datetime.datetime.fromtimestamp(creation_date_utc, datetime.timezone.utc).strftime('%Y-%m-%d')
        score = post.score
        num_comments = post.num_comments
        url = post.url
        over_18 = post.over_18
        post_id = post.id

        post_dict = {
            'Title': post_title,
            'Body': post_body,
            'Author': author,
            'Creation_Date': creation_date,
            'Score': score,
            'Num_Comments': num_comments,
            'URL': url,
            'Over_18': over_18,
            'Post_ID': post_id
        }

        reddit_posts_list.append(post_dict)

    reddit_posts_df = pd.DataFrame(reddit_posts_list)

    return reddit_posts_df

# Function to create a raw_reddits table in the FlyLeads_user.db
def create_raw_reddits_table():
    # connecting to the FlyLeads_user.db. If not existent, creates an empty db
    conn = sqlite3.connect('FlyLeads_user.db')
    c = conn.cursor()

    # IF NOT EXISTS = only creates a table, if the raw_reddits table doesn't exist
    c.execute(
        "CREATE TABLE IF NOT EXISTS raw_reddits(Post_ID TEXT, Title TEXT, Body TEXT, Author TEXT, Creation_Date DATE,\
        Score INTEGER, Num_Comments INTEGER, URL TEXT, Over_18 BOOLEAN)")

    # Closing the connection
    c.close()
    conn.close()

# Function to append records to the raw_reddits table in the FlyLeads_user.db
def data_entry(df):
    # connecting to the db
    conn = sqlite3.connect('FlyLeads_user.db')
    c = conn.cursor()

    # Appends messages to the raw_messages table
    df.to_sql("raw_reddits", conn, if_exists="append", index=False)

    # closing connection
    c.close()
    conn.close()


if __name__ == '__main__':
    # Accessing details from the user from the parameter inputted in the terminal
    client_id, client_secret, user_agent, subreddit_name = get_parameters()

    # Getting the data into pandas df a reddit API
    posts = get_100_posts(subreddit_name, client_id, client_secret, user_agent)
    raw_reddits_df = raw_data_to_df(posts)

    # Creating the table if not exists in the FlyLeads_user.db
    create_raw_reddits_table()

    # Adding the raw_reddits_df into the table of the db
    data_entry(raw_reddits_df)
