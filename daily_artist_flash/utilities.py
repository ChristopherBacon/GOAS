from daily_artist_flash.queries import select_queries
import logging
from datetime import datetime
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
load_dotenv()
# readable print out tables
desired_width = 320
pd.set_option("display.width", desired_width)
pd.set_option("display.max_columns", 12)

artist = 'Clean Bandit x Topic'
track_title = 'Drive (feat. Wes Nelson)'
week_prior = '2021-09-06'
week = '2021-09-13'

def fetch_data_as_df(cur, sql):
    cur.execute(sql)
    rows = 0
    df = pd.DataFrame()
    columns = [i[0] for i in cur.description]

    while True:
        dat = cur.fetchmany(50000)
        if not dat:
            break
        temp = pd.DataFrame(dat, columns=columns)
        rows += df.shape[0]

        df = df.append(temp)

    #logging.info('%s rows extracted from Snowflake' % (rows))

    return df


def save_query_data_snowflake(cursor, query, f_name):
    data_df = fetch_data_as_df(cursor, query)
    output_fp = '%s_%s' % (f_name, datetime.today().strftime('%Y-%m-%d'))
    data_df.to_csv(output_fp, index=False)


def connect_to_snowflake():

    USER_SNOW = os.getenv("USER_SNOW")
    PASSWORD = os.getenv("PASSWORD")
    AUTHENTICATOR = os.getenv("AUTHENTICATOR")
    ACCOUNT = os.getenv("ACCOUNT")
    WAREHOUSE = os.getenv("WAREHOUSE")
    DATABASE = os.getenv("DATABASE")
    SCHEMA = os.getenv("SCHEMA")


    # ctx = snowflake.connector.connect(user='chris.bacon@warnerchappellpm.com', password='Starwars128!',
    #                                   authenticator='https://wmg.okta.com', account='wmg-datalab',
    #                                   warehouse='ATLANTIC_UK_SANDBOX_WH_M', database='DF_PROD_DAP_MISC', schema='DAP')

    ctx = snowflake.connector.connect(user=USER_SNOW, password=PASSWORD,
                                      authenticator=AUTHENTICATOR, account=ACCOUNT,
                                      warehouse=WAREHOUSE, database=DATABASE, schema=SCHEMA)

    cursor = ctx.cursor()

    return cursor


hot_hits_uk, todays_hits_apple_uk, todays_top_hits_spotify, spotify_daily_top_200_gb, query_total_streams_dsp = select_queries(
    2021, 9, 13, 'Clean Bandit', 'Drive (feat. Wes Nelson)')

cursor = connect_to_snowflake()


def get_query_dfs():
    hot_hits_uk_df = fetch_data_as_df(cursor, hot_hits_uk)
    todays_hits_apple_uk_df = fetch_data_as_df(cursor, todays_hits_apple_uk)
    todays_top_hits_spotify_df = fetch_data_as_df(cursor, todays_top_hits_spotify)
    spotify_daily_top_200_gb_df = fetch_data_as_df(cursor, spotify_daily_top_200_gb)
    query_total_streams_dsp_df = fetch_data_as_df(cursor, query_total_streams_dsp)

    return hot_hits_uk_df, todays_hits_apple_uk_df, todays_top_hits_spotify_df, spotify_daily_top_200_gb_df, query_total_streams_dsp_df


hot_hits_uk_df, todays_hits_apple_uk_df, todays_top_hits_spotify_df, spotify_daily_top_200_gb_df, query_total_streams_dsp_df = get_query_dfs()


def week_change_calculator(df_week, df_week_prior):
    if (df_week != 'N/A') & (df_week_prior != 'N/A'):
        df_week_prior = df_week - df_week_prior
    elif (df_week == 'N/A') & (df_week_prior != 'N/A'):
        df_week_prior = -df_week_prior
    elif (df_week != 'N/A') & (df_week_prior == 'N/A'):
        df_week_prior = df_week
    elif df_week == df_week_prior:
        df_week_prior = 0
    elif (df_week == 'N/A') & (df_week_prior == 'N/A'):
        df_week_prior = 0
    else:
        df_week_prior = 'err'
    return df_week_prior


def playlist_data_selector(df, week_prior, week, playlist):
    df['DATE_KEY'] = df['DATE_KEY'].astype(str)
    try:
        df = df.loc[(df['DATE_KEY'] == week_prior) & (df['PLAYLIST_NAME'] == playlist)]
        df_week_prior = df.iloc[0,4]
    except Exception:
        df_week_prior = 'N/A'
    try:
        df = df.loc[(df['DATE_KEY'] == week) & (df['PLAYLIST_NAME'] == playlist)]
        df_week = df.iloc[0, 4]
    except Exception:
        df_week = 'N/A'

    df_week_prior = week_change_calculator(df_week, df_week_prior)

    return df_week_prior, df_week

def spotify_daily_200_gb_selector(df, week_prior, week, playlist):
    df['DATE_KEY'] = df['DATE_KEY'].astype(str)
    try:
        df = df.loc[(df['DATE_KEY'] == week_prior) & (df['CHART_NAME'] == playlist)]
        df_week_prior = df.iloc[0,2]
    except Exception:
        df_week_prior = 'N/A'
    try:
        df = df.loc[(df['DATE_KEY'] == week) & (df['CHART_NAME'] == playlist)]
        df_week = df.iloc[0,2]
    except Exception:
        df_week = 'N/A'

    df_week_prior = week_change_calculator(df_week, df_week_prior)

    return df_week_prior, df_week

def youtube_streams_summed(df, week_prior, week):
    df['DATE_KEY'] = df['DATE_KEY'].astype(str)
    try:
        df_week_prior = df.loc[(df['DATE_KEY'] == week_prior) & (df['CUSTOMER_NAME'] == 'YouTube')]
        df_week_prior = df_week_prior['SUMMED_STREAMS'].sum()
    except Exception:
        df_week_prior = 'N/A'
    try:
        df_week = df.loc[(df['DATE_KEY'] == week) & (df['CUSTOMER_NAME'] == 'YouTube')]
        df_week = df_week['SUMMED_STREAMS'].sum()
    except Exception:
       df_week = 'N/A'

    df_week_prior = week_change_calculator(df_week, df_week_prior)

    return df_week_prior, df_week


def create_row():
    data_row = []
    week_selector = [0,1]
    data_point_gatherer = [
        playlist_data_selector(hot_hits_uk_df, week_prior, week, 'Hot Hits UK'),
        playlist_data_selector(hot_hits_uk_df, week_prior, week, 'Today\'s Top Hits'),
        playlist_data_selector(hot_hits_uk_df, week_prior, week, 'Today\'s Hits'),
        spotify_daily_200_gb_selector(spotify_daily_top_200_gb_df, week_prior, week, 'Top 200'),
        youtube_streams_summed(query_total_streams_dsp_df, week_prior, week)
    ]

    for x in data_point_gatherer:
        for y in week_selector:
            data_row.append(x[y])

    return data_row

data_rows = create_row()

def data_row_dict(row_data):
    data_row = {"Hot Hits UK (Spotify)":{"week prior change": row_data[0], "current week":row_data[1]},
                "Today\'s Top Hits (Spotify)":{"week prior change":row_data[2], "current week":row_data[3]},
                "Today\'s Hits (Apple)": {"week prior change":row_data[4], "current week":row_data[5]},
                "Spotify Daily Top 200 (GB)":{"week prior change":row_data[6], "current week":row_data[7]},
                "Youtube Views (Global)":{"week prior change":row_data[8], "current week":row_data[9]}
    }
    return data_row

data_row = data_row_dict(data_rows)

def create_daily_flash():

    index = pd.Index([], name='artist - song')

    columns = pd.MultiIndex.from_product([['Hot Hits UK (Spotify)','Today\'s Top Hits (Spotify)','Today\'s Hits (Apple)',
                                           'Spotify Daily Top 200 (GB)', 'Youtube Views (Global)'],
                                        ['week prior change', 'current week']], names=['source','week'])

    daily_flash = pd.DataFrame(columns= columns, index=index)

    return daily_flash

daily_flash = create_daily_flash()

print(daily_flash)


def add_row_to_daily_flash(daily_flash, artist, track_title):
    artist_track = artist + ' - ' + track_title
    daily_flash = daily_flash.append(pd.DataFrame.from_dict(data_row).unstack().rename(f"{artist_track}"))

    return daily_flash

daily_flash = add_row_to_daily_flash(daily_flash, artist, track_title)




#save to df
print(daily_flash)
