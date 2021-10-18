from daily_artist_flash.utilities import connect_to_snowflake, get_query_dfs, week_change_calculator, \
    playlist_data_selector, spotify_daily_200_gb_selector, youtube_streams_summed, \
    create_row, data_row_dict, create_daily_flash, add_row_to_daily_flash, fetch_data_as_df

from daily_artist_flash.queries import select_queries

from dotenv import load_dotenv
import os
import datetime
import snowflake.connector
import pandas as pd


def main():
    # load environment variables
    load_dotenv()
    # connect to snowflake
    cursor = connect_to_snowflake()

    daily_flash = create_daily_flash()

    # artists & tracks to check
    artist_track_dict = {
        'Clean Bandit x Topic': 'Drive (feat. Wes Nelson)',
        'Ed Sheeran': 'Bad Habits'
    }
    week_prior = '2021-09-06'
    week = '2021-09-13'

    for k, v in artist_track_dict.items():
        selected_queries = select_queries(2021, 9, 13, k, v)
        query_dfs = get_query_dfs(selected_queries, cursor)
        print(query_dfs[1])
        playlist_data_selector(query_dfs[1], week_prior, week, "Todays Hits")

        data_rows = create_row(query_dfs, week_prior, week)
        print(data_rows)
        data_row = data_row_dict(data_rows)
        daily_flash = add_row_to_daily_flash(data_row, daily_flash, k, v)

    print(daily_flash)




if __name__ == "__main__":
    main()
