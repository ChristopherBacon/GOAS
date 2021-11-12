from daily_artist_flash.utilities import connect_to_snowflake, get_query_dfs, week_change_calculator, \
    hot_hits_uk, todays_top_hits, todays_hits, spotify_daily_200_gb_selector, youtube_streams_summed, \
    create_row, data_row_dict, create_daily_flash, add_row_to_daily_flash, fetch_data_as_df, daily_flash_to_excel

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
    # create daily flash df
    daily_flash = create_daily_flash()

    # artists & tracks to check
    artist_track_dict = {
        'Oliver Tree': 'Life Goes On',
        'Tiësto & KAROL G': "Don''t Be Shy",
        'Mahalia': 'Roadside (feat. AJ Tracey)',
        'Tion Wayne x Jae5 x Davido': "Who''s True",
        'Ed Sheeran': 'Shivers',
        'Charli XCX': 'Good Ones',
        'Tion Wayne x ArrDee': 'Wid It',
        'Joel Corry x Jax Jones': 'OUT OUT (feat. Charli XCX & Saweetie)',
        'Lizzo': 'Rumors (feat. Cardi B)',
        'Clean Bandit x Topic': 'Drive (feat. Wes Nelson)',
        'Anne-Marie x Little Mix': 'Kiss My (Uh Oh)',
        'Galantis': 'Heartbreak Anthem (with David Guetta & Little Mix)',
        'Anne-Marie & Niall Horan': 'Our Song'
    }

    week_prior = '2021-10-15'
    week = '2021-10-22'

    # iterate through utility functions and collect data
    for k, v in artist_track_dict.items():
        selected_queries = select_queries(2021, 10, 22, k, v)
        query_dfs = get_query_dfs(selected_queries, cursor)
        data_rows = create_row(query_dfs, week_prior, week)
        data_row = data_row_dict(data_rows)
        daily_flash = add_row_to_daily_flash(data_row, daily_flash, k, v)

    print(daily_flash)
    # creates excel
    daily_flash_to_excel(daily_flash, artist_track_dict, week)


if __name__ == "__main__":
    main()

