from daily_artist_flash.utilities import connect_to_snowflake, get_query_dfs, \
    create_row, data_row_dict, create_daily_flash, add_row_to_daily_flash, daily_flash_to_excel
from daily_artist_flash.queries import select_queries
from dotenv import load_dotenv
from datetime import datetime, timedelta


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

    # create dates for queries
    current_date = datetime.now().date()
    # moves week back to Thursday just gone to ensure weekly data ingest
    Thursday_delta = timedelta(days=4)
    week = current_date - Thursday_delta
    wk_year = int(week.year)
    wk_month = int(week.month)
    wk_day = int(week.day)
    wk = str(week)

    delta = timedelta(weeks=1)
    week_prior = str(week - delta)


    # iterate through utility functions and collect data
    for k, v in artist_track_dict.items():
        selected_queries = select_queries(wk_year, wk_month, wk_day, k, v)
        query_dfs = get_query_dfs(selected_queries, cursor)
        data_rows = create_row(query_dfs, week_prior, wk)
        data_row = data_row_dict(data_rows)
        daily_flash = add_row_to_daily_flash(data_row, daily_flash, k, v)

    #print(daily_flash)
    # creates excel
    daily_flash_to_excel(daily_flash, artist_track_dict, week)


if __name__ == "__main__":
    main()
