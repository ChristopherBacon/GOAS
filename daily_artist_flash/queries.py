import datetime
# set up logging
import logging


# This includes both UK & NON-UK charts information to be filtered
def select_queries(year, month, day, artist: str, track_title: str):
    date = datetime.date(year, month, day)
    #Gives us the week prior
    week_delta = datetime.timedelta(days=7)

    hot_hits_uk = f"""
    
    SELECT tm.DATE_KEY
        , p.PLAYLIST_NAME
        , pr.ARTIST_DISPLAY_NAME
        , tm.CUSTOMER_TRACK_TITLE
        , tm.TRACK_POSITION
        , tm.COUNTRY_CODE
                
    
    FROM DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm   
            inner join DF_PROD_DAP_MISC.DAP.DIM_PLAYLIST p on p.PLAYLIST_KEY = tm.PLAYLIST_KEY
            inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT pr on pr.PRODUCT_KEY = tm.PRODUCT_KEY
    
    WHERE p.PLAYLIST_NAME LIKE '%Hot Hits UK%'
        and pr.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
        and tm.CUSTOMER_TRACK_TITLE = '{track_title}'
        and (tm.DATE_KEY = '{date - week_delta}' or tm.DATE_KEY = '{date}')

    """

    todays_hits_apple_uk = f"""
    
    SELECT tm.DATE_KEY
       , p.PLAYLIST_NAME
       , pr.ARTIST_DISPLAY_NAME
       , tm.CUSTOMER_TRACK_TITLE
       , tm.TRACK_POSITION
       , tm.COUNTRY_CODE
            

    FROM DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm   
        inner join DF_PROD_DAP_MISC.DAP.DIM_PLAYLIST p on p.PLAYLIST_KEY = tm.PLAYLIST_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT pr on pr.PRODUCT_KEY = tm.PRODUCT_KEY

    WHERE p.PLAYLIST_NAME LIKE '%Today''s Hits%'
        and pr.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
        and tm.CUSTOMER_TRACK_TITLE = '{track_title}'
        and (tm.DATE_KEY = '{date - week_delta}' or tm.DATE_KEY = '{date}')
    
    """

    todays_top_hits_spotify = f"""

    SELECT tm.DATE_KEY
       , p.PLAYLIST_NAME
       , pr.ARTIST_DISPLAY_NAME
       , tm.CUSTOMER_TRACK_TITLE
       , tm.TRACK_POSITION
       , tm.COUNTRY_CODE


    FROM DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm   
        inner join DF_PROD_DAP_MISC.DAP.DIM_PLAYLIST p on p.PLAYLIST_KEY = tm.PLAYLIST_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT pr on pr.PRODUCT_KEY = tm.PRODUCT_KEY

    WHERE p.PLAYLIST_NAME LIKE '%Today''s Top Hits%'
        and pr.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
        and tm.CUSTOMER_TRACK_TITLE = '{track_title}'
        and (tm.DATE_KEY = '{date - week_delta}' or tm.DATE_KEY = '{date}')

    """

    spotify_daily_top_200_gb = f"""
    
    SELECT fc.DATE_KEY
       , fc.TITLE
       , fc.CURRENT_POSITION
       , pr.ARTIST_DISPLAY_NAME
       , ch.CHART_NAME
       , cn.COUNTRY_CODE
       , c.CUSTOMER_NAME
    
    
    FROM DF_PROD_DAP_MISC.DAP.FACT_CHARTS_DAILY fc
        inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT pr on pr.PRODUCT_KEY = fc.PRODUCT_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_CHART ch on ch.CHART_KEY = fc.CHART_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_CUSTOMER c on c.CUSTOMER_KEY = fc.CUSTOMER_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_COUNTRY cn on cn.COUNTRY_KEY = fc.COUNTRY_KEY

    
    WHERE  (fc.DATE_KEY = '{date - week_delta}' or fc.DATE_KEY = '{date}')
        and pr.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
        and fc.TITLE LIKE '{track_title}'
        and c.CUSTOMER_NAME Like '%Spotify%'
        and ch.CHART_NAME like '%Top 200%'
        and cn.COUNTRY_CODE = 'GB'
    
    """

    query_total_streams_dsp = f"""

    SELECT s.DATE_KEY      
       , s.COUNTRY_CODE
       , c.CUSTOMER_NAME
       , p.ARTIST_DISPLAY_NAME
       , p.PRODUCT_TITLE
       , sum(STREAM_COUNT) as summed_streams

    FROM DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING s
        inner join DF_PROD_DAP_MISC.DAP.DIM_CUSTOMER c on c.CUSTOMER_KEY = s.CUSTOMER_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT p on p.PRODUCT_KEY = s.PRODUCT_KEY

    WHERE s.DATE_KEY BETWEEN '{date - week_delta}' AND '{date}'
            and p.PRODUCT_TITLE = '{track_title}'

    GROUP BY 1,2,3,4,5

    """

    return hot_hits_uk, todays_hits_apple_uk, todays_top_hits_spotify, spotify_daily_top_200_gb, query_total_streams_dsp