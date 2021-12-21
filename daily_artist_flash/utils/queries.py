
# This includes both UK & NON-UK charts information to be filtered
def select_queries(artist, track_title):

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
    
    WHERE p.PLAYLIST_NAME = 'Hot Hits UK'
        and pr.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
        and tm.CUSTOMER_TRACK_TITLE LIKE '%{track_title}%'
        and (tm.DATE_KEY = (dateadd(day,-7,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm)))
            or tm.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm))

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

    WHERE p.PLAYLIST_NAME LIKE 'Today''s Hits'
        and pr.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
        and tm.CUSTOMER_TRACK_TITLE LIKE '%{track_title}%'
        and tm.COUNTRY_CODE = 'GB'
        and (tm.DATE_KEY = (dateadd(day,-7,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm)))
             or tm.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm))
    
    """

    todays_top_hits_spotify = f"""

       SELECT tm.DATE_KEY
       , p.PLAYLIST_NAME
       , p.OWNER_ID
       , pr.ARTIST_DISPLAY_NAME
       , tm.CUSTOMER_TRACK_TITLE
       , tm.TRACK_POSITION
       , tm.COUNTRY_CODE
       , c.CUSTOMER_NAME

    FROM DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm   
        inner join DF_PROD_DAP_MISC.DAP.DIM_PLAYLIST p on p.PLAYLIST_KEY = tm.PLAYLIST_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT pr on pr.PRODUCT_KEY = tm.PRODUCT_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_CUSTOMER c on c.CUSTOMER_KEY = tm.CUSTOMER_KEY

    WHERE p.PLAYLIST_NAME LIKE 'Today''s Top Hits'
        and p.OWNER_ID = 'spotify'
        and pr.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
        and tm.CUSTOMER_TRACK_TITLE LIKE '%{track_title}%'
        and (tm.DATE_KEY = (dateadd(day,-7,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm)))
            or tm.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm))
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

    WHERE  (fc.DATE_KEY = (dateadd(day,-7,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_DAILY fc)))
        or fc.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_DAILY fc))
        and pr.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
        and fc.TITLE LIKE '{track_title}'
        and c.CUSTOMER_NAME Like '%Spotify%'
        and ch.CHART_NAME like '%Top 200%'
        and cn.COUNTRY_CODE = 'GB'
    
    """

    spotify_top_200_global = f"""
    
        SELECT w.DATE_KEY
        , w.TITLE
        , p.ARTIST_DISPLAY_NAME
        , w.CURRENT_POSITION
        , c.CHART_NAME
        , c.CHART_KEY
        , cu.CUSTOMER_NAME
        , dc.COUNTRY_CODE
        , dc.COUNTRY_NAME
        
    from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_DAILY w
    inner join DF_PROD_DAP_MISC.DAP.DIM_CHART c on c.CHART_KEY = w.CHART_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_COUNTRY dc on dc.COUNTRY_KEY = w.COUNTRY_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_CUSTOMER cu on cu.CUSTOMER_KEY = w.CUSTOMER_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT p on p.PRODUCT_KEY = w.PRODUCT_KEY
    
    WHERE cu.CUSTOMER_NAME = 'Spotify'
    and c.CHART_NAME = 'Top 200'
    and dc.COUNTRY_CODE = 'WW'
    and (w.DATE_KEY = (dateadd(day,-7,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_DAILY w)))
        or w.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_DAILY w))
    and p.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
    and w.TITLE LIKE '{track_title}'
    
    """

    apple_music_daily_top_100_gb = f"""

        SELECT w.DATE_KEY
        , w.TITLE
        , p.ARTIST_DISPLAY_NAME
        , w.CURRENT_POSITION
        , c.CHART_NAME
        , c.CHART_KEY
        , cu.CUSTOMER_NAME
        , dc.COUNTRY_CODE
        , dc.COUNTRY_NAME    
    
    from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_DAILY w
    inner join DF_PROD_DAP_MISC.DAP.DIM_CHART c on c.CHART_KEY = w.CHART_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_COUNTRY dc on dc.COUNTRY_KEY = w.COUNTRY_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_CUSTOMER cu on cu.CUSTOMER_KEY = w.CUSTOMER_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT p on p.PRODUCT_KEY = w.PRODUCT_KEY
    
    WHERE (w.DATE_KEY = (dateadd(day,-10,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_DAILY w)))
        or w.DATE_KEY = (dateadd(day,-3,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w))))
    and dc.COUNTRY_CODE = 'GB'
    and c.CHART_NAME = 'Top Songs'
    and p.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
    and w.TITLE = '{track_title}'
    and cu.CUSTOMER_NAME LIKE '%Apple Music%'
    and w.CURRENT_POSITION <= 100

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

    WHERE (s.DATE_KEY = (dateadd(day,-9,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING s)))
        or s.DATE_KEY = (dateadd(day,-2,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING s))))
        and p.PRODUCT_TITLE = '{track_title}'
        and p.ARTIST_DISPLAY_NAME LIKE '%{artist}%'

    GROUP BY 1,2,3,4,5

    """

    shazam_top_200_gb = f"""

    SELECT w.DATE_KEY
        , w.TITLE
        , p.ARTIST_DISPLAY_NAME
        , w.CURRENT_POSITION
        , c.CHART_NAME
        , c.CHART_KEY
        , cu.CUSTOMER_NAME
        , dc.COUNTRY_CODE
        , dc.COUNTRY_NAME    
    
    from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w
    inner join DF_PROD_DAP_MISC.DAP.DIM_CHART c on c.CHART_KEY = w.CHART_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_COUNTRY dc on dc.COUNTRY_KEY = w.COUNTRY_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_CUSTOMER cu on cu.CUSTOMER_KEY = w.CUSTOMER_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT p on p.PRODUCT_KEY = w.PRODUCT_KEY
    
    WHERE (w.DATE_KEY = (dateadd(day,-7,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w)))
        or w.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w))
    and dc.COUNTRY_CODE = 'GB'
    and cu.CUSTOMER_NAME LIKE '%Shazam%'
    and c.CHART_NAME = 'SHAZAM TOP 200'
    and w.TITLE = '{track_title}'
    and p.ARTIST_DISPLAY_NAME = '{artist}'

    """

    shazam_top_200_ww = f"""

    SELECT w.DATE_KEY
        , w.TITLE
        , p.ARTIST_DISPLAY_NAME
        , w.CURRENT_POSITION
        , c.CHART_NAME
        , c.CHART_KEY
        , cu.CUSTOMER_NAME
        , dc.COUNTRY_CODE
        , dc.COUNTRY_NAME    
    
    from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w
    inner join DF_PROD_DAP_MISC.DAP.DIM_CHART c on c.CHART_KEY = w.CHART_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_COUNTRY dc on dc.COUNTRY_KEY = w.COUNTRY_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_CUSTOMER cu on cu.CUSTOMER_KEY = w.CUSTOMER_KEY
    inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT p on p.PRODUCT_KEY = w.PRODUCT_KEY
    
    WHERE (w.DATE_KEY = (dateadd(day,-7,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w)))
        or w.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w))
    and dc.COUNTRY_CODE = 'WW'
    and cu.CUSTOMER_NAME LIKE '%Shazam%'
    and c.CHART_NAME = 'SHAZAM TOP 200'
    and w.TITLE = '{track_title}'
    and p.ARTIST_DISPLAY_NAME = '{artist}'

    """

    # updates on a Friday so whenever run have to correct dateadd - back to Friday
    occ_top_100 = f"""

    SELECT w.DATE_KEY
        , w.TITLE
        , p.ARTIST_DISPLAY_NAME
        , w.CURRENT_POSITION
        , c.CHART_NAME
        , c.CHART_KEY
        , c.ACCOUNT
        , cu.CUSTOMER_NAME
        , dc.COUNTRY_CODE
        , dc.COUNTRY_NAME    
        
    from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w
        inner join DF_PROD_DAP_MISC.DAP.DIM_CHART c on c.CHART_KEY = w.CHART_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_COUNTRY dc on dc.COUNTRY_KEY = w.COUNTRY_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_CUSTOMER cu on cu.CUSTOMER_KEY = w.CUSTOMER_KEY
        inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT p on p.PRODUCT_KEY = w.PRODUCT_KEY
    
    WHERE c.ACCOUNT = 'OCC'
    and (w.DATE_KEY = (dateadd(day,-10,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w)))
        or w.DATE_KEY = (dateadd(day,-3,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w))))
    and c.CHART_NAME = 'Top 100 Combined Singles'
    and w.TITLE = '{track_title}'
    and p.ARTIST_DISPLAY_NAME LIKE '%{artist}%'

    """

    return hot_hits_uk, todays_hits_apple_uk, todays_top_hits_spotify, spotify_daily_top_200_gb, query_total_streams_dsp, \
           spotify_top_200_global, apple_music_daily_top_100_gb, shazam_top_200_gb, shazam_top_200_ww, occ_top_100

#Appears to be a 2 day lag on all dates.


    
fact_audio_playlist_track_metrics_latest_ingest = """

SELECT max(tm.DATE_KEY) Date

FROM DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm  

WHERE tm.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm)

"""

fact_charts_daily_latest_ingest  = """

SELECT max(fc.DATE_KEY) Date

FROM DF_PROD_DAP_MISC.DAP.FACT_CHARTS_DAILY fc

WHERE fc.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_DAILY fc)

"""

fact_charts_weekly_latest_ingest = """

SELECT max(DATE_KEY) Date

from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w

WHERE DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY)

"""

fact_audio_streaming_latest_ingest = """

SELECT max(DATE_KEY) Date

FROM DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING s

WHERE DATE_KEY = (dateadd(day,-2,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING s)))

"""

