
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
     , c.CUSTOMER_NAME

    FROM DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm
       inner join DF_PROD_DAP_MISC.DAP.DIM_PLAYLIST p on p.PLAYLIST_KEY = tm.PLAYLIST_KEY
       inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT pr on pr.PRODUCT_KEY = tm.PRODUCT_KEY
       inner join DF_PROD_DAP_MISC.DAP.DIM_CUSTOMER c on c.CUSTOMER_KEY = tm.CUSTOMER_KEY
 
    WHERE  pr.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
        and tm.CUSTOMER_TRACK_TITLE LIKE '%{track_title}%'
        and tm.COUNTRY_CODE = 'GB'
        and p.PLAYLIST_ID = 'pl.f4d106fed2bd41149aaacabb233eb5eb'
        and (tm.DATE_KEY = (dateadd(day,-8,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm)))
           or tm.DATE_KEY = (dateadd(day,-1,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_PLAYLIST_TRACK_METRICS tm))))
    
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

    # updates on a Friday so whenever run have to correct dateadd - back to Friday (-9, -2) week prior and Friday just gone, run on a Monday.

    occ_top_100_singles = f"""

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
    and (w.DATE_KEY = (dateadd(day,-13,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w)))
        or w.DATE_KEY = (dateadd(day,-6,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_CHARTS_WEEKLY w))))
    and c.CHART_NAME = 'Top 100 Combined Singles'
    and w.TITLE = '{track_title}'
    and p.ARTIST_DISPLAY_NAME LIKE '%{artist}%'

    """

    dsp_streams = f"""
    
    SELECT w.DATE_KEY
        , c.CUSTOMER_NAME
        , dp.ARTIST_DISPLAY_NAME
        , dp.PRODUCT_TITLE
        , sum(case when COUNTRY_CODE = 'GB' then STREAM_COUNT else 0 end) streams_uk
        , sum(STREAM_COUNT)                                               global_streams


    from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING_AGG_WEEKLY w
            inner join DF_PROD_DAP_MISC.DAP.REL_PRODUCT_ASSET_SIBLING pas on pas.PRODUCT_KEY = w.PRODUCT_KEY
            inner join DF_PROD_DAP_MISC.DAP.DIM_ARTIST da on da.ARTIST_KEY = pas.AFG_PRIMARY_ARTIST_ID
            inner join DF_PROD_DAP_MISC.DAP.DIM_CUSTOMER c on c.CUSTOMER_KEY = w.CUSTOMER_KEY
            inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT dp on dp.PRODUCT_KEY = w.PRODUCT_KEY
            
    WHERE c.CUSTOMER_KEY in (1, 2, 5, 119735, 155643)
    and dp.PRODUCT_TITLE = '{track_title}'
    and dp.ARTIST_DISPLAY_NAME LIKE '%{artist}%'
      and (w.DATE_KEY = (dateadd(day,-7,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING_AGG_WEEKLY w)))
        or w.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING_AGG_WEEKLY w))
    GROUP BY 1, 2, 3, 4
    
    """

    youtube_ugc_pgc = f"""
    
    select fas.date_key
        , dp.ARTIST_DISPLAY_NAME
        , dp.PRODUCT_TITLE
        , case
            when fas.customer_key = 140003 and cd.account_consumer_dtl_one = 'Premium'
                then 'YouTube Official'
            when fas.customer_key = 140003 and cd.account_consumer_dtl_one = 'UGC'
                then 'YouTube UGC'
            else cus.customer_name
        end                  as customer_name
        , sum(Stream_Count) as Streams


    from  DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING_AGG_WEEKLY fas
            inner join DF_PROD_DAP_MISC.DAP.dim_consumer_details cd on fas.consumer_dtl_key = cd.consumer_dtl_key
            inner join DF_PROD_DAP_MISC.DAP.dim_product prod on fas.product_key = prod.product_key
            inner join DF_PROD_DAP_MISC.DAP.dim_customer cus on fas.customer_key = cus.customer_key
            inner join DF_PROD_DAP_MISC.DAP.REL_PRODUCT_ASSET_SIBLING pas on pas.PRODUCT_KEY = fas.PRODUCT_KEY
            inner join DF_PROD_DAP_MISC.DAP.DIM_ARTIST da on da.ARTIST_KEY = pas.AFG_PRIMARY_ARTIST_ID
            inner join DF_PROD_DAP_MISC.DAP.DIM_PRODUCT dp on dp.PRODUCT_KEY = fas.PRODUCT_KEY

    where 1 = 1
    and (fas.DATE_KEY = (dateadd(day,-7,(select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING_AGG_WEEKLY fas)))
            or fas.DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING_AGG_WEEKLY fas))
    and not (fas.customer_key = 140003 and cd.account_consumer_dtl_one = 'Premium' and prod.product_category = 'Audio')
    and fas.CUSTOMER_KEY = 140003
    and dp.PRODUCT_TITLE = '{track_title}'
    and dp.ARTIST_DISPLAY_NAME = '{artist}'

    group by 1, 2, 3, 4

    """
    

    return hot_hits_uk, todays_hits_apple_uk, todays_top_hits_spotify, spotify_daily_top_200_gb, query_total_streams_dsp, \
           spotify_top_200_global, apple_music_daily_top_100_gb, shazam_top_200_gb, shazam_top_200_ww, occ_top_100_singles, dsp_streams, \
            youtube_ugc_pgc

# appears to be a 2 day lag on all dates.

# the below determine max dates of data ingest in playlists that are used to work current date back to populated fields.
    
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

fact_audio_streaming_agg_weekly_ingest = """

SELECT max(DATE_KEY) Date

FROM DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING_AGG_WEEKLY s

WHERE DATE_KEY = (select max(DATE_KEY) from DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING_AGG_WEEKLY s)

"""


