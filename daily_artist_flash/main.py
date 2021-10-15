from .queries import select_queries
from .utilities import fetch_data_as_df, save_query_data_snowflake
import logging
import datetime
import snowflake.connector
import pandas as pd


if __name__ == "__main__":
