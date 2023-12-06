"""
imported as sql

Contains functions specific to SQL interactions

FUNCTIONS:
    split_tables(): splits combined dataframe
        into tables for upload
    upload(): performs upload of split tables
        to an SQL database
    query(): queries author field in SQL
        database and returns matching records

REQUIREMENTS/DEPENDENCIES: 
    sqlite3
    validators
    pandas as pd
    config_logging as logs
    logger = logs.get_logger()
"""

import sqlite3
import pubmed_tool.validators as validators
import pandas as pd
import pubmed_tool.logs as logs

from .sql_functs import *
