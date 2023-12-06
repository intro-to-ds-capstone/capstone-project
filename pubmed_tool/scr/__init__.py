"""
imported as scr

Contains functions specific to scraping from
PubMED records

FUNCTIONS:
    pubmed_search_ids(): passes query to 
        pubmed, and retrieves a list of
        PMIDs
    pubmed_fetch_records(): obtains XML 
        records from PMID(s), and trims 
        to the Medline Citation
    format_date(): takes a date record 
        from a PubMed XML record and 
        returns a date object
    format_author: takes an author record 
        from a PubMed XML record and 
        returns a dictionary
    issue_vol: takes either an issue or 
        volume record from a PubMed XML 
        record and returns the validated 
        volume/issue name, and other data 
        such as Special No., Part, or 
        Supplement in a dictionary.
    journal_content(): scrapes the 
        journal-level contenet of a 
        PubMed XML record and returns 
        a dictionary
    article_content(): scrapes the 
        article-level content of a PubMed 
        XML record and returns a dictionary
    single_record(): scrapes the 
        entirety of a single PubMed XML 
        record, and returns a dictionary

REQUIREMENTS/DEPENDENCIES: 
    validators
    re
    Bio import Entrez
    datetime as dt
    config_logging as logs
    logger = logs.get_logger()
"""

import pubmed_tool.validators as validators
import re
from Bio import Entrez
import datetime as dt
import pubmed_tool.logs as logs

from .scraper_functs import *
