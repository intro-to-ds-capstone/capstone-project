"""
Root of pubmed tool!

Contains main functions of the tool

FUNCTIONS:
    format_df(): formats the df for SQL and 
        visualizer
    scraper(): performs full scrape
    sql_full(): performs full SQL process
    full_visual(): performs full visual generation

REQUIREMENTS/DEPENDENCIES: 
    os
    requests
    numpy as np
    pandas as pd
    panels as pn
    validators
    scraper_functs as scr
    sql_functs as sql
    vis_functs as vis
    config_logging as logs
    logger = logs.get_logger()
"""

import pubmed_tool.validators as validators
import pubmed_tool.scraper_functs as scr
import pubmed_tool.config_logging as logs
import pubmed_tool.sql_functs as sql
import pubmed_tool.vis_functs as vis
import os
import pandas as pd
import panel as pn
import requests
import numpy as np

logger = logs.get_logger()

def format_df(in_df):
    """
    Performs formatting of the output from the scraper
    to facilitate SQL or visual processing. Uses a
    web-scrape to extract language translations.

    INPUTS:
        in_df (dataframe): data frame of output from the
                scraper

    RETURNS:
        t_df (dataframe): a formatted data frame, where
                authors are exploded into rows, count of
                authors is made, deduplication performed,
                and languages translated.

    REQUIREMENTS/DEPENDENCIES:
        pandas as pd
        requests
        numpy as np
    """      

    t_df = in_df.copy(deep = True)
    # Replace all empty strings with NA
    t_df.replace('', np.nan, inplace = True)

    # Format Keywords as List (from string version of set)
    # =========================================================
    t_df['keywords'].replace(np.nan, '[]', inplace = True)
    t_df['keywords'] = t_df['keywords'].str.replace('{', "[", 
                                                    regex = True)
    t_df['keywords'] = t_df['keywords'].str.replace('}', "]", 
                                                    regex = True)

    # Transform so each row is PMID and author:
    # =========================================================
    # Fill missing values with a blank dictionary string
    t_df['authors'] = t_df['authors'].fillna("[{'last': None}]")
    # Turn these strings into actual lists of dictionaries
    t_df['authors'] = t_df['authors'].apply(eval)
    # 'Explode' dictionary, so each row is now author-pmid
    t_df = t_df.explode(['authors']).reset_index(drop = True)
    t_df = t_df.join(pd.json_normalize(t_df.pop('authors')))
    # Replace any '' values with None
    t_df.replace('', None, inplace = True)
    # Convert any empty values to zero, and make order an int
    t_df.order = t_df.order.fillna(0)
    t_df.order = t_df.order.astype(int)

    # For papers with no authors listed, which gave a single
    # row, similarly replace the last name with "None Listed"
    # a last name value of "None Listed"
    freq_pmids = t_df['pmid'].value_counts()
    single_pmids = freq_pmids[freq_pmids==1].index
    t_df.loc[((t_df['last'].isna()) & 
            (t_df['pmid'].isin(single_pmids))), 'order'] = None
    t_df.loc[((t_df['last'].isna()) & 
            (t_df['pmid'].isin(single_pmids))), 'last'] = "None Listed"
    # Catch the rare paper with failed author extraction
    # This was typically consortia, where it might be stored
    # in a different location. Last name to "None Listed"
    t_df.loc[((t_df['order'] < 2) & 
                (t_df['last'].isna())), 'last'] = 'Failed Capture'

    # Drop extra empty rows from blank authors
    t_df.dropna(subset = ['last'], inplace = True)

    # Calculate the Number of Authors for each PMID:
    # =========================================================
    counts = t_df['pmid'].value_counts().to_dict()
    t_df["numauthors"] = t_df["pmid"].map(counts)
    # Convert count for articles without a listed author to 0, 
    # make the value an integer
    t_df.loc[t_df['last'] == 'None Listed', 'numauthors'] = 0
    t_df['numauthors'] = t_df['numauthors'].astype(int)

    # Convert Order into a True/False, for First Author
    # =========================================================
    t_df['order'] = [order == 1 for order in t_df['order']]
    t_df = t_df.rename(columns = {'order': 'firstauthor'})

    # Convert language values in 'Language' to full names
    # =========================================================
    # Uses the Library of Congress Language Abbreviations, which
    # is what is used by PubMed, to create a list translating
    # each 3-letter abbreviation into the corresponding language
    # in full text
    dict_path = r'https://www.loc.gov/marc/languages/language_code.html'
    r = requests.get(dict_path)
    lang_dict = pd.read_html(r.text)[0]
    lang_dict['code'] = "'" + lang_dict['code'] + "'"
    lang_dict['language'] = "'" + lang_dict['language'] + "'"
    lang_dict = dict(zip(lang_dict['code'], lang_dict['language']))

    # Ensure all languages have single quotes, not double quotes:
    t_df['language'] = t_df['language'].str.replace('"', "'", 
                                                    regex = True)
    # Replace text using regular expressions
    for key in lang_dict.keys():
        t_df['language'] = t_df['language'].str.replace(key, 
                                                lang_dict[key], 
                                                regex = True)
        
    return t_df

def scraper(keyword, start_date, end_date, email, project_dir = None,
        path = 'publications.csv', chunksize = None, max_returns = 200000,
        overwrite = True, return_df = False):
    """
    Performs a search of PubMed using a date range and keyword.
    Can save the data to a CSV file. Can use batch processing, or
    return the dataframe if batch processing is not used.

    INPUTS:
        keyword (string): Keyword term
        start_date (string): Date in YYYY/MM/DD format.
            validated by validate_date()
        end_date (string): Date in YYYY/MM/DD format
            validated by validate_date()
        email (string): email address, required by NCBI
            validated by validate_email()
        path (string,path): string address of output file path
            some validation in validate_path()
        chunksize (int): number of records to batch process
                between opening file, used to optimize run time
                based on capabilities of an individual machine;
                default is None, for bulk processing
        max_returns (int): integer indicating the maximum number of
                records to return; default is 200,000
        overwrite (bool): True/False value indicating if it is
                desired to overwrite the output file, if it
                already exists; default is FALSE
        return_df (bool): True/False value indicating if it is
                desired to return the data frame; not possible
                if batch-processing is being used;
                default is FALSE

    RETURNS:
        chunk (dataframe): if return_df was TRUE; otherwise
        writes records to `path`.

    REQUIREMENTS/DEPENDENCIES:
        os
        pandas as pd
        validators
        scraper_functs as scr
        config_logging as logs
        logger = logs.get_logger()
    """  
    # Validation of Inputs
    # ========================================================================
    # Will throw error if there are validation issues that cannot be overcome

    try:
    # Chunksize: must be an integer
    # ------------------------------------------------------------------------
        if chunksize and not isinstance(chunksize,int):
            if isinstance(chunksize, str) and chunksize.isdigit():
                chunksize = int(chunksize)
            elif chunksize < 1:
                new_size = 100
                message = ''.join([
                    f'<chunksize>: {type(chunksize)} of value {chunksize}',
                    f' is invalid. Adjusted to {new_size}.'
                    ])
    # Informative message informing of correction
                logs.display_message(message, type = 'info')

                chunksize = new_size
                new_size = None                
            else:
    # Informative message informing of correction
                new_size = 100
                message = ''.join([
                    f'<chunksize>: {type(chunksize)} of value {chunksize}',
                    f' is invalid. Adjusted to {new_size}.'
                ])
                logs.display_message(message, type = 'info')

                chunksize = new_size
                new_size = None

    # Chunksize: requires a path
    # ------------------------------------------------------------------------
        if chunksize and not path:
            raise ValueError("Cannot chunk process without a file path!")
        
    # Return_df: cannot occur when using chunk processing. Informative Message
    # ------------------------------------------------------------------------
        if return_df and chunksize:
            message = ''.join([
                'Cannot chunk process and return data frame. Setting',
                ' return_df to False. Use read-in file in separate',
                ' process, if needed.'
                ])
            return_df = False
            logs.display_message(message, type = 'info')

    # Path
    # ------------------------------------------------------------------------
        if path:        
            path = validators.path(project_dir = project_dir, file_name = path, 
                    overwrite = True, req_suffix = ['.csv', '.txt'])
    # Path: if appending and exists, original file must be compatible!
    # ------------------------------------------------------------------------     
            if not overwrite:
                validators.existing_csv(path) # Raises error if incompatible
    # Informative Messages if Errors in Validation
    # ------------------------------------------------------------------------  
    except Exception as e:
        message = f"Error in validations: \n \t {e}"
        logs.display_message(message, type = 'error')

    # Processing
    # ========================================================================
    # Will catch informative errors
    try:
    # Obtain PMIDs from query. Raise error if no IDs returned in search.
    # ------------------------------------------------------------------------  
        target_ids = scr.pubmed_search_ids(keyword, start_date, end_date, email,
                                       max_returns = max_returns)

        if not target_ids or len(target_ids) < 0:
            raise ValueError("No Records Found, Processing stopped.")

    # Chunk processing
    # ------------------------------------------------------------------------  
        if chunksize and path:
    # Divide target_ids into chunks
            for i in range (0, len(target_ids), chunksize):
                chunk_ids = target_ids[i:i+chunksize]
                records = scr.pubmed_fetch_records(chunk_ids, email)
                output_dict = {'pmid': [], 'title': [], 'pubdate': [], 
                   'authors': [], 'keywords':[], 'journal': [], 
                   'isoabbrev': [], 'volume': [], 'issue': [], 
                   'page_start': [], 'page_end': [], 
                   'language': [], 'abstract': [], 'other_type' : [],
                   'other_val' : []
                   }
                
    # Process records              
                for record in records:
                    record_data = scr.single_record(record)
                    if record_data:
                        for key in output_dict.keys():
                            output_dict[key].append(record_data[key])
    
    # Create Data Frame, set index, ensure date column is dates.              
                chunk = pd.DataFrame.from_dict(output_dict)
                chunk.set_index('pmid', inplace = True)
                chunk.pubdate = pd.to_datetime(chunk.pubdate)
                
    # Writing to file. If overwrite, only overwrite with the FIRST chunk
                if not os.path.isfile(path) or overwrite:
                    chunk.to_csv(path_or_buf= path, mode = 'w', index = True,
                             header = True)
                    overwrite = False
                else:
                    chunk.to_csv(path_or_buf= path, mode = 'a', index = True,
                            header = True)
        
    # Non-chunk processing
    # ------------------------------------------------------------------------  
        elif not chunksize:
    # Process records    
            records = scr.pubmed_fetch_records(target_ids,
                                           email=email)
            output_dict = {'pmid': [], 'title': [], 'pubdate': [], 
                'authors': [], 'keywords':[], 'journal': [], 
                'isoabbrev': [], 'volume': [], 'issue': [], 
                'page_start': [], 'page_end': [], 
                'language': [], 'abstract': [], 'other_type' : [],
                'other_val' : []
                }

            for record in records:
                record_data = scr.single_record(record)
                if record_data:
                    for key in output_dict.keys():
                        output_dict[key].append(record_data[key])


    # Create Data Frame, set index, ensure date column is dates.                  
            chunk = pd.DataFrame.from_dict(output_dict)
            chunk.set_index('pmid', inplace=True)
            chunk.pubdate = pd.to_datetime(chunk.pubdate)

    # Writing to file, depending on overwrite choice.
            if path and overwrite:
                chunk.to_csv(path_or_buf= path, mode = 'w', index = True,
                             header = True)
            if path and not overwrite:
                chunk.to_csv(path_or_buf= path, mode = 'a', index = True,
                             header = True)

    # Success Message
    # ========================================================================
        if path:
            message = ''.join([
                f'Success! \n {len(target_ids)} records for ',
                'PubMed Search: \n',
                f'({keyword}) AND ("{start_date}"[Date - Publication]'
                f' : "{end_date}"[Date - Publication]) \n',
                f' processed and written to {path}'
                ])
        else:
            message = ''.join([
                f'Success! \n {len(target_ids)} records for ',
                'PubMed Search: \n',
                f'({keyword}) AND ("{start_date}"[Date - Publication]'
                f' : "{end_date}"[Date - Publication]) \n',
                f' processed.'
                ])
        logs.display_message(message, type = 'info')

    # Return DF, if Return_Df
    # ========================================================================
        if return_df:
            return chunk
        
    # Informative Error Messages
    # ========================================================================
    except Exception as e:
        message = '\n \t'.join([
            'An error occured in processing.',
            'Messages:',
            f'{e}'
            ])
        logs.display_message(message, type = 'error')

def sql_full(t_df, project_dir = None, 
             db_name = 'publications.db', 
             paper_name = 'papers', 
             authors_name = 'authors',
             pairs_name = 'pairs_authorpapers',
             any_nm = None, first_nm = None, 
             last_nm = None, initials_nm = None):
    """
    Processes either a passed data frame or a csv to 
    that data. Performs SQL upload (overwriting any 
    existing tables with the same name in the same 
    database. Queries by name, and returns matches.

    INPUTS:
	    t_df (path or dataframe): either a dataframe
            of papers extracted from scraper, or a
            path to the csv of the same data.
        project_dir (path): path to the project 
            directory. Default is None, which will
            use the current working directory.
        db_name(path): path to the desired output
            database. Default is 'publications.db',
            which will generate in the current
            working directory. Validation by
            validators.path()
        paper_name (string): name for the paper table.
            Default is 'papers'.
        authors_name (string): name for the authors
            table. Default is 'authors'.
        pairs_name (string): name for the pairs table.
            Default is 'pairs_authorpapers'.
        any_nm (string): name to query in any
            field of author name. Default is None.
        last_nm (string): name to query in the
            last name field of author name.
            Default is None.
        first_nm (string): name to query in the
            first name field of author name.
            Default is None.
        initial_nm (string): name to query in the
            initials field of author name.
            Default is None.

    RETURNS:
        matches (dataframe): pandas dataframe of matching
            records. Index is automatic. Dates are
            converted into date objects, and boolean
            'firstauthor' is converted into a bool.

    REQUIREMENTS/DEPENDENCIES:
        pandas as pd
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
        sql_functs as sql
        validators.path()
    """  

    try: 
    # Format and split full data frame from Scraper
    # ========================================================================
    # If t_df is a path and not a data frame, extract from CSV
    # ------------------------------------------------------------------------
        if not isinstance(t_df, pd.DataFrame):
            t_df = validators.path(file_name= t_df, 
                                   project_dir = project_dir,
                                   must_exist = True, overwrite = True,
                                   req_suffix = ['.txt', '.csv'])
            
            t_df = pd.read_csv(t_df, 
                    sep = ',', 
                    header = 0, 
                    dtype={'pmid': int, 'volume': 'Int64', 'issue': 'Int64'})

        t_df = format_df(t_df)
        papers, authors, pairs = sql.split_tables(t_df)

    # Upload data to SQLite database
    # ========================================================================
        sql.upload(papers, authors, pairs, 
                   project_dir = project_dir, 
                   db_name = db_name,
                   paper_name = paper_name, 
                   authors_name = authors_name,
                   pairs_name = pairs_name)

    # Query SQL database based on author name, any field.
    # ========================================================================
        matches = sql.query(db_name = db_name, project_dir = project_dir, 
                            any_nm = any_nm, last_nm = last_nm, 
                            initials_nm = initials_nm, first_nm = first_nm,
                            paper_name = paper_name, 
                            authors_name = authors_name,
                            pairs_name = pairs_name)

    # Return matches from Query
    # ========================================================================
        return matches

    except Exception as e:
        message = ''.join([
            'There was an error in SQL processing.',
            f'\n\t Error details: {e}'
            ])
        logs.display_message(message, type = 'error')
        print(e)

def full_visual(t_df, out_path = 'visual.html', project_dir = None, 
                 mode = 'html', port = 5007, interactive = False,
                 keyword = None, start_date = None, end_date = None, 
                 logo_path = None, primary_color = 'blue', 
                 secondary_color = 'grey', accent_color = 'grey'):
    """
    Takes a data frame from scraper, or reads the scraper output CSV,
    and creates a visual of both the summary stats and temporal trends
    for publications over time.

    INPUTS:
        t_df (path or dataframe): either a dataframe of papers 
            extracted from scraper, or a path to the csv of the same data.
        outpath (path): path used to save the visual as an HTML file.
            Default is 'visual.html'. Validated by validators.path().
        project_dir (path): path to the project 
            directory. Default is None, which will
            use the current working directory.
        mode (string): specifies 'html', 'jupyter', or 'port', with
            default of 'html'.
            html: exports to HTML, requires outpath.
            jupyter: provides rendering for jupyter notebook
            port: exports to HTML using a port and localhost
        port (int): port for exporting if mode 'port' was selected.
            Default is 5007.
        keyword (string): keyword used to generate data frame with
            scraper(). Default is None, which will not display keyword.
        start_date(string): start_date used to generate data frame with
            scraper(). Default is None, which will use data frame
            minimum date.
        end_date(string): end_date used to generate data frame with
            scraper(). Default is None, which will use data frame
            maximum date.
        logo_path(path): path to an image file. Validated by
            validators.path(). Default is None.
        count_color(str): string name or hexidecimal value for the 
            line of counts in the line plot. Default is 'blue'
        secondary_color(str): string name or hexidecimal value for the 
            line of 95% confidence interval thresholds in the line plot,
            and outliers in the boxplot. Default is 'grey'.
        secondary_color(str): string name or hexidecimal value for the 
            mean lines in the line plot.
            Default is 'grey'.

    RETURNS:
        visual (panels): visualization based on specifications.

    REQUIREMENTS/DEPENDENCIES:
        datetime as dt
        pandas as pd
        panel as pn
        validators
        vis_functs as vis
        config_logging as logs
        logger = logs.get_logger()
    """  
    from bokeh.resources import INLINE
    from bokeh.core.validation import silence
    from bokeh.core.validation.warnings import FIXED_SIZING_MODE
    silence(FIXED_SIZING_MODE, True)
    
    try:
    # Validate path for export, if given
    # ========================================================================
        if out_path:
            out_path = validators.path(project_dir = project_dir, 
                                   file_name = out_path, overwrite = True, 
                                   req_suffix = ['.html'])
    # Also catch if save is requested, but no path is given
    # -----------------------------------------------------------------------
        if mode == 'html' and not out_path:
            message = ''.join([
                'Cannot save to path if no path given!',
                'Setting mode to "port".'
            ])
            logs.display_message(message, type = 'warning')

        if mode == 'port' and not port:
            message = ''.join([
                'Cannot send to a port without a port specified.',
                'converting to default of 5007'
            ])
            logs.display_message(message, type = 'warning')

    # Format and split full data frame from Scraper
    # ========================================================================
    # If t_df is a path and not a data frame, extract from CSV
    # ------------------------------------------------------------------------
        if not isinstance(t_df, pd.DataFrame):
            t_df = validators.path(file_name= t_df, 
                                   project_dir = project_dir,
                                   must_exist = True, overwrite = True,
                                   req_suffix = ['.txt', '.csv'])
            
            t_df = pd.read_csv(t_df, 
                    sep = ',', 
                    header = 0, 
                    dtype={'pmid': int, 'volume': 'Int64', 'issue': 'Int64'})

        t_df = format_df(t_df)

        if interactive:
            visual = vis.interactive(t_df, keyword = keyword, 
                                   start_date = start_date, 
                                   end_date = end_date,
                                   logo_path = logo_path,
                                   primary_color = primary_color, 
                                   secondary_color = secondary_color,
                                   accent_color = accent_color)
        else:
            visual = vis.static(t_df, keyword = keyword, 
                                   start_date = start_date, 
                                   end_date = end_date,
                                   logo_path = logo_path,
                                   primary_color = primary_color, 
                                   secondary_color = secondary_color,
                                   accent_color = accent_color)

        if mode == 'html':
            visual.save(out_path, resources = INLINE)
        elif mode == 'jupyter':
            pn.extension()
            return visual.servable()
        elif mode == 'port':
            visual.show(port = port)
        return None
        
    except Exception as e:
        message = ''.join([
            "Exception in full visual construction:",
            f"\n \t {e}"
        ])
        logs.display_message(message, type = 'error')
