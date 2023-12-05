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
import validators
import pandas as pd
import config_logging as logs

logger = logs.get_logger()

def split_tables(in_df):
    """
    Performs the data frame split necessary to create
    a table of publications, a table of authors, and
    a table indicating author - paper pairs for 
    SQL database upload. Requires the output from
    the scraper to have been formatted by 
    format_df()

    INPUTS:
        in_df (dataframe): data frame produced by
            processing the output from the scraper
            with format_df()

    RETURNS:
        papers_df (dataframe): data frame of paper
            specific columns, with pmid as the
            index and a unique key
        authors (dataframe): data frame of author
            specific columns, with a concatenated
            string of first + initial + last name
            fields as fullname, for the the index 
            and unique key
        author_paper_df (dataframe): data frame
            that indicates the matched pmid and
            author fullname, as well as a bool
            indicating if the author was the
            first author of the paper or not

    REQUIREMENTS/DEPENDENCIES:
        pandas as pd
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
    """  
    try:
        t_df = in_df.copy(deep = True)
    # Generate Papers DataFrame
    # =========================================================
    # Exclude author columns
    # ---------------------------------------------------------
        papers_df = t_df.loc[:, 
                                t_df.columns.difference(
                                ['firstauthor', 'first', 
                                        'last', 'initials'])
                        ]
        papers_df.drop_duplicates(inplace = True)
    # Set index (database key) of PMID
    # ----------------------------------------------------------
        papers_df = papers_df.set_index('pmid', drop = True)

    # Generate Author DataFrame
    # =========================================================
        author_df = t_df.loc[:, 
                                ['pmid', 'firstauthor', 'first', 
                                'last', 'initials'
                                ]]

    # Generate key for data frame
    # ---------------------------------------------------------
    # first + initials + last
        author_df['fullname'] = author_df['initials'].str\
                .cat(author_df[['last', 'first']]\
                                        .values, 
                                        sep = ' ')


    # Generate author - paper key DataFrame, as M-to-M
    # ---------------------------------------------------------
        author_paper_df = author_df.copy(deep = True)[
        ['pmid', 'fullname', 'firstauthor']
        ]
        author_paper_df.drop_duplicates(inplace = True)

    # Finalize author data frame with only author-specific cols
    # ---------------------------------------------------------
        author_df = author_df.drop(columns = ['firstauthor', 
                                              'pmid'])
        author_df = author_df.set_index('fullname', drop=True)
        author_df.drop_duplicates(inplace = True)


    # Return DataFrames
    # ========================================================
        return papers_df, author_df, author_paper_df
    except Exception as e:
        message = ''.join([
            'Error occured in split processing.',
            f'\n\t Error messages: {e}'
        ])
        logs.display_message(message, type = 'error')


def upload(papers, authors, pairs, 
               project_dir = None,           
               db_name = 'publications.db', 
               paper_name = 'papers', 
               authors_name = 'authors',
               pairs_name = 'pairs_authorpapers'):
    
    """
    Uploads split data frames to an SQL database.
    Deletes and overwrites any existing matching tables
    at this time. 

    INPUTS:
        papers (dataframe): paper-specific dataframe
            with pmid as unique key
        authors (dataframe): author-specific dataframe
            with fullname as unique key
        pairs (dataframe): dataframe that indicates
            the matched author-paper pairs, with a
            boolean indicator of first authorship
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

    RETURNS:
        None

    REQUIREMENTS/DEPENDENCIES:
        sqlite3
        pandas as pd
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
        validators.path()
    """  
    # Validate Database Path
    # ========================================================

    db_name = validators.path(project_dir = project_dir, 
                              file_name = db_name,
                              req_suffix=['.db'],
                              overwrite = True)

    conx = None

    try:
        message = ''.join([
            'Attempting to connect to SQLite database ',
            f'{db_name}'
            ])
                
        logs.display_message(message, type = 'info')

        conx = sqlite3.connect(db_name)
        cursor = conx.cursor()

        logs.display_message("Connection successful", 
                             type = 'info')
        
    # Papers Table
    # ========================================================
    # Check if Papers table exists
    # ---------------------------------------------------------

        message = ''.join([
            f'Checking if table <{paper_name}> exists in the',
            ' database. Will replace if found.'
            ])


        cursor.execute(''.join([
            "SELECT name FROM sqlite_master WHERE", 
            f" type='table' AND name='{paper_name}'"]
            ))
    # DROP the table if it exists, so we can replace it
    # ---------------------------------------------------------
        if cursor.fetchone():
            message = ''.join([
            f'Database already has table <{paper_name}>. '
            'Dropping the existing table.'
            ])
            
            logs.display_message(message, type = 'warning')

            cursor.execute(f"DROP TABLE IF EXISTS {paper_name}")

        if cursor.fetchone() is None:
    # Create table
    # ---------------------------------------------------------
            cursor.execute(''.join([
                f'CREATE TABLE {paper_name} ',
                '(pmid INT PRIMARY KEY, title TEXT, ',
                'pubdate DATE, abstract TEXT, journal TEXT, ',
                'isoabbrev TEXT, numauthors INT, volume INT, ',
                'issue INT, page_start TEXT, page_end TEXT, ',
                'other_type TEXT, other_val TEXT, ',
                'keywords TEXT, language TEXT);'
                ]))

            message = ''.join([
                f'Table <{paper_name}> successfully created.'
            ])

            logs.display_message(message, type = 'info')

    # Upload paper data using pandas
    # ---------------------------------------------------------
        papers.to_sql(name=paper_name, con = conx, 
                    if_exists = 'append')
        
        message = ''.join([
            f'{len(papers)} unique papers sucessfully ',
            f'uploaded to SQLite table <{paper_name}>.'
            ])
            
        logs.display_message(message, type = 'info')

    # Authors Table
    # ========================================================  
    # Check if Authors table exists
    # ---------------------------------------------------------
        cursor.execute(''.join([
            "SELECT name FROM sqlite_master WHERE",
            f" type='table' AND name='{authors_name}'"]
            ))
    # DROP the table if it exists, so we can replace it
    # ---------------------------------------------------------
        if cursor.fetchone():
            message = ''.join([
            f'Database already has table <{authors_name}>. '
            'Dropping the existing table.'
            ])
            
            logs.display_message(message, type = 'warning')

            cursor.execute(f"DROP TABLE IF EXISTS {authors_name}")

        if cursor.fetchone() is None:
    # Create table
    # ---------------------------------------------------------
            cursor.execute(''.join([
                f'CREATE TABLE {authors_name} ',
                '(fullname TEXT PRIMARY KEY, first TEXT, ',
                'last TEXT, initials TEXT);'
                ]))

            message = ''.join([
                f'Table <{authors_name}> successfully created.'
            ])

            logs.display_message(message, type = 'info')

    # Upload paper data using pandas
    # ---------------------------------------------------------
        authors.to_sql(name=authors_name, con = conx, 
                    if_exists = 'append')

        message = ''.join([
            f'{len(authors)} unique papers sucessfully ',
            f'uploaded to SQLite table <{authors_name}>.'
            ])
            
        logs.display_message(message, type = 'info')

    # Author-Paper Table
    # ======================================================== 
    # Check if Author-Paper Table Exists
    # ---------------------------------------------------------
        cursor.execute(''.join([
            "SELECT name FROM sqlite_master WHERE",
            f" type='table' AND name='{pairs_name}'"]
            ))
    # DROP the table if it exists, so we can replace it
    # ---------------------------------------------------------
        if cursor.fetchone():
            message = ''.join([
            f'Database already has table <{pairs_name}>. '
            'Dropping the existing table.'
            ])
            
            logs.display_message(message, type = 'warning')

            cursor.execute(f"DROP TABLE IF EXISTS {pairs_name}")

        if cursor.fetchone() is None:
    # Create table
    # ---------------------------------------------------------
            cursor.execute(''.join([
                f'CREATE TABLE {pairs_name} ',
                f'(pmid INT REFERENCES {paper_name}(pmid), ',
                f'fullname TEXT REFERENCES {authors_name}',
                '(fullname), firstauthor BOOL);'
                ]))

            message = ''.join([
                f'Table <{pairs_name}> successfully created.'
            ])

            logs.display_message(message, type = 'info')

    # Upload author-paper pair data using pandas
    # ---------------------------------------------------------
        pairs.to_sql(name=pairs_name, con = conx, 
                    if_exists = 'append', index = False)
        
        message = ''.join([
            f'{len(pairs)} unique pairs of author-paper',
            'keys successfully uploaded to SQLite table '
            f'<{pairs_name}>.'
            ])
            
        logs.display_message(message, type = 'info')

    except Exception as e:
        message = ''.join([
            'There was an error in uploading to SQLite.',
            f'\n\t Error details: {e}'
            ])
        logs.display_message(message, type = 'error')

    # End connection, if exists
    finally:
        if conx:
            conx.close()

def query(db_name = 'publications.db', 
              project_dir = None,
              paper_name = 'papers', 
              authors_name = 'authors', 
              pairs_name = 'pairs_authorpapers', 
              any_nm = None, last_nm = None, 
              first_nm = None, initials_nm = None):
    """
    Queries author name in an SQL data base of
    publications, and returns the Author's full name,
    if they were first author, and paper details for
    all matches in a pandas data frame. Uses OR
    comprehension for all fields.

    INPUTS:
        db_name(path): path to the desired output
            database. Default is 'publications.db',
            which will query in the current
            working directory. Validation by
            validators.path()
        project_dir (path): path to the project 
            directory. Default is None, which will
            use the current working directory.
        papers (dataframe): papers-specific table
            in the database. Default is 'papers'.
        authors (dataframe): author-specific table
            in the database. Default is 'authors'.
        pairs (dataframe): author-paper pair
            specific table in the database. 
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
        sqlite3
        pandas as pd
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
        validators.path()
    """      
    try:
    # Validate inputs
    # =========================================================
    # Database Path
    # ---------------------------------------------------------
        db_name = validators.path(project_dir = project_dir,
                                file_name = db_name,
                                must_exist = True,
                                overwrite = True,
                                req_suffix = ['.db'])

    # Strip strings, make names lowercase
    # ---------------------------------------------------------
        if last_nm:
            last_nm = last_nm.strip().lower()
        if first_nm:
            first_nm = last_nm.strip().lower()
        if initials_nm:
            initials_nm = last_nm.strip().lower()
        if any_nm:
            any_nm = any_nm.strip().lower()

    # Table Existence
    # ---------------------------------------------------------
        conx = sqlite3.connect(db_name)
        cursor = conx.cursor()

    # Check if Papers table exists
    # ---------------------------------------------------------
        cursor.execute(''.join([
                    "SELECT name FROM sqlite_master WHERE", 
                    f" type='table' AND name='{paper_name}'"]
                    ))
        if not cursor.fetchone():
            raise ValueError(
                f"Table {paper_name} does not exist!")
        
    # Check if Authors table exists
    # ---------------------------------------------------------
        cursor.execute(''.join([
                    "SELECT name FROM sqlite_master WHERE", 
                    f" type='table' AND name='{authors_name}'"]
                    ))
        if not cursor.fetchone():
            raise ValueError(
                f"Table {authors_name} does not exist!")
        
    # Check if Author-Paper pairs table exists
    # ---------------------------------------------------------
        cursor.execute(''.join([
                    "SELECT name FROM sqlite_master WHERE", 
                    f" type='table' AND name='{pairs_name}'"]
                    ))
        if not cursor.fetchone():
            raise ValueError(
                f"Table {pairs_name} does not exist!")


    # Form Query
    # =========================================================
    # Initialize to return full name, first authorship flag,
    # and all paper details
    # ---------------------------------------------------------
        query = ''.join([
            f'SELECT {pairs_name}.fullname,',
            f' {pairs_name}.firstauthor, {paper_name}.* \n'
            f'FROM {paper_name}, {pairs_name}, {authors_name}',
            ' \n'
        ])

        any_name_query = ''
        name_query = []

    # Process optional pieces of name queries
    # ---------------------------------------------------------
        if any_nm:
            any_name_query = ''.join([
                f"(({authors_name}.last LIKE '%{any_nm}%') OR ",
                f"({authors_name}.first LIKE '%{any_nm}%') OR ",
                f"({authors_name}.initials LIKE '%{any_nm}%'))"
            ])
        if first_nm:
            name_query.append(''.join([
                f"({authors_name}.first LIKE '%{first_nm}%')"
            ]))
        if initials_nm:
            name_query.append(''.join([
                f"({authors_name}.initials LIKE '%{initials_nm}%')"
            ]))
        if last_nm:
            name_query.append(''.join([
                f"({authors_name}.last LIKE '%{last_nm}%')"
            ]))

    # Join name portion of query
    # ---------------------------------------------------------
        if name_query:
            name_query = ' OR '.join(name_query)
        if any_name_query != '' and isinstance(name_query,str):
            name_query = name_query + ' OR ' + any_name_query
        elif (any_name_query != '' and 
            isinstance(name_query, list)):
            name_query = any_name_query

    # Fully join query
    # ---------------------------------------------------------
        if name_query != '':
            query = ''.join([query, 'WHERE ', name_query, 
                    f'AND {pairs_name}.fullname == ',
                    f'{authors_name}.fullname AND ',
                    f'{paper_name}.pmid ',
                    f'== {pairs_name}.pmid', '\n',
                    f'GROUP BY {pairs_name}.fullname;'])

        else:
            query = ''.join([query, 'WHERE ', 
                    f'{pairs_name}.fullname == ',
                    f'{authors_name}.fullname AND ',
                    f'{paper_name}.pmid ',
                    f'== {pairs_name}.pmid', '\n',
                    f'GROUP BY {pairs_name}.fullname;'])

        message = ''.join([
            'Attempting SQLite query on database:',
            f'{db_name} \n \t Query text: \n \t',
            f'{query}'
        ])
        logs.display_message(message, type = 'info')


    # Execute Query
    # =========================================================
        matches = pd.read_sql_query(sql = query, con = conx, 
                        parse_dates= 'pubdate')
    
        matches.firstauthor = matches.firstauthor.astype(bool)

        message = ''.join([
            f'Query successful. Returning {len(matches)} ',
            f'records as pandas dataframe.'
            ])
            
        logs.display_message(message, type = 'info')

        return matches

    except Exception as e:
        message = ''.join([
            'There was an error in querying SQLite.',
            f'\n\t Error details: {e}'
            ])
        logs.display_message(message, type = 'error')


    finally:
        if conx:
            conx.close()