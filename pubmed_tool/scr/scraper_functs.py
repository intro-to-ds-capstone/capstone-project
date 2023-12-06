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

logger = logs.get_logger()

def pubmed_search_ids(
    keyword, start_date, end_date, email, max_returns = 200000
    ):
    """
    Submits a query to PubMed via ENTREZ, and returns a list of matching
    PubMed IDs. Performs limited validation of inputs.
    
    INPUTS:
        keyword (string): Keyword term
        start_date (string, datetime.datetime, or datetime.date):
            Date in YYYY/MM/DD format,
            Validated by validate_date()
        end_date (string, datetime.datetime, or datetime.date):
            Date in YYYY/MM/DD format,
            Validated by validate_date()
        email (string): email address, required by NCBI,
            Validated by validate_email()
        max_returns (int): integer indicating the maximum number of
                records to return; uses default if cannot validate;
                default is 200,000
    RETURNS:
        results (list): list of PMIDs as strings
        Prints error message and suggestions if error message from
        NCBI
    REQUIREMENTS/DEPENDENCIES:
        Entrez from BioPython
        re
        datetime as dt
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
        validators.date()
        validators.email()
    """
    # Validation
    # ========================================================================
    # Validate Keyword
    # ------------------------------------------------------------------------
    k_v = keyword
    try:
        if not isinstance(keyword, str):
            if isinstance(keyword, int) or isinstance(keyword, float):
                keyword = str(keyword)
            else:
                keyword = None
    except:
        keyword = None

    # Informative Failure Messsage
    if not keyword:
        message = ''. join([
                    f"<keyword>: {type(k_v)} and value ",
                    f"'{k_v}' invalid. Omitted. May return ",
                    f'an unexpectedly large number of records'
                ])
        logs.display_message(message, type = 'warning')
        message = None

    k_v = None
    
    # Validate Dates
    # ------------------------------------------------------------------------
    start_date = validators.date(start_date)
    end_date = validators.date(end_date)
    
    # Validate Email
    # ------------------------------------------------------------------------
    email = validators.email(email)
    
    # Validate Maximum Returns. Floor divide floats. Use 200,000 if failed.
    # ------------------------------------------------------------------------
    m_v = max_returns
    try:
        if not isinstance(max_returns, int):
            if isinstance(max_returns, str):
                max_returns = max_returns.strip()
                if max_returns.isdigit():
                    max_returns = int(max_returns)
                else:
                    max_returns = 200000
            elif isinstance(max_returns, float):
                max_returns = int(max_returns // 1)
            else:
                max_returns = 200000
    except:
        max_returns = 200000

    # Informative Failure Messsage, Detailing Conversion
    if max_returns != m_v:
        message = ''. join([
                            f"<max_value>: {type(m_v)} and value ",
                            f"'{m_v}' invalid. Converted to {max_returns}"
                        ])
        logs.display_message(message, type = 'warning')
        message = None
    m_v = None

    # Query
    # ========================================================================
    # Process only if all required inputs have passed validation
    # ------------------------------------------------------------------------
    if start_date and end_date and email and keyword and max_returns:

    # Generate Query String
    # ------------------------------------------------------------------------
        query = (f'({keyword}) AND ("{start_date}"[Date - Publication]'
                f' : "{end_date}"[Date - Publication])'
                )
 
        # Logger message for beginning processing
        message = '\n \t'.join([
                    f"Initiating PubMed search.",
                    f"Query: {query}",
                    f"Entrez email: {email}", 
                    f"Max returns: {max_returns}"
                ])
        logs.display_message(message, type = 'info')

        Entrez.email = email

    # Attempt Query
    # ------------------------------------------------------------------------    
        try:
            handle = Entrez.esearch(db='pubmed',
                                    sort='relevance',
                                    retmax = max_returns,
                                    retmode='xml',
                                    term=query)
            results = Entrez.read(handle)
            results = results['IdList']
            handle.close()

        
    # Informative message for success conditions
    # ------------------------------------------------------------------------
            message = ' '.join([
                    f'Search Successful! Obtained {len(results)} PMID(s).'
                    ])
            logs.display_message(message, type = 'info')
            message = None
                   
            return results
        except Exception as e:

    # Informative message for failure conditions, such as HTTP errors
    # ------------------------------------------------------------------------
            message = ' '.join([
                    'An error occured when contacting NCBI servers.',
                    'Check your query terms. Consider reattempting',
                    'outside of peak hours. Message from request: \n',
                    f'{e}'])
            logs.display_message(message, type = 'error')

            return None

    # Message for failed validation, and aborted processing.
    # ------------------------------------------------------------------------ 
    else:
        message = ' '.join([
                    'Unable to process search due to invalid inputs.',
                    'Please review and try again.'
                    ])
        logs.display_message(message, type = 'error')
        return None
    
def pubmed_fetch_records(target_ids, email):
    """
    Queries IDs from PubMed using PMIDs.
    Automatically trims output to the Medline Citation

    INPUTS:
        target_ids (list): list of PMIDs stored as strings
            or integers. May also pass a single ID.
        email (string): email address, required by NCBI,
            Validated by validate_email()
    RETURNS:
        results (list): list of MedlineCitation entries in each result
        Prints error message and suggestions if error message from
        NCBI, and returns None
    REQUIREMENTS/DEPENDENCIES:
        Entrez from BioPython
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
        validators.pmid()
        validators.email()
    """
    # Validation
    # ========================================================================
    # Validate Target IDs
    # ------------------------------------------------------------------------
    if not isinstance(target_ids, list) or isinstance(target_ids, tuple):
        if isinstance(target_ids, int) or isinstance(target_ids, str):
            target_ids = validators.pmid(target_ids)
            if target_ids:
                num_ids = 1
        else:
            target_ids = None
    else:
        target_ids = [validators.pmid(t_id) for t_id in 
                      target_ids if validators.pmid(t_id)]
        
        num_ids = len(target_ids)

    # Validate Email
    # ------------------------------------------------------------------------
    email = validators.email(email)
    Entrez.email = email

    # Query
    # ========================================================================
    # Perform only if required inputs passed validation.
    # ------------------------------------------------------------------------
    if target_ids and email:
        try:
            message = ''.join([
                    f'Initiating PubMed search. Requesting article',
                    f' records for {num_ids} PMID(s)'
                ])
            logs.display_message(message, type = 'info')
            message = None

            handle = Entrez.efetch(db = 'pubmed',
                                retmode = 'xml',
                                id = target_ids)
                                
            results = [result['MedlineCitation'] for result in
                       Entrez.read(handle)['PubmedArticle']]
            handle.close()

    # Informative message for success conditions
    # ------------------------------------------------------------------------
            message = ' '.join([
                    'Search Successful! Obtained records for',
                    f' {len(results)} PMID(s).'
                    ])
            logs.display_message(message, type = 'info')
            message = None

            return results
        
        except Exception as e:
    # Informative Message for Failure
    # ------------------------------------------------------------------------
            message = ' '.join(['An error occured when contacting NCBI servers.',
                                'Check your query terms. Consider reattempting',
                                'outside of peak hours. Message: \n',
                                f'{e}'])
            logs.display_message(message, type = 'error')
            message = None

            return None
        
    else:
    # Message for failed validation, and aborted processing.
    # ------------------------------------------------------------------------ 
        message = ' '.join([
                    'Unable to process search due to invalid inputs.',
                    'Please review and try again.'
                    ])
        logs.display_message(message, type = 'error')
        message = None

        return None

def format_date(date_obj, pmid = None):
    """
    Takes a date object and returns a formatted date time object

    INPUTS:
        date_obj (dict): Dictionary, which may contain keys
                        ['Year', 'Month', 'Day'] with single
                        string content.
                        'Year' is expected to be 4 digit, if exists
                        'Month' is expected to be 2 digit or
                                3 character string, if exists
                        'Day' is expected to be 2 digit, if exists
        pmid (int): PubMed ID of associated Record, used for
                informative messages
    RETURNS:
        date (date): Date, with missing/invalid month or day
                     rounded to '01'.
                     Returns None if invalid or missing
    REQUIREMENTS/DEPENDENCIES:
        datetime as dt
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
    """
    # Month Dictionary (3 letter abbreviation to numbers)
    # ========================================================================
    # Seasons based on the month of their first day on US calendars
    months_dict = {
                'jan': '01', 'feb': '02', 'mar': '03', 
                'apr': '04', 'may': '05', 'jun': '06' ,
                'jul': '07', 'aug': '08', 'sep': '09', 
                'oct': '10', 'nov': '11', 'dec': '12',
                'spr': '03', 'sum': '06', 'fal': '09', 
                'win': '11'
                }

    # Validation
    # ========================================================================
    # Year
    # ------------------------------------------------------------------------
    try:
        if 'Year' in date_obj.keys():
            year = date_obj['Year'].lower().strip()
            if len(year) < 4 or not year.isdigit():
                year = None
        else:
            year = None

    # Month
    # ------------------------------------------------------------------------
        if 'Month' in date_obj.keys():
            month = date_obj['Month'].lower().strip()
            if not month.isdigit() and month in months_dict.keys():
                month = months_dict[month]
            elif (month.isdigit() and int(month) in range(1,13) and 
                  len(month) < 2):
                month = '0' + str(int(month))
            else:
                month = None
        else:
            month = None
    # Day
    # ------------------------------------------------------------------------
        if 'Day' in date_obj.keys():
            day = date_obj['Day'].lower().strip()
            if day.isdigit() and int(day) in range(1,13) and len(day) < 2:
                day = '0' + str(int(day))
            else:
                day = None
        else:
            day = None

    # Process MedlineDate element, if no other date element present
    # ========================================================================

        if not (year):
            if 'MedlineDate' in date_obj.keys():
    # Regular Expression to attempt to extract Year
    # -----------------------------------------------------------------------
                year = re.search(r'[0-9]{4}', date_obj['MedlineDate'])
                if year:
                    year = year[0]

    # Regular Expression to extract and process Month
    # -----------------------------------------------------------------------
    # May have month range (e.g.'Jan-Mar'). Extract only first possible match.
                month = re.search(r'[A-Za-z]{3}', date_obj['MedlineDate'])
                if month and month in months_dict.keys():
                    month = months_dict[month[0].lower().strip()]
                else:
                    month = None

    # Attempt to Format
    # ========================================================================
    # datetime automatically rounds missing components to the first.
    # Failed formatting indicates a date value is invalid. (e.g. "Feb 31")
        try:
            if year and month and day:
                date = dt.datetime.strptime(f'{year}/{month}/{day}', r'%Y/%m/%d').date()
            elif year and month:
                date = dt.datetime.strptime(f'{year}/{month}', r'%Y/%m').date()
            elif year:
                date = dt.datetime.strptime(f'{year}', r'%Y').date()
            else:
                date = None
        except:
            date = None
    # Return output
    # -----------------------------------------------------------------------        
        return date
    
    except Exception as e:
    # Message for failed processing.
    # ------------------------------------------------------------------------ 
        message = ' '.join([
                    f'Unable to process search due to error in record: {pmid}.',
                    f' In Date. Errors: {e}'
                    ])
        logs.display_message(message, type = 'error')
        return None
    
def format_author(author_dict, index = 0, pmid = None):
    """
    Formats an Author from a PubMed Author List into a dictionary
    in {First, Last, Initials} keys, with values all in lowercase

    INPUTS:
        author_dict (dict): Single entry from a PubMed MedlineCitation 
                already sliced to ['Article']['AuthorList'].
                Expected to potentially contain the single values
                for keys []'ForeName', 'LastName', 'Initials']
        index (int): Index of entry in order, for use in list
                comprehension; default is 0.
        pmid (int): PubMed ID of associated Record, used for
                informative messages
    RETURNS:
        name_dict (dict): a dictionary of author name fields where
                missing fields are None.
    REQUIREMENTS/DEPENDENCIES:
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
    """
    poss_initials = None
    # Extract Elements
    # ========================================================================
    # Make lowercase, strip leading/trailing spaces
    # Remove any spaces between initials
    try:
        if ('ForeName' in author_dict.keys() and 
            len(author_dict['ForeName']) > 0):
            first_nm = author_dict['ForeName'].lower().strip()
        else:
            first_nm  = None
        if ('Initials' in author_dict.keys() and 
            len(author_dict['Initials']) > 0):
            initials = author_dict['Initials'].lower().strip().replace(" ", "")
        else:
            initials = None
        if ('LastName' in author_dict.keys() and 
            len(author_dict['LastName']) > 0):
            last_nm = author_dict['LastName'].lower().strip()
        else:
            last_nm = None

    # Basic Checks and Standardization
    # ========================================================================
    # If the first name is the same as initials, just with a space in the
    # middle, remove the first name
    # ------------------------------------------------------------------------
        if first_nm:
            if first_nm.replace(" ", "") == initials:  
                first_nm = None

    # If there is a space in the first name, keep only the first of the names
    # ------------------------------------------------------------------------
        if first_nm:
            if " " in first_nm:
            # Extract all pieces separated by ' ', that are not empty, after
            # removing periods and commas
                first_name_pieces = [
                    name.strip().replace('.', '').replace(',', '') for name in 
                    first_nm.split(' ') if 
                    name.strip().replace('.', '').replace(',', '') != ''
                    ]
            # Extract possible initials from these pieces, by taking the first
            # character/letter
                poss_initials = ''.join([name[0] for name in first_name_pieces])
            # Reduce list of first name pieces down to any component that is not
            # a single character in length
                first_name_pieces = [name for name in first_name_pieces if 
                                    len(name) > 1]
                
            # If the name was determined to have extra initials, reform the name
            # without these elements so that first name IS first name
                if initials:
                    if poss_initials == initials:
                        first_nm = ' '.join(first_name_pieces)

                first_name_pieces = None

    # If initials do not exist, try to replace them from first name
    # ------------------------------------------------------------------------
        if not initials:
            if poss_initials:
                initials = poss_initials
            elif first_nm:
                initials = first_nm[0]

    # Format into Dictionary
    # =========================================================================
        name_dict = {'order': str(index+1),
                        'first': first_nm,
                        'last': last_nm,
                        'initials': initials
                        }
    # Return Output
    # -----------------------------------------------------------------------    
        return name_dict
    except Exception as e:
    # Message for failed processing.
    # ------------------------------------------------------------------------ 
        message = ' '.join([
                    f'Unable to process search due to error in record: {pmid}.',
                    f' In Author Processing. Errors: {e}'
                    ])
        logs.display_message(message, type = 'error')
        return None

def issue_vol(in_vol,pmid = None):
    """
    Takes in a potential volume or issue field value, and checks for
    known variations that are non-integer and potentially belong to
    other fields. Any non-present field returns 'None'

    INPUTS:
        in_vol (str): Single entry from a PubMed MedlineCitation's 
                ['Article']['Journal']['JournalIssue'] items of
                ['Volume'] or ['Issue']
        pmid (int): PubMed ID of associated Record, used for
                informative messages
    RETURNS:
        vol (int): Volume number, if it was found
        other_type (str): 'Supplement', 'Issue Range', 'Part', or
                'Special No.', if those items were identified
        other (int): Value of any 'other' field, such as 
                part number.
    REQUIREMENTS/DEPENDENCIES:
        re
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
    """
    # Initialize outputs
    # ========================================================================
    vol = None
    other_type = None
    other = None
    try:
    # Initial Formatting
    # ========================================================================
    # Make lowercase, strip leading/trailing spaces.
    # Address unicode issues: \xa0 corresponds to a 'nonbreak space'
        in_vol = str(in_vol).replace(r'\xa0', ' ').lower().strip()


    # Processing
    # ========================================================================
    # If the string contains a non-digit character, investigate
    # -----------------------------------------------------------------------
        if re.search("[^0-9]", in_vol):

    # Search for DIGIT STRING DIGIT pattern, which is the most typical
    # -----------------------------------------------------------------------
            search = re.findall('^([0-9]*)([^0-9]*)([0-9]*)', in_vol)[0]
            if search:

    # Volume/Issue
    # -----------------------------------------------------------------------
    # Most likely to be in the first portion of the regex match, but if
    # the STRING was 'volume', 'issue', or abbreviations of those, the
    # number is likely in the third portion of the regex match

                if search[0] != '':
                    vol = int(search[0])
                elif (('vol' in search[1] or 'iss' in search[1]) 
                    and search[2].isdigit()):
                    vol = int(search[2])
                elif search[1] == '' and search[2] != '':
                    vol = int(search[2])

    # Other Type
    # -----------------------------------------------------------------------
    # Most likely to be in the second portion of the regex match. 
    # Most likely to be 'supplement', 'part', 'special no', or a variation.
    # May also catch an Issue Range (e.g. '3-4'). 
    # 'cz' is a non-English abbreviation for 'part'.

                if search[1] == '-':
                    other_type = 'Issue Range'
                elif any('sup' in s for s in search):
                    other_type = 'Supplement'
                elif (any('part' in s for s in search) or 
                    any('pt' in s for s in search) or
                    any('cz' in s for s in search)):
                    other_type = 'Part'
                elif (any('spec' in s for s in search)):
                    other_type = 'Special No.'
            
    # Other Value
    # -----------------------------------------------------------------------
    # If there is a third portion of the regex match and it was not used
    # for Volume/Issue number, use it for the other value
                if not (('vol' in search[1] or 'iss' in search[1]) 
                    and search[2].isdigit()):
                    other = search[2]

    # Use input if regex processing had no matches, and it is a digit
    # -----------------------------------------------------------------------    
        elif in_vol.isdigit():
            vol = int(in_vol)

    # Return values
    # -----------------------------------------------------------------------
        return vol, other_type, other
    
    except Exception as e:
    # Message for failed processing.
    # ------------------------------------------------------------------------ 
        message = ' '.join([
                    f'Unable to process search due to error in record: {pmid}.',
                    f' In Volume/Issue Processing. Errors: {e}'
                    ])
        logs.display_message(message, type = 'error')

        return None

def journal_content(record, pmid = None):
    """
    Scrapes ['Article']['Journal'] level content from
    a PubMed MedlineCitation entry:
    Journal Title, Journal ISO Abbreviation, Volume, Issue,
    and Publication Date

    INPUTS:
        record (dict): PubMed MedlineCitation already sliced to
                ['Article']['Journal']
        pmid (int): PubMed ID of associated Record, used for
                informative messages
    RETURNS:
        jour_data (dict): dictionary of ['Journal'] data items;
                {journal, isoabbrev, volume, issue, other_type, 
                other_val, pubdate}
                missing fields are NoneType
    REQUIREMENTS/DEPENDENCIES:
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
        format_date(): datetime as dt
        issue_vol(): re
    """
    # Initialize Output
    # ========================================================================
    jour_data = {}

    # Processing
    # ========================================================================
    # Title
    # ------------------------------------------------------------------------
    try:
        if 'Title' in record.keys() and len(record['Title']) >0:
            jour_data['journal'] = str(record['Title']).lower().strip()
        else:
            jour_data['journal'] = None

    # ISO Abbreviation
    # ------------------------------------------------------------------------
        if ('ISOAbbreviation' in record.keys() and 
            len(record['ISOAbbreviation']) >0):
            abbrev = str(record['ISOAbbreviation']).lower().strip()
            jour_data['isoabbrev'] = abbrev
        else:
            jour_data['isoabbrev'] = None

    # Journal Issue Items
    # ------------------------------------------------------------------------
        if 'JournalIssue' in record.keys():
    # ISSUE
            if ('Issue' in record['JournalIssue'].keys() and
                len(record['JournalIssue']['Issue']) >0):
                iss, oth, val=issue_vol(record['JournalIssue']['Issue'], pmid)
                jour_data['issue'], jour_data['other_type'] = iss, oth
                jour_data['other_val'] = val
            else:
                jour_data['issue'], jour_data['other_type'] = None, None
                jour_data['other_val'] = None
    # VOLUME
            if ('Volume' in record['JournalIssue'].keys() and
                len(record['JournalIssue']['Volume']) >0):
                vol, oth, val=issue_vol(record['JournalIssue']['Volume'], pmid)
                jour_data['volume'], jour_data['other_type'] = vol, oth
                jour_data['other_val'] = val
            else:
                jour_data['volume'] = None
    # PUBLICATION DATE
            if ('PubDate' in record['JournalIssue'].keys() and
                len(record['JournalIssue']['PubDate']) >0):
                date = format_date(record['JournalIssue']['PubDate'], pmid)
                jour_data['pubdate'] = date
            else:
                jour_data['pubdate'] = None
        else:
            jour_data['issue'], jour_data['volume'] = None, None
            jour_data['other_type'], jour_data['other_val'] = None, None
            jour_data['pubdate'] = None

    # Return Output
    # ========================================================================
        return jour_data
    except Exception as e:
    # Message for failed processing.
    # ------------------------------------------------------------------------ 
        message = ' '.join([
                    f'Unable to process search due to error in record: {pmid}.',
                    f' In Journal Processing. Errors: {e}'
                    ])
        logs.display_message(message, type = 'error')
        return None

def article_content(record, pmid = None):
    """
    Scrapes ['Article'] level content from
    a PubMed MedlineCitation entry:
    Title, Abstract, Pageination, Publication Date, Language,
    Authors, Journal Title, Journal ISO Abbreviation,
    Journal Volume, Journal Issue, Other_Type, Other_Value.
    Prefers JOURNAL PUBLICATION DATE over Article Date, if
    it is present.

    INPUTS:
        record (dict): PubMed MedlineCitation already sliced to
                ['Article']
        pmid (int): PubMed ID of associated Record, used for
                informative messages
    RETURNS:
        article_data (dict): dictionary of ['Article'] data items;
                {title, abstract, pubdate, page_start, page_end,
                language = ['lang1', 'lang2'], 
                authors = [{Order, First, Last, Initials}],
                volume, issue, other_type, other_val}
                Missing items are Nonetype.
    REQUIREMENTS/DEPENDENCIES:
        re
        datetime as dt
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
        format_date()
        format_author()
        journal_content(): issue_vol()
    """
    # Initialize Output
    # ========================================================================
    article_data = {}

    # Regular Expression
    # ========================================================================
    pgn_regex = r'^([0-9a-z]+)(?:[ -:/]+?)([0-9a-z]+)$'

    # Processing
    # ========================================================================
    # Title
    # ------------------------------------------------------------------------
    try:
        if 'ArticleTitle' in record.keys() and len(record['ArticleTitle']) >0:
            article_data['title'] = str(record['ArticleTitle']).lower().strip()
        else:
            article_data['title'] = None

    # Abstract
    # ------------------------------------------------------------------------
        if 'Abstract' in record.keys() and len(record['Abstract']) >0:
            if ('AbstractText' in record['Abstract'].keys() and
                len(record['Abstract']['AbstractText']) > 0):
                article_data['abstract'] = ' '.join(
                    record['Abstract']['AbstractText']
                    )
            else:
                article_data['abstract'] = None
        else:
            article_data['abstract'] = None

    # Date (to use if no date in Journal)
    # ------------------------------------------------------------------------
        if 'ArticleDate' in record.keys() and len(record['ArticleDate']) > 0:
            date = format_date(record['ArticleDate'][0], pmid)
        else:
            date = None

    # Pagination
    # ------------------------------------------------------------------------
    # Must remain a string, as some page formats include characters
    # yet are still valid page numbers
        if 'Pagination' in record.keys() and len(record['Pagination']) > 0:
            if ('StartPage' in record['Pagination'].keys() and
                len(record['Pagination']['StartPage']) > 0):
                page_start = record['Pagination']['StartPage']
            else:
                page_start = None
            if ('EndPage' in record['Pagination'].keys() and
                len(record['Pagination']['EndPage']) > 0):
                page_end = record['Pagination']['EndPage']
            else:
                page_end = None
    # Attempt to extract from MedlinePgn if nothing found in Pagination
            if not (page_start or page_end):
                if ('MedlinePgn' in record['Pagination'].keys() and
                    len(record['Pagination']['MedlinePgn']) > 0):
                    search = re.search(pgn_regex, record['Pagination']['MedlinePgn'])
                    if search:
                        page_start = search[1]
                        page_end = search[2]
    # If there is a start page but no end page, make end page the start page
            if (not page_end) and (page_start):
                page_end = page_start
                
            article_data['page_start'] = page_start
            article_data['page_end'] = page_end
        else:
            article_data['page_start'] = None
            article_data['page_end'] = None

    # Authors
    # ------------------------------------------------------------------------
        if 'AuthorList' in record.keys() and len(record['AuthorList']) > 0:
            authors = [format_author(record['AuthorList'][i], i, pmid) for
                    i in range(0, len(record['AuthorList']))]
            article_data["authors"] = authors
        else:
            article_data["authors"] = None

    # Languages
    # ------------------------------------------------------------------------
        if 'Language' in record.keys() and len(record['Language']) > 0:
            article_data['language'] = record['Language']
        else:
            article_data['language'] = None

    # ['Journal'] level content
    # ------------------------------------------------------------------------
        jour_content = journal_content(record['Journal'], pmid)
        for key in jour_content.keys():
            article_data[key] = jour_content[key]

    # Add Article Date if Journal Date is absent
    # ------------------------------------------------------------------------
        if not article_data['pubdate']:
            if date:
                article_data['pubdate'] = date
    
    # Return Output
    # ========================================================================
        return article_data
    except Exception as e:
    # Message for failed processing.
    # ------------------------------------------------------------------------ 
        message = ' '.join([
                    f'Unable to process search due to error in record: {pmid}.',
                    f' In Article Processing. Errors: {e}'
                    ])
        logs.display_message(message, type = 'error')
        return None

def single_record(record):
    """
    Scrapes a MedlineCitation of a PubMed article.
    Returns article data as a dictionary, to facilitate
    creation of a pandas data frame.

    INPUTS:
        record (dict): PubMed MedlineCitation
    RETURNS:
        content (dict): dictionary of article data;
                {pmid, pubdate, title, abstract
                journal, isoabbrev, vol, issue, other_type,
                other_val, keywords, language = ['lang1', 'lang2']
                authors = [{Order, First, Last, Initials}],
                page_start, page_end, keywords = ['key1', 'key2']}
                Missing fields are Nonetype
    REQUIREMENTS/DEPENDENCIES:
        re
        datetime as dt
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
        format_date()
        format_author()
        issue_vol()
        validators.pmid()
        journal_content()
        article_content()
    """
    # Initialize Output
    # ========================================================================
    content = {}

    # Processing
    # ========================================================================
    # PMID
    # ------------------------------------------------------------------------
    try:
        if 'PMID' in record.keys() and len(record['PMID']) > 0:
            pmid = validators.pmid(str(record['PMID']).lower().strip())
            content['pmid'] = pmid
        else:
            content['pmid'] = None
    # Keywords
    # ------------------------------------------------------------------------
        if 'KeywordList' in record.keys() and len(record['KeywordList']) > 0:
            content['keywords'] = set()
            for i in range(0,len(record['KeywordList'])):
                for j in range(0,len(record['KeywordList'][i])):
                    val = record['KeywordList'][i][j].strip().lower()
                    content['keywords'].add(val)
            content['keywords'] = list(content['keywords'])
        else:
            content['keywords'] = None

    # ['Article'] content
    # ------------------------------------------------------------------------
        article_items = article_content(record['Article'], pmid)
        for key in article_items.keys():
            content[key] = article_items[key]
    
    # Return Output
    # ========================================================================
        return content
    except Exception as e:
    # Message for failed processing.
    # ------------------------------------------------------------------------ 
        message = ' '.join([
                    f'Unable to process search due to error in record',
                    f' in any processing. Record: {record} \n \t',
                    f'Errors: {e}'
                    ])
        logs.display_message(message, type = 'error')
        return None