import re
import os
import datetime as dt
import pandas as pd
import pubmed_tool.logs as logs

logger = logs.get_logger()

def path(
    project_dir = None, file_name = None, req_suffix = ['.txt'],
    must_exist = False, overwrite = False
    ):
    """
    Takes in a potential project directory and/or path, with customizeable 
    required suffixes. Provides limited path validation.

    INPUTS:
        project_dir (str): potential project directory path.
            Default is None, which will set to the current working directory
        file_name (str): potential file name. Can be an absolute path.
            Default is None, which will use a default file name of 'default'.
        req_suffix (list): list of required file suffixes. Default is '.txt'
        must_exist (boolean/logical): indicates if the desired file path
            must exist to be valid. Default is False.
        overwrite (boolean/logical): indicates if it is desired to overwrite the
            file or not. Default is False, which will block validation if the
            desired file already exists. CANNOT be False if must_exist is True
    RETURNS:
        path (str): validated absolute path. If it does not pass, returns None.
    REQUIREMENTS/DEPENDENCIES:
        os
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
    """
    try:
    # Constants
    # ========================================================================
        default_file = 'default'
        path = None
        message = list()

    # Check Must_Exist and Overwrite
    # ========================================================================
        if must_exist and not overwrite:
            raise OSError("Cannot have Must_Exist without Overwrite")

    # File_Name Validation
    # ========================================================================
    # Use default, if not given
    # -----------------------------------------------------------------------
        if not file_name:
            file_name = default_file + req_suffix[0]
            message.append(''.join([
                    'No file name given. File name set',
                    f' to {file_name}.'
                ]))

    # Check suffix. Fix if missing or wrong suffix
    # -----------------------------------------------------------------------
        if file_name:
            file_name = file_name.strip()
            file_nm, extension = os.path.splitext(file_name)
            if extension not in req_suffix:
                file_nm = file_nm + req_suffix[0]
                message.append(''.join([
                    f"File name '{file_name}' does not end with",
                    f' any approved suffix option ({"".join(req_suffix)}).',
                    f" File name changed to '{file_nm}'"            
                    ]))
                file_name = file_nm
                file_nm = None

    # Project Directory
    # ========================================================================
    # Use current working directory, if not given.
    # -----------------------------------------------------------------------
        if not project_dir:
            if not os.path.isabs(file_name):
                project_dir = os.getcwd()
                path = os.path.join(project_dir, file_name)
                message.append(''.join([
                    'No project directory given. Directory set',
                    ' to the current working directory.'
                ]))
            else:
                path = file_name

    # Replace project directory, if absolute path was given for file name.
    # -----------------------------------------------------------------------
        if project_dir:
            project_dir = project_dir.strip()
            if os.path.isabs(file_name):
                project_dir = os.path.split(file_name)
            path = os.path.join(project_dir, file_name)

    # Try to make project directory, if it does not already exist.
    # -----------------------------------------------------------------------
        if not os.path.isdir(project_dir):
            os.makedirs(project_dir)
            message.append(''.join([
                f'Directory {project_dir} did not exist.',
                ' Created new directory.'
            ]))

    # Must Exist and Overwrite Checks
    # ========================================================================
    # Verify file does not exist, if Overwrite == False
    # -----------------------------------------------------------------------
        if not overwrite and os.path.exists(path):
            message.append(''.join([
                'Overwrite is set to FALSE, but the desired ',
                f'file path of {path} already exists. ',
                'Processing stopped. Consider overwrite or ',
                'a new destination file name.'
            ]))
            path = None
            message = '\n \t'.join(message)
            raise OSError(message)

    # Verify file DOES exist, if Must_Exist == True
    # -----------------------------------------------------------------------
        if must_exist and not os.path.exists(path):
            message.append(''.join([
                'Must_Exist is set to FALSE, but the desired ',
                f'file path of {path} does not exist. ',
                'Processing stopped. Consider a new file name.'
                ]))
            path = None
            message = '\n \t'.join(message)
            raise OSError(message)           

    # Informative Success Message
    # ========================================================================
        if path:
            notice = 'Path validation completed.'
            logs.display_message(message, type = 'info')

    except Exception as e:
    # Informative Error/Failure Message
    # ========================================================================
        path = None
        notice = '\n \t'.join([
            'Error occured in path validation: ',
            'Error message(s):',
            f'{e}'
            ])
        logs.display_message(notice, type = 'error')

    # Return Output
    # ========================================================================
    return path

def email(email = None):
    """
    Takes a potential email string, and uses a
    a regular expression to validate. 
    Returns email (lowercase, stripped) if valid,
    returns None if invalid.
    
    INPUTS:
        email (string): email address
    RETURNS:
        email (string): validated, lowercase, stripped
            email address. If email was invalid,
            returns None
    REQUIREMENTS/DEPENDENCIES:
        re
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
    """
    try:
    # Regular Expression
    # ========================================================================
        email_regex = re.compile("^[a-z0-9!#$%&'*+/=?^_`{|}~-]"
            "+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)"
            "*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)"
            "+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$"
            )
    
    # Validation using Regular Expression
    # ======================================================================= 
    # Also strips and formats email to lowercase
        e_v = email  
        if email:
            email = email.lower().strip()
            if not re.search(email_regex, email):
                email = None
                raise ValueError()    
    except Exception as e:
        email = None
    # Informative Message for Failure
    # ------------------------------------------------------------------------
        message = ''. join([
                    f"<email>: {type(e_v)} and value ",
                    f"'{e_v}' is not valid.",
                    f'\n \tAdditional messages: {e}'
                ])
        logs.display_message(message, type = 'warning')

        message = None
        e_v = None

    return email

def date(date):
    """
    Takes a potential date (string, datetime.date, 
    or datetime.datetime), validates the date, and 
    returns as a string in YYYY/MM/DD format.
    
    INPUTS:
        date (string, datetime.datetime, datetime.date):
            date with a 4 digit year, 1-2 digit month, and
            1-2 digit day in YEAR MONTH DAY order. May
            use '-',' ', or '/' separators.
    RETURNS:
        date (string): validated, formatted string of date
        in YYYY/MM/DD format. If date was invalid,
        returns None
    REQUIREMENTS/DEPENDENCIES:
        datetime as dt
        re
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
    """
    # Regular Expression
    # =======================================================================
    # Requires 4 digit year, 1-2 digit month, and 1-2 digit day.
    # Accounts for possible separation by '/', ' ', '-', or a combination.

    date_regex = r'^([0-9]{4})[ -/]*?([0-9]{1,2})[ -/]*?([0-9]{1,2})$'

    # Validation
    # =======================================================================
    d_v = date
    year = None
    month = None
    day = None
    try:
    # Convert datetime.datetime or datetime.date instances to string output
    # ------------------------------------------------------------------------

        if isinstance(date, dt.datetime) or isinstance(date, dt.date):
            date = date.strftime(r'%Y/%m/%d')

    # Examine strings for validity of input
    # ------------------------------------------------------------------------

        elif isinstance(date, str):
            date = date.lower().strip()

    # Check string date elements for validity using regular expression
    # ------------------------------------------------------------------------
            date_elem = re.findall(date_regex, date)

            if date_elem:
    # Extract Year, Month, and Day integer components from matches
    # ------------------------------------------------------------------------
                year, month, day = [int(i) for i in date_elem[0]]
            
    # Validate Date Elements
    # ------------------------------------------------------------------------
    # Checks Year, Month, Day elements. 
    # '0' is considered invalid in any field.
    # Ensures month is between 1 and 12. 
    # Calculates day based on month, accounting for leap years.

            while year:
                if month < 1 or year < 1 or day < 1:
                    year = None
                    break
                if month > 12:
                    year = None
                    break
                leap = True if year % 4 == 0 else False
                if leap and year == 2 and day > 29:
                    year = None
                    break
                elif month == 2 and day > 28:
                    year = None
                    break
                elif month in [4,6,9,11] and day > 30:
                    year = None
                    break
                elif day > 31:
                    year = None
                    break
                break

    # Create String Output in YYYY/MM/DD Format if validation passed
    # ------------------------------------------------------------------------
            if year:
                date = '/'.join([
                        f'{year:0>4}',
                        f'{month:0>2}',
                        f'{day:0>2}'
                        ])
            else:
                date = None
                raise ValueError()
        else:
            date = None
            raise ValueError()
    except Exception as e:
        date = None

    # Informative Message for Failure
    # ------------------------------------------------------------------------
        message = ''. join([
                            f"<date>: {type(d_v)} and value ",
                            f"'{d_v}' is not valid.",
                            f'\n \tAdditional messages: {e}'
                        ])
        logs.display_message(message, type = 'warning')
        d_v = None
        message = None

    return date

def pmid(pmid):
    """
    Takes a potential PubMedID (string, or int), validates it,
    and returns the PMID as an integer.
    
    INPUTS:
        pmid (string or int): potential PubMed ID
    RETURNS:
        pmid (int or None): validated PubMed ID. If invalid,
            returns None
    REQUIREMENTS/DEPENDENCIES:
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
    """
    # Validation
    # =======================================================================
    # PubMed IDs are integers, 1-8 digits in length, without leading zeroes
    # https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html
    p_v = pmid
    try:
        # Must be String or Integer
        if not (isinstance(pmid,int) or isinstance(pmid,str)):
            pmid = None
            raise ValueError()
 
    # Convert integer to string for checks
    # ------------------------------------------------------------------------           
        if isinstance(pmid, int):
            pmid = str(pmid)

    # Remove leading/trailing zeroes, check length. Reject '0'.
    # ------------------------------------------------------------------------    
        if isinstance(pmid, str):
            pmid = str(int(pmid.lower().strip()))
            if len(pmid) < 1 or len(pmid) > 8:
                pmid = None
                raise ValueError("Must be between 1 and 8 digits.")
            if pmid == '0':
                pmid = None
                raise ValueError("Cannot equal 0")
            pmid = int(pmid)
    except Exception as e:
        pmid = None

    # Informative Message for Failure
    # ------------------------------------------------------------------------
        message = ''. join([
                        f"<pmid>: {type(p_v)} and value ",
                        f"'{p_v}' is not valid. Removing.",
                        f'\n \tAdditional details: {e}'
                    ])
        logs.display_message(message, type = 'warning')
        p_v = None
        message = None

    return pmid

def existing_csv(path):
    """
    Takes in a potential path to an existing publication CSV, and ensures
    it has the required column names.

    INPUTS:
        path (str): path of potential existing publication CSV file.
        req_suffix (list): list of required file suffixes. Default is '.txt'
        must_exist (boolean/logical): indicates if the desired file path
            must exist to be valid. Default is False.
        overwrite (boolean/logical): indicates if it is desired to overwrite the
            file or not. Default is False, which will block validation if the
            desired file already exists. CANNOT be False if must_exist is True
    RETURNS:
        path (str): validated absolute path. If it does not pass, returns None.
    REQUIREMENTS/DEPENDENCIES:
        os
        pandas as pd
        config_logging as logs
        logger = logs.get_logger()
        logs.display_message()
    """
    if not os.path.exists(path):
        raise OSError ("Path does not exist!")
    else:
        read_in = pd.read_csv(path, delimiter=',', header=0, nrows = 0)
        if (read_in.columns.tolist() != [
            'pmid', 'title', 'pubdate', 'authors', 
            'keywords', 'journal', 'isoabbrev', 
            'volume', 'issue', 'page_start', 'page_end', 'language',
            'abstract', 'other_type', 'other_val']):
                message = ''.join([
                    'Appending indicated (overwrite == False), but',
                    'desired file path exists, and does not have',
                    'a matching format. Check file and retry.'
                    ])
                path = None
                raise ValueError(message)
        else:
            return True