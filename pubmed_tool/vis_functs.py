"""
imported as vis

Contains functions specific to Visualizations
# =========================================================================
FUNCTIONS:
    filter_transform_df(): filters and transforms a dataframe
        into count-month format
    subset_dates(): trims leading and trailing months with no publications
    describe_stats(): creates summary statistics table
    date_range_text(): creates descriptive text describing the data
    create_line_plot(): creates a line plot
    create_boxplot(): creates a box plot
    create_histogram(): creates a histogram
    interactive(): creates an interactive visualization panel
    static(): creates a static visualization panel

REQUIREMENTS/DEPENDENCIES: 
    numpy as np
    datetime as dt
    pandas as pd
    holoviews as hv
    holoviews.pandas
    panel as pn
    validators
    config_logging as logs
    logger = logs.get_logger()
"""

import numpy as np
import datetime as dt
import pandas as pd
import holoviews as hv
import hvplot.pandas
import panel as pn
import pubmed_tool.validators as validators
import pubmed_tool.config_logging as logs

logger = logs.get_logger()

def filter_transform_df(
    in_df,
    dates = None,
    journal = None,
    num_authors = None,
    languages = None
    ):
    """
    Filters a data frame based on date range, journal name, number of authors,
    and languages for interactive visualization. Converts the data frame into 
    Date: number of articles format.

    INPUTS:
        in_df (dataframe): data frame of output from the scraper, that has
            been formatted by format_df()
        dates (tuple of date objects): tuple of dates for filtering in
            (min_date, max_date) format. Default is None, which omits
            this filtering.
        journal (list of strings): list of journals for filtering.
            Default is None, which omits this filtering.
        num_authors (tuple of integers): tuple of the desired range of the
            number of authors, in (min_num, max_num) format.
            Default is None, which omits this filtering.
        languages (list of strings): list of languages for filtering.
            Default is None, which omits this filtering.

    RETURNS:
        counts_table(dataframe): dataframe table that counts the number of
            articles in the data frame per month.

    REQUIREMENTS/DEPENDENCIES:
        pandas as pd
        numpy as np
        format_df()
        datetime as dt
        config_logging as logs
        logger = logs.get_logger()
    """      
    try:
        subset = in_df.copy(deep = True)

    # Convert publication date into date object
    # =========================================================================
        subset['pubdate'] = pd.to_datetime(subset['pubdate'])
    # Add a version of publication date where day == 1, 
    # which will enable counting based on publication month
        subset['pubym'] = subset['pubdate'].dt.to_period('M')\
            .dt.to_timestamp()

    # Subset based on criteria
    # =========================================================================
    # Check after each step to ensure the subset is not
    # empty, which would throw an error if subsetting
    # was allowed to proceed. If it is empty, stop
    # subsetting, and move to processing.
        while True:
            # Dates 
            if dates:
                subset = subset[subset['pubdate']\
                                .between(dates[0], dates[1])]
            if subset.shape[0] == 0:
                break
            # Journal
            if journal:
                subset = subset[subset['journal'].isin(journal)]
            if subset.shape[0] == 0:
                break       
            # Number of Authors
            if num_authors:
                subset = subset[subset['numauthors']\
                        .between(num_authors[0], num_authors[1])]
            if subset.shape[0] == 0:
                break   
            # Languages
            if languages:
                subset = subset[subset['language']\
                            .str.contains('|'.join(languages))]
            if subset.shape[0] == 0:
                break   
            # if fully reached the end of subsetting, BREAK!
            break

    # Convert to Date - Count Format
    # =========================================================================
    # Pivot the table to a list of dates within the range
    # of the query, with counts of articles for each month.
    # If the subset is empty, return counts of zero. 
    # zero, with the subset table and full table
    # equal - this avoids errors with the visualizer
    # by avoiding an empty input.

    # Subset to only the relevant columns for counting
    # -------------------------------------------------------------------------
    # drop duplicates to ensure unique PMIDs only
        subset = subset.loc[:, ['pmid', 'pubym', 'pubdate']]
        subset.drop_duplicates(inplace = True)

    # Raise error if any PMIDs are still present more
    # than once
        if subset['pmid'].nunique() != subset.shape[0]:
            message = ''.join([
                'PMIDs must be unique, but at least one',
                'PMID is listed with more than one',
                'publication date!'
            ])
            raise ValueError(message)

    # Generate range of dates within the subset
    # -------------------------------------------------------------------------
    # If dates do not exist, set as the minimum and maximum 
    # dates in the subset
        if not dates:
            dates = (subset['pubdate'].min(), 
                        subset['pubdate'].max())

    # Generate the range of dates, in month intervals
    # -------------------------------------------------------------------------
    # accounts for months with no publications!

        dates = pd.date_range(start = dates[0], end = dates[1],
                freq = 'MS'
                ).tolist()

        dates = pd.to_datetime(dates, format =r'%Y-%m-%d')\
            .rename('Publication Date')

    # Initiate table of counts using dates
    # -------------------------------------------------------------------------
        counts_table = pd.DataFrame(dates)

    # If subset was empty, return a single row
    # -------------------------------------------------------------------------
        if subset.shape[0] == 0:
            counts_table['NumCounts'] = 0
    # Otherwise, calculate counts and replace values
    # -------------------------------------------------------------------------
        else:
            base_counts = subset.groupby('pubym')['pmid'].count().T.to_dict()
            counts_table['NumCounts'] = counts_table['Publication Date']\
                                        .replace(base_counts)
            counts_table['NumCounts'] = pd.to_numeric(counts_table['NumCounts'],
                                                      errors='coerce')\
                                                      .fillna(0, downcast='int') 
            counts_table['Statistic Value'] = counts_table['NumCounts']    

    # Return the converted table
    # -------------------------------------------------------------------------
        return counts_table

    except Exception as e:
        message = ''.join([
            'Error in filtering data frame into month - count format.',
            f'\n \t Additional information: \n \t {e}'
        ])
        logs.display_message(message, type = 'error')


def subset_dates(counts_table):
    """
    Takes a counts table and subsets to remove leading or trailing months
    with no publications.

    INPUTS:
        counts_table(dataframe): data frame with columns 'Publication Date'
            and 'NumCounts', likely output from filter_transform_df()
                
    RETURNS:
        out_table(dataframe): revised counts_table with leading and
            trailing months with 0 publications removed

    REQUIREMENTS/DEPENDENCIES:
        pandas as pd
        datetime as dt
        config_logging as logs
        logger = logs.get_logger()
    """      

    try:
    # If counts_table has a maximum count of 0, don't bother subsetting
    # =========================================================================
        if counts_table['NumCounts'].max() == 0:
            out_table = counts_table
    # Otherwise, calculate the maximum and minimum dates with publications,
    # and trim to those values
    # =========================================================================           
        else:
            max_date = counts_table[counts_table['NumCounts'] != 0]\
                ['Publication Date'].max()
            min_date = counts_table[counts_table['NumCounts'] != 0]\
                ['Publication Date'].min()
            out_table = counts_table[counts_table['Publication Date']\
                                     .between(min_date, max_date)]
        return out_table

    except Exception as e:
        message = ''.join([
            'Error occured in removing leading and trailing 0 publication months.',
            f'\n\t Additional information: \n\t {e}'
        ])
        logs.display_message(message, type = 'error')

def describe_stats(counts_table):
    """
    Takes a counts table and generates summary statistics for display.

    INPUTS:
        counts_table(dataframe): data frame with columns 'Publication Date'
            and 'NumCounts', likely output from filter_transform_df().

    RETURNS:
        outtable(dataframe): pandas dataframe of summary statistics.

    REQUIREMENTS/DEPENDENCIES:
        pandas as pd
        config_logging as logs
        logger = logs.get_logger()
    """      
    try:
        counts_table['Statistic Value'] = counts_table['NumCounts']
        outtable = counts_table['Statistic Value'].describe()\
            .rename(index = {'count': 'months'})
        return outtable
    except Exception as e:
        message = ''.join([
            'Error occured in generating summary statistics table.',
            f'\n\t Additional information: \n\t {e}'
        ])
        logs.display_message(message, type = 'error')

def date_range_text(counts_table):
    """
    Takes a counts table and generates title text.

    INPUTS:
        counts_table(dataframe): data frame with columns 'Publication Date'
            and 'NumCounts', likely output from filter_transform_df().

    RETURNS:
        text (str): formatted string text title based on the date range
            in counts_table

    REQUIREMENTS/DEPENDENCIES:
        datetime as dt
        pandas as pd
        config_logging as logs
        logger = logs.get_logger()
    """      
    try:
        start_date = counts_table['Publication Date'].min()\
            .strftime(format = '%B %Y')
        end_date = counts_table['Publication Date'].max()\
            .strftime(format = '%B %Y')
        count = counts_table['NumCounts'].sum()
        text = f'Articles between {start_date} -- {end_date}: {count:,} articles'
        return text
    except Exception as e:
        message = ''.join([
            'Error occured in generating title text.',
            f'\n\t Additional information: \n\t {e}'
        ])
        logs.display_message(message, type = 'error')

def create_line_plot(counts_table, title = 'Articles per Month',
                     count_color = 'blue', 
                     mean_color = 'grey', 
                     ci_color = 'grey'):
    """
    Takes a counts table and generates a line plot of articles over time,
    by month. Overlay includes mean and 95% CI boundaries.

    INPUTS:
        counts_table(dataframe): data frame with columns 'Publication Date'
            and 'NumCounts', likely output from filter_transform_df().
        title (text): text for the title of the plot. Default is 
            'Articles per Month'
        count_color(str): string name or hexidecimal value for the 
            line of counts in the line plot. Default is 'blue'
        mean_color(str): string name or hexidecimal value for the 
            line of mean counts in the line plot. Default is 'grey'.
        ci_color(str): string name or hexidecimal value for the 
            95% confidence interval lines in the line plot.
            Default is 'grey'.
                
    RETURNS:
        plot(plot): holoviz line plot

    REQUIREMENTS/DEPENDENCIES:
        pandas as pd
        datetime as dt
        numpy as np
        holoviews as hv
        config_logging as logs
        logger = logs.get_logger()
    """      

    try:
    # Calculations
    # =========================================================================
        min_date = counts_table['Publication Date'].min()
        max_date = counts_table['Publication Date'].max()
        mean = counts_table['NumCounts'].describe()['mean']
        lowci = max(
                counts_table['NumCounts'].describe()['mean'] - 1.96 * \
                    counts_table['NumCounts'].describe()['std'],
                    0)
        highci = min(
                counts_table['NumCounts'].describe()['mean'] + 1.96 * \
                    counts_table['NumCounts'].describe()['std'], 
                    max(counts_table['NumCounts']))

    # Create the actual plot
    # =========================================================================
        line_plot = counts_table.hvplot.line(
            x='Publication Date', 
            y='NumCounts', 
            title = title, 
            xlabel = 'Published Date', ylabel = 'Number of Articles', 
            label = 'Number of Publications',
            line_color = count_color,
            legend='top', height=250, width=800)

    # Line for Mean value
    # =========================================================================
        mean_line = hv.Curve(
            [[min_date, mean], [max_date, mean]], 
            label = 'Average Number of Publications per Month')

        mean_line.opts(
            color=mean_color, 
            line_dash='dashed', 
            line_width=1.5
        )

    # Line for 95% CIs of Mean
    # =========================================================================
    # Lower CI: mean - 1.96 * standard deviation or 0, whichever is higher
    # -------------------------------------------------------------------------
        lowci_line = hv.Curve(
            [[min_date, lowci], [max_date, lowci]], 
            label = '95% CI Boundary')

        lowci_line.opts(
            color=ci_color,  
            line_width=1.0
        )
    # Upper CI: mean + 1.96 * standard deviation or max, whichever is less
    # -------------------------------------------------------------------------
        highci_line = hv.Curve(
            [[min_date, highci], [max_date, highci]])

        highci_line.opts(
            color=ci_color, 
            line_width=1.0
        )

    # Generate bokeh plot
    # -------------------------------------------------------------------------
        plot = line_plot * mean_line * highci_line * lowci_line
    # Return Plot
    # -------------------------------------------------------------------------
        return plot
    except Exception as e:
        message = ''.join([
            'Error occured in processing line plot of articles over time.',
            f'\n\t Additional information: \n\t {e}'
        ])
        logs.display_message(message, type = 'error')

def create_boxplot(counts_table, 
                   title = 'Distribution of Publications per Month',
                   box_color = 'blue', outlier_color = 'grey'):
    """
    Takes a counts table and generates a boxplot of the
    distribution of articles over time, by month.

    INPUTS:
        counts_table(dataframe): data frame with columns 'Publication Date'
            and 'NumCounts', likely output from filter_transform_df().
        title (text): text for the title of the plot. Default is 
            a default of 'Distribution of Publications per Month'
        box_color(str): string name or hexidecimal value for the 
            box of counts in the box plot. Default is 'blue'
        outlier_color(str): string name or hexidecimal value for the 
            outliers in the box plot. Default is 'grey'.

                
    RETURNS:
        boxplot(plot): holoviz box plot

    REQUIREMENTS/DEPENDENCIES:
        pandas as pd
        datetime as dt
        numpy as np
        holoviews as hv
        config_logging as logs
        logger = logs.get_logger()
    """    
    try:
    # Create the actual plot
    # =========================================================================
        boxplot = counts_table.hvplot\
            .box(y = 'NumCounts', 
                 ylabel = 'Number of Publications per Month', 
                 title = title,
                 box_color = box_color, outlier_color = outlier_color,
                 width = 450, height=250)
        return boxplot
    except Exception as e:
        message = ''.join([
            'Error occured in processing box plot of articles over time.',
            f'\n\t Additional information: \n\t {e}'
        ])
        logs.display_message(message, type = 'error')

def create_histogram(counts_table, title = 'Number of Publications per Month',
                     hist_color = 'blue'):
    """
    Takes a counts table and generates a histogram of the
    distribution of articles over time, by month.

    INPUTS:
        counts_table(dataframe): data frame with columns 'Publication Date'
            and 'NumCounts', likely output from filter_transform_df().
        title (text): text for the title of the plot. Default is 
            'Number of Publications per Month'
        hist_color(str): string name or hexidecimal value for the 
            boxes in the histogram. Default is 'blue'.

                
    RETURNS:
        histogram(plot): holoviz histogram

    REQUIREMENTS/DEPENDENCIES:
        pandas as pd
        datetime as dt
        numpy as np
        holoviews as hv
        config_logging as logs
        logger = logs.get_logger()
    """    
    try:
    # Create the actual plot
    # =========================================================================
        histogram = counts_table.hvplot\
            .hist(y = 'NumCounts', 
                  ylabel = 'Number of Months', 
                  xlabel = 'Number of Publications per Month', 
                  title = title, 
                  bin_range=(1,24), color = hist_color,
                  width = 450, height=250)
        return histogram
    except Exception as e:
        message = ''.join([
            'Error occured in processing histogram of articles over time.',
            f'\n\t Additional information: \n\t {e}'
        ])
        logs.display_message(message, type = 'error')

def interactive(in_df, 
                keyword = None, start_date = None, end_date = None, 
                logo_path = None, primary_color = 'blue', 
                secondary_color = 'grey', 
                accent_color = 'grey'):
    """
    Takes a data frame, formatted by format_df(), and creates an
    interactive visualizer.

    INPUTS:
        in_df(dataframe): data frame of publications, from format_df()
        keyword (string): keyword used to generate data frame with
            scraper(). Default is None, which will not display keyword.
        start_date(string): start_date used to generate data frame with
            scraper(). Default is None, which will use data frame
            minimum date.
        end_date(string): end_date used to generate data frame with
            scraper(). Default is None, which will use data frame
            maximum date.
        count_color(str): string name or hexidecimal value for the 
            line of counts in the line plot. Default is 'blue'
        secondary_color(str): string name or hexidecimal value for the 
            line of 95% confidence interval thresholds in the line plot,
            and outliers in the boxplot. Default is 'grey'.
        secondary_color(str): string name or hexidecimal value for the 
            mean lines in the line plot.
            Default is 'grey'.

    RETURNS:
        gspec (panels): interactive panels visualiser object.

    REQUIREMENTS/DEPENDENCIES:
        datetime as dt
        pandas as pd
        holoviews as hv
        panels as pn
        valiators
        filter_transform_df()
        subset_dates()
        filter_transform_df()
        subset_dates()
        describe_stats()
        date_range_text()
        create_line_plot()
        create_boxplot()
        create_histogram()
        config_logging as logs
        logger = logs.get_logger()
    """      
    try:
    # Constants
    # =========================================================================
        vis_df = in_df.copy(deep = True)
        vis_df['pubdate'] = pd.to_datetime(vis_df['pubdate'])

        # Constants from Data Frame
        # ------------------------------------------------------------------------
        min_date = vis_df['pubdate'].min()
        min_date = dt.datetime(min_date.year, min_date.month, 1)
        max_date = vis_df['pubdate'].max()
        max_date = dt.datetime(max_date.year, max_date.month, 1)

        if not start_date:
            start_date = min_date
        if not end_date:
            end_date = max_date    

    # Format Logo and Subtitle
    # ------------------------------------------------------------------------
        if logo_path:
            logo = validators.path(logo_path)
        if logo_path:
            logo = pn.panel(logo_path, width=200, align='start')
        else:
            logo = pn.panel('',width=200, align='start')

        start_date_text = pd.to_datetime(start_date, format = r"%Y/%m/%d")\
            .strftime(format = r"%B %d, %Y")
        end_date_text = pd.to_datetime(end_date, format = r"%Y/%m/%d")\
                .strftime(format = r"%B %d, %Y")

        if keyword:
            text = ''.join([
                'Dataset: PubMed records for articles published between ',
                f'{start_date_text} and ',
                f'{end_date_text} ',
                f' for search term: {keyword}'
                ])
        else:
            text = ''.join([
                'Dataset: PubMed records for articles published between ',
                f'{start_date_text} and ',
                f'{end_date_text} '
                ])

    # Title 
    # ------------------------------------------------------------------------
        title = '# Articles per Month'

    # Build Visualizer Widgets
    # =========================================================================

    # Date Range Selection Widget
    # ------------------------------------------------------------------------
        date_widget = pn.widgets.DateRangeSlider(
            name = 'Date Range',
            start = min_date, 
            end = max_date,
            value = (min_date, max_date),
            step = 1,
            value_throttled = (min_date, max_date)
        )

    # Journal Selection Widget
    # ------------------------------------------------------------------------
        journals_list = sorted(vis_df['journal'].unique()\
                            .tolist())
        journal_widget = pn.widgets.MultiSelect(
        name = "Journal Name", 
        value = journals_list, 
        options = journals_list
        )

    # Number of Authors Widget
    # ------------------------------------------------------------------------
        longest_authors = int(vis_df['numauthors'].max())
        author_range_widget = pn.widgets.RangeSlider(
            name = 'Number of Authors', 
            start = 0, 
            end = longest_authors, 
            value = (0, longest_authors), 
            step = 1,
            value_throttled = (0, longest_authors)
            )

    # Language Widget
    # ------------------------------------------------------------------------
        languages_list = vis_df['language'].apply(eval).explode('language')\
            .unique().tolist()
        language_widget = pn.widgets.MultiSelect(
        name = "Languages", 
        value = languages_list, 
        options = languages_list
        )

    # Functions that are bound to interactable parameters
    # =========================================================================
    # Data Frame
    # ------------------------------------------------------------------------
        @pn.depends(date_widget.param.value_throttled, 
                    journal_widget.param.value, 
                    author_range_widget.param.value, 
                    language_widget.param.value)
        def call_bulk_df(
        # Calls filter_transform_df() based on interactable parameters
            dates = date_widget.value_throttled,
            journal = journal_widget.value,
            num_authors = author_range_widget.value,
            languages = language_widget.value
            ):
            return filter_transform_df(vis_df, dates, journal, num_authors, 
                                    languages)

    # Generators
    # ------------------------------------------------------------------------
    # Bulk Data, no filtering
        bulk_text = pn.bind(date_range_text, counts_table = call_bulk_df)
        bulk_stats = pn.bind(describe_stats, counts_table = call_bulk_df)
        bulk_line = pn.bind(create_line_plot, counts_table = call_bulk_df, 
                            count_color = primary_color, 
                            mean_color = accent_color, 
                            ci_color = secondary_color)
        bulk_box = pn.bind(create_boxplot, counts_table = call_bulk_df,
                           box_color = primary_color,
                           outlier_color = secondary_color)
        bulk_hist = pn.bind(create_histogram, counts_table = call_bulk_df,
                           hist_color = primary_color)
        
    # Filtered Data, leading/trailing months with 0 publications removed
        subset_data = pn.bind(subset_dates, counts_table = call_bulk_df)
        subset_text = pn.bind(date_range_text, counts_table = subset_data)
        subset_stats = pn.bind(describe_stats, counts_table = subset_data)
        subset_line = pn.bind(create_line_plot, counts_table = subset_data, 
                            count_color = primary_color, 
                            mean_color = accent_color, 
                            ci_color = secondary_color)
        subset_box = pn.bind(create_boxplot, counts_table = subset_data,
                           box_color = primary_color,
                           outlier_color = secondary_color)
        subset_hist = pn.bind(create_histogram, counts_table = subset_data,
                           hist_color = primary_color)

    # VISUALIZER
    # =========================================================================

        gspec = pn.GridSpec(sizing_mode='scale_both', min_height = 1600, 
                            min_width = 800)

    # Header Panel
        gspec[0:3,   0:] = pn.Row(
            logo, 
            pn.Column(pn.pane.Markdown(title), pn.pane.Markdown("#### " + text)), 
            height = 300,
            align="center"
            )
    # Sidebar Panel - Widgets
        gspec[3:,   0] = pn.WidgetBox(
            pn.pane.Markdown("### Subset Selectors", align = 'center'),
            pn.layout.Divider(),
            date_widget, 
            author_range_widget, 
            language_widget, 
            journal_widget,
            width = 200,
            align = "center",
            width_policy = 'min'
            )
    # Summary Stats and Time Plot for total range
        gspec[3:20,   1:4] = pn.Card(
            pn.Column(pn.Row(pn.Column(bulk_stats), 
                             pn.Row(bulk_box, bulk_hist, 
                                    height_policy = 'min')), 
                pn.Row(bulk_line, height_policy ='min', 
                             sizing_mode = 'scale_both'),
                pn.layout.Divider()
                ),
            title = bulk_text)
    # Summary Stats and Time Plot for range with articles
        gspec[20:40,   1:4] = pn.Card(
            pn.pane.Markdown(
                'Subset to exclude leading or trailing months without articles.'),
            pn.Column(pn.Row(pn.Column(subset_stats), 
                             pn.Row(subset_box, subset_hist, 
                                    height_policy = 'min')), 
                pn.Row(subset_line, height_policy ='min', 
                             sizing_mode = 'scale_both'),
                ),
            title = subset_text)
        
        return gspec
    except Exception as e:
        message = ''.join([
            'Error occured in generating interactive',
            'visualizer. '
            f'\n\t Additional information: \n\t {e}'
        ])
        logs.display_message(message, type = 'error')

def static(in_df,
                keyword = None, start_date = None, end_date = None, 
                logo_path = None, primary_color = 'blue', 
                secondary_color = 'grey', 
                accent_color = 'grey'):
    """
    Takes a data frame, formatted by format_df(), and creates an
    static visual of summary statistics, including a histogram,
    boxplot, and line plot for publications by months.

    INPUTS:
        in_df(dataframe): data frame of publications, from format_df()
        keyword (string): keyword used to generate data frame with
            scraper(). Default is None, which will not display keyword.
        start_date(string): start_date used to generate data frame with
            scraper(). Default is None, which will use data frame
            minimum date.
        end_date(string): end_date used to generate data frame with
            scraper(). Default is None, which will use data frame
            maximum date.
        count_color(str): string name or hexidecimal value for the 
            line of counts in the line plot. Default is 'blue'
        secondary_color(str): string name or hexidecimal value for the 
            line of 95% confidence interval thresholds in the line plot,
            and outliers in the boxplot. Default is 'grey'.
        secondary_color(str): string name or hexidecimal value for the 
            mean lines in the line plot.
            Default is 'grey'.

    RETURNS:
        gspec (panels): static panels visualiser object.

    REQUIREMENTS/DEPENDENCIES:
        datetime as dt
        pandas as pd
        holoviews as hv
        panels as pn
        config_logging as logs
        logger = logs.get_logger()
    """      
    try:
    # Constants
    # =========================================================================
        vis_df = in_df.copy(deep = True)
        vis_df['pubdate'] = pd.to_datetime(vis_df['pubdate'])

        counts_table = filter_transform_df(vis_df)

        # Constants from Data Frame
        # ------------------------------------------------------------------------
        min_date = vis_df['pubdate'].min()
        min_date = dt.datetime(min_date.year, min_date.month, 1)
        max_date = vis_df['pubdate'].max()
        max_date = dt.datetime(max_date.year, max_date.month, 1)

        if not start_date:
            start_date = min_date
        if not end_date:
            end_date = max_date    

    # Format Logo and Subtitle
    # ------------------------------------------------------------------------
        if logo_path:
            logo = pn.panel(logo_path, width=200, align='start')
        else:
            logo = pn.panel('',width=200, align='start')

        start_date_text = pd.to_datetime(start_date, format = r"%Y/%m/%d")\
            .strftime(format = r"%B %d, %Y")
        end_date_text = pd.to_datetime(end_date, format = r"%Y/%m/%d")\
                .strftime(format = r"%B %d, %Y")

        if keyword:
            text = ''.join([
                'Dataset: PubMed records for articles published between ',
                f'{start_date_text} and ',
                f'{end_date_text} ',
                f' for search term: {keyword}'
                ])
        else:
            text = ''.join([
                'Dataset: PubMed records for articles published between ',
                f'{start_date_text} and ',
                f'{end_date_text} '
                ])

    # Title 
    # ------------------------------------------------------------------------
        title = '# Articles per Month'

    # VISUALIZER
    # =========================================================================
        gspec = pn.GridSpec(sizing_mode='stretch_both', max_height=1600)

    # Header Panel
        gspec[0,   0:] = pn.Row(
            logo, 
            pn.Column(pn.pane.Markdown(title), pn.pane.Markdown("#### " + text)),
            height_policy = 'min', 
            height = 200,
            align="center"
            )
    # Summary Stats and Time Plot for total range
        gspec[1:6,   0:4] = pn.Card(
            pn.Column(pn.Row(pn.Column(describe_stats(counts_table)), 
                             pn.Row(
                                 create_boxplot(counts_table, 
                                                title = date_range_text(counts_table),
                                                box_color = primary_color,
                                                outlier_color = secondary_color), 
                                    create_histogram(counts_table,
                                                     title = date_range_text(counts_table),
                                                     hist_color = primary_color), 
                                    height_policy = 'min')), 
                pn.Row(create_line_plot(counts_table, title = date_range_text(counts_table),
                                        count_color = primary_color,
                                        mean_color = accent_color,
                                        ci_color = secondary_color), 
                        height_policy ='min', 
                        sizing_mode = 'scale_both'),
                pn.layout.Divider()
                ),
            title = date_range_text(counts_table))
    
        return gspec
    except Exception as e:
        message = ''.join([
            'Error occured in generating static',
            'visualizer. '
            f'\n\t Additional information: \n\t {e}'
        ])
        logs.display_message(message, type = 'error')