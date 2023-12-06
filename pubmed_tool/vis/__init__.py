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
import pubmed_tool.logs as logs

from .vis_functs import *
