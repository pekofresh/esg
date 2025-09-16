# -*- coding: utf-8 -*-
"""
lib_utils – now sourcing config from zenith_dev_gold.esg_config schema
"""

import os, sys, json
import pandas as pd
from pathlib import Path
from typing import Any, Mapping, Sequence
from pyspark.sql import SparkSession
from connections import *   # your existing connections

spark = SparkSession.builder.getOrCreate()

# ----------------------------------------------------------------------
# Generic loader from SQL config
# ----------------------------------------------------------------------
def _load_config():
    df = spark.sql("SELECT item_key, item_type, value_json FROM zenith_dev_gold.esg_config.config")
    return {row.item_key: (json.loads(row.value_json) if row.item_type in ("list","dict") else row.value_json)
            for row in df.collect()}

def _load_resources():
    df = spark.sql("SELECT name, path FROM zenith_dev_gold.esg_config.resources")
    return {row.name: row.path for row in df.collect()}

def _load_definitions():
    df = spark.sql("SELECT field, definition FROM zenith_dev_gold.esg_config.definitions")
    return {row.field: row.definition for row in df.collect()}

# ----------------------------------------------------------------------
# Hydrate globals from SQL
# ----------------------------------------------------------------------
CONFIG = _load_config()
dictDremio = _load_resources()
dictDefinitions = _load_definitions()

# Central constants
HOLDING_COLUMNS             = CONFIG["HOLDING_COLUMNS"]
EQ_FI_SPLIT_IDS             = CONFIG["EQ_FI_SPLIT_IDS"]
XLS_DEFAULTS                = CONFIG["XLS_DEFAULTS"]
DEFAULT_STRINGS             = CONFIG["DEFAULT_STRINGS"]

# Refinitiv
REFINITIV_EXCLUDE_FILES     = CONFIG["REFINITIV_EXCLUDE_FILES"]
REFINITIV_DROP_COLUMNS      = CONFIG["REFINITIV_DROP_COLUMNS"]
REFINITIV_RENAME_COLUMNS    = CONFIG["REFINITIV_RENAME_COLUMNS"]
REFINITIV_PRE_MERGE_CONSTANTS = CONFIG["REFINITIV_PRE_MERGE_CONSTANTS"]
REFINITIV_POST_MERGE_COPY   = CONFIG["REFINITIV_POST_MERGE_COPY"]
REFINITIV_POST_MERGE_CONSTANTS = CONFIG["REFINITIV_POST_MERGE_CONSTANTS"]

# Holdings dtypes/defaults
NUMERIC_HOLDING_COLUMNS     = CONFIG["NUMERIC_HOLDING_COLUMNS"]
HOLDING_DTYPES              = CONFIG["HOLDING_DTYPES"]
HOLDING_DEFAULTS            = CONFIG["HOLDING_DEFAULTS"]
HOLDINGS_LOADERS            = CONFIG["HOLDINGS_LOADERS"]

# Paths / cache
PATHS                       = CONFIG["PATHS"]
CACHE                       = CONFIG["CACHE"]
FILENAMES                   = CONFIG["FILENAMES"]

# Defaults
DEFAULT_IDS_FUND_IDS        = CONFIG["DEFAULT_IDS_FUND_IDS"]

# Analytics lists
lVarsDremioPct              = CONFIG["VARS_DREMIO_PCT"]
lVarListInt                 = CONFIG["VARS_INT"]
lGroupByVariables           = CONFIG["GROUPBY_PORTFOLIO"]
lGroupByVariablesShort      = CONFIG["GROUPBY_SHORT"]
lVarListSectors             = CONFIG["SECTOR_KEYS"]
lVarListIssuer              = CONFIG["ISSUER_KEYS"]
lVarListSec                 = CONFIG["SECURITY_KEYS"]
lVarListTrueFalse           = CONFIG["TF_FLAGS"]
lVarListWide                = CONFIG["WIDE_COLUMNS"]

# Regions
lNA                         = CONFIG["REGION_NA"]
lEROP                       = CONFIG["REGION_EROP"]
lAPAC                       = CONFIG["REGION_APAC"]

# Sorting
lSortColumns                = CONFIG["SORT_COLUMNS"]
