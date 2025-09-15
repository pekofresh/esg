# -*- coding: utf-8 -*-
"""
Created on Tue Sep 17 11:29:19 2024

@author: A00008106
"""

import os
import sys
import json
import time
import logging
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Sequence, Tuple

import numpy as np
import pandas as pd

pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)

import pickle
import math
# import matplotlib.pyplot as plt
from statsmodels.distributions.empirical_distribution import ECDF
from datetime import datetime

# import blpapi
# import pdblp
from pulp import *
import lib_utils_AP as lib_utils

import openpyxl
from openpyxl import Workbook
import re

from pyspark.sql.types import StringType


# ---------------------------------------------------------------------------
# Configuration and logging
# ---------------------------------------------------------------------------


@dataclass
class SmaConfig:
    """Central configuration for lib_sma_AP paths and table names."""

    ids_holdings_table: str = field(
        default_factory=lambda: os.getenv(
            "DREMIO_IDS_HOLDINGS_TABLE", lib_utils.dictDremio["dfIDSHoldings"]
        )
    )
    ids_fund_table: str = field(
        default_factory=lambda: os.getenv(
            "DREMIO_IDS_FUND_TABLE", lib_utils.dictDremio["dfIDSFund"]
        )
    )
    country_dim_table: str = field(
        default_factory=lambda: os.getenv(
            "DREMIO_COUNTRY_DIM_TABLE", lib_utils.dictDremio["dfCountryDim"]
        )
    )
    portfolio_dim_table: str = field(
        default_factory=lambda: os.getenv(
            "DREMIO_PORTFOLIO_DIM_TABLE", lib_utils.dictDremio["dfPortfolioDim"]
        )
    )
    target_funds_cache_parquet: str = field(
        default_factory=lambda: os.getenv(
            "TARGET_FUNDS_CACHE_PARQUET",
            "dbfs:/Volumes/zenith_dev_gold/esg_impact_analysis/cache_volume/dfExt.parquet",
        )
    )
    target_funds_cache_pickle: str = field(
        default_factory=lambda: os.getenv(
            "TARGET_FUNDS_CACHE_PICKLE",
            os.path.join(lib_utils.sTargetFundsFolder, "dfExt.pkl"),
        )
    )
    countries_cache_path: str = field(
        default_factory=lambda: os.getenv(
            "COUNTRY_CACHE_PATH",
            "dbfs:/Volumes/zenith_dev_gold/esg_impact_analysis/cache_volume/dfCountries.parquet",
        )
    )
    cedar_cache_path: str = field(
        default_factory=lambda: os.getenv(
            "CEDAR_CACHE_PATH",
            "dbfs:/Volumes/zenith_dev_gold/esg_impact_analysis/cache_volume/dfCEDAR.parquet",
        )
    )
    refinitiv_data_dir: str = field(
        default_factory=lambda: os.getenv("REFINITIV_DATA_DIR", "")
    )


CONFIG = SmaConfig()


class JsonFormatter(logging.Formatter):
    """Minimal JSON log formatter."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        log_record = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        return json.dumps(log_record)


logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def log_timing(func):
    """Decorator to log the execution time of functions."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        duration = time.perf_counter() - start
        logger.info("%s completed in %.2fs", func.__name__, duration)
        return result

    return wrapper


def _build_in_clause(values: Sequence[Any]) -> Tuple[str, list[Any]]:
    """Return a parametrised SQL IN clause and corresponding parameters."""

    if not values:
        raise ValueError("Sequence cannot be empty")
    placeholders = ", ".join(["?"] * len(values))
    return f"({placeholders})", list(values)


def _read_refinitiv_file(file_path: str) -> pd.DataFrame:
    """Read a single Refinitiv CSV file and filter valid holdings."""
    df = pd.read_csv(file_path)
    return df.loc[df.Allocation_Type == "FLHLD"]


# load master data
@log_timing
def _load_refinitiv(
    refresh: bool,
    spark=None,
    data_dir: str | None = None,
    cache_path: str | None = None,
    exclude_files: set[str] | None = None,
) -> pd.DataFrame:
    """Load Refinitiv target fund holdings.

    Parameters
    ----------
    refresh : bool
        Rebuild cache from raw CSV files when True, otherwise load cached data.
    spark : SparkSession, optional
        Active Spark session used to read/write Parquet caches on DBFS.
    data_dir : str, optional
        Directory containing the Refinitiv CSV files. Defaults to the
        ``REFINITIV_DATA_DIR`` environment variable.
    cache_path : str, optional
        Location of the cached DataFrame. Defaults to a parquet file on DBFS if
        ``spark`` is provided, otherwise to a local pickle file in
        ``lib_utils.sTargetFundsFolder``.
    exclude_files : set[str], optional
        File names to skip while scanning ``data_dir``.
    """

    if data_dir is None:
        data_dir = CONFIG.refinitiv_data_dir
        if not data_dir:
            raise EnvironmentError(
                "REFINITIV_DATA_DIR environment variable or data_dir parameter must be set"
            )

    if exclude_files is None:
        exclude_files = lib_utils.REFINITIV_EXCLUDE_FILES

    if cache_path is None:
        if spark:
            cache_path = CONFIG.target_funds_cache_parquet
        else:
            cache_path = CONFIG.target_funds_cache_pickle

    logger.info("Loading target fund holdings...")

    if refresh:
        files = [
            f for f in os.listdir(data_dir) if f not in exclude_files
        ]
        if not files:
            raise FileNotFoundError(f"No Refinitiv files found in {data_dir}")

        frames = []
        for fname in files:
            file_path = os.path.join(data_dir, fname)
            logger.debug("Processing %s", fname)
            frames.append(_read_refinitiv_file(file_path))

        df_out = pd.concat(frames, ignore_index=True)
        df_out.Allocation_Percentage = df_out.Allocation_Percentage.astype(float)
        df_out = df_out.drop_duplicates()

        df_out = lib_utils.apply_column_mappings(
            df_out,
            rename_map=lib_utils.REFINITIV_RENAME_COLUMNS,
            drop_cols=lib_utils.REFINITIV_DROP_COLUMNS,
            fill_values=lib_utils.REFINITIV_PRE_MERGE_CONSTANTS,
        )

        df_out = pd.merge(
            df_out,
            lib_utils.dfSecurities,
            how="left",
            left_on="id_isin",
            right_on="ID_ISIN",
            validate="m:1",
        )

        df_out = lib_utils.apply_column_mappings(
            df_out,
            copy_map=lib_utils.REFINITIV_POST_MERGE_COPY,
            fill_values=lib_utils.REFINITIV_POST_MERGE_CONSTANTS,
        )

        df_out["weight"] = df_out["weight"] / 100

        if spark:
            save_cache(df_out, cache_path, spark)
        else:
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            df_out.to_pickle(cache_path)

    else:
        if spark:
            df_out = load_cache(cache_path, spark)
        else:
            df_out = pd.read_pickle(cache_path)

    return df_out

@log_timing
def _load_ids_portfolio(sDate: list[str], sLeftOn: str) -> pd.DataFrame:
    """Load IDS portfolio holdings for the provided dates."""

    date_clause, date_params = _build_in_clause(sDate)
    fund_ids = [40002847, 40011162, 40002955, 40002956]
    fund_clause, fund_params = _build_in_clause(fund_ids)

    sql = f"""
    SELECT a.edm_fund_id as portfolio_id,
           100 as benchmark_id,
           b.fund_name as portfolio_name,
           INSTRUMENT_IDENTIFIER as id_isin,
           INSTRUMENT_WEIGHT_IN_FUND as weight,
           VALIDITY_DATE as effective_date,
           validity_date
    FROM {CONFIG.ids_holdings_table} a
    LEFT JOIN (
        SELECT DISTINCT edm_fund_id, fund_name
        FROM {CONFIG.ids_fund_table}
        WHERE edm_fund_id IN {fund_clause}
    ) b ON a.edm_fund_id = b.edm_fund_id
    WHERE CAST(a.validity_date AS DATE) IN {date_clause}
      AND a.edm_fund_id IN {fund_clause}
    """
    params = date_params + fund_params + fund_params
    logger.debug("Executing IDS portfolio SQL", extra={"sql": sql})
    dfPf = pd.read_sql(sql, lib_utils.conndremio, params=params)

    dfPf["durable_key_type"] = "xls"
    dfPf["portfolio_type"] = "Fund"
    dfPf = pd.merge(
        dfPf,
        lib_utils.dfSecurities,
        how="left",
        left_on=sLeftOn,
        right_on="ID_ISIN",
        validate="m:1",
    )

    dfPf["ID_BB_COMPANY"] = dfPf["ID_BB_COMPANY"].astype(float)
    dfPf["ID_BB_PARENT_CO"] = dfPf["ID_BB_PARENT_CO"].astype(float)
    dfPf["ID_BB_ULTIMATE_PARENT_CO"] = dfPf["ID_BB_ULTIMATE_PARENT_CO"].astype(float)

    dfPf["issuer_long_name"] = dfPf["LONG_COMP_NAME"]
    dfPf["long_name"] = dfPf["SECURITY_DES"]
    dfPf["gics_sector_description"] = dfPf["INDUSTRY_SECTOR"].str.upper()

    dfPf = pd.merge(
        dfPf,
        lib_utils.dfCountry,
        how="left",
        left_on="ULT_PARENT_CNTRY_DOMICILE",
        right_on="country_code",
        validate="m:1",
    )
    dfPf["country_issue_name"] = dfPf["country_name"]

    return dfPf

@log_timing
def load_IDS(sDate: list[str], iRefresh: bool) -> pd.DataFrame:
    """Load IDS fund holdings and optionally refresh the cache."""

    logger.info("Loading IDS fund holdings...")
    date_clause, date_params = _build_in_clause(sDate)

    if iRefresh:
        sql = f"""
        SELECT VALIDITY_DATE as effective_date,
               FUND_IDENTIFIER as portfolio_id,
               INSTRUMENT_IDENTIFIER as id_isin,
               BBG_COMPANY_ID as ID_BB_COMPANY,
               INSTRUMENT_NAME as long_name,
               INSTRUMENT_ASSET_CLASS as agi_instrument_asset_type_description,
               INSTRUMENT_WEIGHT_IN_FUND as weight
        FROM {CONFIG.ids_holdings_table}
        WHERE FUND_IDENTIFIER IS NOT NULL
          AND CAST(VALIDITY_DATE AS DATE) IN {date_clause}
        """
        logger.debug("Executing IDS holdings SQL", extra={"sql": sql})
        dfHoldings = pd.read_sql(sql, lib_utils.conndremio, params=date_params)
        dfHoldings = pd.merge(
            dfHoldings,
            lib_utils.dfHierarchy,
            how="left",
            on="ID_BB_COMPANY",
            validate="m:1",
        )

        dfHoldings["gics_sector_description"] = dfHoldings["INDUSTRY_SECTOR"]

        dfHoldings["issuer_long_name"] = dfHoldings["LONG_COMP_NAME"]
        dfHoldings["country_issue_name"] = dfHoldings["ULT_PARENT_CNTRY_DOMICILE"]

        sql = f"""
            SELECT DISTINCT FUND_IDENTIFIER as portfolio_id,
                            FUND_NAME as portfolio_name,
                            FUND_TYPE as portfolio_type
            FROM {CONFIG.ids_fund_table}
            WHERE FUND_IDENTIFIER IS NOT NULL
              AND CAST(VALIDITY_DATE AS DATE) IN {date_clause}
        """
        dfFundNames = pd.read_sql(sql, lib_utils.conndremio, params=date_params)

        dfHoldings = pd.merge(
            dfHoldings,
            dfFundNames,
            how="left",
            on="portfolio_id",
            validate="m:1",
        )
        dfHoldings["portfolio_type"] = dfHoldings["portfolio_type"].map(
            {"EF": "External Target Fund", "F": "Internal Target Fund"}
        )

        dfHoldings.to_pickle(CONFIG.target_funds_cache_pickle)
    else:
        dfHoldings = pd.read_pickle(CONFIG.target_funds_cache_pickle)

    return dfHoldings

def save_cache(df_pandas, path, spark, format='parquet'):
    """
    Save DataFrame to DBFS or local filesystem using Parquet (default) or Pickle.
    
    Args:
        df_pandas (pd.DataFrame): DataFrame to save
        path (str): Path to save to (Parquet path or Pickle file path)
        spark (SparkSession): Active Spark session
        format (str): 'parquet' or 'pickle' (default = 'parquet')
    """
    if format.lower() == 'parquet':
        df_spark = spark.createDataFrame(df_pandas)

        # Handle VOID types
        for col_name, dtype in df_spark.dtypes:
            if dtype.lower() == "void":
                logger.warning(
                    "Casting VOID column %s to StringType for Parquet compatibility",
                    col_name,
                )
                df_spark = df_spark.withColumn(
                    col_name, df_spark[col_name].cast(StringType())
                )

        df_spark.write.mode("overwrite").parquet(path)
    
    elif format.lower() == 'pickle':
        # Convert dbfs:/ to /dbfs/ for local file writing in Databricks
        if path.startswith("dbfs:/"):
            path = "/dbfs/" + path[6:]
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df_pandas.to_pickle(path)
    
    else:
        raise ValueError("Invalid format. Supported formats: 'parquet', 'pickle'.")

def load_cache(path, spark, format='parquet'):
    """
    Load DataFrame from DBFS or local filesystem using Parquet (default) or Pickle.

    Args:
        path (str): Path to load from (Parquet path or Pickle file path)
        spark (SparkSession): Active Spark session
        format (str): 'parquet' or 'pickle' (default = 'parquet')
    
    Returns:
        pd.DataFrame
    """
    if format.lower() == 'parquet':
        return spark.read.parquet(path).toPandas()
    
    elif format.lower() == 'pickle':
        if path.startswith("dbfs:/"):
            path = "/dbfs/" + path[6:]
        return pd.read_pickle(path)
    
    else:
        raise ValueError("Invalid format. Supported formats: 'parquet', 'pickle'.")

@log_timing
def load_cntry(
    iRefresh: bool,
    spark,
    cache_path: str | None = None,
) -> pd.DataFrame:
    cache_path = cache_path or CONFIG.countries_cache_path
    if iRefresh:
        sql = f"SELECT * FROM {CONFIG.country_dim_table}"
        df_spark = spark.sql(sql)
        dfCountries = df_spark.toPandas()
        save_cache(dfCountries, cache_path, spark)
    else:
        dfCountries = load_cache(cache_path, spark)
    return dfCountries


@log_timing
def search_cedar(
    iRefresh: bool,
    spark,
    lIdentifiers: list[str] | None = None,
    sType: str | None = None,
    cache_path: str | None = None,
) -> pd.DataFrame:
    lIdentifiers = lIdentifiers or []
    cache_path = cache_path or CONFIG.cedar_cache_path

    if iRefresh:
        sql = f"""
            SELECT * FROM {CONFIG.portfolio_dim_table}
            WHERE is_current = TRUE
        """
        df_spark = spark.sql(sql)
        dfCEDAR = df_spark.toPandas()
        save_cache(dfCEDAR, cache_path, spark)
    else:
        dfCEDAR = load_cache(cache_path, spark)

    if lIdentifiers and sType:
        pattern = "|".join(lIdentifiers)
        dfCEDAR = dfCEDAR[dfCEDAR[sType].str.contains(pattern, na=False)]
        dfCEDAR = dfCEDAR[dfCEDAR.portfolio_status == "Active"]

    return dfCEDAR[
        [
            "portfolio_id",
            "portfolio_name",
            "portfolio_status",
            "bbg_account",
            "strategy_name_long",
            "esg_categorization",
        ]
    ]

def calc_universe_Oktagon(sDate,iRefresh,iExport):
    sPathName                   = lib_utils.sPathNameGeneral
    sWorkFolder                 = sPathName + '15. Investment process/Universes/'

    if iRefresh == True:

        # Europe IMI
        dfAllIndices             = load_holdings("index", 584, sDate, position_type = 1)
        
        # We start with the MSCI ACWI IMI Indices as a proxy for our investable universe
        dfUniverse = dfAllIndices.copy()
        dfUniverse['Scope'] = 1

        dfSRI = load_dremio_data('dfSRI',['universe_name'],sDateCol = 'effective_date',iMastering = True) 
        dfExclusions = load_dremio_data('dfSusMinExclusions',['id_iss_list'],sDateCol = 'effective_date_key') 
        
        dfRatingsUniv = merge_esg_data_new(dfUniverse,dfSRI,['sri_rating_final','sri_rating_final < 1.5'],'_SRI',False)
        dfRatingsUniv = merge_esg_data_new(dfRatingsUniv,dfExclusions,None,'_Exclusion',False)
        
        dfRes = calc_coverage(dfRatingsUniv,['sri_rating_final'],'Scope')
        dfRes = dfRes[['effective_date','portfolio_name','id_isin','ID_BB_COMPANY','issuer_long_name','weight','Uncovered_Issuer_sri_rating_final','sri_rating_final','sri_rating_final < 1.5','Exposure_Exclusion']]
        dfRes = dfRes.sort_values('weight',ascending = False)
        
        dRes = {'data' : dfRes, 
            'sSheetName' : 'Oktagon Positive List',
            'sPctCols':['weight'],
            'sBars':['weight','sri_rating_final'],
            'sWide':['portfolio_name','issuer_long_name'],
            'sFalseTrue': ['Uncovered_Issuer_sri_rating_final','sri_rating_final < 1.5','Exposure_Exclusion'] ,
            'sTrueFalse':False,
            'sColsHighlight':['sri_rating_final < 1.5','Exposure_Exclusion']}
 
      
        excel_export(pd.ExcelWriter(sWorkFolder + 'Oktagon.xlsx'),[dRes])
        
def calc_universe_Unicredit(sDate,iRefresh,iExport):
    sPathName                   = lib_utils.sPathNameGeneral
    sWorkFolder                 = sPathName + '15. Investment process/Universes/'

    if iRefresh == True:
 
        # We start with the MSCI ACWI IMI Indices as a proxy for our investable universe
        dfUniverse = pd.read_sql("""select * from SU.dbo.tcSU_GHGint_cutoffs_univ_for_SMA""",lib_utils.conngenie)
        dfUniverse['Scope'] = 1
        dfUniverse = pd.merge(dfUniverse,lib_utils.dfSecurities[['ID_BB_COMPANY','LONG_COMP_NAME']].drop_duplicates(),how = 'left',left_on = 'ID_BB_Company',right_on = 'ID_BB_COMPANY',validate = 'm:1')
        dfUniverse = dfUniverse.drop('ID_BB_COMPANY',axis = 1)
        
        dfThresholds = dfUniverse.loc[dfUniverse.isUnivCutoff_GHG == 1][['ID_BB_Company','Region','GICSSector','isUnivCutoff_GHG', 'msci_carbon_emissions_scope_12_inten']].groupby(['GICSSector','Region']).median()['msci_carbon_emissions_scope_12_inten'].reset_index()
        
        dfKPIBS = pd.merge(dfUniverse,dfThresholds, how = 'left', left_on = ['Region','GICSSector'],right_on = ['Region','GICSSector'],validate = 'm:1', suffixes = ('','_TH'))   

        dfKPIBS['Restricted'] = False        
        dfKPIBS.loc[dfKPIBS['msci_carbon_emissions_scope_12_inten']>=dfKPIBS['msci_carbon_emissions_scope_12_inten_TH'],'Restricted'] = True
        

        # export
        dfKPIBS['isUnivCutoff_GHG'] = dfKPIBS['isUnivCutoff_GHG'].astype(bool)
        dfKPIBS['portfolio_type'] = 'Universe'
        dfKPIBS['portfolio_name'] = 'Best Styles Universe'

        dfKPIBS.to_pickle(sWorkFolder + 'Best Styles KPI Universe ' + datetime.strptime(sDate[0], '%Y-%m-%d').strftime('%Y%m%d') + '.pkl')
        
    else:
        lDatNames = os.listdir(sWorkFolder)
        lDatNames = [s for s in lDatNames if 'Best Styles KPI Universe' in s]
        lDatNames = [s[-12:-4] for s in lDatNames]
        dfKPIBS = pd.read_pickle(sWorkFolder + 'Best Styles KPI Universe ' + max(lDatNames) + '.pkl')
    
    if iExport == True:

        dRes2 = {'data' : dfKPIBS.drop(['Scope'],axis = 1), 
            'sSheetName' : 'Best Styles KPI Universe',
            'sPctCols':False,
            'sBars':['msci_carbon_emissions_scope_12_inten','msci_carbon_emissions_scope_12_inten_TH'],
            'sWide':['portfolio_name','gics_sector_description','issuer_long_name','country_issue_name'],
            'sFalseTrue': ['Restricted'] ,
            'sTrueFalse':['isUniv_Cutoff'],
            'sColsHighlight':['GICSSector','Region','ACWI','msci_carbon_emissions_scope_12_inten','msci_carbon_emissions_scope_12_inten_TH','Restricted']}
 
        dRes3 = {'data' : dfKPIBS[['GICSSector','Region','msci_carbon_emissions_scope_12_inten_TH']].drop_duplicates(), 
            'sSheetName' : 'Thresholds',
            'sPctCols':False,
            'sBars':['final_override','final_override_TH'],
            'sWide':['portfolio_name','gics_sector_description','issuer_long_name','country_issue_name'],
            'sFalseTrue': ['Restricted'] ,
            'sTrueFalse':['isUniv_Cutoff'],
            'sColsHighlight':['GICSSector','Region','ACWI','msci_carbon_emissions_scope_12_inten','msci_carbon_emissions_scope_12_inten_TH','Restricted']}
        
        excel_export(pd.ExcelWriter(sWorkFolder + 'BestStyles_KPIUniverse_' + datetime.strptime(sDate[0], '%Y-%m-%d').strftime('%Y%m%d') + '.xlsx', engine='xlsxwriter'),[dRes2,dRes3])

def calc_universe_Best_Styles(sDate,iRefresh,iExport):
    sPathName                   = lib_utils.sPathNameGeneral.replace("\\", "/")
    sWorkFolder                 = sPathName + '15. Investment process/Universes/'

    if iRefresh == True:

        
      

        # load Best-Styles universe with sectors and regions
        dfUniverse                  = pd.read_sql("""select * from SU.dbo.tcSU_SRI_cutoffs_univ_for_SMA""",lib_utils.conngenie)
        dfUniverse['Scope']         = 1
        
        # we usually use GlobalEqExEurope universe. For Best Styles we need MAEurope
        if len([s for s in os.listdir(lib_utils.sWorkFolder) if 'dfSRI.pkl' in s]) > 0:
            user_input              = input('Old SRI ratings detected. Do you want to delete the file dfSRI.pkl? [Y/N]: ').strip().upper()
            if user_input == 'Y':
                os.remove(lib_utils.sWorkFolder + 'dfSRI.pkl')
            
        
        # load SRI scores
        dfSRI                       = load_dremio_data('dfSRI',['universe_name'],sDateCol = 'effective_date',iMastering = False) 
        dfSRI                       = dfSRI.loc[dfSRI.universe_name == 'MAEurope']
        dfRatingsUniv               = merge_esg_data_new(dfUniverse,dfSRI,['effective_date','final_override','long_comp_name'],'_SRI',False,sLeftOn = 'ID_BB_Company')

        # Perform the cutoff calculation for each region and sector bucket, where cutoff for a region and sector bucket equals to SRI Rating that excludes 20% of the names by count in this region and sector bucket
        dfRatingsUnivQ              = dfRatingsUniv.copy()[['ID_BB_Company','Region','GICSSector','isUniv_Cutoff','final_override']]

        dfThresholds                = dfRatingsUnivQ.loc[dfRatingsUnivQ.isUniv_Cutoff == 1].groupby(['GICSSector','Region']).quantile(0.2,interpolation = 'lower')['final_override'].reset_index()
        
        # Since ratings >= 2 are generally accepted to be Best-in-Class at AllianzGI, an upper bound for the cutoff of 1.99 has to be introduced, so that companies with a rating of 2 remain eligible. 
        dfThresholds.loc[dfThresholds['final_override']> 2,'final_override'] = 2
        
        # As a compensation, also a lower bound of 0.49 is  introduced, so that no company with a rating below 0.5 is eligible. This ensures that overall at least 20% are excluded. 
        dfThresholds.loc[dfThresholds['final_override']< 0.5,'final_override'] = 0.5

        
        dfSRIBS                     = pd.merge(dfRatingsUniv,dfThresholds, how = 'left', left_on = ['Region','GICSSector'],right_on = ['Region','GICSSector'],validate = 'm:1', suffixes = ('','_TH'))   

        dfSRIBS['Restricted']       = False
        dfSRIBS['final_override_TH'] = np.round(dfSRIBS['final_override_TH'],2)
        dfSRIBS.loc[dfSRIBS['final_override']<dfSRIBS['final_override_TH'],'Restricted'] = True
        
        # flag ACWI constituents
        dfACWI                      = load_holdings("index", [1181], ['2023-09-08'], position_type = 1)
        dfSRIBS['ACWI']             = False
        dfSRIBS.loc[dfSRIBS.ID_BB_COMPANY.isin(dfACWI['ID_BB_COMPANY'].drop_duplicates().to_list()),'ACWI'] = True
        
        # export
        dfSRIBS['isUniv_Cutoff']    = dfSRIBS['isUniv_Cutoff'].astype(bool)
        dfSRIBS['portfolio_type']   = 'Universe'
        dfSRIBS['portfolio_name']   = 'Best Styles Universe'

        dfSRIBS.to_pickle(sWorkFolder + 'Best Styles Universe ' + datetime.strptime(sDate[0], '%Y-%m-%d').strftime('%Y%m%d') + '.pkl')
        
        
        
    
    else:
        lDatNames                   = os.listdir(sWorkFolder)
        lDatNames                   = [s for s in lDatNames if 'Best Styles Universe' in s]
        lDatNames                   = [s[-12:-4] for s in lDatNames]
        dfSRIBS                     = pd.read_pickle(sWorkFolder + 'Best Styles Universe ' + max(lDatNames) + '.pkl')
    
    if iExport == True:
        
      
        
    
        dRes2 = {'data' : dfSRIBS.drop(['Scope','ID_BB_Company'],axis = 1), 
            'sSheetName' : 'Best Styles Universe',
            'sPctCols':False,
            'sBars':['final_override','final_override_TH'],
            'sWide':['portfolio_name','gics_sector_description','issuer_long_name','country_issue_name'],
            'sFalseTrue': ['Restricted'] ,
            'sTrueFalse':['isUniv_Cutoff'],
            'sColsHighlight':['GICSSector','Region','ACWI','final_override','final_override_TH','Restricted']}
 
        dRes3 = {'data' : dfSRIBS[['GICSSector','Region','final_override_TH']].drop_duplicates(), 
            'sSheetName' : 'Thresholds',
            'sPctCols':False,
            'sBars':['final_override','final_override_TH'],
            'sWide':['portfolio_name','gics_sector_description','issuer_long_name','country_issue_name'],
            'sFalseTrue': ['Restricted'] ,
            'sTrueFalse':['isUniv_Cutoff'],
            'sColsHighlight':['GICSSector','Region','ACWI','final_override','final_override_TH','Restricted']}

        
     

        excel_export(pd.ExcelWriter(sWorkFolder + 'BestStyles_Universe_' + datetime.strptime(sDate[0], '%Y-%m-%d').strftime('%Y%m%d') + '.xlsx', engine='xlsxwriter'),[dRes2,dRes3])

    
    return dfSRIBS

def calc_universe_Best_Styles_PSR(sDate,iRefresh,iExport):
    sPathName                   = lib_utils.sPathNameGeneral.replace("\\", "/")
    sWorkFolder                 = sPathName + '15. Investment process/Universes/'

    if iRefresh == True:

        
      

        # load Best-Styles universe with sectors and regions
        dfUniverse                  = pd.read_sql("""select * from SU.dbo.tcSU_SRI_cutoffs_univ_for_SMA""",lib_utils.conngenie)
        dfUniverse['Scope']         = 1
        
        # load PSS scores from Databricks table
        dfSRI                       = load_dremio_data('dfSRI',['universe_name'],sDateCol = 'effective_date',iMastering = False) 
        dfSRI                       = dfSRI.loc[dfSRI.universe_name == 'PSS']
        dfRatingsUniv               = merge_esg_data_new(dfUniverse,dfSRI,['effective_date','pss_score_final_absolute','long_comp_name'],'_SRI',False,sLeftOn = 'ID_BB_Company')

        #dfSRI = pd.read_excel(lib_utils.sWorkFolder + 'PSR_scores_20250213.xlsx',skiprows = 1)
        #dfRatingsUniv               = merge_esg_data_new(dfUniverse,dfSRI,['PSS_score_final_absolute','LONG_COMP_NAME'],'_PSR',False,sLeftOn = 'ID_BB_Company')

        # Perform the cutoff calculation for each region and sector bucket, where cutoff for a region and sector bucket equals to SRI Rating that excludes 20% of the names by count in this region and sector bucket
        dfRatingsUnivQ              = dfRatingsUniv.copy()[['ID_BB_Company','Region','GICSSector','isUniv_Cutoff','pss_score_final_absolute']]

        dfThresholds                = dfRatingsUnivQ.loc[dfRatingsUnivQ.isUniv_Cutoff == 1].groupby(['GICSSector','Region']).quantile(0.2,interpolation = 'lower')['pss_score_final_absolute'].reset_index()
        
        # Since ratings >= 2 are generally accepted to be Best-in-Class at AllianzGI, an upper bound for the cutoff of 1.99 has to be introduced, so that companies with a rating of 2 remain eligible. 
        dfThresholds.loc[dfThresholds['pss_score_final_absolute']> 2,'pss_score_final_absolute'] = 2
        
        # As a compensation, also a lower bound of 0.49 is  introduced, so that no company with a rating below 0.5 is eligible. This ensures that overall at least 20% are excluded. 
        dfThresholds.loc[dfThresholds['pss_score_final_absolute']< 0.5,'pss_score_final_absolute'] = 0.5

        
        dfSRIBS                     = pd.merge(dfRatingsUniv,dfThresholds, how = 'left', left_on = ['Region','GICSSector'],right_on = ['Region','GICSSector'],validate = 'm:1', suffixes = ('','_TH'))   

        dfSRIBS['Restricted']       = False
        dfSRIBS['pss_score_final_absolute_TH'] = np.round(dfSRIBS['pss_score_final_absolute_TH'],2)
        dfSRIBS.loc[dfSRIBS['pss_score_final_absolute']<dfSRIBS['pss_score_final_absolute_TH'],'Restricted'] = True
        
        # flag ACWI constituents
        dfACWI                      = load_holdings("index", [1181], ['2023-09-08'], position_type = 1)
        dfSRIBS['ACWI']             = False
        dfSRIBS.loc[dfSRIBS.ID_BB_COMPANY.isin(dfACWI['ID_BB_COMPANY'].drop_duplicates().to_list()),'ACWI'] = True
        
        # export
        dfSRIBS['isUniv_Cutoff']    = dfSRIBS['isUniv_Cutoff'].astype(bool)
        dfSRIBS['portfolio_type']   = 'Universe'
        dfSRIBS['portfolio_name']   = 'Best Styles Universe'

        dfSRIBS.to_pickle(sWorkFolder + 'Best Styles Universe PSR ' + datetime.strptime(sDate[0], '%Y-%m-%d').strftime('%Y%m%d') + '.pkl')
        
        
        
    
    else:
        lDatNames                   = os.listdir(sWorkFolder)
        lDatNames                   = [s for s in lDatNames if 'Best Styles Universe PSR' in s]
        lDatNames                   = [s[-12:-4] for s in lDatNames]
        dfSRIBS                     = pd.read_pickle(sWorkFolder + 'Best Styles Universe PSR ' + max(lDatNames) + '.pkl')
    
    if iExport == True:
        
      
        
    
        dRes2 = {'data' : dfSRIBS.drop(['Scope','ID_BB_Company'],axis = 1), 
            'sSheetName' : 'Best Styles Universe',
            'sPctCols':False,
            'sBars':['pss_score_final_absolute','pss_score_final_absolute_TH'],
            'sWide':['portfolio_name','gics_sector_description','ISSUER_LONG_NAME','country_issue_name'],
            'sFalseTrue': ['Restricted'] ,
            'sTrueFalse':['isUniv_Cutoff'],
            'sColsHighlight':['GICSSector','Region','ACWI','pss_score_final_absolute','pss_score_final_absolute_TH','Restricted']}
 
        dRes3 = {'data' : dfSRIBS[['GICSSector','Region','pss_score_final_absolute_TH']].drop_duplicates(), 
            'sSheetName' : 'Thresholds',
            'sPctCols':False,
            'sBars':['pss_score_final_absolute','pss_score_final_absolute_TH'],
            'sWide':['portfolio_name','gics_sector_description','issuer_long_name','country_issue_name'],
            'sFalseTrue': ['Restricted'] ,
            'sTrueFalse':['isUniv_Cutoff'],
            'sColsHighlight':['GICSSector','Region','ACWI','pss_score_final_absolute','pss_score_final_absolute_TH','Restricted']}

        
     

        excel_export(pd.ExcelWriter(sWorkFolder + 'BestStyles_Universe_PSR_' + datetime.strptime(sDate[0], '%Y-%m-%d').strftime('%Y%m%d') + '.xlsx', engine='xlsxwriter'),[dRes2,dRes3])

    
    return dfSRIBS

def load_sec_hierarchy(iRefresh, spark, cache_path='dbfs:/Volumes/zenith_dev_gold/esg_impact_analysis/cache_volume/dfSecurities.parquet'):
    if iRefresh:

        sql = f"SELECT * FROM {lib_utils.dictDremio['dfInstrument']}"
        df_spark = spark.sql(sql)
        dfSecurities = df_spark.toPandas()

        save_cache(dfSecurities, cache_path, spark)
    else:
        dfSecurities = load_cache(cache_path, spark)
    
    return dfSecurities

def load_fund_perf(sPMLastName,sDate):
    sql = """
    SELECT b.ret_mtd,
           e.ret_mtd as ret_mtd_benchmark,
           b.ret_ytd,
           e.ret_ytd as ret_ytd_benchmark,
           a.portfolio_durable_key,
           a.portfolio_id,
           a.portfolio_name,
           d.benchmark_durable_key,
           d.benchmark_name,
           a.bbg_account,
           a.gidp_reporting_location,
           --a.prod_line_name_long,
           a.investment_region,
           a.sri_esg,
           a.asset_class,
           a.investment_approach,
           a.market_cap
    FROM   """ + lib_utils.dictDremio['dfPortfolioDim'] + """ a
           LEFT JOIN """ + lib_utils.dictDremio['dfPortfolioPerf'] + """ b
                  ON a.portfolio_durable_key = b.portfolio_durable_key
           LEFT JOIN """ + lib_utils.dictDremio['dfPortfolioBenchmarkBridge'] + """ c
                  ON a.portfolio_durable_key = c.portfolio_durable_key
           LEFT JOIN """ + lib_utils.dictDremio['dfBenchmarkDim'] + """ d
                  on c.benchmark_durable_key = d.benchmark_durable_key
           LEFT JOIN """ + lib_utils.dictDremio['dfBenchmarkPerf'] + """ e
                  on c.benchmark_durable_Key = e.benchmark_durable_key and e.portfolio_durable_key  = b.portfolio_durable_key and b.valuation_date_key = e.valuation_date_key and e.perf_type_key = 2 and e.ret_type_key = 2
    WHERE  a.portfolio_durable_key IN (SELECT portfolio_durable_key
                                       FROM   """ + lib_utils.dictDremio['dfPortfolioManager'] + """
                                       WHERE  last_name = '""" + str(sPMLastName) + """')
           AND a.is_current = true
           AND b.valuation_date_key = """ + datetime.strptime(sDate, '%Y-%m-%d').strftime('%Y%m%d') + """
           AND b.perf_type_key = 6
           AND b.ret_type_key = 2
           AND d.is_current = true
    ORDER  BY portfolio_name
    """
    dfOut = pd.read_sql(sql,lib_utils.conndremio)

    return dfOut

def load_green_bonds():
    sql = """
    select * from """ + lib_utils.dictDremio['dfGB']
    dfGB = pd.read_sql(sql,lib_utils.conndremio)
    dfGB = dfGB[['ID_ISIN','GREEN_BOND_LOAN_INDICATOR','SOCIAL_BOND_IND','SUSTAINABILITY_BOND_IND']].drop_duplicates(subset = ['ID_ISIN'])
    dfGB = dfGB.loc[dfGB[['GREEN_BOND_LOAN_INDICATOR','SOCIAL_BOND_IND','SUSTAINABILITY_BOND_IND']].max(axis = 1) == 1]
    dfGB['Exposure_GB'] = True
    
    return dfGB

# load ESG data
def load_c4f_data(sDatname):

    sPathName                   = lib_utils.sPathNameGeneral
    sWorkFolder                 = sPathName + '15. Investment process/Impact Assessment/'
    dfSecurities                = load_sec_hierarchy(lib_utils.conn,False)

    dfC4F                       = pd.read_excel(sWorkFolder + sDatname)
    dfC4F                       = pd.merge(dfC4F,dfSecurities,how = 'left',left_on = 'isin',right_on = 'ID_ISIN')
    dfC4F                       = dfC4F[['ID_BB_COMPANY','msappbtiPerBeurosTurnoverAdjusted']].drop_duplicates()
    dfC4F                       = dfC4F.sort_values(['ID_BB_COMPANY','msappbtiPerBeurosTurnoverAdjusted'])
    dfC4F                       = dfC4F.groupby('ID_BB_COMPANY').last().reset_index()   

    return dfC4F

def convert_date(sDate,sInput,sOutput):

    sOut = datetime.strptime(sDate, sInput)   
    sOut = sOut.strftime(sOutput)
    
    return sOut

def categorize_market_cap(market_cap):
    if pd.isna(market_cap):
        return np.nan
    elif market_cap < 2e9:
        return 'small'
    elif 2e9 <= market_cap < 10e9:
        return 'mid'
    elif market_cap >= 10e9:
        return 'large'

def dremio_mastering(sDataset,dfData):
    
    # dremio imports
    if sDataset == 'dfSusMinExclusions':
        dfData                              = dfData.loc[dfData.id_exl_list == 3]
    # dremio imports
    if sDataset == 'dfSusMinExclusionsEnh':
        dfData                              = dfData.loc[dfData.id_exl_list == 65]

    if sDataset == 'dfTowardsSustainability':
        dfData                              = dfData.loc[dfData.id_exl_list.isin([6,7,53])]

    if sDataset == 'dfFebelfin':
        dfData                              = dfData.loc[dfData.id_exl_list == 6]


    if sDataset == 'dfFirmwide':
        dfData                              = dfData.loc[dfData.id_exl_list == 2]

    if sDataset == 'dfFH':
        dfData                              = dfData.loc[dfData.edition == 2024]
        dfData['status']                    = dfData['status'].apply(lambda x: True if x == 'NF' else False)
        dfData['FH Total Score < 35'] = False
        dfData.loc[dfData['total'] < 35,'FH Total Score < 35'] = True
        
    if sDataset == 'dfSRI':    
        dfData                              = dfData.loc[dfData.universe_name == 'GlobalEquityExEurope']
        dfData['sri_rating_final < 1.5']      = False
        dfData.loc[dfData['sri_rating_final'] < 1.5 ,'sri_rating_final < 1.5'] = True
        dfData['sri_rating_final < 1']      = False
        dfData.loc[dfData['sri_rating_final'] < 1 ,'sri_rating_final < 1'] = True
        dfData['sri_rating_final < 2']      = False
        dfData.loc[dfData['sri_rating_final'] < 2 ,'sri_rating_final < 2'] = True
        dfData['final_override < 1.5']      = False
        dfData.loc[dfData['final_override'] < 1.5 ,'final_override < 1.5'] = True
        dfData['final_override < 1']      = False
        dfData.loc[dfData['final_override'] < 1 ,'final_override < 1'] = True
        dfData['final_override < 2']      = False
        dfData.loc[dfData['final_override'] < 2 ,'final_override < 2'] = True       

    if sDataset == 'dfPSR':    
        dfData                              = dfData.loc[dfData.universe_name == 'PSS']
        dfData['pss_score_final_relative_postflags < 1']      = False
        dfData.loc[dfData['pss_score_final_relative_postflags'] < 1 ,'pss_score_final_relative_postflags < 1'] = True
        dfData['pss_score_final_relative_postflags < 2']      = False
        dfData.loc[dfData['pss_score_final_relative_postflags'] < 2 ,'pss_score_final_relative_postflags < 2'] = True


    if sDataset == 'dfMSCI':
        # 1. Convert 'issuer_market_cap_usd' to float, coercing errors to NaN
        dfData['issuer_market_cap_usd'] = pd.to_numeric(dfData['issuer_market_cap_usd'], errors='coerce')

        # 2. Drop or temporarily exclude rows with NaN in 'issuer_market_cap_usd' when applying pd.cut
        mask = dfData['issuer_market_cap_usd'].notnull()

        dfData.loc[mask, 'market_cap_sector'] = pd.cut(
            dfData.loc[mask, 'issuer_market_cap_usd'],
            bins=[0, 300_000_000, 2_000_000_000, 10_000_000_000, 200_000_000_000, 10_000_000_000_000],
            labels=['micro cap', 'small cap', 'mid cap', 'large cap', 'mega cap'],
            include_lowest=True  # Optional: include the lowest bin edge
        )

        # 3. Fill missing labels with '_OTHER'
        dfData['market_cap_sector'] = dfData['market_cap_sector'].astype(str)
        dfData.loc[dfData['market_cap_sector'].isin(['nan', 'NaN', 'None']), 'market_cap_sector'] = '_OTHER'

        # 4. Apply categorize_market_cap safely, only on valid numeric values
        dfData['market_cap_category'] = dfData['issuer_market_cap_usd'].apply(
            lambda x: categorize_market_cap(x) if pd.notnull(x) else None
        )

                   #self.dfMSCI = pd.read_pickle(lib_utils.sWorkFolder + 'dfMSCI.pkl')
            # self.dfMSCI['ESG_SCORE<3']                 = False
            # self.dfMSCI.loc[self.dfMSCI.esg_score<3,'ESG_SCORE<3'] = True
            # self.dfMSCI['region'] = 'EM'
            # self.dfMSCI.loc[self.dfMSCI.country_name.isin(['Germany',
            #   'Denmark',
            #   'Sweden',
            #   'Switzerland',
            #   'Netherlands',
            #   'Hong Kong',
            #   'United Kingdom',
            #   'Singapore',
            #   'Norway',
            #   'United States',
            #   'Australia',
            #   'France',
            #   'Japan',
            #   'New Zealand',
            #   'Ireland',
            #   'Luxembourg',
            #   'Spain',
            #   'Canada',
            #   'Italy',
            #   'Belgium',
            #   'Israel',
            #   'Finland',
            #   'Austria',
            #   'Portugal']),'region'] = 'DM'
            # self.dfMSCI['Green instrument'] = False
            # self.dfMSCI.loc[(self.dfMSCI['region'] == 'DM')&(self.dfMSCI['esg_rating'].isin(['AAA','AA','A','BBB'])),'Green instrument'] = True
            # self.dfMSCI.loc[(self.dfMSCI['region'] == 'EM')&(self.dfMSCI['esg_rating'].isin(['AAA','AA','A','BBB','BB'])),'Green instrument'] = True

    if sDataset == 'dfKPI':
        dfData['carbon_emissions_evic_scope_123_inten'] = dfData['carbon_emissions_evic_scope_12_inten'] + dfData['carbon_emissions_scope_3_tot_evic_inten']
        dfData['evic_usd'] = dfData['evic_eur'].astype(float)*1.17*1000000
        dfData['evic_category'] = dfData['evic_usd'].apply(lambda x: categorize_market_cap(x) if pd.notnull(x) else None)
    
    if sDataset == 'dfSus':
        dfData['non_zero_sustainable_investment_share_environmental_post_dnsh'] = dfData['sustainable_investment_share_environmental_post_dnsh'].fillna(0).astype(bool)
        dfData['non_zero_sustainable_investment_share_social_post_dnsh'] = dfData['sustainable_investment_share_social_post_dnsh'].fillna(0).astype(bool)
        dfData['non_zero_sustainable_investment_share_post_dnsh'] = dfData['sustainable_investment_share_post_dnsh'].fillna(0).astype(bool)
        dfData['SIS_issuer_threshold_50'] = dfData['sustainable_investment_share_post_dnsh']
        dfData.loc[dfData['sustainable_investment_share_post_dnsh'] > 0.5, 'SIS_issuer_threshold_50'] = 1           
        # dfData['SIS_issuer_threshold_33'] = dfData['sustainable_investment_share_post_dnsh']
        # dfData.loc[dfData['sustainable_investment_share_post_dnsh'] >= 0.33, 'SIS_issuer_threshold_33'] = 1 
        # dfData['SIS_issuer_threshold_25'] = dfData['sustainable_investment_share_post_dnsh']
        # dfData.loc[dfData['sustainable_investment_share_post_dnsh'] >= 0.25, 'SIS_issuer_threshold_25'] = 1 
        # dfData['SIS_issuer_threshold_20'] = dfData['sustainable_investment_share_post_dnsh']
        # dfData.loc[dfData['sustainable_investment_share_post_dnsh'] >= 0.2, 'SIS_issuer_threshold_20'] = 1     
        dfData['SIS_issuer_binary_50'] = False
        dfData.loc[dfData['sustainable_investment_share_post_dnsh'] >= 0.5, 'SIS_issuer_binary_50'] = True  
        dfData['SIS_issuer_binary_33'] = False
        dfData.loc[dfData['sustainable_investment_share_post_dnsh'] >= 0.33, 'SIS_issuer_binary_33'] = True              
        dfData['SIS_issuer_binary_25'] = False
        dfData.loc[dfData['sustainable_investment_share_post_dnsh'] >= 0.25, 'SIS_issuer_binary_25'] = True  
        dfData['SIS_issuer_binary_20'] = False
        dfData.loc[dfData['sustainable_investment_share_post_dnsh'] >= 0.2, 'SIS_issuer_binary_20'] = True    
        dfData['SIS_issuer_binary_05'] = False
        dfData.loc[(dfData['sustainable_investment_share_post_dnsh'] >= 0.05)&(dfData['sustainable_investment_share_post_dnsh'] < 0.2), 'SIS_issuer_binary_05'] = True  
        dfData['SIS_issuer_binary_below_05'] = False
        dfData.loc[dfData['sustainable_investment_share_post_dnsh'] < 0.05, 'SIS_issuer_binary_below_05'] = True 
            

    if sDataset == 'dfNZ':
        mapping_dict = {
            '5. Not aligned': 'Not Aligned',
            '4. Committed to aligning': 'Committed',
            '3. Aligning towards a Net Zero pathway': 'Aligning',
            '1. Achieving Net Zero': 'Achieving',
            }
        dfData['nz_alignment_status'] = dfData['nz_alignment_status'].map(mapping_dict)
        mapping_dict = {
            'Validated': True,
            'Not Validated': False,
            }   
        dfData['c1_status'] = dfData['c1_status'].map(mapping_dict)
        dfData['c2_status'] = dfData['c2_status'].map(mapping_dict)
        dfData['c4_status'] = dfData['c4_status'].map(mapping_dict)
        dfData['c5_status'] = dfData['c5_status'].map(mapping_dict)

    if sDataset == 'dfPAI':
        dfData.loc[dfData['pai_5']> 1,'pai_5'] = dfData['pai_5']/100

    if sDataset == 'dfKPISov':
        dfData['carbon_government_ghg_intensity_debt'] = dfData['carbon_government_ghg']/(dfData['carbon_government_gdp_nominal_usd']*dfData['carbon_government_raw_public_debt']/100)*1000000

        dfDataTrucost = pd.read_excel(lib_utils.sWorkFolder + 'Trucost sovereign dataset.xlsx')
        dfData['carbon_government_gdp_pps_usd'] = dfData['carbon_government_ghg']/dfData['carbon_government_ghg_intensity_pps']*1000
        dfDataTrucost['Scopes 1 + 2 + 3 (in mn tCO2)'] = dfDataTrucost[['Scopes 1 + 2 (in mn tCO2)','Scope 3 (in mn tCO2)']].sum(axis = 1)
        
        
        dfData = pd.merge(dfData,lib_utils.dfCountry[['country_code','country_code3']],how = 'left',left_on = 'country_code',right_on = 'country_code',validate = 'm:1')
        
        dfData = pd.merge(dfData,dfDataTrucost,how = 'left',left_on = 'country_code3',right_on = 'Country ISO',validate = 'm:1')
        
        dfData['carbon_government_ghg12_intensity_debt'] = dfData['Scopes 1 + 2 (in mn tCO2)']*1000000/(dfData['carbon_government_gdp_nominal_usd']*dfData['carbon_government_raw_public_debt']/100)*1000000                    
        dfData['carbon_government_ghg123_intensity_debt'] = dfData['Scopes 1 + 2 + 3 (in mn tCO2)']*1000000/(dfData['carbon_government_gdp_nominal_usd']*dfData['carbon_government_raw_public_debt']/100)*1000000
        dfData['carbon_government_ghg12_intensity_gdp'] = dfData['Scopes 1 + 2 (in mn tCO2)']*1000000/dfData['carbon_government_gdp_pps_usd']*1000000                    
        dfData['carbon_government_ghg123_intensity_gdp'] = dfData['Scopes 1 + 2 + 3 (in mn tCO2)']*1000000/dfData['carbon_government_gdp_pps_usd']*1000000
        
        dfTemp = pd.read_excel(lib_utils.sWorkFolder + 'Ircantec sovereign data.xlsx')
        dfData = pd.merge(dfData,dfTemp,how = 'left',left_on = 'country_code',right_on = 'ISSUER_CNTRY_DOMICILE',validate = 'm:1')

    if sDataset == 'dfSBTI':
        dfData['near_term_target_year'] = dfData['near_term_target_year'].apply(extract_max_year)
        dfData['short_term_targets_2025']                 = False
        dfData.loc[(dfData['near_term_target_status'] == 'Targets Set')
                        &(dfData['near_term_target_year'] == 2025),'short_term_targets_2025'] = True
        
        dfData['mid_term_targets_2030-35']                 = False
        dfData.loc[(dfData['near_term_target_status'] == 'Targets Set')
                        &(dfData['near_term_target_year'] >= 2030)
                        &(dfData['near_term_target_year'] <= 2035),'mid_term_targets_2030-35'] = True
        
        dfData['net_zero_targets']                 = False
        dfData.loc[dfData.net_zero_committed == 'Yes','net_zero_targets'] = True
        
        dfData['carbon_reduction_targets_2030']                 = False
        dfData.loc[(dfData['near_term_target_status'] == 'Targets Set')
                        &(dfData['near_term_target_year'] == 2030),'carbon_reduction_targets_2030'] = True
        
        dfData['sbti_approved_net_zero_strategy']                 = False
        dfData.loc[dfData.long_term_target_status == 'Targets Set']

    if sDataset == 'dfNZAgg':
        dfNZSov = pd.read_excel(lib_utils.sWorkFolder + 'NZAS sov_2024 11.xlsx',sheet_name = 'NZAS Sov').drop_duplicates(subset = ['id_bb_company']).dropna(subset = ['id_bb_company'])
        dfNZSov.nz_alignment_status_sov = dfNZSov.nz_alignment_status_sov.map({'Committed to Aligning':'Committed','Not Aligned':'Not Aligned','Aligned to Net Zero':'Aligned','Aligning towards Net Zero':'Aligning','Achieving Net Zero':'Achieving'})
        dfNZSov.rename(columns = {'nz_alignment_status_sov':'nz_alignment_status'},inplace = True)

        dfData = pd.concat([dfData,dfNZSov])
        dfData.rename(columns = {'nz_alignment_status':'nz_alignment_status_agg'},inplace = True)
    return dfData

# dbx
def load_dremio_data(sDataset,sGroupBy = [],sDate=False,sDateCol = '',iHist = False,iMastering = True, cache_base_path="dbfs:/Volumes/zenith_dev_gold/esg_impact_analysis/cache_volume/"):
    print('*******************************')
    sDremioTableName        = lib_utils.dictDremio[sDataset]
    cache_path = f"{cache_base_path}{sDataset}.parquet"

    # Try to load from cache if available
    try:
        dfLocalData = load_cache(cache_path, lib_utils.spark,format = 'parquet')
        if 'effective_date' in dfLocalData.columns:
            sUseDate = 'effective_date'
        elif 'effective_date_key' in dfLocalData.columns:
            sUseDate = 'effective_date_key'
        elif 'dal_effective_date' in dfLocalData.columns:
            sUseDate = 'dal_effective_date'
        else:
            sUseDate = sDateCol

        sLastLocalDate = dfLocalData[sUseDate].astype(str).sort_values().tail(1).values.item()
        print(f"Found cached {sDataset}. Last local {sUseDate}: {sLastLocalDate}")
    except Exception as e:
        print(f"No cached copy found for {sDataset}: {str(e)}")
        sUseDate = sDateCol
        sLastLocalDate = '1900-01-01'
        

    if sDate == False:
        if sDataset == 'dfSusMinExclusions':
            sql = """SELECT max(""" + sUseDate + """) as max_date
                                FROM """ + sDremioTableName + """ where id_exl_list = 3 """ 
        if sDataset == 'dfSusMinExclusionsEnh':
            sql = """SELECT max(""" + sUseDate + """) as max_date
                                FROM """ + sDremioTableName + """ where id_exl_list = 65 """ 
        else:
            sql                 = """SELECT max(""" + sUseDate + """) as max_date
                                    FROM """ + sDremioTableName    
        
        #sLastDremioDate     = pd.read_sql(sql,lib_utils.conndremio)
        df_spark            = lib_utils.spark.sql(sql)
        sLastDremioDate     = df_spark.toPandas()
        sEffectiveDate      = sLastDremioDate['max_date'].astype(str).values.item()
    else:
        sEffectiveDate      = sDate

    # check if newer data is available
    if  sLastLocalDate  == sEffectiveDate:
        print(sDremioTableName + ' last ' + sUseDate + ': ' + sEffectiveDate + '. Using local copy.')
        # done
        return dfLocalData
    else:
        # get fresh data
        print(sDremioTableName + ' last ' + str(sUseDate) + ': ' + str(sEffectiveDate) + '. Downloading dremio data...')
        
        if sDataset in ['dfSRI','dfPSR']:
            sql = """select * from """ + sDremioTableName + """ where """ + str(sUseDate) + """ = '""" + str(sEffectiveDate) + """'""" 
        else:
            sql                 = """SELECT f.*, e.id_bb_company as id_bb_company_e, e.party_name
                                FROM """ + sDremioTableName + """ f
                                INNER JOIN """ + lib_utils.dictDremio['dfEntityDim'] + """ e ON e.entity_key = f.entity_key
                                where """ + str(sUseDate) + """  = '""" + str(sEffectiveDate) + """'"""
        sql_hist                 = """SELECT f.*, e.id_bb_company as id_bb_company_e, e.party_name
                                FROM """ + sDremioTableName + """ f
                                INNER JOIN """ + lib_utils.dictDremio['dfEntityDim'] + """ e ON e.entity_key = f.entity_key
                                where """ + str(sUseDate) + """  <= '""" + str(sEffectiveDate) + """'"""
        
        if iHist == False:
            # global last effective date
            print(sql)
            #dfDremioData        = pd.read_sql(sql,lib_utils.conndremio)
            df_spark            = lib_utils.spark.sql(sql)
            dfDremioData        = df_spark.toPandas()            
            dfDremioData        = dfDremioData.rename(columns=lambda x: x.lower())
            dfDremioData = dfDremioData.apply(pd.to_numeric, errors='ignore')

        else:
            # last effective date per entity
            #dfDremioData        = pd.read_sql(sql_hist,lib_utils.conndremio)  
            df_spark            = lib_utils.spark.sql(sql)
            dfDremioData        = df_spark.toPandas()  
            dfDremioData        = dfDremioData.rename(columns=lambda x: x.lower())
            dfDremioData = dfDremioData.apply(pd.to_numeric, errors='ignore')
            
        
        if 'id_bb_company' not in dfDremioData.columns.str.lower().to_list():
            
            dfDremioData['id_bb_company'] = dfDremioData['id_bb_company_e']
                 
        try:
            dfDremioData        = dfDremioData.sort_values(sGroupBy + ['id_bb_company'] + [sUseDate] + ['entity_durable_key'])        
        except:
            if 'entity_key' in dfDremioData.columns.to_list():
                dfDremioData        = dfDremioData.sort_values(sGroupBy + ['id_bb_company'] + [sUseDate] + ['entity_key']) 
            else:
                dfDremioData        = dfDremioData.sort_values(sGroupBy + ['id_bb_company'] + [sUseDate] + ['edm_pty_id'])  
        dfDremioData        = dfDremioData.groupby(sGroupBy + ['id_bb_company']).last().reset_index()
        
        # convert pct columns to decimal 
        lVarsPct = [s for s in dfDremioData.columns.to_list() if s in lib_utils.lVarsDremioPct]
        if len(lVarsPct) > 0:
            dfDremioData = calc_decimal(dfDremioData,lVarsPct)
        
        # apply minor mastering steps
        if iMastering == True:
            dfDremioData = dremio_mastering(sDataset,dfDremioData) 
        
        if len(dfDremioData[['id_bb_company'] + sGroupBy].drop_duplicates()) == len(dfDremioData): 
            save_cache(dfDremioData,cache_path,lib_utils.spark,format = 'parquet')
            #dfDremioData.to_pickle(lib_utils.sWorkFolder + sDataset + '.pkl')
            return dfDremioData
        else:
            raise Exception('ID_BB_COMPANY not unique')

def load_underlyings():
    sPathName                   = lib_utils.sPathNameGeneral
    sWorkFolder                 = sPathName + '15. Investment process/Derivatives/'

    
    dfDerivatives               = pd.read_excel(sWorkFolder + 'Derivatives_List.xlsx')
    
    # Ext funds
    dfExt                       = load_holdings("refinitiv", False)
    
    # Filter for rows with missing ID2
    dfMissings = dfExt[dfExt['ID_BB_COMPANY'].isna()]
    dfManualISIN = pd.read_excel(lib_utils.sWorkFolder + 'Manual_Mapping.xlsx',sheet_name = 'id_isin')
    dfManualName = pd.read_excel(lib_utils.sWorkFolder + 'Manual_Mapping.xlsx',sheet_name = 'long_name')
    
    # Map values from df2 by ID2 for rows with missing ID2
    dfExt = dfExt.merge(dfManualISIN, on='id_isin', how='left')
    dfExt['ID_BB_COMPANY'] = dfExt['ID_BB_COMPANY_x'].fillna(dfExt['ID_BB_COMPANY_y'])
    dfExt.drop(['ID_BB_COMPANY_x','ID_BB_COMPANY_y'],axis = 1,inplace = True)

    dfExt = dfExt.merge(dfManualName, on='long_name', how='left')
    dfExt['ID_BB_COMPANY'] = dfExt['ID_BB_COMPANY_x'].fillna(dfExt['ID_BB_COMPANY_y'])
    dfExt.drop(['ID_BB_COMPANY_x','ID_BB_COMPANY_y'],axis = 1,inplace = True)
    
    dfExt = dfExt.loc[dfExt.portfolio_id.isin(dfDerivatives['ETF ISIN'].drop_duplicates().to_list())]
    
    # fill up with data from MEMB 
    dfBBG                       = pd.read_excel(sWorkFolder + 'Derivatives_List.xlsx',sheet_name = '20240709')
    dfBBG['effective_date'] = '2024-07-09'

    dfBBG['weight'] = dfBBG['weight'] / dfBBG.groupby('portfolio_id')['weight'].transform('sum')
    dfBBG['agi_instrument_asset_type_description'] = 'Equity'
    dfBBG = pd.merge(dfBBG,lib_utils.dfSecurities,how = 'left',left_on = 'id_isin',right_on = 'ID_ISIN',validate = 'm:1')
    dfBBG['long_name'] = dfBBG['LONG_COMP_NAME']
    dfBBG['issuer_long_name'] = dfBBG['LONG_COMP_NAME']
    
    dfFutures = pd.concat([dfExt,dfBBG])
    dfFutures['portfolio_type'] = 'Equity Index'
   
        

    dfCDX = pd.read_excel(sWorkFolder + 'Derivatives_List.xlsx',sheet_name = '20240709_CDX')  
    dfCDX['weight'] = dfCDX['weight'] / dfCDX.groupby('portfolio_id')['weight'].transform('sum')
    dfCDX['agi_instrument_asset_type_description'] = 'Fixed Income'
    dfCDX = pd.merge(dfCDX,lib_utils.dfSecurities,how = 'left',left_on = 'id_isin',right_on = 'ID_ISIN',validate = 'm:1')
    dfCDX['issuer_long_name'] = dfCDX['LONG_COMP_NAME']
    dfCDX['portfolio_type'] = 'CDX'
    dfCDX['effective_date'] = '2024-07-09'

    
    # dfExt.loc[dfExt['long_name'] == "L'OREAL S.A., PARIS ORD",'ID_BB_COMPANY'] = 115414
    # dfExt.loc[dfExt['long_name'] == "L'OREAL ORD",'ID_BB_COMPANY'] = 115414
    # dfExt.loc[dfExt['long_name'] == "Commerzbank AG ORD",'ID_BB_COMPANY'] = 115684
    # dfExt.loc[dfExt['long_name'] == "Siemens Energy AG",'ID_BB_COMPANY'] = 68938862
    # dfExt.loc[dfExt['long_name'] == "Air Liquide Prime Fidelite",'ID_BB_COMPANY'] = 115230
    # dfExt.loc[dfExt['long_name'] == "Air Liquide SA Prime de Fidelite 2023",'ID_BB_COMPANY'] = 115230
    # dfExt.loc[dfExt['long_name'] == "Air Liquide SA Prime Fidelite 2024",'ID_BB_COMPANY'] = 115230
    # dfExt.loc[dfExt['long_name'] == "GRAB HOLDINGS LTD - CL A",'ID_BB_COMPANY'] = 69555483
    # dfExt.loc[dfExt['long_name'] == "Natwest Group PLC ORD",'ID_BB_COMPANY'] = 112194
    # dfExt.loc[dfExt['long_name'] == "NORTHAM PLATINUM HOLDINGS LT",'ID_BB_COMPANY'] = 69676157

    dfUnderlyings = pd.concat([dfFutures,dfCDX])
    dfUnderlyings['gics_sector_description'] = dfUnderlyings['INDUSTRY_SECTOR']

    
    return  dfUnderlyings

def calc_coverage(dfPortfolio,lVars,sScope = 'Scope'):
    
    
    dfOut = dfPortfolio.copy()
    
    
    for i in lVars:
        dfOut['Issuer_has_Observation_'+i] = np.nan
        
        dfOut.loc[(dfOut[sScope] == True)&(~dfOut[i].isna()),'Issuer_has_Observation_'+i] = True
        dfOut.loc[(dfOut[sScope] == True)&(dfOut[i].isna()),'Issuer_has_Observation_'+i] = False
    
        dfOut['Uncovered_Issuer_'+i] = np.where(dfOut['Issuer_has_Observation_'+i].isnull(), 
                                                          pd.NA,
                                                          np.where(dfOut['Issuer_has_Observation_'+i]==1., 
                                                                   False, 
                                                                   True))
        dfOut['Uncovered_Issuer_'+i] = dfOut['Uncovered_Issuer_'+i].fillna(value = np.nan)
    
  
    
    return dfOut

def calc_weighted_average_esg_new(dfPortfolio,lRebased,lRebasedScope,lUnrebased,lUnrebasedScope,sGroupBy,iLookthrough = False):


    # dfPortfolio = myAssessment.dfAllPortfolios.copy()
    # lRebased = myAssessment.lRebased + myAssessment.lCoverageRebased
    # lRebasedScope = myAssessment.lRebasedScope + myAssessment.lCoverageRebasedScope
    # lUnrebased = myAssessment.lUnrebased + myAssessment.lCoverageUnrebased
    # lUnrebasedScope = myAssessment.lUnrebasedScope + myAssessment.lCoverageUnrebasedScope
    # sGroupBy = myAssessment.lGroupByVariables
    # iLookthrough = False
    
    ####
    if iLookthrough == True:
        dfPortfolio['Target Fund Exposure'] = False
        dfPortfolio.loc[(dfPortfolio.portfolio_type == 'Fund')&
                            (dfPortfolio.agi_instrument_asset_type_description == 'Funds')&
                            ~(dfPortfolio.agi_instrument_type_description.fillna('').str.contains('REIT')),'Target Fund Exposure'
                           ] = True
        dfPortfolio.loc[dfPortfolio['Target Fund Exposure'] == True,'Scope'] = True
        
        lUnrebased                  = lUnrebased + ['Target Fund Exposure']
        lOutVars                    = lRebased + lUnrebased
        lOutVarsTemp                = [s for s in lOutVars if s != 'Target Fund Exposure']    
        dfOutAgg, dfOut, dfTemp     = calc_weighted_average_esg_new(dfPortfolio.loc[dfPortfolio.portfolio_type.isin(['External Target Fund','Internal Target Fund'])],
                                                                    lRebased,
                                                                    lRebasedScope,
                                                                    lUnrebased,
                                                                    lUnrebasedScope,
                                                                    ['portfolio_name','portfolio_id'],
                                                                    iLookthrough = False)
        dfTargetFunds               = dfOutAgg.reset_index()[['portfolio_id'] + lOutVarsTemp]
        dfPortfolio = pd.merge(dfPortfolio,dfTargetFunds,how = 'left',left_on = 'id_isin',right_on = 'portfolio_id',suffixes = ('','_TargetFunds'),validate = 'm:1')
        for var in lOutVarsTemp:
            dfPortfolio.loc[dfPortfolio['Target Fund Exposure'] == True, var] = dfPortfolio.loc[dfPortfolio['Target Fund Exposure'] == True][var + '_TargetFunds']

    # compute weighted sum for all metrics
    sMetrics = lRebased + lUnrebased
    sMetricsScope = lRebasedScope + lUnrebasedScope
    # dfPortfolio['weight_eq'] = dfPortfolio.groupby(sGroupBy)['ID_BB_COMPANY'].transform(
    #     lambda x: 1 / x.nunique() 
    # )    
    dfOut = dfPortfolio.copy()
    
    # we need set the values to null for all issuers that are not in scope. scope is variable specific
    dfOut[sMetrics] = dfOut[sMetrics].multiply(dfOut['weight'], axis='index').astype(float)
    for m, s in zip(sMetrics,sMetricsScope):

        dfOut.loc[dfOut[s] == False,m] = np.nan

    dfOut = dfOut.select_dtypes(exclude=['datetime64[ns]'])

    dfOutAgg = dfOut[sGroupBy + sMetrics].groupby(sGroupBy).sum()[sMetrics]

    # rebase variables from lRebased with coverage rate 
    dfOutRebase = dfPortfolio.copy()
    for m, s in zip(lRebased,lRebasedScope):
        dfOutRebase.loc[dfOutRebase[s] == False,m] = np.nan
    dfOutRebase = dfOutRebase.set_index(sGroupBy)
    dfOutRebase = dfOutRebase[lRebased].notnull().multiply(dfOutRebase['weight'],axis = 'index').groupby(level = [*range(0,len(sGroupBy),1)]).sum()      
    dfOutAgg[lRebased] = dfOutAgg[lRebased]/dfOutRebase[lRebased]
    
    
    # Add _ew columns using weight_eq
    if 1 == 0:
        # Apply equal weights to all metrics
        #dfOut_ew = dfPortfolio.copy()
        dfOut_ew = dfPortfolio[sGroupBy + ['ID_BB_COMPANY','weight_eq','Scope_Inv','Scope_Corp','Scope_Sov'] + sMetrics].copy().drop_duplicates()

        dfOut_ew[sMetrics] = dfOut_ew[sMetrics].multiply(dfOut_ew['weight_eq'], axis='index').astype(float)
        dfOutAgg_ew = dfOut_ew[sGroupBy + sMetrics].groupby(sGroupBy).sum()[sMetrics]

        # Rebase for equal-weighted metrics
        dfOutRebase_ew = dfPortfolio[sGroupBy + ['ID_BB_COMPANY','weight_eq','Scope_Inv','Scope_Corp','Scope_Sov'] + sMetrics].copy().drop_duplicates()
        for m, s in zip(lRebased, lRebasedScope):
            dfOutRebase_ew.loc[dfOutRebase_ew[s] == False, m] = np.nan
        dfOutRebase_ew = dfOutRebase_ew.set_index(sGroupBy)
        dfOutRebase_ew = dfOutRebase_ew[lRebased].notnull().multiply(dfOutRebase_ew['weight_eq'], axis='index').groupby(
            level=[*range(0, len(sGroupBy), 1)]
        ).sum()
        print(dfOutAgg_ew.T)
        print(dfOutRebase_ew.T)
      
        dfOutAgg_ew[lRebased] = dfOutAgg_ew[lRebased] / dfOutRebase_ew[lRebased]

        # Add _ew columns to dataframes
        for col in sMetrics:
            dfOutAgg[col + '_ew'] = dfOutAgg_ew[col]
            #dfOut[col + '_ew'] = dfOut[col] / dfOutRebase_ew[col]
            #dfPortfolio[col + '_ew'] = dfPortfolio[col] * dfPortfolio['weight_eq']
    
    
    return dfOutAgg, dfOut, dfPortfolio
   
# merge functions
def merge_esg_data_new(dfPortfolios,dfESGIn,sVariables,sESGSuffix,iWaterfall=False,sLeftOn='ID_BB_COMPANY',sRightOn='ID_BB_COMPANY',sScope = 'Scope'):
    
    dfPortfolio = dfPortfolios.copy()
    dfESG = dfESGIn.copy(deep = True)
    dfESG.rename(columns = {'id_bb_company':'ID_BB_COMPANY'},inplace = True)
    
    
    if sVariables is not None:
        # keep only ID and data in right data set
        dfESG = dfESG[[sRightOn]+sVariables]
        # drop data if already exist in left data set
        lDrop = [s for s in dfPortfolio.columns if s in sVariables]
    else:
        lDrop = []
    if len(lDrop) > 0:
        dfPortfolio.drop(lDrop,axis = 1,inplace = True)
    
    if sVariables is not None:
        # drop NA in right data set
        dfESG.dropna(subset = [sVariables][0],inplace = True)

    
    if dfESG[sRightOn].nunique() != len(dfESG):
        raise Exception("ESG data set not unique")
    

    if iWaterfall ==False:
        if sESGSuffix == '_FH':
            dfOut = pd.merge(dfPortfolio,dfESG,how = 'left',left_on = sLeftOn,right_on = sRightOn, suffixes = ('',sESGSuffix), validate = 'm:1')
            dfOut['FH'] = False
            dfOut.loc[(dfOut.Sovereigns == 1)&(dfOut.status == 1),'FH'] = True
            dfOut['status'] = dfOut['FH']
            dfOut.loc[dfOut[sScope]==False,'status'] = np.nan
            
            dfOut['status'] = np.where(dfOut['status'].isnull(), 
                                                          pd.NA,
                                                          np.where(dfOut['status']==1., 
                                                                   True, 
                                                                   False))
            dfOut['status'] = dfOut['status'].fillna(value = np.nan)
        
        else:
            lDrop = [s for s in dfPortfolio.columns if s == 'Scope' + sESGSuffix]
            if len(lDrop) > 0:
                dfPortfolio.drop(lDrop,axis = 1,inplace = True)
            if sVariables is not None:
                dfOut = pd.merge(dfPortfolio,dfESG.assign(Scope = True),how = 'left',left_on = [sScope, sLeftOn], right_on = ['Scope', sRightOn],suffixes=('', sESGSuffix),validate = 'm:1')     
            else:
                dfOut = pd.merge(dfPortfolio,dfESG.assign(Scope = True),how = 'left',left_on = [sScope, sLeftOn], right_on = ['Scope', sRightOn],suffixes=('', sESGSuffix),indicator = 'Exposure'+sESGSuffix,validate = 'm:1')
                dfOut['Exposure'+sESGSuffix] = dfOut['Exposure'+sESGSuffix].map({'both': True, 'left_only': False})
                dfOut.loc[dfOut[sScope] == False,'Exposure'+sESGSuffix] = np.nan
    else:
        dfOut = calc_waterfall(dfPortfolio,dfESG,sESGSuffix+'_used')
        lDrop = [s for s in dfOut.columns if s == 'Scope' + sESGSuffix]
        
        if len(lDrop) > 0:
            dfOut.drop(lDrop,axis = 1,inplace = True)
        if sVariables is not None:
            dfOut = pd.merge(dfOut,dfESG.assign(Scope = True),how = 'left',left_on = [sScope,sLeftOn+sESGSuffix+'_used'],right_on = ['Scope', sRightOn],suffixes=('', sESGSuffix),validate = 'm:1')
        else:
            dfOut = pd.merge(dfOut,dfESG.assign(Scope = True),how = 'left',left_on = [sScope,sLeftOn+sESGSuffix+'_used'],right_on = ['Scope', sRightOn],suffixes=('', sESGSuffix),indicator = 'Exposure'+sESGSuffix,validate = 'm:1')
            dfOut['Exposure'+sESGSuffix] = dfOut['Exposure'+sESGSuffix].map({'both': True, 'left_only': False})
            dfOut.loc[dfOut[sScope] == False,'Exposure'+sESGSuffix] = np.nan
        
        lDrop = [s for s in dfOut.columns if s == 'ID_BB_COMPANY' + sESGSuffix]
        dfOut.drop(lDrop,axis = 1,inplace = True)
    
    if sESGSuffix == '_GB':
            dfOut.loc[(dfOut['Exposure_GB'] == 1)&(dfOut['dnsh_flag_overall'] == False),'sustainable_investment_share_pre_dnsh']= 1
            dfOut.loc[(dfOut['Exposure_GB'] == 1)&(dfOut['dnsh_flag_overall'] == False),'sustainable_investment_share_post_dnsh']= 1
            # dfOut.loc[(dfOut['Exposure_GB'] == 1)&(dfOut['DNSH_FLAG_OVERALL_CURRENT'] == False),'SIS_currentPC_CURRENT']= 1
            # dfOut.loc[(dfOut['Exposure_GB'] == 1)&(dfOut['DNSH_FLAG_OVERALL_CURRENT'] == False),'SIS_newPC_CURRENT']= 1
            # dfOut.loc[(dfOut['Exposure_GB'] == 1)&(dfOut['DNSH_FLAG_OVERALL_COMBO'] == False),'SIS_newPC_COMBO']= 1
    
    if sVariables == ['GREEN_BOND_LOAN_INDICATOR']:
            dfOut.loc[(dfOut['GREEN_BOND_LOAN_INDICATOR'] == 1)&(dfOut['dnsh_flag_overall'] == False),'sustainable_investment_share_environmental_post_dnsh']= 1
            dfOut.loc[(dfOut['GREEN_BOND_LOAN_INDICATOR'] == 1)&(dfOut['dnsh_flag_overall'] == False),'sustainable_investment_share_social_post_dnsh']= 0

    if sVariables == ['SOCIAL_BOND_IND']:
            dfOut.loc[(dfOut['SOCIAL_BOND_IND'] == 1)&(dfOut['dnsh_flag_overall'] == False),'sustainable_investment_share_social_post_dnsh']= 1
            dfOut.loc[(dfOut['SOCIAL_BOND_IND'] == 1)&(dfOut['dnsh_flag_overall'] == False),'sustainable_investment_share_environmental_post_dnsh']= 0
            
    if sVariables == ['SUSTAINABILITY_BOND_IND']:
            dfOut.loc[(dfOut['SUSTAINABILITY_BOND_IND'] == 1)&(dfOut['dnsh_flag_overall'] == False),'sustainable_investment_share_social_post_dnsh']= 0.5
            dfOut.loc[(dfOut['SUSTAINABILITY_BOND_IND'] == 1)&(dfOut['dnsh_flag_overall'] == False),'sustainable_investment_share_environmental_post_dnsh']= 0.5

    if sVariables == ['CO2_Engaged']:
            dfOut['CO2_Engaged'] = 0
            dfOut.loc[dfOut['Exposure_Eng'] == True,'CO2_Engaged'] = dfOut['carbon_emissions_scope_12']

    if sESGSuffix == '_Ex':
            lVars = dfESG.drop(['ID_BB_COMPANY'],axis = 1).columns.to_list()
            dfOut[lVars] = dfOut[lVars].fillna(False)

    if sESGSuffix == '_Comp':
            dfOut['Exposure_Comp'] = dfOut[['Exposure_BS','status']].max(axis = 1)

        
    return dfOut

def calc_new_variables(dfPortfolios):
    dfPortfolios['Uncovered_Issuer_AGI Custom ESMA PAB Results - 2024 November_exSRI'] = False
    
    dfPortfolios['AGI Custom ESMA PAB Results - 2024 November_exSRI'] = False
    dfPortfolios.loc[(dfPortfolios.Exposure_Sus == False) & (dfPortfolios['AGI Custom ESMA PAB Results - 2024 November'] == True),'AGI Custom ESMA PAB Results - 2024 November_exSRI'] = True
    

    # dfPortfolios['Worst_Quintile_PSR'] = (dfPortfolios['PSR_score_final_relative_postFlags'] 
    #                                   <= dfPortfolios.drop_duplicates(subset=['ID_BB_COMPANY'])['PSR_score_final_relative_postFlags'].quantile(0.2)
    #                                   ).where(dfPortfolios['PSR_score_final_relative_postFlags'].notna())
    # dfPortfolios['Worst_Quintile_SRI'] = (dfPortfolios['SRI_Rating_Final'] 
    #                                   <= dfPortfolios.drop_duplicates(subset=['ID_BB_COMPANY'])['SRI_Rating_Final'].quantile(0.2)
    #                                   ).where(dfPortfolios['SRI_Rating_Final'].notna())

    # #dfPortfolios['Worst_Quintile_SRI'] = (dfPortfolios['SRI_Rating_Final']<= dfPortfolios.drop_duplicates(subset = ['ID_BB_COMPANY'])['SRI_Rating_Final'].quantile(0.2)).astype(bool)
    # dfPortfolios['Downgrades'] = ((dfPortfolios['Worst_Quintile_SRI'] == False) & 
    #                           (dfPortfolios['Worst_Quintile_PSR'] == True)).astype(bool)
    # dfPortfolios['Upgrades'] = ((dfPortfolios['Worst_Quintile_SRI'] == True) & 
    #                           (dfPortfolios['Worst_Quintile_PSR'] == False)).astype(bool)

    # dfPortfolios['Align_Add_Cov_MSCI'] = dfPortfolios['Uncovered_Issuer_nz_alignment_status'].astype(bool) & ~dfPortfolios['Uncovered_Issuer_PAII_NZIF_ALIGNMENT'].astype(bool)
    # dfPortfolios['Align_Lost_Cov_MSCI'] = ~dfPortfolios['Uncovered_Issuer_nz_alignment_status'].astype(bool) & dfPortfolios['Uncovered_Issuer_PAII_NZIF_ALIGNMENT'].astype(bool)
    # dfPortfolios['Align_Overlap_Cov_MSCI'] = ~dfPortfolios['Uncovered_Issuer_nz_alignment_status'].astype(bool) & ~dfPortfolios['Uncovered_Issuer_PAII_NZIF_ALIGNMENT'].astype(bool)

    # dfPortfolios['c1_Add_Cov_MSCI'] = dfPortfolios['Uncovered_Issuer_c1_status'].astype(bool) & ~dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_1'].astype(bool)
    # dfPortfolios['c1_Lost_Cov_MSCI'] = ~dfPortfolios['Uncovered_Issuer_c1_status'].astype(bool) & dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_1'].astype(bool)
    # dfPortfolios['c1_Overlap_Cov_MSCI'] = ~dfPortfolios['Uncovered_Issuer_c1_status'].astype(bool) & ~dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_1'].astype(bool)

    # dfPortfolios['c2_Add_Cov_MSCI'] = dfPortfolios['Uncovered_Issuer_c2_status'].astype(bool) & ~dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_2'].astype(bool)
    # dfPortfolios['c2_Lost_Cov_MSCI'] = ~dfPortfolios['Uncovered_Issuer_c2_status'].astype(bool) & dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_2'].astype(bool)
    # dfPortfolios['c2_Overlap_Cov_MSCI'] = ~dfPortfolios['Uncovered_Issuer_c2_status'].astype(bool) & ~dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_2'].astype(bool)
    
    # dfPortfolios['c4_Add_Cov_MSCI'] = dfPortfolios['Uncovered_Issuer_c4_status'].astype(bool) & ~dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_4'].astype(bool)
    # dfPortfolios['c4_Lost_Cov_MSCI'] = ~dfPortfolios['Uncovered_Issuer_c4_status'].astype(bool) & dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_4'].astype(bool)
    # dfPortfolios['c4_Overlap_Cov_MSCI'] = ~dfPortfolios['Uncovered_Issuer_c4_status'].astype(bool) & ~dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_4'].astype(bool)

    # dfPortfolios['c5_Add_Cov_MSCI'] = dfPortfolios['Uncovered_Issuer_c5_status'].astype(bool) & ~dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_5'].astype(bool)
    # dfPortfolios['c5_Lost_Cov_MSCI'] = ~dfPortfolios['Uncovered_Issuer_c5_status'].astype(bool) & dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_5'].astype(bool)
    # dfPortfolios['c5_Overlap_Cov_MSCI'] = ~dfPortfolios['Uncovered_Issuer_c5_status'].astype(bool) & ~dfPortfolios['Uncovered_Issuer_PAII_NZIF_CRITERIA_5'].astype(bool)
    
    # dfPortfolios['Align_match'] = dfPortfolios['nz_alignment_status'] == dfPortfolios['PAII_NZIF_ALIGNMENT']
    # dfPortfolios['c1_match'] = dfPortfolios['c1_status'] == dfPortfolios['PAII_NZIF_CRITERIA_1']
    # dfPortfolios['c2_match'] = dfPortfolios['c2_status'] == dfPortfolios['PAII_NZIF_CRITERIA_2']
    # dfPortfolios['c4_match'] = dfPortfolios['c4_status'] == dfPortfolios['PAII_NZIF_CRITERIA_4']
    # dfPortfolios['c5_match'] = dfPortfolios['c5_status'] == dfPortfolios['PAII_NZIF_CRITERIA_5']

    # dfPortfolios['carbon_emissions_scope_123_total_sales_inten'] = dfPortfolios['carbon_emissions_scope_3_total_sales_inten'] + dfPortfolios['carbon_emissions_scope_12_inten']
    # dfPortfolios['carbon_emissions_scope_123_total_evic_inten'] = dfPortfolios['carbon_emissions_scope_3_tot_evic_inten'] + dfPortfolios['carbon_emissions_evic_scope_12_inten']
    #dfPortfolios['apportioned_revenues'] = dfPortfolios['sales_usd_recent']*0.9/dfPortfolios['evic_eur']
    #dfPortfolios['ethica'] = dfPortfolios[['AGI Custom Ethica_2025 March','sri_rating_final < 2','Exposure_Sus']].any(axis=1)
    # print('test')
    # quantiles = dfPortfolios.groupby(['gics_sector_description','country_name'])['industry_adjusted_score'].quantile(0.3,interpolation = 'lower').reset_index()
    # quantiles.columns = ['gics_sector_description'  ,  'country_name',  'industry_adjusted_score_']
    # dfPortfolios = pd.merge(dfPortfolios,quantiles,how = 'left',on = ['gics_sector_description','country_name'])
    # dfPortfolios['is_worst_30'] = False
    # dfPortfolios.loc[dfPortfolios['industry_adjusted_score']<dfPortfolios['industry_adjusted_score_'],'is_worst_30'] = True
    # print(dfPortfolios['is_worst_30'])
    #dfPortfolio['carbon_emissions_scope_12_inten_apportioned_sales'] = dfPortfolios['']
    # dfPortfolios['PAB_sustainable_investment_share_post_dnsh'] = dfPortfolios['sustainable_investment_share_post_dnsh']
    #dfPortfolios.loc[dfPortfolios['AGI Custom ESMA PAB (CW RED)_Issuers_20240117'] == True,'PAB_sustainable_investment_share_post_dnsh'] = 0
    
    # dfPortfolios['region_DM'] = False
    # dfPortfolios.loc[dfPortfolios.country_name.isin(['Germany',
    #  'Denmark',
    #  'Sweden',
    #  'Switzerland',
    #  'Netherlands',
    #  'Hong Kong',
    #  'United Kingdom',
    #  'Singapore',
    #  'Norway',
    #  'United States',
    #  'Australia',
    #  'France',
    #  'Japan',
    #  'New Zealand',
    #  'Ireland',
    #  'Luxembourg',
    #  'Spain',
    #  'Canada',
    #  'Italy',
    #  'Belgium',
    #  'Israel',
    #  'Finland',
    #  'Austria',
    #  'Portugal']),'region_DM'] = True
    # dfPortfolios['region_EM'] = ~dfPortfolios['region_DM']
    # dfPortfolios['Green instrument'] = ''
    # dfPortfolios.loc[(dfPortfolios.Scope_Inv == True)&(dfPortfolios['region'] == 'DM')&(dfPortfolios['esg_rating'].isin(['AAA','AA','A','BBB'])),'Green instrument'] = True
    # dfPortfolios.loc[(dfPortfolios.Scope_Inv == True)&(dfPortfolios['region'] == 'EM')&(dfPortfolios['esg_rating'].isin(['AAA','AA','A','BBB','BB'])),'Green instrument'] = True
    #dfPortfolios['SI_NZ_share'] = dfPortfolios['sustainable_investment_share_post_dnsh']
    #dfPortfolios.loc[(dfPortfolios['dnsh_flag_overall']== False)&
    #                (dfPortfolios.nz_alignment_status.isin(['3. Aligning towards a Net Zero pathway','1. Achieving Net Zero'])),'SI_NZ_share'] = 1
    #dfPortfolios['total_exclusions'] = dfPortfolios[['Exposure_Sus','Exposure_TS','Exposure_Febelfin','Exposure_Febelfin25','Exposure_Urgewald','Exposure_GOGEL','Exposure_GCEL']].max(axis = 1)
    # dfPortfolios['Target Fund Exposure'] = 0
    # dfPortfolios['Score downgrade < 2'] = False
    # dfPortfolios['Score upgrade > 2'] = False
    # dfPortfolios['Score downgrade < 1'] = False
    # dfPortfolios['Score upgrade > 1'] = False
    # # dfPortfolios['Rating downgrade < E'] = False
    # # dfPortfolios['Rating upgrade > E'] = False
    # # dfPortfolios['Rating downgrade < D'] = False
    # # dfPortfolios['Rating upgrade > D'] = False
    # dfPortfolios.loc[(dfPortfolios['SRI_Rating_Final']>=2)&(dfPortfolios['PSR_score_final_relative_postFlags']<2),'Score downgrade < 2'] = True
    # dfPortfolios.loc[(dfPortfolios['SRI_Rating_Final']<2)&(dfPortfolios['PSR_score_final_relative_postFlags']>=2),'Score upgrade > 2'] = True
    # dfPortfolios.loc[(dfPortfolios['SRI_Rating_Final']>=1)&(dfPortfolios['PSR_score_final_relative_postFlags']<1),'Score downgrade < 1'] = True
    # dfPortfolios.loc[(dfPortfolios['SRI_Rating_Final']<1)&(dfPortfolios['PSR_score_final_relative_postFlags']>=1),'Score upgrade > 1'] = True
    # dfPortfolios.loc[(dfPortfolios['sri_rating_final']>=1)&(dfPortfolios['PSR_rating_final'].isin(['E'])),'Rating downgrade < E'] = True
    # dfPortfolios.loc[(dfPortfolios['sri_rating_final']<1)&(dfPortfolios['PSR_rating_final'].isin(['A','B','C','D'])),'Rating upgrade > E'] = True
    # dfPortfolios.loc[(dfPortfolios['sri_rating_final']>=2)&(dfPortfolios['PSR_rating_final'].isin(['D','E'])),'Rating downgrade < D'] = True
    # dfPortfolios.loc[(dfPortfolios['sri_rating_final']<2)&(dfPortfolios['PSR_rating_final'].isin(['A','B','C'])),'Rating upgrade > D'] = True
    # dfPortfolios['Coverage_Lost'] = False
    # dfPortfolios.loc[(dfPortfolios.Sovereigns == False)&(dfPortfolios.PSR_score_final_raw.isna())&(~dfPortfolios.sri_rating_final.isna()),'Coverage_Lost'] = True
    # dfPortfolios['Coverage_Gained'] = False
    # dfPortfolios.loc[(dfPortfolios.Sovereigns == False)&(~dfPortfolios.PSR_score_final_raw.isna())&(dfPortfolios.sri_rating_final.isna()),'Coverage_Gained'] = True
    # dfPortfolios['Scope'] = dfPortfolios['Scope_Corp']
    return dfPortfolios

def calc_waterfall(dfPortfolio,dfESG,sESGSuffix):
    dfOut                                           = dfPortfolio.copy()
    dfOut['ID_BB_COMPANY'+sESGSuffix]               = dfOut['ID_BB_COMPANY'].mask(~dfOut.ID_BB_COMPANY.isin(dfESG.ID_BB_COMPANY))
    dfOut['ID_BB_PARENT_CO'+sESGSuffix]             = dfOut['ID_BB_PARENT_CO'].mask(~dfOut.ID_BB_PARENT_CO.isin(dfESG.ID_BB_COMPANY))
    dfOut['ID_BB_ULTIMATE_PARENT_CO'+sESGSuffix]    = dfOut['ID_BB_ULTIMATE_PARENT_CO'].mask(~dfOut.ID_BB_ULTIMATE_PARENT_CO.isin(dfESG.ID_BB_COMPANY))
    
    dfOut['ID_BB_COMPANY' + sESGSuffix]             = dfOut['ID_BB_COMPANY'+sESGSuffix].fillna(dfOut['ID_BB_PARENT_CO'+sESGSuffix]).fillna(dfOut['ID_BB_ULTIMATE_PARENT_CO'+sESGSuffix])
    return dfOut.drop(['ID_BB_PARENT_CO'+sESGSuffix,'ID_BB_ULTIMATE_PARENT_CO'+sESGSuffix],axis = 1)

# load holdings
def load_portfolio_benchmark_bridge(lPortfolioID):
    sql = """
    SELECT b.portfolio_id,
       b.portfolio_name AS portfolio_benchmark_group_name,
       a.benchmark_durable_key,
       c.benchmark_name
    FROM   """ + lib_utils.dictDremio['dfPortfolioBenchmarkBridge'] + """ a
    LEFT JOIN """ + lib_utils.dictDremio['dfPortfolioDim'] + """ b
              ON a.portfolio_durable_key = b.portfolio_durable_key
    LEFT JOIN """ + lib_utils.dictDremio['dfBenchmarkDim'] + """ c
              ON a.benchmark_durable_key = c.benchmark_durable_key
    WHERE  b.is_current = true
       AND c.is_current = true  """
    dfBenchmarkBridge           = pd.read_sql(sql,lib_utils.conndremio)
    dfBenchmarkBridge           = dfBenchmarkBridge.loc[dfBenchmarkBridge.portfolio_id.isin(lPortfolioID)]
    return dfBenchmarkBridge

def _load_dremio_holdings(lPortfolioIDs, sDate, position_type = 4, iBenchmarks = False, split_ids = None):
    """Function loads the holdings from Dremio
    
    Args:
    portfolio_IDs (int, string, list)... Specification of the portfolio identifiers (CEDAR Portfolio ID) that should be loaded. Provide either single identifier or a list/tuple of identifiers
    date (str)... Format yyyy-mm-dd, Date for which the holdings should be extracted
    lib_utils.conn_dremio (lib_utils.connection object)... lib_utils.connection to Database Dremio Production (Tables used: holding_fact, security_master_dim, portfolio_dim)
    position_type (int from 1 to 5)... Position type from GIDP, according to following logic 
                                1 - Settled view, End of Day    
                                2 - Settled view, Start of Day
                                3 - Traded view, Start of Day
                                4 - Traded view, End of Day     (default)
                                5 - Traded view, Month End      (filled only for the last month)
    
    
    Returns:
    dfHoldings (pd.DataFrame)... Dataframe containing the Holdings for the portfolio_ids based on DAL data 
    """
 
    
    if len(lPortfolioIDs) == 1:
        lPortfolioID = '(''{}'')'.format(str(lPortfolioIDs[0]))
    else:
        lPortfolioID = str(tuple(lPortfolioIDs))

    if len(sDate)>1:
        date = str(tuple(sDate))
    else:
        #date = '({})'.format(str(date[0]))
        date = "('" + sDate[0] + "')"
    query = """SELECT h.effective_date,
       'Fund'                                             AS portfolio_type,
       p.portfolio_durable_key,
       p.portfolio_id,
       p.portfolio_name,
       p.gidp_reporting_location,
       --p.prod_line_name_long,
       p.investment_region,
       p.sri_esg,
       p.asset_class,
       p.investment_approach,
       p.market_cap,
       n.market_value_clean_portfolio,
       n.market_value_clean_firm,
       n.portfolio_currency_code,
       v.eu_sfdr_sustainability_category,
       v.internal_asset_class,
       v.domicile,
       h.holding_type,
       h.percentage_of_market_value_clean_portfolio / 100 AS weight,
       h.futures_exposure_firm/n.market_value_clean_firm as futures_exposure_firm,
       s.country_issue_name,
       e.country_name,
       e.country_code3,
       s.currency_traded,
       'NA' as ml_high_yield_sector,
       s.agi_instrument_asset_type_description,
       s.security_durable_key,
       s.id_underlying_durable_key,
       s.issuer_long_name,
       s.long_name,
       s.agi_instrument_type_description,
       g.GICS as gics_sector_description,
       bics.BICS as bics_sector_description,
       b3.BICS as bics_l3_description,
       b4.BBG as BBG_l2_description,
       s.collat_type,

       'NA' as gics_industry_group_description,
       'NA' as gics_industry_description,
       s.bloomberg_security_typ,
       s.id_isin,
       s.id_cusip,
       s.security_des,
       s.issue_description,
       s.issuer_id_bloomberg_company                      AS ID_BB_COMPANY
    FROM """ + lib_utils.dictDremio['dfHoldings'] + """ h
    JOIN """ + lib_utils.dictDremio['dfPortfolioDim'] + """ p
         ON p.portfolio_durable_key = h.portfolio_durable_key
    LEFT JOIN """ + lib_utils.dictDremio['dfSecurityMasterDim'] + """ s
              ON s.security_durable_key = h.security_durable_key
                 AND s.is_current = True
    LEFT JOIN """ + lib_utils.dictDremio['dfCountryDim'] + """ e
              ON s.country_issue = e.country_code
    LEFT JOIN """ + lib_utils.dictDremio['dfPortfolioTotal'] + """ n
              ON p.portfolio_durable_key = n.portfolio_durable_key
                 AND n.date_key = h.date_key
                 AND n.position_type_key = h.position_type_key
    LEFT JOIN """ + lib_utils.dictDremio['dfVehicleDim'] + """ v
              ON p.portfolio_durable_key = v.portfolio_durable_key
                 AND v.is_current = True
                 AND v.vehicle_type_level = 'vehicle'
    LEFT JOIN (
        SELECT distinct a.security_durable_key, l1_desc as GICS FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_GICS' and a.is_current = True and a.security_durable_key is not null
        ) g on s.security_durable_key = g.security_durable_key
    LEFT JOIN (
        SELECT distinct a.security_durable_key, l1_desc as BICS FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_BICS' and a.is_current = True and a.security_durable_key is not null
        ) bics on s.security_durable_key = bics.security_durable_key
    LEFT JOIN ( SELECT distinct a.security_durable_key, l3_desc as BICS FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_BICS' and a.is_current = True and a.security_durable_key is not null ) b3 
        on s.security_durable_key = b3.security_durable_key
     LEFT JOIN ( SELECT distinct a.security_durable_key, l2_desc as BBG FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_BBG' and a.is_current = True and a.security_durable_key is not null ) b4
        on s.security_durable_key = b4.security_durable_key
    WHERE  h.effective_date IN """ + date + """
       AND p.portfolio_id IN """ + str(lPortfolioID) + """
       AND h.position_type_key = """ + str(position_type) + """
       AND p.is_current = True
       AND p.cluster_type <> 'Operations-Only Portfolio'
       -- excludes all Feeder, Client-Advised, etc. 
       AND p.portfolio_status <> 'Terminated'
       -- excludes all Terminated portfolios
       AND p.hierarchy_type NOT IN ( 'Void' ) -- excludes all Sub Portfolios or Void (used for Terminated)
    """
    print('*********************************************')
    print('Loading holdings data as of ' + date + '...')
    print('*********************************************')
    print(query)
    dfHoldings = pd.read_sql(query, lib_utils.conndremio)

    dfBenchmarkBridge = load_portfolio_benchmark_bridge(lPortfolioIDs)

    if len(dfBenchmarkBridge) > 0:
        dfHoldings = pd.merge(
            dfHoldings,
            dfBenchmarkBridge,
            how="left",
            left_on="portfolio_id",
            right_on="portfolio_id",
            validate="m:1",
        )
        dfHoldings["benchmark_id"] = dfHoldings["benchmark_durable_key"]
        dfHoldings = dfHoldings.drop(columns=["benchmark_durable_key"], errors="ignore")
        lBenchmarkIDs = dfBenchmarkBridge.benchmark_durable_key.drop_duplicates().to_list()

        if len(lBenchmarkIDs) == 1:
            lBenchmarkIDs = lBenchmarkIDs[0]


        dfAllBenchmarks = load_holdings("benchmark", lBenchmarkIDs, sDate, position_type=1)
        dfAllBenchmarks["portfolio_id"] = dfAllBenchmarks["benchmark_id"]
        dfHoldings = pd.concat([dfHoldings, dfAllBenchmarks])


    dfHoldings = pd.merge(
        dfHoldings,
        lib_utils.dfHierarchy,
        how="left",
        left_on="ID_BB_COMPANY",
        right_on="ID_BB_COMPANY",
        validate="m:1",
    )


    if split_ids is None:
        split_ids = lib_utils.EQ_FI_SPLIT_IDS
    if split_ids:
        dfHoldings = split_eq_fi_sleeves(dfHoldings, split_ids)

    if iBenchmarks == True:
        return dfHoldings, dfBenchmarkBridge
    else:
        return dfHoldings

def _load_benchmark_holdings(ids, sDate, position_type = 'C'):
    """Function loads the Benchmark holdings from Dremio
    
    Args:
    portfolio_durable_keys (int, string, list)... Specification of the portfolio identifiers (portfolio_durable_key) for which benchmark data should be loaded. Provide either single identifier or a list/tuple of identifiers
    date (str)... Format yyyy-mm-dd, Date for which the holdings should be extracted
    lib_utils.conn_dremio (lib_utils.connection object)... lib_utils.connection to Database Dremio Production (Tables used: holding_fact, security_master_dim, portfolio_dim)
    position_type (str 'O' or 'C')... Position type from Benchmark holdings, according to following logic 
                                C - weights from close    (default)
                                O - weights from open
    
    
    Returns:
    dfBMKHoldings (pd.DataFrame)... Dataframe containing the Holdings for the Benchmarks related to the portfolio_durable_keys based on DAL data 
    """
    if isinstance(ids,int):
        ids = '(''{}'')'.format(str(ids))
    else:
        ids = str(tuple(ids))

    if len(sDate)>1:
        date = str(tuple(sDate))
    else:
        #date = '({})'.format(str(date[0]))
        date = "('" + sDate[0] + "')"

    query = """
    SELECT cast(d.full_date AS DATE)                   AS effective_date,
       'Benchmark'                   AS portfolio_type,
       'benchmark_durable_key'       AS durable_key_type,
       b.benchmark_durable_key       AS durable_key,
       'benchmark_key'               AS key_type,
       b.benchmark_key               AS key,
       b.benchmark_durable_key       AS benchmark_id,
       b.benchmark_name              AS portfolio_name,
       b.benchmark_name              AS benchmark_name,
       b.class                       AS asset_class,
       s.country_issue_name,
       e.country_name,
       e.country_code3,
       s.currency_traded,
       'NA' as ml_high_yield_sector,
       s.agi_instrument_asset_type_description,
       s.security_durable_key,
       s.long_name,
       s.issuer_long_name,
       s.agi_instrument_type_description,
       g.GICS as gics_sector_description,
       bics.BICS as bics_sector_description,
       b3.BICS as bics_l3_description,
       b4.BBG as BBG_l2_description,
       s.collat_type,
       'NA' as gics_industry_group_description,
       'NA' as gics_industry_description,
       s.bloomberg_security_typ,
       s.id_isin,
       s.id_cusip,
       s.security_des,
       s.issue_description,
       s.issuer_id_bloomberg_company AS ID_BB_COMPANY,
       h.weight_close / 100          weight
    FROM """ + lib_utils.dictDremio['dfBenchmarkConst'] + """ h
    JOIN """ + lib_utils.dictDremio['dfBenchmarkDim'] + """ b
         ON b.benchmark_durable_key = h.benchmark_durable_key
            AND b.is_current = True
    JOIN """ + lib_utils.dictDremio['dfDateDim'] + """ d
         ON h.date_key = d.date_key
    LEFT JOIN """ + lib_utils.dictDremio['dfSecurityMasterDim'] + """ s
              ON s.security_durable_key = h.security_durable_key
                 AND s.is_current = True
    LEFT JOIN """ + lib_utils.dictDremio['dfCountryDim'] + """ e
              ON e.country_code = s.country_issue 
    LEFT JOIN (
        SELECT distinct a.security_durable_key, l1_desc as GICS FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_GICS' and a.is_current = True and a.security_durable_key is not null
        ) g on s.security_durable_key = g.security_durable_key
    LEFT JOIN (
        SELECT distinct a.security_durable_key, l1_desc as BICS FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_BICS' and a.is_current = True and a.security_durable_key is not null
        ) bics on s.security_durable_key = bics.security_durable_key
    LEFT JOIN ( SELECT distinct a.security_durable_key, l3_desc as BICS FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_BICS' and a.is_current = True and a.security_durable_key is not null ) b3 
        on s.security_durable_key = b3.security_durable_key
     LEFT JOIN ( SELECT distinct a.security_durable_key, l2_desc as BBG FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_BBG' and a.is_current = True and a.security_durable_key is not null ) b4 
        on s.security_durable_key = b4.security_durable_key   
    WHERE h.benchmark_durable_key in """ + ids + """
       AND cast(d.full_date AS DATE) in """ + date 
    #
    print('*********************************************')
    print('Loading benchmark data as of ' + date + '...')
    print('*********************************************')
    print(query)
    dfBMKHoldings = pd.read_sql(query, lib_utils.conndremio)
    return dfBMKHoldings

def _load_index_holdings(ids, sDate, position_type = 'C'):
    """Function loads the Benchmark holdings from Dremio
    
    Args:
    portfolio_durable_keys (int, string, list)... Specification of the portfolio identifiers (portfolio_durable_key) for which benchmark data should be loaded. Provide either single identifier or a list/tuple of identifiers
    date (str)... Format yyyy-mm-dd, Date for which the holdings should be extracted
    lib_utils.conn_dremio (lib_utils.connection object)... lib_utils.connection to Database Dremio Production (Tables used: holding_fact, security_master_dim, portfolio_dim)
    position_type (str 'O' or 'C')... Position type from Benchmark holdings, according to following logic 
                                C - weights from close    (default)
                                O - weights from open
    
    
    Returns:
    dfBMKHoldings (pd.DataFrame)... Dataframe containing the Holdings for the Benchmarks related to the portfolio_durable_keys based on DAL data 
    """
    if len(ids) == 1:
        ids = '(''{}'')'.format(str(ids[0]))
    else:
        ids = str(tuple(ids))
    
    # if isinstance(ids,int):
    #     ids = '(''{}'')'.format(str(ids))
    # else:
    #     ids = str(tuple(ids))
        
    if len(sDate)>1:
        date = str(tuple(sDate))
    else:
        #date = '({})'.format(str(date[0]))
        date = "('" + sDate[0] + "')"
        
    query = """
    SELECT cast(d.full_date AS DATE)                         AS effective_date,
       'Index'                             AS portfolio_type,
       'index_durable_key'                 AS durable_key_type,
       b.index_durable_key                 AS durable_key,
       'index_key'                         AS key_type,
       b.index_durable_key                 AS portfolio_id,
       -b.index_durable_key                AS benchmark_id,
       b.index_name                        AS portfolio_name,
       b.index_cdve_code,
       b.market_sector_des                 AS asset_class,
       s.country_issue_name,
       e.country_name,
       e.country_code3,
       s.currency_traded,
       'NA' as ml_high_yield_sector,
       s.agi_instrument_asset_type_description,
       s.security_durable_key,
       s.long_name,
       s.issuer_long_name,
       s.agi_instrument_type_description,
       g.GICS as gics_sector_description,
       bics.BICS as bics_sector_description,
       b3.BICS as bics_l3_description,
       b4.BBG as BBG_l2_description,
       s.collat_type,
       'NA' as gics_industry_group_description,
       'NA' as gics_industry_description,
       s.bloomberg_security_typ,
       s.id_isin,
       s.id_cusip,
       s.security_des,
       s.issue_description,
       s.issuer_id_bloomberg_company       AS ID_BB_COMPANY,
       h.constituent_weight_in_index / 100 AS weight
    FROM """ + lib_utils.dictDremio['dfIndexConst'] + """ h
    JOIN """ + lib_utils.dictDremio['dfIndexDim'] + """  b
         ON h.index_durable_key = b.index_durable_key
            AND b.is_current = true
    JOIN """ + lib_utils.dictDremio['dfDateDim'] + """ d
         ON d.date_key = h.date_key
    LEFT JOIN """ + lib_utils.dictDremio['dfSecurityMasterDim'] + """ s
              ON s.security_durable_key = h.security_durable_key
                 AND s.is_current = true
    LEFT JOIN """ + lib_utils.dictDremio['dfCountryDim'] + """ e
              ON s.country_issue = e.country_code 
    LEFT JOIN (
        SELECT distinct a.security_durable_key, l1_desc as GICS FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_GICS' and a.is_current = True and a.security_durable_key is not null
        ) g on s.security_durable_key = g.security_durable_key
    LEFT JOIN (
        SELECT distinct a.security_durable_key, l1_desc as BICS FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_BICS' and a.is_current = True and a.security_durable_key is not null
        ) bics on s.security_durable_key = bics.security_durable_key
    LEFT JOIN ( SELECT distinct a.security_durable_key, l3_desc as BICS FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_BICS' and a.is_current = True and a.security_durable_key is not null ) b3 
        on s.security_durable_key = b3.security_durable_key
    LEFT JOIN ( SELECT distinct a.security_durable_key, l2_desc as BBG FROM """ + lib_utils.dictDremio['dfSecuritySector'] + """ a 
        left join """ + lib_utils.dictDremio['dfSecurityMaster'] + """ b on a.security_durable_key = b.security_durable_key
        where a.sector_scheme = 'IDS_BBG' and a.is_current = True and a.security_durable_key is not null ) b4
        on s.security_durable_key = b4.security_durable_key
   WHERE h.index_durable_key in """ + ids + """ and cast(d.full_date AS DATE) in """ + date
    print('*********************************************')
    print('Loading index holdings as of '+ date + '...')
    print('*********************************************')
    print(query)

    dfBMKHoldings = pd.read_sql(query, lib_utils.conndremio)
    dfBMKHoldings = pd.merge(
        dfBMKHoldings,
        lib_utils.dfHierarchy,
        how="left",
        left_on="ID_BB_COMPANY",
        right_on="ID_BB_COMPANY",
        validate="m:1",
    )
    return dfBMKHoldings

def _load_xls_portfolio(lDatnames,lBenchmarkNames,lPortfolioTypes,lFolder,sLeftOn = 'id_isin'):

    print('*********************************************')
    print('Loading customized portfolios...')
    print('*********************************************')
    i = 0
    for i in range(len(lDatnames)):
        dfBenchmark = pd.read_excel(lFolder + lDatnames[i])
        sBenchmarkName = lBenchmarkNames[i]
        
        dfBenchmark['Benchmark'] = sBenchmarkName
        dfBenchmark['durable_key_type'] = 'xls'
        dfBenchmark['durable_key'] = lBenchmarkNames[i]
        dfBenchmark['benchmark_id'] = lDatnames[i]
        dfBenchmark['portfolio_id'] = lDatnames[i]
        dfBenchmark['portfolio_name'] = lBenchmarkNames[i]
        dfBenchmark['portfolio_type'] = lPortfolioTypes[i]
        
        if i == 0:
            dfAllBenchmarks = pd.DataFrame(dfBenchmark)
        else:
            dfAllBenchmarks = pd.concat([dfAllBenchmarks,dfBenchmark])   
    
    if sLeftOn == 'id_isin':
        dfAllBenchmarks = pd.merge(dfAllBenchmarks,lib_utils.dfSecurities, how = 'left',left_on = sLeftOn,right_on = 'ID_ISIN',validate = 'm:1')
        sSecurityDes = 'SECURITY_DES'
    elif sLeftOn == 'ID_BB_COMPANY' or sLeftOn == 'issuer_id_bloomberg_company':
        dfAllBenchmarks = pd.merge(dfAllBenchmarks,lib_utils.dfHierarchy, how = 'left',left_on = sLeftOn,right_on = 'ID_BB_COMPANY',validate = 'm:1')
        sSecurityDes = 'LONG_COMP_NAME'
    #dfAllBenchmarks.dropna(subset = ['ID_BB_COMPANY'],inplace = True)
    dfAllBenchmarks['ID_BB_COMPANY'] = dfAllBenchmarks['ID_BB_COMPANY'].astype(float)
    dfAllBenchmarks['ID_BB_PARENT_CO'] = dfAllBenchmarks['ID_BB_PARENT_CO'].astype(float)
    dfAllBenchmarks['ID_BB_ULTIMATE_PARENT_CO'] = dfAllBenchmarks['ID_BB_ULTIMATE_PARENT_CO'].astype(float)
    #dfAllBenchmarks['id_isin'] = dfAllBenchmarks['ID_ISIN']
    
    #dfAllBenchmarks['weight'] = dfAllBenchmarks['weight'] / dfAllBenchmarks.groupby('portfolio_name')['weight'].transform('sum')
    #dfAllBenchmarks['weight'] = dfAllBenchmarks['weight']/100
    dfAllBenchmarks["issuer_long_name"] = dfAllBenchmarks["LONG_COMP_NAME"]
    dfAllBenchmarks["long_name"] = dfAllBenchmarks[sSecurityDes]
    dfAllBenchmarks["gics_sector_description"] = dfAllBenchmarks["INDUSTRY_SECTOR"].str.upper()

    dfAllBenchmarks = pd.merge(
        dfAllBenchmarks,
        lib_utils.dfCountry,
        how="left",
        left_on="ULT_PARENT_CNTRY_DOMICILE",
        right_on="country_code",
        validate="m:1",
    )
    dfAllBenchmarks["country_issue_name"] = dfAllBenchmarks["country_name"]

    dfAllBenchmarks.dropna(subset=[sLeftOn], inplace=True)


    return dfAllBenchmarks
  
# AMF minimum standards cat 1 funds
def calc_corp_eligible_univ(dfPortfolio,iLookthrough = False):
    
    lSolidarityAssetCUSIPs = 	['PP30KWR12','PPE9EMHU7', 'PPE1399U4', 'PP9I7DSD7','PPE60EWP0','PPE83X7U8','PP9FDPE51','PPE1EGPJ3','PPE0AN3Y5']

    dfPortfolio.loc[(dfPortfolio.bloomberg_security_typ.isin(['Unit','Stapled Security']))&(dfPortfolio.agi_instrument_type_description == 'Funds'),'agi_instrument_asset_type_description'] = 'Equities'
    dfPortfolio.loc[dfPortfolio.agi_instrument_type_description == 'Funds / REIT','agi_instrument_asset_type_description'] = 'Equities'
    dfPortfolio['Cash'] = dfPortfolio['agi_instrument_asset_type_description'].isin(['Referential Instruments','Cash','Cash Equivalent','Not Available','Account'])
    dfPortfolio['Private Equity'] = dfPortfolio['bloomberg_security_typ'].isin(['Private Eqty'])
    dfPortfolio['Funds and Derivatives'] = dfPortfolio['agi_instrument_asset_type_description'].isin(['Derivative','Fund','Funds','Futures','Forward','Listed Options','OTC Options', 'Swaps','Open End Funds','Interest Rate Swap','Rights','Certificates','FX Forward','Listed Derivatives','Credit Default Swap','Futures -Financial','Total Return Swap','Structured Other','Equity Option','Cash Option','Commodity Future Option'])
    dfPortfolio['Real Estate'] = dfPortfolio['agi_instrument_asset_type_description'].isin(['Real Estate','Mortgage'])
    dfPortfolio['Loan'] = dfPortfolio['agi_instrument_asset_type_description'].isin(['Loan'])
    dfPortfolio['sov1'] = dfPortfolio['ml_high_yield_sector'].isin(['QGVT','SOV'])
    lSovTypes = ['Debts / Bonds / Fixed / Government',
    'Debts / Bonds / Fixed / Government',
    'Debts / Bonds / Variable / Government',
    'Debts / Bonds / Zero / Government',
    'Debts / MM / Fixed / Government',
    'Debts / MM / Variable / Government',
    'Debts / MM / Zero / Government',
    'Debts / MTN / Fixed / Government',
    'Debts / MTN / Variable / Government',
    'Debts / MTN / Zero / Government',
    'Debts / Municipals']
    dfPortfolio['sov2'] = dfPortfolio['agi_instrument_type_description'].isin(lSovTypes)
    dfPortfolio['sov3'] = dfPortfolio['agi_instrument_asset_type_description'].isin(['Government','Treas','Sovereign'])
    dfPortfolio.loc[dfPortfolio.gics_sector_description.str.upper() == 'GOVERNMENT','sov3'] = 1
    #dfPortfolio['sov4'] = dfPortfolio['collat_type'].str.contains('GOVT',na = False)
    #dfPortfolio['sov4'] = dfPortfolio['BBG_l2_description'].isin(['AGENCIES','TREASURIES','SOVEREIGN','SUPRANATIONAL','LOCAL_AUTHORITIES'])


    dfPortfolio['Sovereigns'] = dfPortfolio[['sov1','sov2','sov3']].max(axis = 1)
    dfPortfolio['Scope_Sov'] = dfPortfolio['Sovereigns']
    dfPortfolio['Solidarities'] = dfPortfolio['id_cusip'].isin(lSolidarityAssetCUSIPs)
    dfPortfolio['Corporates eligible assets'] = ~dfPortfolio[['Cash','Funds and Derivatives','Sovereigns','Solidarities','Private Equity','Real Estate','Loan']].max(axis = 1).astype(bool)

    # Overrides    
    dfPortfolio.loc[(dfPortfolio.gics_sector_description.isin(['REAL ESTATE','Real Estate']))&(dfPortfolio.agi_instrument_asset_type_description == 'Funds'),'Corporates eligible assets'] =  True
    #dfPortfolio.loc[(dfPortfolio.benchmark_id.isin([-344,166,-1537,94795]))&(dfPortfolio.Sovereigns == True),'Corporates eligible assets'] = True
    dfPortfolio.loc[(dfPortfolio.benchmark_id.isin([-344,166,94795]))&(dfPortfolio.Sovereigns == True),'Corporates eligible assets'] = True

    # dnsh
    #lNotEligible = ['Funds','Swaps','Futures','Referential Instruments']
    #dfPortfolio.loc[dfPortfolio.agi_instrument_asset_type_description.isin(lNotEligible),'Scope'] = False
    
    dfPortfolio['Scope_Corp']= dfPortfolio['Corporates eligible assets']
    dfPortfolio['Scope_NAV'] = True
    dfPortfolio['Scope_Inv'] = ~dfPortfolio[['Cash','Funds and Derivatives']].max(axis = 1).astype(bool)

    #dfPortfolio.loc[(dfPortfolio.gics_sector_description == 'REAL ESTATE')&(dfPortfolio.agi_instrument_asset_type_description == 'Funds'),'Scope_Inv'] =  True

    
    if iLookthrough == True:
        dfPortfolio.loc[dfPortfolio['agi_instrument_asset_type_description'] == 'Funds','Scope'] = True
        dfPortfolio.loc[dfPortfolio['agi_instrument_asset_type_description'] == 'Funds','Scope_Inv'] = True
        dfPortfolio.loc[dfPortfolio['agi_instrument_asset_type_description'] == 'Funds','Scope_Corp'] = True
    
    #checks:
    dfClassification = dfPortfolio.loc[dfPortfolio['Corporates eligible assets'] == True][['agi_instrument_asset_type_description','agi_instrument_type_description','ml_high_yield_sector','bloomberg_security_typ']].drop_duplicates()
    
    

    return dfPortfolio, dfClassification, lSovTypes

# export
def item_positions(lItems,lList):
    #lList = L
    #lItems = ['Coverage']
    N = []

    for i in range(len(lList)):
    
        if lList[i] in lItems:
            N.append(i)
            
    return N

def strip_tz_from_object_cols(df):
    for col in df.select_dtypes(include='object').columns:
        if df[col].apply(lambda x: isinstance(x, datetime) and x.tzinfo is not None).any():
            df[col] = df[col].apply(lambda x: x.replace(tzinfo=None) if isinstance(x, datetime) else x)
    return df

def excel_export(usewriterpf,dfExcelExport):


    with usewriterpf as writerpf:

        for d in dfExcelExport:
            
            x=d # dfExcelExport[d]
            
            dfRes           = x['data']
            dfRes           = strip_tz_from_object_cols(dfRes)
            sSheetName      = x['sSheetName']
            sPctCols        = x['sPctCols']
            sBars           = x['sBars']
            sWide           = x['sWide']
            sTrueFalse      = x['sTrueFalse']
            sFalseTrue      = x['sFalseTrue']
            sColsHighlight  = x['sColsHighlight']
            
            a_set = set(lib_utils.lSortColumns)
            b_set = set(dfRes.columns.to_list())
            srtcols = [*(x for x in lib_utils.lSortColumns if x in b_set), *(x for x in dfRes.columns.to_list() if x not in a_set)]    
            dfRes = dfRes.reindex(columns = srtcols)            
            
            # list of col headers
            dfRes = dfRes.reset_index()

            if 'index' in dfRes:
                dfRes = dfRes.drop('index', axis = 1)
            L = dfRes.columns.to_list()
            # define formats                        
            workbook  = writerpf.book

            fmtpct = workbook.add_format({'num_format': '0.0%'})
            fmtdcml = workbook.add_format({'num_format': '0.0'})
            
            header_format_highlight = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#76933C',
                'border': 1})
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'border': 1})
            falsefmt = workbook.add_format({'bg_color':'#FF0000'})
            truefmt = workbook.add_format({'bg_color':'#FF0000'})


            # write
            #dfRes.to_excel(writerpf,sheet_name = sSheetName,freeze_panes = [1,1],index = False)
            dfRes.to_excel(writerpf,sheet_name = sSheetName,freeze_panes = [1,0],index = False)
            
            # zoom
            writerpf.sheets[sSheetName].set_zoom(75)

            # autofilter
            writerpf.sheets[sSheetName].autofilter(0, 0, len(dfRes), len(dfRes.columns)-1)



            # format decimal points
            for n, value in pd.Series(L).items():
                if value not in lib_utils.lVarListInt:    
                    writerpf.sheets[sSheetName].set_column(n,n,None,fmtdcml)


            # format columns
            if sPctCols != False:
                for n in item_positions(sPctCols,L):
                    writerpf.sheets[sSheetName].set_column(n,n,None,fmtpct)
            
            if sWide != False:
                for n in item_positions(sWide,L):
                    writerpf.sheets[sSheetName].set_column(n,n,50)
            
            if sBars != False:
                for n in item_positions(sBars,L):
                    writerpf.sheets[sSheetName].conditional_format(1, n, len(dfRes), n, {'type': 'data_bar',
                                                                                             'bar_border_color':'#1F497D'})            
            
            if sTrueFalse != False:
                for n in item_positions(sTrueFalse,L):
                    writerpf.sheets[sSheetName].conditional_format(1, n, len(dfRes), n, {'type':     'text',
                                                                                    'criteria': 'containing',
                                                                                    'value':    'FALSE',
                                                                                    'format':   falsefmt})            
            if sFalseTrue != False:
                for n in item_positions(sFalseTrue,L):
                    writerpf.sheets[sSheetName].conditional_format(1, n, len(dfRes), n, {'type':     'text',
                                                                                    'criteria': 'containing',
                                                                                    'value':    'TRUE',
                                                                                    'format':   truefmt})  
            
            
            for col_num, value in pd.Series(L).items():
                writerpf.sheets[sSheetName].write(0, col_num, value, header_format)
                if value in lib_utils.dictDefinitions:
                    writerpf.sheets[sSheetName].write_comment(0, col_num, lib_utils.dictDefinitions[value], {'x_scale': 1.2, 'y_scale': 0.8})
            
            if sColsHighlight != False:
                for col_num in item_positions(sColsHighlight,L):
                    writerpf.sheets[sSheetName].write(0, col_num, pd.Series(L)[col_num], header_format_highlight)

def calc_decimal(dfData,lColNames):
    for i in lColNames:
        dfData[i] = dfData[i]/100
    return dfData

def calc_group_average(dfData,lVar,sGroupVar):
    dfData[lVar]            = dfData[lVar].astype(float)
    dfData[sGroupVar]       = dfData[sGroupVar].fillna('_OTHER')
                       
    dfRes                   = dfData.groupby(lib_utils.lGroupByVariablesShort + [sGroupVar],dropna=False).apply(lambda x: mylambda(x,lVar))
    dfRes['all issuer']     = dfRes.groupby(level = lib_utils.lGroupByVariablesShort,dropna=False)['issuer'].transform('sum')
    dfRes['issuer_pct']     = dfRes['issuer']/dfRes['all issuer']

    dfAll                   = dfData.copy()
    dfAll[sGroupVar]        = 'All'

    dfAll                   = dfAll.groupby(lib_utils.lGroupByVariablesShort + [sGroupVar],dropna=False).apply(lambda x: mylambda(x,lVar))
    dfAll['all issuer']     = dfAll.groupby(level = lib_utils.lGroupByVariablesShort,dropna=False)['issuer'].transform('sum')
    dfAll['issuer_pct']     = dfAll['issuer']/dfAll['all issuer']

    return pd.concat([dfRes,dfAll]).reset_index().sort_values(lib_utils.lGroupByVariablesShort + ['weight'],ascending = False).drop('all issuer',axis = 1)
    
def mylambda(x,lVar):
    
    #x = test.copy()
    #lVar = ['PSR_score_final_raw','PSR_score_final_bic']
    temp = np.ma.average(np.ma.masked_array(x[lVar], np.isnan(x[lVar])),weights=x['weight'],axis = 0)
    temp = pd.Series(temp,[lVar])
    temp.index = temp.index.map('_'.join)
    
    # weight per group
    temp2 = np.sum(x['weight'])
    temp2 = pd.Series(temp2,['weight'])
    
    # issuer count
    temp3 = len(np.unique(x['ID_BB_COMPANY']))
    temp3 = pd.Series(temp3,['issuer'])
    

    
    return pd.concat([pd.concat([temp,temp2]),temp3])

def sort_cols(lVarsIn):
    
    a_set = set(lib_utils.lSortColumns)
    b_set = set(lVarsIn)
    lVarsOut = [*(x for x in lib_utils.lSortColumns if x in b_set), *(x for x in lVarsIn if x not in a_set)]    

    return lVarsOut

def concat_portfolio_benchmark(dfAllPortfolios,dfBenchmarkBridge):
    dfOut = []
    for index, row in dfBenchmarkBridge.iterrows():                
        print(row['benchmark_durable_key'])
        print(row['portfolio_id'])
        dfTemp = pd.concat([dfAllPortfolios.loc[dfAllPortfolios.portfolio_id == row['portfolio_id']],dfAllPortfolios.loc[dfAllPortfolios.portfolio_id == row['benchmark_durable_key']]])
        dfTemp['portfolio_benchmark_group_name'] = row['portfolio_benchmark_group_name']
        dfOut.append(dfTemp)
        
    return pd.concat(dfOut)

def calc_active_weights(dfPfBenchmark,dfBenchmarkBridge,sVar,lVarList):
    
    dfPfBenchmark                   = dfPfBenchmark.loc[dfPfBenchmark['Scope_Corp'] == True]
    
    #if len(dfPfBenchmark[lVarList + [sVar]].drop_duplicates()) != len(dfPfBenchmark[lVarList].drop_duplicates()):
    #    raise Exception("ESG data not unique")            
    
    
    dfPfBenchmark['contribution']   = dfPfBenchmark['weight'].multiply(dfPfBenchmark[sVar],axis = 0)
    
    dfPfBenchmark.loc[~dfPfBenchmark[sVar].isna(),'adjweight'] = dfPfBenchmark.loc[~dfPfBenchmark[sVar].isna()].groupby(['portfolio_id','portfolio_name','benchmark_id','benchmark_name'])['weight'].transform('sum')
    
    dfPfBenchmark['contribution'] = dfPfBenchmark['contribution']/dfPfBenchmark['adjweight']
    
    dfPfBenchmark                   = concat_portfolio_benchmark(dfPfBenchmark,dfBenchmarkBridge)    

    lVarListBench                   = [s for s in lVarList if s not in ['portfolio_id','portfolio_name','benchmark_id','benchmark_name']]
    
    dfPfBenchmark                   = dfPfBenchmark.groupby(lVarList,
                                                            dropna=False).agg({'weight':lambda x: x.sum(min_count=1),
                                                                               'contribution':lambda x: x.sum(min_count=1),
                                                                               sVar:'mean'}).reset_index()
    
                                                                               
                                                                               
    dfPfBenchmark                   = dfPfBenchmark.groupby(lVarListBench, 
                                                            sort=False,
                                                            dropna=False)[['weight','contribution']+[sVar]].first().unstack('portfolio_type')
    
    dfPfBenchmark.columns           = [s1 + '_' + str(s2) for (s1,s2) in dfPfBenchmark.columns.tolist()]
    
    dfPfBenchmark[sVar]             = dfPfBenchmark[sVar + '_Fund'].fillna(dfPfBenchmark[sVar + '_Benchmark'])
    
    dfPfBenchmark                   = dfPfBenchmark.drop([sVar + '_Fund',sVar + '_Benchmark'],axis = 1)
    
    dfPfBenchmark['diff']           = dfPfBenchmark['contribution_Fund'].fillna(0) - dfPfBenchmark['contribution_Benchmark'].fillna(0)
  
    return dfPfBenchmark

def calc_yend():
    sql = """
    select * from """ + lib_utils.dictDremio['dfDateDim'] + """ where last_business_day_in_month_flag = 'Y' and "month" in (12) and "year" between 2018 and 2024 order by full_date desc
    """
    return pd.read_sql(sql,lib_utils.conndremio)

def calc_qend():
    sql = """
    select * from """ + lib_utils.dictDremio['dfDateDim'] + """ where last_business_day_in_month_flag = 'Y' and month in (3,6,9,12) and year between 2018 and 2025 order by full_date desc
    """
    return pd.read_sql(sql,lib_utils.conndremio)

def calc_mend():
    sql = """
    select * from """ + lib_utils.dictDremio['dfDateDim'] + """ where last_business_day_in_month_flag = 'Y' and "year" between 2023 and 2024 order by full_date desc
    """
    return pd.read_sql(sql,lib_utils.conndremio)

def get_names(dfPortfolios):
    
    lMissings = dfPortfolios.loc[dfPortfolios['issuer_long_name'].isna()]['ID_BB_COMPANY'].dropna().drop_duplicates() 
    print('**************************************')
    print('Looking for ' + str(len(lMissings)) + ' missing issuer names in bloomberg...')
    print('**************************************')
    if len(lMissings) > 100:
        print('******')
        print(str(len(lMissings)))
        if os.environ.get("BBG_API_PASSWORD") != "BBG":
            raise Exception('Not allowed to prevent API freeze')
    if len(lMissings) > 0:
        lIDBB = ('/companyid/' + lMissings.astype(str)).to_list()
        lib_utils.connBBG.start()
        dfBBG = lib_utils.connBBG.ref(lIDBB, ['LONG_COMP_NAME','COUNTRY_FULL_NAME'])
        dfBBG['ID_BB_COMPANY'] = dfBBG.ticker.str[11:].astype(float)
        dfBBG = dfBBG.pivot_table(index = 'ID_BB_COMPANY',columns = 'field', values = 'value',aggfunc='first').reset_index()
    
        dfPortfolios = pd.merge(dfPortfolios,dfBBG,how = 'left',left_on = 'ID_BB_COMPANY', right_on = 'ID_BB_COMPANY',validate = 'm:1',suffixes = ('','_BBG'))
        dfPortfolios['issuer_long_name'] = dfPortfolios['issuer_long_name'].fillna(dfPortfolios['LONG_COMP_NAME_BBG'])
        dfPortfolios['long_name'] = dfPortfolios['long_name'].fillna(dfPortfolios['LONG_COMP_NAME_BBG'])
        dfPortfolios['country_issue_name'] = dfPortfolios['country_issue_name'].fillna(dfPortfolios['COUNTRY_FULL_NAME'])
        
    return dfPortfolios

def search_cedar_vehicles(iRefresh,lIdentifiers = [],sType = []):
    
    if iRefresh == True:
        
        sql                         = """
                                select  *
                                from  """ + lib_utils.dictDremio['dfVehicleDim'] + """ 
                                where is_current = True
                                """
        dfVehicles                      = pd.read_sql(sql,lib_utils.conndremio)
        dfVehicles.to_pickle(lib_utils.sWorkFolder + 'dfVehicles.pkl')
        
    else:
        dfVehicles = pd.read_pickle(lib_utils.sWorkFolder + 'dfVehicles.pkl')
    
    if lIdentifiers:
        
        dfVehicles = dfVehicles.loc[dfVehicles[sType].astype(str).str.contains('|'.join(lIdentifiers),na = False)]
        dfVehicles = dfVehicles.loc[dfVehicles.vehicle_status == 'Active']
        dfVehicles = dfVehicles.loc[dfVehicles.is_leading_share_class == 'Y']

                                                                               
    return dfVehicles

def split_eq_fi_sleeves(dfAllPortfolios, lPortfolioIDs=None):
    if lPortfolioIDs is None:
        lPortfolioIDs = lib_utils.EQ_FI_SPLIT_IDS

    dfPortfolios = dfAllPortfolios.loc[dfAllPortfolios.portfolio_id.isin(lPortfolioIDs)]
    # drop from input data
    dfAllPortfolios = dfAllPortfolios.loc[~dfAllPortfolios.portfolio_id.isin(lPortfolioIDs)]
    dfFI = dfPortfolios.loc[dfPortfolios.agi_instrument_asset_type_description == 'Debts']
    dfEQ = dfPortfolios.loc[dfPortfolios.agi_instrument_asset_type_description == 'Equities']
    
    dfFI['wgt_sum'] = dfFI.groupby('portfolio_id')['weight'].transform('sum')
    dfEQ['wgt_sum'] = dfEQ.groupby('portfolio_id')['weight'].transform('sum')
    
    dfFI['weight'] = dfFI['weight']/dfFI['wgt_sum']
    dfEQ['weight'] = dfEQ['weight']/dfEQ['wgt_sum']
    
    dfFI['portfolio_id'] = dfFI['portfolio_id']*100
    dfFI['portfolio_name'] = dfFI['portfolio_name'] + '_FI'
    dfEQ['portfolio_id'] = dfEQ['portfolio_id']*10
    dfEQ['portfolio_name'] = dfEQ['portfolio_name'] + '_EQ'    
    
    dfAllPortfolios = pd.concat([dfAllPortfolios,dfFI,dfEQ])
    
    return dfAllPortfolios

def get_pm_names(dfAllPortfolios):
    
    lPortfolioIDs = dfAllPortfolios.loc[dfAllPortfolios.portfolio_type.isin(['Fund','Portfolio'])]['portfolio_durable_key'].drop_duplicates()
    
    sql = """
    SELECT * FROM """ + lib_utils.dictDremio['dfPortfolioManagerBridge'] + """ a 
    left join """ + lib_utils.dictDremio['dfPortfolioManagerDim'] + """ b on a.portfolio_manager_durable_key = b.portfolio_manager_durable_key where a.is_current = True and b.is_current = True """
    
    dfPMs = pd.read_sql(sql,lib_utils.conndremio)
    
    lList = dfPMs.loc[dfPMs.portfolio_durable_key.isin(lPortfolioIDs)]['last_name'].drop_duplicates().to_list()
    
    return ';'.join(lList)

def save_copies_with_sheetname_suffix(file_path):
    # Load the workbook
    workbook = openpyxl.load_workbook(file_path)
    sheet_names = workbook.sheetnames
    
    # List to store the names of the saved files
    saved_files = []
    
    # Get the directory and base file name to construct new file names
    directory, base_name = os.path.split(file_path)
    base_name_no_ext = os.path.splitext(base_name)[0]

    for sheet_name in sheet_names:
        # Create a new workbook for each sheet to save
        new_workbook = Workbook()
        new_workbook.remove(new_workbook.active)  # Remove the default sheet
        
        # Copy content from old sheet to new workbook
        source = workbook[sheet_name]
        target = new_workbook.create_sheet(sheet_name)
        for row in source:
            for cell in row:
                target[cell.coordinate].value = cell.value
        
        # Construct new file name
        new_file_name = f"{base_name_no_ext}_{sheet_name}.xlsx"
        new_file_path = os.path.join(directory, new_file_name)
        
        # Save the new workbook
        new_workbook.save(new_file_path)
        print(f"Saved {new_file_path}")
        
        # Append the new file name to the list
        saved_files.append(f"{base_name_no_ext}_{sheet_name}")
        
    # Close the original workbook
    workbook.close()
    
    # Return the list of saved files
    return saved_files
    
def compare_two_lists(dfPrev,dfCurrent,lIDs):
    
    dfCurrent                                   = dfCurrent.merge(dfPrev[lIDs],how='left',on=lIDs, indicator=True).rename(columns={'_merge': 'NEW_FLAG'})
    dMap                                    = {
                                                        'left_only':    'NEW',
                                                        'right_only':   'REMOVED',
                                                        'both':         ''
                                                    }
    dfCurrent['NEW_FLAG']                     = dfCurrent['NEW_FLAG'].map(dMap)

    dfExits                                 = dfPrev[lIDs + ['LONG_COMP_NAME']].merge(dfCurrent.drop(['NEW_FLAG','LONG_COMP_NAME'],axis = 1),how = 'left',on=lIDs, indicator=True).rename(columns={'_merge': 'NEW_FLAG'})
    #dfExits                                 = dfPrev[lIDs + ['ISSUER_NAME']].merge(dfCurrent.drop(['NEW_FLAG','ISSUER_NAME'],axis = 1),how = 'left',on=lIDs, indicator=True).rename(columns={'_merge': 'NEW_FLAG'})

    dMap                                    = {
                                                        'left_only':    'REMOVED',
                                                        'right_only':   'NEW',
                                                        'both':         ''
                                                    }
    dfExits['NEW_FLAG']                       = dfExits['NEW_FLAG'].map(dMap)
                                                                               
    dfAdditions                             = dfCurrent[dfCurrent['NEW_FLAG'] == 'NEW']
    dfExits                                 = dfExits[dfExits['NEW_FLAG'] == 'REMOVED'][lIDs + ['LONG_COMP_NAME']]
    #dfExits                                 = dfExits[dfExits['NEW_FLAG'] == 'REMOVED'][lIDs + ['ISSUER_NAME']]
         
    return dfCurrent.copy(),dfAdditions,dfExits

def extract_and_join(input_string):
    # Find all substrings within <>
    matches = re.findall(r'<(.*?)>', input_string)
    # Join the matches with ;
    result = ';'.join(matches)
    return result

def compare_lists(list_a, list_b):
    # Split the input strings into lists
    list_a_split = list_a.split(';')
    list_b_split = list_b.split(';')
    
    # Convert lists to sets and find the difference
    difference = set(list_a_split) - set(list_b_split)
    
    # Join the result with ;
    result = ';'.join(difference)
    return result

def calculate_ratios(df, group_col, id_col, bool_columns):
    result = (
        df.groupby(group_col)
        .apply(lambda group: {
            f"{col}_ratio": group[group[col]][id_col].nunique() / group[id_col].nunique()
            for col in bool_columns
        })
        .reset_index(name="ratios")
    )

    # Expand the dictionary into separate columns for readability
    ratios_df = pd.DataFrame(result["ratios"].tolist())
    result = pd.concat([result[group_col], ratios_df], axis=1)
    return result

def extract_max_year(expression):
    if pd.isna(expression):
        return None  # Handle NaN or None entries
    # Ensure the expression is treated as a string
    expression = str(expression)
    # Use regular expression to find all years in the format 'FYYYYY' or 'YYYY'
    years = re.findall(r'FY(\d{4})|(\d{4})', expression)
    if not years:
        return None  # or some default value, e.g., float('nan')
    # Flatten the list of tuples and filter out None values
    years = [float(year) for match in years for year in match if year]
    # Return the maximum year as float
    return max(years)


# Columns shared across all holding loaders
def _ensure_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """Ensure all required columns exist in ``df``."""

    for col in columns:
        if col not in df.columns:
            df[col] = np.nan
    return df[list(columns)]


def _standardize_holdings(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """Return ``df`` with required columns, types, and default values."""

    df = _ensure_columns(df, columns)
    df = df.fillna({col: lib_utils.HOLDING_DEFAULTS[col] for col in columns})
    df = df.astype({col: lib_utils.HOLDING_DTYPES[col] for col in columns}, errors="ignore")

    df = _assign_region(df)
    return df


def _assign_region(df: pd.DataFrame) -> pd.DataFrame:
    df["region"] = np.nan
    df.loc[df.country_code3.isin(lib_utils.lNA), "region"] = "NA"
    df.loc[df.country_code3.isin(lib_utils.lAPAC), "region"] = "APAC"
    df.loc[df.country_code3.isin(lib_utils.lEROP), "region"] = "EROP"
    df["region"] = df["region"].fillna("EM")
    return df


def load_holdings(
    source: str,
    *args,
    columns: Sequence[str] = lib_utils.HOLDING_COLUMNS,
    **kwargs,
):
    """Unified loader for holdings data from multiple sources."""

    try:
        loader_name = lib_utils.HOLDINGS_LOADERS[source.lower()]
    except KeyError as exc:
        raise ValueError(f"Unknown data source: {source}") from exc
    loader = getattr(sys.modules[__name__], loader_name, None)
    if loader is None:
        raise ValueError(f"Loader function {loader_name} is not defined")

    result = loader(*args, **kwargs)
    if isinstance(result, tuple):
        df = _standardize_holdings(result[0], columns)
        return (df,) + result[1:]
    else:
        return _standardize_holdings(result, columns)
