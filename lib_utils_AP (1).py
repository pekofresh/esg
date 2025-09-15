# -*- coding: utf-8 -*-
"""
Created on Fri Dec 30 08:45:01 2022

@author: A00008106
"""
from pathlib import Path
import os
import sys
import numpy as np
import pandas as pd
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


import pickle
import math
#import matplotlib.pyplot as plt
from statsmodels.distributions.empirical_distribution import ECDF
from datetime import datetime
from typing import Any, Mapping, Sequence

#import blpapi
#import pdblp
from pulp import *

# own modules
from connections import *

# central constants
HOLDING_COLUMNS = [
    "effective_date",
    "portfolio_type",
    "durable_key_type",
    "durable_key",
    "key_type",
    "key",
    "portfolio_id",
    "benchmark_id",
    "portfolio_name",
    "benchmark_name",
    "asset_class",
    "country_issue_name",
    "country_name",
    "country_code3",
    "currency_traded",
    "ml_high_yield_sector",
    "agi_instrument_asset_type_description",
    "security_durable_key",
    "id_underlying_durable_key",
    "issuer_long_name",
    "long_name",
    "agi_instrument_type_description",
    "gics_sector_description",
    "bics_sector_description",
    "bics_l3_description",
    "BBG_l2_description",
    "collat_type",
    "gics_industry_group_description",
    "gics_industry_description",
    "bloomberg_security_typ",
    "id_isin",
    "id_cusip",
    "security_des",
    "issue_description",
    "ID_BB_COMPANY",
    "weight",
    "futures_exposure_firm",
    "market_value_clean_portfolio",
    "market_value_clean_firm",
    "portfolio_currency_code",
    "eu_sfdr_sustainability_category",
    "internal_asset_class",
    "domicile",
    "region",
]

EQ_FI_SPLIT_IDS = [10000136, 10000133, 10000131]

XLS_DEFAULT_PORTFOLIO_ID = 10
XLS_DEFAULT_BENCHMARK_ID = 100
XLS_DEFAULT_EFFECTIVE_DATE = "2025-08-28"
DEFAULT_BICS_SECTOR_DESCRIPTION = "NA"
DEFAULT_BBG_L2_DESCRIPTION = "NA"
DEFAULT_COLLAT_TYPE = "NA"

REFINITIV_EXCLUDE_FILES = {
    "archiv",
    "MA_holding_description_List1_20241125.csv",
    "MA_holding_description_List2_20241125.csv",
}

REFINITIV_DROP_COLUMNS = ["Asset_Type"]

REFINITIV_RENAME_COLUMNS = {
    "ISIN": "portfolio_id",
    "Allocation_ISIN": "id_isin",
    "Security_Description": "portfolio_name",
    "Allocation_Percentage": "weight",
    "Allocation_Item": "long_name",
    "Allocation_Asset_Type": "agi_instrument_asset_type_description",
    "Allocation_Date": "effective_date",
}

REFINITIV_PRE_MERGE_CONSTANTS = {
    "Fund_Type": "External Fund",
    "ID_Type": "ISIN",
}

REFINITIV_POST_MERGE_COPY = {
    "gics_sector_description": "INDUSTRY_SECTOR",
    "issuer_long_name": "LONG_COMP_NAME",
    "country_issue_name": "ULT_PARENT_CNTRY_DOMICILE",
}

REFINITIV_POST_MERGE_CONSTANTS = {
    "portfolio_type": "External Target Fund",
    "benchmark_id": 0,
    "gics_industry_group_description": "",
    "gics_industry_description": "",
    "bloomberg_security_typ": np.nan,
    "ml_high_yield_sector": np.nan,
    "agi_instrument_type_description": np.nan,
    "id_cusip": np.nan,
}


# data types and default values for holdings
NUMERIC_HOLDING_COLUMNS = [
    "benchmark_id",
    "ID_BB_COMPANY",
    "weight",
    "futures_exposure_firm",
    "market_value_clean_portfolio",
    "market_value_clean_firm",
]

HOLDING_DTYPES = {col: "float64" for col in NUMERIC_HOLDING_COLUMNS}
HOLDING_DEFAULTS = {col: 0 for col in NUMERIC_HOLDING_COLUMNS}
HOLDING_DEFAULTS.update(
    {
        "bics_sector_description": DEFAULT_BICS_SECTOR_DESCRIPTION,
        "BBG_l2_description": DEFAULT_BBG_L2_DESCRIPTION,
        "collat_type": DEFAULT_COLLAT_TYPE,
    }
)
for col in HOLDING_COLUMNS:
    HOLDING_DEFAULTS.setdefault(col, "NA")
    HOLDING_DTYPES.setdefault(col, "object")


def apply_column_mappings(
    df: pd.DataFrame,
    rename_map: Mapping[str, str] | None = None,
    drop_cols: Sequence[str] | None = None,
    copy_map: Mapping[str, str] | None = None,
    fill_values: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Apply common column transformations."""

    if drop_cols:
        df = df.drop(columns=list(drop_cols), errors="ignore")
    if rename_map:
        df = df.rename(columns=rename_map)
    if copy_map:
        for dest, src in copy_map.items():
            df[dest] = df[src]
    if fill_values:
        for col, val in fill_values.items():
            df[col] = val
    return df

import lib_sma_AP as sma
  
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()



# user
email = os.environ.get("COMPANY_EMAIL")
if not email:
    raise EnvironmentError("COMPANY_EMAIL environment variable is required")

# SusIE
conn = susie_connection(email=email, database_type=os.environ.get("SUSIE_DB_TYPE", "prod"))
#conntest                    = susie_connection(email = email,database_type = 'test')
#conndev                     = susie_connection(email = email,database_type = 'dev')


# dremio

conngenie = genie_connection()
user = os.environ.get("USER_INITIALS")
if not user:
    raise EnvironmentError("USER_INITIALS environment variable is required")
conndremio = dbx_connection(user=user)
conndremio_dev = dbx_connection_dev(user=user)

#conndremio                  = dremio_connection(email = email)

# bbg
#connBBG                     = pdblp.BCon(debug=True, port=8194, timeout=5000)
sys.path.append(str(Path(os.getcwd()).home()).replace("\\", "/") +'/Allianz Global Investors/Sustainability Standards and Analytics-SMA - Documents/11. Tools/MERLIN 2.0/merlin')

# paths
sPathNameGeneral            = str(Path(os.getcwd()).home()).replace("\\", "/") + '/Allianz Global Investors/Sustainability Standards and Analytics-SMA - Documents/' 
sPathNameData               = str(Path(os.getcwd()).home()).replace("\\", "/") + '/Allianz Global Investors/Sustainability Standards and Analytics-SMA - Documents/11. Tools/MERLIN 2.0/data/' 

# sharepoint
sWorkFolder                 = sPathNameGeneral + '15. Investment process/Impact Assessment/'
sTargetFundsFolder          = sPathNameGeneral + '15. Investment process/PAI Consideration for Multi Asset FoF/'
sBenchmarkFolder            = sPathNameGeneral + '15. Investment process/Universes/'  
sExclusionsFolder           = sPathNameGeneral + '2. Exclusion/3 BAU Process/1 Exclusion Lists/2023 November/0. ISS Export/'


# unity catalog


# dremio data sets
dictDremio =                {# 
                             'dfIDSHoldings':       '"Common Data Model".ESG."ids_target_fund"."ids_holdings_level_esg"',
                             'dfIDSFund':           '"Common Data Model".ESG."ids_target_fund"."ids_fund_level_esg"',
                             'dfVehicleDim':        '"Common Data Model"."Dimensional Model"."vehicle_dim"',
                             'dfPortfolioDim':      '"Common Data Model"."Dimensional Model"."portfolio_dim"',
                             'dfPortfolioPerf':     '"Common Data Model"."Dimensional Model"."portfolio_performance_fact"',
                             'dfPortfolioBenchmarkBridge': '"Common Data Model"."Dimensional Model"."portfolio_benchmark_bridge"',
                             'dfPortfolioManager':  '"Common Data Model"."Portfolio"."portfolio_manager_latest"',
                             'dfPortfolioTotal':    '"Common Data Model"."Dimensional Model"."portfolio_total_fact"',
                             'dfHoldings':          '"Common Data Model"."Dimensional Model"."holding_fact"',

                             'dfBenchmarkDim':      '"Common Data Model"."Dimensional Model"."benchmark_dim"',
                             'dfBenchmarkPerf':     '"Common Data Model"."Dimensional Model"."benchmark_performance_fact"',
                             'dfBenchmarkConst':    '"Common Data Model"."Dimensional Model"."benchmark_position_fact"',
                             'dfIndexConst':        '"Common Data Model"."Dimensional Model"."index_position_fact"',
                             'dfIndexDim':          '"Common Data Model"."Dimensional Model"."index_dim"',
                             
                             'dfEntityDim':         '"Common Data Model"."Dimensional Model"."entity_dim"',
                             'dfSecurityMasterDim': '"Common Data Model"."Dimensional Model"."security_master_dim"',
                             'dfCountryDim':        '"Common Data Model"."Dimensional Model"."country_dim"',                             
                             'dfGICS':              '"@Alexandr.Pekelis@allianzgi.com"."gics_sectors"',

                             'dfDateDim':           '"Common Data Model"."Dimensional Model"."date_dim"',
                             'dfSecuritySector':    '"Common Data Model"."Dimensional Model"."security_sectors_bridge"',
                             'dfSecurityMaster':    '"Common Data Model".Security."security_master"',
    
    
                                # Business Views
                              'dfSRI':              '"Common Data Model".ESG."business_views"."sri_rating_universe"',
                              'dfKPI':              '"Common Data Model".ESG."business_views"."msci"."msci_esg_carbon_metrics"',
                              'dfKPISov':           '"Common Data Model".ESG."business_views"."msci"."msci_esg_government_carbon"',
                              'dfSus':              '"Common Data Model".ESG."business_views"."regulation_issuer"."sustainable_investment_share"',
                              'dfTaxo':             '"Common Data Model".ESG."business_views"."regulation_issuer"."taxonomy_alignment"',
                              'dfDNSH':             '"Common Data Model".ESG."business_views"."regulation_issuer"."dnsh_flag"',
                              'dfSusMinExclusions': '"Common Data Model".ESG."business_views"."iss_exclusion_lists"."susie_exclusion_lists_pivot"',
                              'dfTowardsSustainability': '"Common Data Model".ESG."business_views"."iss_exclusion_lists"."susie_exclusion_lists_pivot"',
                              'dfFirmwide':         '"Common Data Model".ESG."business_views"."iss_exclusion_lists"."susie_exclusion_lists_pivot"',
                              'dfFH':               '"Common Data Model".ESG."business_views"."freedom_house"."freedom_house_fact"',
                              'dfMSCI':             '"Common Data Model".ESG."business_views".msci."msci_esg_scores"',
                              'dfMSCISov':          '"Common Data Model".ESG."business_views".msci."msci_esg_government_ratings"',
                              'dfMSCIPAI':          '"Common Data Model".ESG."business_views".msci."msci_sfdr_pai_kpi"',
                              'dfSBTI':             '"Common Data Model".ESG."business_views".sbti."sbti_companies_taking_action_fact"',
                              'dfTrucost':          '"Common Data Model".ESG."business_views".trucost."carbon_only"',
                              'dfITR':              '"Common Data Model".ESG."business_views".msci."msci_sm_ta_itr_warming_potential_fact"',


                              # raw data
                              'dfMoody':          '"@Alexandr.Pekelis@allianzgi.com"."Moodys_ESG"',
                              'dfESG_C':            '"Common Data Model".ESG.MSCI."esg_carbon_metrics"',
                              'dfGB':               '"Common Data Model".Security.Bloomberg.bonds."sustainableDebtBondV2_out"',

                              # Dimensional Model
                              'dfSDG':              '"Common Data Model"."Dimensional Model"."sma_sdg_alignment_share_fact"',
                              'dfNZ':               '"Common Data Model"."Dimensional Model"."net_zero_alignment_fact"',
                              'dfKPIScope3':        '"Common Data Model"."Dimensional Model"."carbon_intensity_scope3_fact"',
                              'dfPAI':              '"Common Data Model"."Dimensional Model"."pai_indicator_fact"',
                              'dfFemaleBoard':      '"Common Data Model"."Dimensional Model"."msci_esg_governance_factors_fact"',
                              'dfSocial':           '"Common Data Model"."Dimensional Model"."msci_esg_social_timeseries_fact"'}

dictDremio = {                  'dfIDSHoldings':  'dal_dev_silver.ids_target_funds.ids_holdings_level_esg',
                                'dfIDSFund': 'dal_dev_silver.ids_target_funds.ids_fund_level_esg',
                                'dfVehicleDim': 'dal_dev_gold.dimensions.vehicle_d',
                                'dfPortfolioDim': 'dal_dev_gold.dimensions.portfolio_d',
                                'dfPortfolioPerf': 'dal_dev_gold.dimensions.portfolio_performance_f',
                                'dfPortfolioBenchmarkBridge': 'dal_dev_gold.dimensions.portfolio_benchmark_b',
                                'dfPortfolioTotal': 'dal_dev_gold.dimensions.portfolio_total_f',
                                'dfPortfolioManagerBridge':  'dal_dev_gold.dimensions.portfolio_manager_b',
                                'dfPortfolioManagerDim':  'dal_dev_gold.dimensions.portfolio_manager_d',

                                'dfHoldings': 'dal_dev_gold.dimensions.holding_f',
                                'dfBenchmarkDim': 'dal_dev_gold.dimensions.benchmark_d',
                                'dfBenchmarkPerf': 'dal_dev_gold.dimensions.benchmark_performance_f',
                                'dfBenchmarkConst': 'dal_dev_gold.dimensions.benchmark_position_f',
                                'dfIndexConst': 'dal_dev_gold.dimensions.index_position_f',
                                'dfIndexDim': 'dal_dev_gold.dimensions.index_d',
                                'dfEntityDim': 'dal_dev_gold.dimensions.entity_d',
                                'dfSecurityMasterDim': 'dal_dev_gold.dimensions.security_master_d',
                                'dfCountryDim': 'dal_dev_gold.dimensions.country_d',
                                'dfDateDim': 'dal_dev_gold.dimensions.date_d',
                                'dfSecuritySector': 'dal_dev_gold.dimensions.security_sectors_b',
                                'dfSecurityMaster': 'dal_dev_gold.dimensions.security_master_d',
                                'dfGIDPMaster': 'dal_dev_gold.bbo.instrument_company',

                                'dfSRI':              'dal_dev_silver.sri_mastered.ss_sri_afi',
                                'dfKPI':              'dal_dev_gold.dimensions.msci_esg_carbon_metrics_f',
                                'dfKPISov':           'dal_dev_gold.dimensions.msci_esg_government_carbon_f',
                                'dfSus':              'dal_dev_gold.dimensions.sustainable_investment_share_f',
                                'dfTaxo':             'dal_dev_gold.dimensions.taxonomy_alignment_f',
                                'dfDNSH':             'dal_dev_gold.dimensions.dnsh_flag_f',
                                'dfSusMinExclusions': 'dal_dev_gold.dimensions.susie_exclusion_lists_pivot_f',
                                'dfTowardsSustainability': 'dal_dev_gold.dimensions.susie_exclusion_lists_pivot_f',
                                'dfFirmwide':         'dal_dev_gold.dimensions.susie_exclusion_lists_pivot_f',
                                'dfFH':               'dal_dev_gold.dimensions.freedom_house_f',
                                'dfMSCI':             'dal_dev_gold.dimensions.msci_esg_scores_f',
                                'dfMSCISov':          'dal_dev_gold.dimensions.msci_esg_government_ratings_f',
                                'dfMSCIPAI':          'dal_dev_gold.dimensions.msci_sfdr_pai_kpi_f',
                                'dfSBTI':             'dal_dev_gold.dimensions.sbti_companies_taking_action_f',
                                'dfTrucost':          'dal_dev_gold.dimensions.trucost_carbon_only_f',
                                'dfITR':              'dal_dev_gold.dimensions.msci_sm_ta_itr_warming_potential_f',

  
  
                                'dfESG_C': 'dal_dev_silver.msci_esg_api.esg_carbon_metrics',
                                'dfGB': 'dal_dev_silver.bbo.sustainabledebtbondv2_out',
                                  
                                'dfSDG': 'dal_dev_gold.dimensions.sma_sdg_alignment_share_f',
                                'dfNZ': 'dal_dev_gold.dimensions.net_zero_alignment_f',
                                'dfKPIScope3': 'dal_dev_gold.dimensions.carbon_intensity_scope3_f',
                                'dfPAI': 'dal_dev_gold.dimensions.pai_indicator_f',
                                'dfFemaleBoard': 'dal_dev_gold.dimensions.msci_esg_governance_factors_f',
                                'dfSocial': 'dal_dev_gold.dimensions.msci_esg_social_timeseries_f'}
  
dictDremio = {                  'dfIDSHoldings':  'dal_prod_silver.ids_target_funds.ids_holdings_level_esg',
                                'dfIDSFund': 'dal_prod_silver.ids_target_funds.ids_fund_level_esg',
                                'dfVehicleDim': 'dal_prod_gold.dimensions.vehicle_d',
                                'dfPortfolioDim': 'dal_prod_gold.dimensions.portfolio_d',
                                'dfPortfolioPerf': 'dal_prod_gold.dimensions.portfolio_performance_f',
                                'dfPortfolioBenchmarkBridge': 'dal_prod_gold.dimensions.portfolio_benchmark_b',
                                'dfPortfolioTotal': 'dal_prod_gold.dimensions.portfolio_total_f',
                                'dfPortfolioManagerBridge':  'dal_prod_gold.dimensions.portfolio_manager_b',
                                'dfPortfolioManagerDim':  'dal_prod_gold.dimensions.portfolio_manager_d',

                                'dfHoldings': 'dal_prod_gold.dimensions.holding_f',
                                'dfBenchmarkDim': 'dal_prod_gold.dimensions.benchmark_d',
                                'dfBenchmarkPerf': 'dal_prod_gold.dimensions.benchmark_performance_f',
                                'dfBenchmarkConst': 'dal_prod_gold.dimensions.benchmark_position_f',
                                'dfIndexConst': 'dal_prod_gold.dimensions.index_position_f',
                                'dfIndexDim': 'dal_prod_gold.dimensions.index_d',
                                'dfEntityDim': 'dal_prod_gold.dimensions.entity_d',
                                'dfSecurityMasterDim': 'dal_prod_gold.dimensions.security_master_d',
                                'dfCountryDim': 'dal_prod_gold.dimensions.country_d',
                                'dfDateDim': 'dal_prod_gold.dimensions.date_d',
                                'dfSecuritySector': 'dal_prod_gold.dimensions.security_sectors_b',
                                'dfSecurityMaster': 'dal_prod_gold.dimensions.security_master_d',
                                'dfGIDPMaster': 'dal_prod_gold.bbo.instrument_company',

                                'dfSRI':              'dal_prod_silver.sri_mastered.ss_sri_afi',
                                'dfPSR':              'dal_prod_silver.sri_mastered.ss_sri_afi',

                                'dfKPI':              'dal_prod_gold.dimensions.msci_esg_carbon_metrics_f',
                                'dfKPISov':           'dal_prod_gold.dimensions.msci_esg_government_carbon_f',
                                'dfSus':              'dal_prod_gold.dimensions.sustainable_investment_share_f',

                                'dfTaxo':             'dal_prod_gold.dimensions.taxonomy_alignment_f',
                                'dfTaxoAct':             'dal_prod_silver.iss_eu_taxonomy.taxo_activity_level',

                                'dfDNSH':             'dal_prod_gold.dimensions.dnsh_flag_f',
                                'dfSusMinExclusions': 'dal_prod_gold.dimensions.susie_exclusion_lists_pivot_f',
                                'dfSusMinExclusionsEnh': 'dal_prod_gold.dimensions.susie_exclusion_lists_pivot_f',
                                'dfTowardsSustainability': 'dal_prod_gold.dimensions.susie_exclusion_lists_pivot_f',
                                'dfFirmwide':         'dal_prod_gold.dimensions.susie_exclusion_lists_pivot_f',
                                'dfFH':               'dal_prod_gold.dimensions.freedom_house_f',
                                'dfMSCI':             'dal_prod_gold.dimensions.msci_esg_scores_f',
                                'dfMSCISov':          'dal_prod_gold.dimensions.msci_esg_government_ratings_f',
                                'dfMSCIPAI':          'dal_prod_gold.dimensions.msci_sfdr_pai_kpi_f',
                                'dfSBTI':             'dal_prod_gold.dimensions.sbti_companies_taking_action_f',
                                'dfTrucost':          'dal_prod_gold.dimensions.trucost_carbon_only_f',
                                'dfITR':              'dal_prod_gold.dimensions.msci_sm_ta_itr_warming_potential_f',

  
  
                                'dfESG_C': 'dal_prod_silver.msci_esg_api.esg_carbon_metrics',
                                'dfGB': 'dal_prod_silver.bbo.sustainabledebtbondv2_out',
                                  
                                'dfSDG': 'dal_prod_gold.dimensions.sma_sdg_alignment_share_f',
                                'dfNZ': 'dal_prod_gold.dimensions.net_zero_alignment_f',
                                'dfKPIScope3': 'dal_prod_gold.dimensions.carbon_intensity_scope3_f',
                                'dfPAI': 'dal_prod_gold.dimensions.pai_indicator_f',
                                'dfFemaleBoard': 'dal_prod_gold.dimensions.msci_esg_governance_factors_f',
                                'dfSocial': 'dal_prod_gold.dimensions.msci_esg_social_timeseries_f',
                                
                                'dfCVARTrans': 'dal_prod_gold.dimensions.msci_sm_cvar_transit_risk_opps_f',
                                'dfCVARPhys': 'dal_prod_gold.dimensions.msci_sm_cvar_phys_risk_opps_2deg_and_below_f',
                                'dfISS': 'dal_prod_gold.dimensions.iss_corp_controversy_issuers_f',
                                'dfVarDef': 'zenith_dev_gold.esg_impact_analysis.esg_var_def',
                                'dfLoadConfig': 'zenith_dev_gold.esg_impact_analysis.esg_load_config',
                                'dfInstrument': 'zenith_dev_gold.esg_impact_analysis.v_instrument_company'}
  
  
    
  

  
  
  
  
 
# decimals
lVarsDremioPct =                                                ['SIS current',
                                                                'SIS new DNSH',
                                                                'SIS new DNSH + NZAS boost',
                                                                'sustainable_investment_share_pre_dnsh',
                                                                'sustainable_investment_share_post_dnsh',
                                                                'sustainable_investment_share_environmental_post_dnsh',
                                                                'sustainable_investment_share_social_post_dnsh',
                                                                'eu_taxonomy_aligned_turnover_percent_pre_dnsh',
                                                                'eu_taxonomy_aligned_turnover_percent_post_dnsh',
                                                                'eu_taxonomy_aligned_capex_percent_post_dnsh',
                                                                'eu_taxonomy_aligned_opex_percent_post_dnsh',
                                                                'eu_taxonomy_eligible_turnover_percent_post_dnsh',
                                                                 'eu_taxonomy_aligned_turnover_enabling_activities_percent_post_dnsh',
                                                                 'eu_taxonomy_aligned_capex_enabling_activities_percent_post_dnsh',
                                                                 'eu_taxonomy_aligned_opex_enabling_activities_percent_post_dnsh',
                                                                 'eu_taxonomy_aligned_turnover_transitional_activities_percent_post_dnsh',
                                                                 'eu_taxonomy_aligned_capex_transitional_activities_percent_post_dnsh',
                                                                 'eu_taxonomy_aligned_opex_transitional_activities_percent_post_dnsh',
                                                                'EUTaxTotalEligibleActivityRev',
                                                                'EUTaxTotalAlignedRevenues',
                                                                'EUTaxTotalLikelyAlignedRevenues',
                                                                'female_directors_pct',
                                                                'gender_pay_gap_ratio',
                                                                'board_indep_pct',
                                                                'sdg_01_share_post_dnsh',
                                                                'sdg_01_share_pre_dnsh',
                                                                'sdg_02_share_post_dnsh',
                                                                'sdg_02_share_pre_dnsh',
                                                                'sdg_03_share_post_dnsh',
                                                                'sdg_03_share_pre_dnsh',
                                                                'sdg_04_share_post_dnsh',
                                                                'sdg_04_share_pre_dnsh',
                                                                'sdg_05_share_post_dnsh',
                                                                'sdg_05_share_pre_dnsh',
                                                                'sdg_06_share_post_dnsh',
                                                                'sdg_06_share_pre_dnsh',
                                                                'sdg_07_share_post_dnsh',
                                                                'sdg_07_share_pre_dnsh',
                                                                'sdg_08_share_post_dnsh',
                                                                'sdg_08_share_pre_dnsh',
                                                                'sdg_09_share_post_dnsh',
                                                                'sdg_09_share_pre_dnsh',
                                                                'sdg_10_share_post_dnsh',
                                                                'sdg_10_share_pre_dnsh',
                                                                'sdg_11_share_post_dnsh',
                                                                'sdg_11_share_pre_dnsh',
                                                                'sdg_12_share_post_dnsh',
                                                                'sdg_12_share_pre_dnsh',
                                                                'sdg_13_share_post_dnsh',
                                                                'sdg_13_share_pre_dnsh',
                                                                'sdg_14_share_post_dnsh',
                                                                'sdg_14_share_pre_dnsh',
                                                                'sdg_15_share_post_dnsh',
                                                                'sdg_15_share_pre_dnsh',
                                                                'sdg_16_share_post_dnsh',
                                                                'sdg_16_share_pre_dnsh',
                                                                'sdg_17_share_post_dnsh',
                                                                'sdg_17_share_pre_dnsh',
                                                                'sdg_alignment_share_post_dnsh',
                                                                'sdg_alignment_share_pre_dnsh',
                                                                'pai_5',
                                                                'pai_12',
                                                                'pai_13',
                                                                'pct_nonrenew_consump_prod',
                                                                'women_workforce_pct',
                                                                'women_exec_mgmt_pct_recent',
                                                                'women_directors_pct_recent',
                                                                'SIS_currentPC - pre DNSH',	'SIS_currentPC_CURRENT',	'SIS_newPC - pre DNSH',	'SIS_newPC_CURRENT',	'SIS_newPC_COMBO',
                                                                'CBN_RENEW_ENERGY_USAGE_PCT_RECENT'
                                                                ]

# integers
lVarListInt =                                                   ['portfolio_id',
                                                                'benchmark_id',
                                                                'benchmark_id_pivot',
                                                                'market_value_clean_firm',
                                                                'ID_BB_COMPANY',
                                                                'id_bb_company',
                                                                'id_exl_list',
                                                                'id_provider_issuer',
                                                                'issuerID',
                                                                'ID_EXL_ISSUER_LIST',
                                                                'ID_EXL_LIST',
                                                                'EDM_PTY_ID',
                                                                'ID_PROVIDER_ISSUER',
                                                                'N_ISSUERS_INITIAL',
                                                                'N_ISSUERS',	
                                                                'N_ADDITIONS',	
                                                                'N_EXITS'
]

# portfolio level identifier
lGroupByVariables =                                             ['portfolio_type',
                                                                'portfolio_name',
                                                                'portfolio_id',
                                                                'benchmark_id',
                                                                'effective_date',
                                                                'eu_sfdr_sustainability_category',
                                                                'domicile',
                                                                'internal_asset_class',
                                                                'market_value_clean_firm']

lGroupByVariablesShort =                                        ['portfolio_type',
                                                                'portfolio_name']

# sector level identifier
lVarListSectors =                                               ['portfolio_type',
                                                                'benchmark_name',
                                                                'portfolio_benchmark_group_name',
                                                                'BBG_l2_description']
 
# issuer level identifier
lVarListIssuer =                                                ['portfolio_type',
                                                                'portfolio_id',
                                                                'portfolio_name',
                                                                'portfolio_benchmark_group_name',
                                                                'benchmark_name',
                                                                'benchmark_id',
                                                                'issuer_long_name',
                                                                'ID_BB_COMPANY',
                                                                #'country_issue_name',
                                                                'gics_sector_description',
                                                                'bics_sector_description',
                                                                'BBG_l2_description',
                                                                'collat_type'
                                                                ]

# security level identifier
lVarListSec =                                                   ['portfolio_type',
                                                                'portfolio_name',
                                                                'benchmark_id',
                                                                'benchmark_name',
                                                                'effective_date',
                                                                'issuer_long_name',
                                                                'long_name',
                                                                'id_isin',
                                                                'ID_BB_COMPANY',
                                                                'country_issue_name',
                                                                'gics_sector_description',
                                                                'bics_sector_description',
                                                                'BBG_l2_description',
                                                                'collat_type',
                                                                #'market_cap_sector',
                                                                'agi_instrument_asset_type_description',
                                                                'Scope_Inv',
                                                                'Scope_Corp',
                                                                'Scope_Sov',
                                                                'weight']
                                                                #'weight_eq']

# formatting red background
lVarListTrueFalse =                                             ['Scope',
                                                                'Scope_NAV',
                                                                'Scope_Inv',
                                                                'Scope_Corp',
                                                                'Scope_Sov',
                                                                'GB']

# formatting wide columns
lVarListWide =                                                  ['portfolio_name',
                                                                'benchmark_name',
                                                                'agi_instrument_type_description',
                                                                'issuer_long_name',
                                                                'long_name',
                                                                'portfolio_nameBenchmark',
                                                                'portfolio_nameFund']


 
  
 
# security master
dfSecurities                                                    = sma.load_sec_hierarchy(False,spark)
dfHierarchy                                                     = dfSecurities[['ID_BB_COMPANY',
                                                                                'ID_BB_PARENT_CO',
                                                                                'ID_BB_ULTIMATE_PARENT_CO',
                                                                                'LONG_COMP_NAME',
                                                                                'ULT_PARENT_CNTRY_DOMICILE',
                                                                                'INDUSTRY_SECTOR']].drop_duplicates().drop_duplicates(subset = ['ID_BB_COMPANY'])
dfCountry                                                       = sma.load_cntry(False,spark)


dfCEDAR                                                         = sma.search_cedar(False,spark)
# data definitions
dictDefinitions = {
                                                               'eu_sfdr_sustainability_category':'Sustainability category according to European Union Sustainable Finance Disclosure Regulation. This sustainability category depends on specific requirements as defined by the regulator. Reference regulation: Regulation (EU) 2019/2088. Source: "Common Data Model"."Dimensional Model".vehicle_dim.',
                                                               'internal_asset_class':'Internal Asset Class. Source: "Common Data Model"."Dimensional Model".vehicle_dim.',
                                                               'market_value_clean_portfolio':'Market Value in Portfolio Currency. Source: "Common Data Model"."Dimensional Model".portfolio_total_fact.',
                                                               'portfolio_type':'Portfolio category such as Fund, Benchmark, Index, Target Fund, Restricted Universe...',
                                                               'portfolio_name':	'Source: "Common Data Model"."Dimensional Model".portfolio_dim.',
                                                               'benchmark_id': 'benchmark_durable_key (associated with portfolio). Source: "Common Data Model"."Dimensional Model".benchmark_dim.',
                                                               'benchmark_name': 'Source: "Common Data Model"."Dimensional Model".benchmark_dim.',
                                                               'effective_date': 'Holdings date.',
                                                               'issuer_long_name': 'Long Company Name. Source: "Common Data Model"."Dimensional Model".security_master_dim.',
                                                               'long_name':'Security Name. Source: "Common Data Model"."Dimensional Model".security_master_dim.',
                                                               'id_isin'	: 'Security ISIN. Source: "Common Data Model"."Dimensional Model".security_master_dim.',
                                                               #'ID_BB_COMPANY' : 'Issuer Bloomberg Bloomberg Identifier. Source: "Common Data Model"."Dimensional Model".security_master_dim.',
                                                               'country_issue_name': 'Country Issue Name: Provides the ISO code for the country in which the security is issued. Non-ISO codes will return for supranational (SNAT) and multi-national (MULT) securities. Equities Returns the ISO country code of where the trading engine is located. Fixed Income Returns the ISO country code of where the issuer is incorporated. Mortgages Returns the ISO country code of where the issuer is incorporated. Index Data is provided for Index Options only. Money Markets: Returns the ISO country code of where issuer is domiciled. Source: "Common Data Model"."Dimensional Model".security_master_dim.',	
                                                               'gics_sector_description': 'GICS Sector Description: Numeric code indicating GICS sector classification. GICS (Global Industry Classification Standard) is an industry classification standard developed by MSCI in collaboration with Standard & Poors (S&P). The Global Industry Classification Standard consists of 11 sectors, 24 industry groups, 62 industries, and 132 sub-industries. The GICS classification assigns a sector code to each company according to its principal business activity. Source: "Common Data Model"."Dimensional Model".security_master_dim.',
                                                               'gics_industry_group_description': 'GICS Industry Group Description: Numeric code indicating GICS sector classification. GICS (Global Industry Classification Standard) is an industry classification standard developed by MSCI in collaboration with Standard & Poors (S&P). The Global Industry Classification Standard consists of 11 sectors, 24 industry groups, 62 industries, and 132 sub-industries. The GICS classification assigns a sector code to each company according to its principal business activity. Source: "Common Data Model"."Dimensional Model".security_master_dim.',
                                                               'gics_industry_description': 'GICS Industry Group Description: Numeric code indicating GICS sector classification. GICS (Global Industry Classification Standard) is an industry classification standard developed by MSCI in collaboration with Standard & Poors (S&P). The Global Industry Classification Standard consists of 11 sectors, 24 industry groups, 62 industries, and 132 sub-industries. The GICS classification assigns a sector code to each company according to its principal business activity. Source: "Common Data Model"."Dimensional Model".security_master_dim.',
                                            
                                                               'agi_instrument_asset_type_description': 'CFI description of AGI CFI Code. Source: "Common Data Model"."Dimensional Model".security_master_dim.',
                                                               'Scope_Inv': 'Physical assets excluding cash.',
                                                               'Scope_Corp': 'Corporate assets identified based on agi_instrument_asset_type_description, agi_instrument_type_description and gics_sector_description. Source: "Common Data Model"."Dimensional Model".security_master_dim.',
                                                               'weight': 'Funds: percentage_of_market_value_clean_portfolio, Source: "Common Data Model"."Dimensional Model".holding_fact, Benchmarks: weight_close, Source: "Common Data Model"."Dimensional Model".benchmark_position_fact, Index: constituent_weight_in_index, Source: "Common Data Model"."Dimensional Model".index_position_fact',
                                                               
                                                               'carbon_emissions_scope_12_inten':'This figure represents the companys most recently reported or estimated Scope 1 + Scope 2 greenhouse gas emissions normalized by sales in USD, which allows for comparison between companies of different sizes. Source: "Common Data Model".ESG."business_views".msci."msci_esg_carbon_metrics".',
                                                               'Uncovered_Issuer_carbon_emissions_scope_12_inten': 'Issuer not covered (as a share of NAV, Scope_Corp or Scope_Inv).',
                                                               'weight_Benchmark':'Percentage of market value benchmark.',
                                                               'weight_Fund':'Percentage of market value portfolio.',
                                                               'contribution_Benchmark':'Contribution to benchmark carbon intensity based on weight_Benchmark and carbon_emissions_scope_12_inten.',
                                                               'contribution_Fund':'Contribution to portfolio carbon intensity based on weight_Fund and carbon_emissions_scope_12_inten.',   
                                                               'contribution_FundPct':'Contribution to portfolio carbon intensity as a share of aggregated portfolio carbon intensity.',   
                                                               'diff': 'Difference between contribution_Fund and contribution_Benchmark.',
                   
}
 
# regions
lNA                                                             = ['USA', 'CAN']
lEROP                                                           = ['GBR','AUT','BEL','CHE','DEU','DNK','ESP','FIN','FRA','IMN','IRL','ITA','NLD','NOR','PRT','SWE','LUX']
lAPAC                                                           = ['NZL','HKG','JPN','AUS','SGP']

# sort columns
lSortColumns =                                                  ['NEW_FLAG',
                                                                 'ID_EXL_LIST',
                                                                 'ID_PROVIDER_ISSUER',
                                                                 'EDM_PTY_ID',
                                                                 'INSERT_DATE',
                                                                'portfolio_type',
                                                                'portfolio_name',
                                                                'index_name',
                                                                'fund_name',
                                                                'portfolio_id',
                                                                'benchmark_id',
                                                                'benchmark_name',	
                                                                'effective_date',
                                                                'issuer_long_name',
                                                                'long_name',
                                                                'long_comp_name',
                                                                'id_isin',
                                                                'ID_BB_COMPANY',
                                                                'ID_BB_Company',
                                                                'LONG_COMP_NAME',
                                                                'unique issuer',
                                                                'country_issue_name',
                                                                'region',
                                                                'Region',
                                                                'gics_sector_description',
                                                                'bics_sector_description',
                                                                'BBG_l2_description',
                                                                'collat_type',
                                                                'gics_industry_group_description',
                                                                'gics_industry_description',
                                                                'GICSSector',
                                                                'market_cap_sector',
                                                                'issuer_market_cap_usd',
                                                                'agi_instrument_asset_type_description',
                                                                'Scope_Inv',
                                                                'Scope_Corp',
                                                                'Scope_Sov',
                                                                'weight',
                                                                'issuer',	
                                                                'all issuer',	
                                                                'issuer_pct',
                                                                
                                                                
                                                                'eu_sfdr_sustainability_category',
                                                                'domicile',
                                                                'internal_asset_class',
                                                                'market_value_clean_firm',
                                                                
                                                                'Exposure_FW',
                                                                'Exposure_Sus',
                                                                'Exposure_Ex',
                                                                'Exposure_ExSusie',
                                                                'Exposure_BVI',
                                                                'Exposure_BVI_new',
                                                                
                                                                
                                                                'Uncovered_Issuer_sri_rating_final',
                                                                'Uncovered_Issuer_SRI Rating Final',
                                                                'Uncovered_Issuer_SRI_FINAL_OVERRIDE',
                                                                
                                                                'final_override',
                                                                'final_override_TH',
                                                                'sri_rating_final',
                                                                'SRI_FINAL_OVERRIDE',
                                                                'SRI Rating Final',
                                                                'PSR_score_final_raw',
                                                                'PSR_score_final_bic',
                                                                'sri_rating_final < 1',
                                                                'SRI_FINAL_OVERRIDE < 1',
                                                                'SRI < 1',
                                                                'PSR_score_final_bic < 1',
                                                                'sri_rating_final < 2',
                                                                'SRI_FINAL_OVERRIDE < 2',
                                                                'SRI < 2',
                                                                'PSR_score_final_bic < 2',
                                                                'sri_rating_final < 1.5',
                                                                'hr_flag',
                                                                
                                                                'Uncovered_Issuer_industry_adjusted_score',
                                                                'industry_adjusted_score',
                                                                
                                                                'Uncovered_Issuer_PSR_rating_final',
                                                                'PSR_D_and_SRI_above_1',
                                                                'PSR_D_and_SRI_above_2',
                                                                'PSR_rating_final_A',
                                                                'PSR_rating_final_B',
                                                                'PSR_rating_final_C',
                                                                'PSR_rating_final_D',
                                                                
                                                                
                                                                'PSR_score_bucket_0',
                                                                'PSR_score_bucket_1',
                                                                'PSR_score_bucket_2', 
                                                                'PSR_score_bucket_3',
                                                                'PSR_score_bucket_4',
                                                                'PSR_score_bucket_5',
                                                                'PSR_score_bucket_6',
                                                                'PSR_score_bucket_7',
                                                                
                                                                'In_SRI_universe',
                                                                'In_PSR_universe',
                                                                'In_PSR_universe_before_removing_issuers_without_GICS',
                                                                'Company has an active SRI Override?',
                                                                'Company has an active SRI Override but no PSR score?',
                                                                'Coverage Gap_To be analyzed due to new coverage',
                                                                'Coverage Gap_To be analyzed due to lost coverage',
                                                                'Coverage Gap_NA',
                                                                
                                                                'Holding_Art_8_9_Portfolio',
                                                                
                                                                'PSR_indicator_coverage',
                                                                'PSR_num_factors_proxied_0.0',
                                                                'PSR_num_factors_proxied_1.0',
                                                                'PSR_num_factors_proxied_2.0',
                                                                'PSR_num_factors_proxied_3.0',
                                                                'PSR_num_factors_proxied_4.0',
                                                                'PSR_num_factors_proxied_5.0',
                                                                'PSR_num_factors_proxied_6.0',
                                                                'PSR_num_factors_proxied_7.0',
                                                                'PSR_num_factors_proxied_8.0',
                                                                'PSR_num_factors_proxied_9.0',
                                                                'PSR_num_factors_proxied_10.0',
                                                                'PSR_num_factors_proxied_11.0',
                                                                
                                                                'PSR_robustness_indicator_Strong',
                                                                'PSR_robustness_indicator_Medium',
                                                                'PSR_robustness_indicator_Light',
                                                                
                                                                
                                                                
                                                                'NZ sector classification_Lower impact',
                                                                'NZ sector classification_High impact',
                                                                'NZ sector classification_High impact by default',
                                                                
                                                                
                                                                'Final Status_Not Aligned',
                                                                'Final Status_Committed to aligning',
                                                                'Final Status_Aligning towards a Net Zero pathway',
                                                                'Final Status_Achieving Net Zero',
                                                                'Final Status_No assessment',
                                                                 
                                                                'Uncovered_Issuer_carbon_emissions_scope_12_inten',
                                                                'carbon_emissions_scope_12_inten',
                                                                'carbon_emissions_evic_scope_12_inten',
                                                                
                                                                'Uncovered_Issuer_ITR',
                                                                'ITR',
                                                                'Uncovered_Issuer_near_term_target_status',
                                                                'near_term_target_status_Targets Set',
                                                                'near_term_target_status_Committed',
                                                                
                                                                
                                                                'Uncovered_Issuer_female_directors_pct',
                                                                'female_directors_pct',
                                                                'Uncovered_Issuer_WOMEN_WORKFORCE_PCT_RECENT',
                                                                'WOMEN_WORKFORCE_PCT_RECENT',
                                                                
                                                                'Uncovered_Issuer_board_indep_pct',
                                                                'board_indep_pct',
                                                                'Uncovered_Issuer_mech_un_global_compact',
                                                                'mech_un_global_compact_No evidence',
                                                                'mech_un_global_compact_Yes',
                                                                
                                                                  
                                                                'Uncovered_Issuer_sustainable_investment_share_pre_dnsh',
                                                                'sustainable_investment_share_pre_dnsh',
                                                                'sustainable_investment_share_post_dnsh',
                                                                'Uncovered_Issuer_eu_taxonomy_aligned_turnover_percent_post_dnsh',
                                                                'eu_taxonomy_aligned_turnover_percent_post_dnsh',
                                                                'Uncovered_Issuer_eu_taxonomy_aligned_capex_percent_post_dnsh',
                                                                'eu_taxonomy_aligned_capex_percent_post_dnsh',
                                                                'Uncovered_Issuer_eu_taxonomy_aligned_opex_percent_post_dnsh',
                                                                'eu_taxonomy_aligned_opex_percent_post_dnsh',
                                                                'Exposure_GB',
                                                                'Uncovered_Issuer_dnsh_flag_overall',
                                                                'dnsh_flag_overall',
                                                                
                                                                'sdg_01_share_post_dnsh',
                                                                'sdg_02_share_post_dnsh',	
                                                                'sdg_03_share_post_dnsh',	
                                                                'sdg_04_share_post_dnsh',	
                                                                'sdg_05_share_post_dnsh',	
                                                                'sdg_06_share_post_dnsh',	
                                                                'sdg_07_share_post_dnsh',	
                                                                'sdg_08_share_post_dnsh',	
                                                                'sdg_09_share_post_dnsh',	
                                                                'sdg_10_share_post_dnsh',	
                                                                'sdg_11_share_post_dnsh',	
                                                                'sdg_12_share_post_dnsh',	
                                                                'sdg_13_share_post_dnsh',	
                                                                'sdg_14_share_post_dnsh',	
                                                                'sdg_15_share_post_dnsh',	
                                                                'sdg_16_share_post_dnsh',	
                                                                'sdg_17_share_post_dnsh',
                                                             
                                                                
                                                                'Uncovered_Issuer_nz_alignment_status',
                                                                'nz_alignment_status_Achieving',
                                                                'nz_alignment_status_Aligning',
                                                                'nz_alignment_status_Committed',
                                                                'nz_alignment_status_Not Aligned',
                                                                'Uncovered_Issuer_nz_alignment_status_agg',
                                                                'nz_alignment_status_agg_Aligned',
                                                                'nz_alignment_status_agg_Achieving',
                                                                'nz_alignment_status_agg_Aligning',
                                                                'nz_alignment_status_agg_Committed',
                                                                'nz_alignment_status_agg_Not Aligned',                                                                
                                                                'Uncovered_Issuer_nz_sector_classification',
                                                                'nz_sector_classification_High impact',
                                                                'nz_sector_classification_High impact by default',
                                                                'nz_sector_classification_Lower impact',

                                                                'Uncovered_Issuer_dur_mac',
                                                                'dur_mac',
                                                                'Uncovered_Issuer_dur_mod',
                                                                'dur_mod',
                                                                'Uncovered_Issuer_dur_spread_oa',
                                                                'dur_spread_oa',
                                                                'Uncovered_Issuer_yield_maturity',
                                                                'yield_maturity',
                                                                'Uncovered_Issuer_yield_worst',
                                                                'yield_worst',
                                                                
                                                                'Uncovered_Issuer_RTG_FIT_LT',
                                                                'RTG_FIT_LT_score',
                                                                'RTG_FIT_LT_score_bucket_A',
                                                                'RTG_FIT_LT_score_bucket_B',
                                                                'RTG_FIT_LT_score_bucket_nan',
                                                                'RTG_FIT_LT_AAA',
                                                                'RTG_FIT_LT_AA+',
                                                                'RTG_FIT_LT_AA',
                                                                'RTG_FIT_LT_AA-',
                                                                'RTG_FIT_LT_A+',
                                                                'RTG_FIT_LT_A',
                                                                'RTG_FIT_LT_A-',
                                                                'RTG_FIT_LT_BBB+',
                                                                'RTG_FIT_LT_BBB',
                                                                'RTG_FIT_LT_BBB-',
                                                                'RTG_FIT_LT_BB+',
                                                                'RTG_FIT_LT_BB',
                                                                'RTG_FIT_LT_BB-',
                                                                'RTG_FIT_LT_B+',
                                                                'RTG_FIT_LT_NR',
                                                                
                                                                'Uncovered_Issuer_RTG_MDY_LT',
                                                                'RTG_MDY_LT_score',
                                                                'RTG_MDY_LT_score_bucket_A',
                                                                'RTG_MDY_LT_score_bucket_B',
                                                                'RTG_MDY_LT_score_bucket_nan',
                                                                'RTG_MDY_LT_Aaa',
                                                                'RTG_MDY_LT_Aa1',
                                                                'RTG_MDY_LT_Aa2',
                                                                'RTG_MDY_LT_Aa3',
                                                                'RTG_MDY_LT_A1',
                                                                'RTG_MDY_LT_A2',
                                                                'RTG_MDY_LT_A3',
                                                                'RTG_MDY_LT_Baa1',
                                                                'RTG_MDY_LT_Baa2',
                                                                'RTG_MDY_LT_Baa3',
                                                                'RTG_MDY_LT_Ba1',
                                                                'RTG_MDY_LT_Ba2',
                                                                'RTG_MDY_LT_Ba3',
                                                                'RTG_MDY_LT_NR',
                                                                
                                                                'Uncovered_Issuer_RTG_SP_LT',
                                                                'RTG_SP_LT_score',
                                                                'RTG_SP_LT_score_bucket_A',
                                                                'RTG_SP_LT_score_bucket_B',
                                                                'RTG_SP_LT_score_bucket_nan',
                                                                'RTG_SP_LT_AAA',
                                                                'RTG_SP_LT_AA+',
                                                                'RTG_SP_LT_AA',
                                                                'RTG_SP_LT_AA-',
                                                                'RTG_SP_LT_A+',
                                                                'RTG_SP_LT_A',
                                                                'RTG_SP_LT_A-',
                                                                'RTG_SP_LT_BBB+',
                                                                'RTG_SP_LT_BBB',
                                                                'RTG_SP_LT_BBB-',
                                                                'RTG_SP_LT_BB+',
                                                                'RTG_SP_LT_BB',
                                                                'RTG_SP_LT_BB-',
                                                                'RTG_SP_LT_NR'

                                                                ]
                                                                      