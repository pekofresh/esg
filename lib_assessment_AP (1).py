# -*- coding: utf-8 -*-
"""
Created on Wed Dec 28 08:07:40 2022

@author: A00008106
"""
import pandas as pd
import numpy as np
import lib_utils_AP as lib_utils
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
import lib_sma_AP as sma
from sqlalchemy import create_engine
import urllib
from io import BytesIO



class Portfolio:
    """
    Object loading portfolio constituents for different portfolio types.
    
    - "Portfolios":     fund holdings for investment vehicles identified by CEDAR IDs. Benchmarks are included automatically
    - "Derivatives":    futures underlying indices (based on ETF holdings) and iBoxx
    - "Indices":        constituents for broad market (equity and fixed income) indices
    - "Universes":      customized universes like market cap constraints, xls benchmarks, etc...
    - "Target Funds":   Refinitiv Lipper data
    
    Can be initialized by providing the portfolio types, dates and list of CEDAR IDs.
    """
    def __init__(self, lSources, sDate, lPortfolioIDs = [], lIndexIDs = []):
        
        self.iLookthrough = False
        self.lPortfolioIDs = lPortfolioIDs
        
        if 'Portfolios' in lSources: 
        # loading dremio IBOR data from dremio
            if type(lPortfolioIDs) == int:
                lPortfolioID                        = [lPortfolioIDs]
            else:
                lPortfolioID                        = lPortfolioIDs

            if len(lPortfolioIDs)>0:
                dfAllPortfolios, dfBenchmarkBridge      = sma.load_holdings('holdings', lPortfolioID, sDate, position_type = 4, iBenchmarks = True)
            #dfAllPortfolios, dfBenchmarkBridge      = sma.load_all_holdings(sDate, position_type = 4, iBenchmarks = False)
           
        if 'Refinitiv' in lSources:
            dfRefinitiv = sma.load_holdings('refinitiv', False)
            if 'dfAllPortfolios' in locals():
                dfAllPortfolios = pd.concat([dfAllPortfolios, dfRefinitiv])
            else:
                dfAllPortfolios = dfRefinitiv.copy()

        if 'Custom' in lSources:
        # loading customized portfolio or benchmark data in excel format (columns required: id_isin, weight in decimals)
            # dfXLSPf                                 = sma.load_xls_portfolio(   ['NZAS database.xlsx',
            #                                                                      'MSCI database.xlsx',
            #                                                                      'Composite Universe.xlsx',
            #                                                                      'SBTi covered.xlsx',
            #                                                                      'TPI covered.xlsx',
            #                                                                      'CDP covered.xlsx',
            #                                                                      'Align match.xlsx',
            #                                                                      'NZAS aligning.xlsx'],
            #                                                                                 ['NZAS database',
            #                                                                                  'MSCI database',
            #                                                                                  'Composite universe',
            #                                                                                  'SBTi covered',
            #                                                                                  'TPI covered',
            #                                                                                  'CDP covered',
            #                                                                                  'Align match',
            #                                                                                  'NZAS aligning'],
            #                                                                                 ['Universe',
            #                                                                                  'Universe',
            #                                                                                  'Universe',
            #                                                                                  'Subsample',
            #                                                                                  'Subsample',
            #                                                                                  'Subsample',
            #                                                                                  'Subsample',
            #                                                                                  'Subsample'],
            #                                      f                                           lib_utils.sWorkFolder,
            #                                                                                 sLeftOn = 'ID_BB_COMPANY')
            dfXLSPf                                 = sma.load_holdings('xls_portfolio',   ['cgpa_model ptf and bench_Model portfolio.xlsx','cgpa_model ptf and bench_Benchmark.xlsx'],
                                                                                            ['cgpa_model ptf and bench_Model portfolio','cgpa_model ptf and bench_Benchmark'],
                                                                                            ['Fund','Benchmark'],
                                                                                            lib_utils.sWorkFolder,
                                                                                            sLeftOn = 'ID_BB_COMPANY')
            # dfXLSPf                                 = sma.load_xls_portfolio(   ['QW5K Index as of Dec 02 20241.xlsx'],
            #                                                                     ['QW5K Index as of Dec 02 20241'],
            #                                                                     ['Index'],
            #                                                                     lib_utils.sWorkFolder,
            #                                                                     sLeftOn = 'id_isin')
            
            if len(lPortfolioIDs)>0:
                dfAllPortfolios = pd.concat([dfAllPortfolios,dfXLSPf])
            else:
                print('no portfolios imported from dremio')
                dfAllPortfolios = dfXLSPf.copy()
       
        if 'IDS' in lSources:
            dfPf = sma.load_holdings('ids_portfolio', sDate,'id_isin')
            if len(lPortfolioIDs)>0:
                dfAllPortfolios = pd.concat([dfAllPortfolios,dfPf])
            else:
                print('no portfolios imported from dremio')
                dfAllPortfolios = dfPf.copy()
            
        if 'Derivatives' in lSources:
            
            dfUnderlyings                           = sma.load_underlyings()
            
            if len(lPortfolioIDs)>0:
                dfAllPortfolios = pd.concat([dfAllPortfolios,dfUnderlyings])
            else:
                dfAllPortfolios = dfUnderlyings.copy()
            
        
        if 'Indices' in lSources:
            #a0 = [1574,2148,173,1357,1077,1319,2118,963,687,344,418,357,17,596,1642,824,1185,701,1697,929,164,4316,1466,1556,2014,1935,1537,837,1737,882,5481,1944,1319,1632,1181,1235,59,584,1041,5771]
            #a = [288,1466,1574,173,824,756,824,173,1466,1383, 2014,701,979,1750]
            #b = [824,173,1466,1383]
            #c = [701,373,1624,929,1556,6144,94]
            #d = [701,2014,2155,189,979,288,1243,1077]
            #e = [5844, 5550, 1052, 517, 824, 6346, 1466, 344, 5847, 701]
            #f = [2192,5594] 
            #lMarketIndices = list(set(a0+a+b+c+d+e+f))
            #lMarketIndices = [2014,1466,1383,1537]
            #lMarketIndices = [1185]
            #,701,1645,2014,824,1537,1185,1466,837]
            #lMarketIndices = [2014,1466,1642,701,1185,979,963,1537,288,837,596,1935,2192,1750,1624,1077,1357,824,925]
            #lMarketIndices = [7068,7069,7070,1642,701,824]
            #lMarketIndices = [2014,824,1466,701,1642]
            #lMarketIndices = [2014,1466,1383,701,979,288,1750,824]
            #lMarketIndices = [979]
            #lMarketIndices = [1642,979]
            #lMarketIndices = [373,6237]
            
            #lMarketIndices = [1537,7270,1383,144,69,2124,2035,842,1574,1507,1314]
            #lMarketIndices = [1596,1466,5610,5438,874,5844,7072,5604,2014] 
            #lMarketIndices = [1507,1314,1537,7091,1383]
            #lMarketIndices = [2014,1596,1466,701,2192,5594,1750]
            #lMarketIndices = [7460]
            #lMarketIndices = [7279,1293] # RWE
            #lMarketIndices = [5610,5612,5438,555,1653,1178]
            #lMarketIndices = [7460]
            #lMarketIndices = [7460] # iBOXX
            #lMarketIndices = [344,357,596,979,2014]
            dfAllIndices                            = sma.load_holdings('index', lIndexIDs, sDate, position_type = 'C')
    

            if 'dfAllPortfolios' in locals():
                dfAllPortfolios = pd.concat([dfAllPortfolios,dfAllIndices])
            else:
                dfAllPortfolios = dfAllIndices.copy()
            
        
        if 'Universes' in lSources:
            """
            customizes universes:
            - 1 French equity team MSCI Europe IMI > 1 bn EUR market cap
            - 2 French money market fund universe
            - 3 current BestStyles universe
            """


            dfEuropeIMI                        = sma.load_holdings('index', [584], sDate, position_type = 'C')
            # dfEuropeIMI                         = sma.load_xls_portfolio(   ['MSCI_EUROPE_IMI.xlsx'],
            #                                                                                 ['MSCI Europe IMI (> 1 bn EUR market cap)'],
            #                                                                                 ['Index'],
            #                                                                                 lib_utils.sWorkFolder,
            #                                                                                 sLeftOn = 'id_isin')
            if 1 == 0:
                # possibility to refresh the market cap
                lISINs = ('/isin/' + dfEuropeIMI.id_isin.dropna()).to_list()
                lib_utils.connBBG.start()
                dfTickers = lib_utils.connBBG.ref(lISINs, ['EQY_FUND_TICKER'])
                lTickers = (dfTickers['value'] + ' Equity').to_list()
                dfBBG = lib_utils.connBBG.ref(lTickers, ['CUR_MKT_CAP'],ovrds =[('EQY_FUND_CRNCY', 'EUR')])
                dfTickers['ISIN'] = dfTickers['ticker'].str[6:19]
                dfTickers['ticker'] = dfTickers['value'] + ' Equity'
                dfBBG = pd.merge(dfTickers.drop(['field','value'],axis = 1),dfBBG.drop_duplicates().drop_duplicates(subset = ['ticker']),how = 'left',left_on = 'ticker',right_on = 'ticker',validate = 'm:1')
                dfBBG.to_pickle(lib_utils.sWorkFolder + 'dfBBG.pkl')
            else:   
                #dfBBG  = pd.read_pickle(lib_utils.sWorkFolder + 'dfBBG.pkl').drop_duplicates(subset = ['ISIN'])
                dfMSCI  = pd.read_pickle(lib_utils.sWorkFolder + 'dfMSCI.pkl')
            #dfEuropeIMI                             = pd.merge(dfEuropeIMI,dfBBG,how = 'left',left_on = 'id_isin',right_on = 'ISIN',validate = 'm:1')
            dfEuropeIMI = pd.merge(dfEuropeIMI,dfMSCI[['id_bb_company','issuer_market_cap_usd']],how = 'left',left_on = 'ID_BB_COMPANY',right_on = 'id_bb_company',validate = 'm:1')
            #dfEuropeIMI                             = dfEuropeIMI.loc[dfEuropeIMI.value > 1000000000]
            dfEuropeIMI                             = dfEuropeIMI.loc[dfEuropeIMI.issuer_market_cap_usd > 1000000000].drop(['issuer_market_cap_usd'],axis = 1)
            dfEuropeIMI['portfolio_id']             = 1
            dfEuropeIMI['benchmark_id']             = -1
            dfEuropeIMI['portfolio_name']           = 'MSCI Europe IMI (> 1 bn EUR market cap)'
            dfEuropeIMI['weight'] = dfEuropeIMI['weight'] / dfEuropeIMI.groupby('portfolio_name')['weight'].transform('sum')
            dfMMBenchmark                                 = sma.load_holdings('xls_portfolio',   ['MM Defined Universe.xlsx'],
                                                                                ['MM Defined Benchmark'],
                                                                                ['Benchmark'],
                                                                                lib_utils.sBenchmarkFolder,
                                                                                'ID_BB_COMPANY',
                                                                                )
            
            dfMMBenchmark['benchmark_id'] = 2
            dfMMBenchmark['portfolio_id'] = 2
            dfMMBenchmark['portfolio_type'] = 'CustomUniverse'

            dfMMBenchmark['weight'] = 0
            dfMMBenchmark.loc[dfMMBenchmark.ID_BB_COMPANY.notna(),'weight'] = 1/dfMMBenchmark.ID_BB_COMPANY.dropna().count()
  
            # dfMMBenchmark['weight'] = 1/len(dfMMBenchmark)
            
            

            dfSRIBS = sma.calc_universe_Best_Styles_PSR(sDate,False,False)
            dfSRIBS['benchmark_id'] = -3
            dfSRIBS['portfolio_id'] = 3
            dfSRIBS['portfolio_type'] = 'CustomUniverse'
            dfSRIBS['issuer_long_name'] = dfSRIBS['long_comp_name']
            dfSRIBS['long_name'] = dfSRIBS['long_comp_name']
            dfSRIBS['weight'] = 0
            dfSRIBS.loc[dfSRIBS.ID_BB_COMPANY.notna(),'weight'] = 1/dfSRIBS.ID_BB_COMPANY.dropna().count()

            
            dfXLSPf                                 = sma.load_holdings('xls_portfolio',   ['Book1.xlsx'],
                                                                                ['BBG MSCI Euro Corp Climate Transition EVIC Intensity Index'],
                                                                                ['CustomUniverse'],
                                                                                lib_utils.sWorkFolder,
                                                                                sLeftOn = 'id_isin')
            dfXLSPf['portfolio_id'] = 100
            dfXLSPf['benchmark_id'] = 100
            dfXLSPf['effective_date'] = sDate[0]
            if len(lPortfolioIDs)>0:
                dfAllPortfolios = pd.concat([dfAllPortfolios,dfMMBenchmark,dfEuropeIMI,dfSRIBS,dfXLSPf])
            else:
                dfAllPortfolios = pd.concat([dfMMBenchmark,dfEuropeIMI,dfSRIBS])
                    
            # some issuers in custom universes are not covered in bbo_sec_credit_risk_hrchy
            if 1==0:
                dfAllPortfolios = sma.get_names(dfAllPortfolios)

        if 'Target Funds' in lSources:
            dfExt = sma.load_holdings('refinitiv', False)
            # dfExt = dfExt.loc[dfExt.portfolio_id.isin(lTargetFunds)]
            if len(lPortfolioIDs)>0:
                dfAllPortfolios         = pd.concat([dfAllPortfolios,dfExt])
            else:
                dfAllPortfolios         = dfExt.copy()
            self.dfExt              = dfExt
            self.iLookthrough       = True
        
        self.dfAllPortfolios, dfClassification, lSovTypes = sma.calc_corp_eligible_univ(dfAllPortfolios,self.iLookthrough)

        if len(lPortfolioIDs)>0:
            self.dfBenchmarkBridge      = dfBenchmarkBridge      

class Universe:

    def __init__(self,dfAllPortfolios,dfExclusions,iPortfolioID,iBenchmarkID,sBenchmarkType,sApproach,fReduction,fLowerBound,sName,sPfName):
        self.dfPortfolio = dfAllPortfolios.loc[(dfAllPortfolios.portfolio_type == 'Fund')&(dfAllPortfolios.portfolio_id == iPortfolioID)]
        self.dfPortfolio['portfolio_type'] = 'Portfolio'
        self.dfUniverse = dfAllPortfolios.loc[(dfAllPortfolios.portfolio_type == sBenchmarkType)&(dfAllPortfolios.portfolio_id == iBenchmarkID)]
        self.dfUniverse['portfolio_type']  = 'Universe'
        self.dfUniverse['portfolio_name']  = sName + '_' + self.dfUniverse['portfolio_name'] + '_' + str(fReduction*100) + '_Pct'
        self.dfUniverse['portfolio_id']     = iPortfolioID

        self.lUniverse = self.dfUniverse['ID_BB_COMPANY'].dropna().drop_duplicates().to_list()
        self.iPortfolioID = iPortfolioID
        self.iBenchmarkID = iBenchmarkID
        self.sBenchmarkType = sBenchmarkType
        self.fReduction = fReduction
        self.dfExclusions = dfExclusions['id_bb_company'].dropna().drop_duplicates()
        self.sApproach = sApproach
        self.fLowerBound = fLowerBound
        self.sName = sName
        self.sPfName = sPfName

        if iBenchmarkID in (2, 63702):
            self.iWaterfall = True
        else:
            self.iWaterfall = False
        
    def calc_eligible_universe(self):
        print('*********************************************')
        print('calculating eligible universe for ' + self.sName + ' based on ' + self.sPfName)
        print('*********************************************')
        if self.sApproach == 'em_debt':
            dfCovered                           = self.dfUniverse[['ID_BB_COMPANY','country_code3']].drop_duplicates().dropna()
            self.lCountries                     = pd.read_excel(lib_utils.sWorkFolder + 'SMA_ESG_Framework_June2024_flat.xlsx',sheet_name = 'Sheet1')['Country_Code'].to_list()
            self.lRestrictions                  = dfCovered.loc[dfCovered['country_code3'].isin(self.lCountries)]['ID_BB_COMPANY'].to_list()

        elif self.sApproach == 'sri_rating_final_BS':
            dfCovered                           = self.dfUniverse.dropna(subset = ['sri_rating_final'])[['ID_BB_COMPANY','sri_rating_final']].drop_duplicates() 
            #self.lRestrictions                  = self.dfUniverse.loc[self.dfUniverse.Restricted == True]['ID_BB_COMPANY'].dropna().drop_duplicates().to_list()
            dfRestrictions                      = pd.read_pickle('C:/Users/A00008106.EU1/Allianz Global Investors/Sustainability Standards and Analytics-SMA - Documents/15. Investment process/Universes/Best Styles Universe 20241201.pkl')
            self.lRestrictions                  = dfRestrictions.loc[dfRestrictions.Restricted == True]['ID_BB_COMPANY'].to_list()
        
  
        elif self.sApproach == 'sri_rating_final_evic':
    
            self.dfUniverse = self.dfUniverse.dropna(subset = ['evic_usd'])
            #self.dfUniverse['market_cap_category'] = self.dfUniverse['market_cap_category'].fillna('small')
            dfCovered = (self.dfUniverse[['ID_BB_COMPANY', 'evic_category', 'weight']]
                         .groupby(['ID_BB_COMPANY', 'evic_category'], as_index=False)
                         .agg({'weight': 'sum'}))
            sum_weights = dfCovered.groupby('evic_category')['weight'].sum()
            sum_weights = (sum_weights / sum_weights.sum())

            unique_companies = dfCovered.groupby('evic_category')['ID_BB_COMPANY'].nunique()
            weights_adj = sum_weights / unique_companies
        
            
            # Assign back to the original dataframe, ignoring rows with NA in sri_rating_final
            self.dfUniverse['sum_weights'] = self.dfUniverse['evic_category'].map(sum_weights)
            self.dfUniverse['unique_companies'] = self.dfUniverse['evic_category'].map(unique_companies)
            self.dfUniverse['weights_adj'] = self.dfUniverse['evic_category'].map(weights_adj)
            self.dfUniverse['Exposure_Ex'] = self.dfUniverse['ID_BB_COMPANY'].isin(self.dfExclusions.to_list())
            print(self.dfUniverse[['evic_category','sum_weights','unique_companies','weights_adj']].drop_duplicates())
            
            dfUnique = (self.dfUniverse.dropna(subset=['sri_rating_final'])
                         [['ID_BB_COMPANY', 'sri_rating_final', 'evic_category', 'weights_adj','evic_usd']].drop_duplicates()
                         .groupby(['ID_BB_COMPANY', 'sri_rating_final', 'evic_usd','evic_category'], as_index=False)
                         .agg({'weights_adj': 'sum'}))
            
            
            # flag exclusions
            dfUnique['is_restricted'] = dfUnique['ID_BB_COMPANY'].isin(self.dfExclusions.to_list())
        
            # Sort by sri_rating_final (ascending, worst-rated first), with restricted issuers prioritized
            dfUnique = dfUnique.sort_values(by=['is_restricted', 'sri_rating_final','evic_usd'], ascending=[False, True,True])
        
            # Calculate the cumulative weight and determine the 20% threshold
            dfUnique['cumulative_weight'] = dfUnique['weights_adj'].cumsum()
            total_weight = dfUnique['weights_adj'].sum()
            print(total_weight)
            print(len(dfUnique))
            weight_threshold = self.fReduction * total_weight
            print(weight_threshold/total_weight)
            # Identify rows to exclude based on the weight threshold
            dfUnique['to_exclude'] = dfUnique['cumulative_weight'] <= weight_threshold
            
            # Ensure at least the first row above the threshold is included in the exclusion
            if not dfUnique[dfUnique['to_exclude']]['cumulative_weight'].max() >= weight_threshold:
                first_above_threshold_idx = dfUnique[~dfUnique['to_exclude']].index[0]
                dfUnique.loc[first_above_threshold_idx, 'to_exclude'] = True

            
            if self.iPortfolioID == 10000430:
                dfUnique.to_excel(lib_utils.sWorkFolder + 'dfUnique_10000430.xlsx')
            # Filter out the excluded rows
            self.lRestrictions = dfUnique.loc[dfUnique['to_exclude'] == True]['ID_BB_COMPANY'].to_list()

        elif self.sApproach == 'sri_rating_final_mc':
    
            self.dfUniverse = self.dfUniverse.dropna(subset = ['issuer_market_cap_usd'])
            #self.dfUniverse['market_cap_category'] = self.dfUniverse['market_cap_category'].fillna('small')
            dfCovered = (self.dfUniverse[['ID_BB_COMPANY', 'market_cap_category', 'weight']]
                         .groupby(['ID_BB_COMPANY', 'market_cap_category'], as_index=False)
                         .agg({'weight': 'sum'}))
            sum_weights = dfCovered.groupby('market_cap_category')['weight'].sum()
            sum_weights = (sum_weights / sum_weights.sum())

            unique_companies = dfCovered.groupby('market_cap_category')['ID_BB_COMPANY'].nunique()
            weights_adj = sum_weights / unique_companies
        
            
            # Assign back to the original dataframe, ignoring rows with NA in sri_rating_final
            self.dfUniverse['sum_weights'] = self.dfUniverse['market_cap_category'].map(sum_weights)
            self.dfUniverse['unique_companies'] = self.dfUniverse['market_cap_category'].map(unique_companies)
            self.dfUniverse['weights_adj'] = self.dfUniverse['market_cap_category'].map(weights_adj)
            self.dfUniverse['Exposure_Ex'] = self.dfUniverse['ID_BB_COMPANY'].isin(self.dfExclusions.to_list())
            print(self.dfUniverse[['market_cap_category','sum_weights','unique_companies','weights_adj']].drop_duplicates())
            
            dfUnique = (self.dfUniverse.dropna(subset=['sri_rating_final'])
                         [['ID_BB_COMPANY', 'sri_rating_final', 'market_cap_category', 'weights_adj','issuer_market_cap_usd']].drop_duplicates()
                         .groupby(['ID_BB_COMPANY', 'sri_rating_final', 'issuer_market_cap_usd','market_cap_category'], as_index=False)
                         .agg({'weights_adj': 'sum'}))
            
            
            # flag exclusions
            dfUnique['is_restricted'] = dfUnique['ID_BB_COMPANY'].isin(self.dfExclusions.to_list())
        
            # Sort by sri_rating_final (ascending, worst-rated first), with restricted issuers prioritized
            dfUnique = dfUnique.sort_values(by=['is_restricted', 'sri_rating_final','issuer_market_cap_usd'], ascending=[False, True,True])
        
            # Calculate the cumulative weight and determine the 20% threshold
            dfUnique['cumulative_weight'] = dfUnique['weights_adj'].cumsum()
            total_weight = dfUnique['weights_adj'].sum()
            print(total_weight)
            print(len(dfUnique))
            weight_threshold = self.fReduction * total_weight
            print(weight_threshold/total_weight)
            # Identify rows to exclude based on the weight threshold
            dfUnique['to_exclude'] = dfUnique['cumulative_weight'] <= weight_threshold
            
            # Ensure at least the first row above the threshold is included in the exclusion
            if not dfUnique[dfUnique['to_exclude']]['cumulative_weight'].max() >= weight_threshold:
                first_above_threshold_idx = dfUnique[~dfUnique['to_exclude']].index[0]
                dfUnique.loc[first_above_threshold_idx, 'to_exclude'] = True

            
            if self.iPortfolioID == 10000430:
                dfUnique.to_excel(lib_utils.sWorkFolder + 'dfUnique_10000430.xlsx')
            # Filter out the excluded rows
            self.lRestrictions = dfUnique.loc[dfUnique['to_exclude'] == True]['ID_BB_COMPANY'].to_list()
        
        else:
            # take only covered
            dfCovered                           = self.dfUniverse.dropna(subset = [self.sApproach])[['ID_BB_COMPANY',self.sApproach]].drop_duplicates()
            # count issuer
            iUnrestricted                       = dfCovered['ID_BB_COMPANY'].nunique()
            print(str(iUnrestricted) + ' covered issuers')
            iTotalRestricted                    = np.ceil(iUnrestricted * self.fReduction).astype(int)
            print(str(iTotalRestricted) + ' restricted based on ' + str(self.fReduction) + ' reduction rate')
            # count excluded
            iExcluded                           = dfCovered.loc[dfCovered['ID_BB_COMPANY'].isin(self.dfExclusions.to_list())]['ID_BB_COMPANY'].nunique()
            print(str(iExcluded) + ' issuers on exclusion list')
            # further reduction
            if self.sApproach == 'carbon_emissions_scope_12_inten':
                iRestricted                     = iTotalRestricted
            else:   
                iRestricted                     = iTotalRestricted - iExcluded
            print(str(iRestricted) + ' issuers to be excluded based on score' )
            # issuers not excluded
            dfUnrestricted                      = dfCovered.loc[~dfCovered['ID_BB_COMPANY'].isin(self.dfExclusions.to_list())].sort_values(self.sApproach)
            
            if self.sApproach == 'carbon_emissions_scope_12_inten':
                self.lRestrictions              = dfCovered.sort_values(self.sApproach).tail(iRestricted)['ID_BB_COMPANY'].to_list()
                 
            elif self.sApproach == 'sri_rating_final':
                #self.lRestrictions             = dfUnrestricted.head(iRestricted)['ID_BB_COMPANY'].to_list()
                fThreshold                      = dfUnrestricted.head(iRestricted)[self.sApproach].max()
                self.lRestrictions               = dfUnrestricted.loc[dfUnrestricted[self.sApproach]<=fThreshold]['ID_BB_COMPANY'].to_list()
                if self.fLowerBound > 0:
                    self.lRestrictions          = dfUnrestricted.loc[dfUnrestricted[self.sApproach] < self.fLowerBound]['ID_BB_COMPANY'].to_list()
                    

        self.lRestrictions                  = self.lRestrictions + self.dfExclusions.to_list()
        self.dfEligibles                    = self.dfUniverse.loc[~self.dfUniverse['ID_BB_COMPANY'].isin(self.lRestrictions)]
        self.dfEligibles['portfolio_type']  = 'Eligible Universe'
        self.dfEligibles['portfolio_name']  = self.sName + '_Universe_' + self.sPfName
         
        # print(str(self.dfEligibles.loc[self.dfEligibles['ID_BB_COMPANY'].isin(dfCovered['ID_BB_COMPANY'])]['ID_BB_COMPANY'].nunique()) + ' eligible covered issuers' )

        # flag exposure against restrictions
        self.dfPortfolio['Worst Rated Benchmark'] = False
        self.dfUniverse['Worst Rated Benchmark'] = False
        self.dfEligibles['Worst Rated Benchmark'] = False        
        self.dfPortfolio['Outside Benchmark'] = False
        self.dfUniverse['Outside Benchmark'] = False
        self.dfEligibles['Outside Benchmark'] = False
        self.dfPortfolio['Exposure_Ex'] = False
        self.dfUniverse['Exposure_Ex'] = False
        self.dfEligibles['Exposure_Ex'] = False

        self.dfPortfolio.loc[(self.dfPortfolio.Scope_Corp == True)&
                             (self.dfPortfolio.ID_BB_COMPANY.isin(self.dfExclusions.to_list())),'Exposure_Ex'] = True
        self.dfUniverse.loc[(self.dfUniverse.Scope_Corp == True)&
                             (self.dfUniverse.ID_BB_COMPANY.isin(self.dfExclusions.to_list())),'Exposure_Ex'] = True
        self.dfEligibles.loc[(self.dfEligibles.Scope_Corp == True)&
                             (self.dfEligibles.ID_BB_COMPANY.isin(self.dfExclusions.to_list())),'Exposure_Ex'] = True
        
        self.dfPortfolio.loc[(self.dfPortfolio.Scope_Corp == True)&
                             (self.dfPortfolio.ID_BB_COMPANY.isin(self.lRestrictions)),'Worst Rated Benchmark'] = True
        if self.iWaterfall == False:
            self.dfPortfolio.loc[(self.dfPortfolio.Scope_Corp == True)&
                                 (~self.dfPortfolio.ID_BB_COMPANY.isin(self.lUniverse)),'Outside Benchmark'] = True
        else:
            self.dfPortfolio.loc[(self.dfPortfolio.Scope_Corp == True)&
                                 (~self.dfPortfolio[['ID_BB_COMPANY','ID_BB_PARENT_CO','ID_BB_ULTIMATE_PARENT_CO']].isin(self.lUniverse).max(axis = 1)),'Outside Benchmark'] = True
        self.dfUniverse.loc[(self.dfUniverse.Scope_NAV == True)&
                             (self.dfUniverse.ID_BB_COMPANY.isin(self.lRestrictions)),'Worst Rated Benchmark'] = True

        
class Variable:
    """
    Object for variable definition. Includes references such as data set name, variable name, identifier, logic applied for waterfalling and rebasing. 
    
    Can be initialized by:
        - data set name (should correspond to the ones included in class "Data")
        - suffix (short name)
        - variable name if known, otherwise "None"
        - varibale type (should be one of "bool","float","perc","str")
        
    Standard set of variables below
    """    
    def __init__(self,sDataSetName,sSuffix,sVarName,sVarType,iCoverage = True,iRebasing = False,iWaterfall = False,sLeftOn = 'ID_BB_COMPANY',sRightOn = 'ID_BB_COMPANY',sScope = 'Scope_Corp',iCalculated = False):
    
        self.sDataSetName   = sDataSetName
        self.sSuffix        = sSuffix
        self.sVarName       = sVarName
        self.sVarType       = sVarType
        self.iCoverage      = iCoverage
        self.iRebasing      = iRebasing
        self.iWaterfall     = iWaterfall
        self.sLeftOn        = sLeftOn
        self.sRightOn       = sRightOn
        self.sScope         = sScope
        self.iCalculated    = iCalculated

class Module:
    """
    Object storing ESG data from different datasets and meta information.
    
    Can be intialized by
        - list of instances from class Variable
        - short name
        - date (vec)
    """
    
    def __init__(self,lVariables,sModuleName,sDate,iLookthrough = False, lFileNames=None):

        # this object 
        lDataSetNames           = []
        for instVariable in lVariables:
            lDataSetNames.append(instVariable.sDataSetName)
        
        lDataSetNames           = list(dict.fromkeys(lDataSetNames))

        self.ModuleData         = Data(lDataSetNames, lFileNames)
        self.ModuleName         = sModuleName
        self.ModuleVariables    = lVariables
        # exclusions are loaded in the class Data and Variables are created generically
        if any('dfExclusions' in s for s in [s.sDataSetName for s in self.ModuleVariables]):
            self.ModuleVariables = self.ModuleVariables + self.ModuleData.lExclusions
        self.iLookthrough       = iLookthrough
        self.sDate              = sDate

class Assessment:
    
    """
    Object performing impact assessments. 
    
    Can be initialized by
        - Instance of class Portfolio
        - Instance of class Module
    
    Functions:
        - map_data:         maps ESG data to portfolio and benchmark holdings, calculates coverage and creates list of columns used for aggregation 
        - calc_universes:   based on initial universes calculates exclusions lists based on external criteria
        - calc_impact:      calculate portfolio level estimates for ESG metrics, exposure to exclusions, coverage ratios
        - export_results:   xls output for instrument, sector, portfolio level results and benchmark relative analytics
    """
    
    def __init__(self,instPortfolio,instModule,iAMF = False):
        # copy portfolio datalVariables
        self.dfAllPortfolios = instPortfolio.dfAllPortfolios.copy(deep = True)
        if len(instPortfolio.lPortfolioIDs) > 0:
            self.dfBenchmarkBridge = instPortfolio.dfBenchmarkBridge.copy()
        self.instModule                 = instModule    
        # get rid of redundant variables

        if iAMF == True:
            print('caution')
            self.instModule.ModuleVariables = [s for s in lVariables if s.sVarName in ['sri_rating_final',
                                                                                       'carbon_emissions_scope_12_inten',
                                                                                       'market_cap_category',
                                                                                       'market_cap_category_large',
                                                                                       'market_cap_category_mid',
                                                                                       'market_cap_category_small',
                                                                                       'issuer_market_cap_usd',
                                                                                       'evic_category',
                                                                                       'evic_category_large',
                                                                                       'evic_category_mid',
                                                                                       'evic_category_small',
                                                                                       'evic_usd'
                                                                                       ] or s.sSuffix in ['_Sus']]
            #self.instModule.ModuleVariables = [s for s in lVariables if s.sVarName in ['sri_rating_final'] or s.sSuffix in ['_Sus']]
        self.map_data()
        if iAMF == True:
            self.calc_universes_new()
        self.calc_impact()
        
    def map_data(self): 
        print('Mapping ESG data to portfolio and benchmark holdings...')
        lCategoricalRebased = []
        lCategoricalUnrebased = []
        lCategoricalRebasedScope = []
        lCategoricalUnrebasedScope = []

        # loop through the variables
        print('Looping through variables...')
        for instVariable in [s for s in self.instModule.ModuleVariables if s.iCalculated == False]:
            # load properties
            sDataSetName                = getattr(instVariable,'sDataSetName')
            dfESGData                   = getattr(self.instModule.ModuleData,sDataSetName)
            sVarName                    = getattr(instVariable,'sVarName')
            sVarType                    = getattr(instVariable,'sVarType')
            sSuffix                     = getattr(instVariable,'sSuffix')
            iWaterfall                  = getattr(instVariable,'iWaterfall')
            iCoverage                   = getattr(instVariable,'iCoverage')
            iRebasing                   = getattr(instVariable,'iRebasing')
            sLeftOn                     = getattr(instVariable,'sLeftOn')
            sRightOn                    = getattr(instVariable,'sRightOn')
            sScope                      = getattr(instVariable,'sScope')
            if sVarName == None:
                print('All variables in data set '+ sDataSetName)
            else:
                print(sVarName + ' of type ' + sVarType + ' in data set '+ sDataSetName)

            # Option A: Merge entire ESG data set to holdings without specifying variable    
            if sVarName == None:
                self.dfAllPortfolios    = sma.merge_esg_data_new(self.dfAllPortfolios,dfESGData,None,sSuffix,iWaterfall,sLeftOn,sRightOn,sScope)

            # Option B: Merge specific ESG variables to holdings
            else:
                self.dfAllPortfolios    = sma.merge_esg_data_new(self.dfAllPortfolios,dfESGData,[sVarName],sSuffix,iWaterfall,sLeftOn,sRightOn,sScope)
                
                # categorical variables need to be pivotized, otherwise the waterfall can not be applied
                if sVarType == 'str':
                    print('Found a categorical variable.')
                    if iRebasing == True:
                        lCategoricalRebased             = lCategoricalRebased + [sVarName + '_' + s for s in self.dfAllPortfolios[sVarName].dropna().drop_duplicates().to_list()]
                        lCategoricalRebasedScope        = lCategoricalRebasedScope + [sScope for s in self.dfAllPortfolios[sVarName].dropna().drop_duplicates().to_list()]
                    else:
                        lCategoricalUnrebased           = lCategoricalUnrebased + [sVarName + '_' + s for s in self.dfAllPortfolios[sVarName].dropna().drop_duplicates().to_list()]
                        lCategoricalUnrebasedScope      = lCategoricalUnrebasedScope +  [sScope for s in self.dfAllPortfolios[sVarName].dropna().drop_duplicates().to_list()]


                    self.dfAllPortfolios[sVarName + 'temp'] = self.dfAllPortfolios[sVarName]
                    self.dfAllPortfolios    = pd.get_dummies(self.dfAllPortfolios,columns=[sVarName])            
                    self.dfAllPortfolios    = self.dfAllPortfolios.rename(columns = {sVarName + 'temp':sVarName})
                
                # flag uncovered issuers    
                if iCoverage == True:
                    self.dfAllPortfolios    = sma.calc_coverage(self.dfAllPortfolios,[sVarName],sScope)
                
        if len([s.iCalculated for s in self.instModule.ModuleVariables if s.iCalculated is True]) > 0:
            print('calculating variables')
            sma.calc_new_variables(self.dfAllPortfolios)
        # differentiate between rebased and unrebased variables for aggregation
        self.lRebased                       = [s.sVarName for s in self.instModule.ModuleVariables if s.sVarName is not None and s.iRebasing == True and s.sVarType not in ['ignore','str']] + lCategoricalRebased
        self.lRebasedScope                  = [s.sScope for s in self.instModule.ModuleVariables if s.sVarName is not None and s.iRebasing == True and s.sVarType not in ['ignore','str']] + lCategoricalRebasedScope
        self.lUnrebased                     = [s.sVarName for s in self.instModule.ModuleVariables if s.sVarName is not None and s.iRebasing != True and s.sVarType not in ['ignore','str']] + lCategoricalUnrebased #+ ['Scope']
        self.lUnrebasedScope                = [s.sScope for s in self.instModule.ModuleVariables if s.sVarName is not None and s.iRebasing != True and s.sVarType not in ['ignore','str']] + lCategoricalUnrebasedScope + ['Scope']

        self.lCoverageRebased               = ['Exposure' + s.sSuffix for s in self.instModule.ModuleVariables if s.sVarName == None and s.iRebasing == True] + ['Uncovered_Issuer_' + s.sVarName for s in self.instModule.ModuleVariables if s.sVarName != None and s.iCoverage == True and s.iRebasing == True]
        self.lCoverageRebasedScope          = [s.sScope for s in self.instModule.ModuleVariables if s.sVarName == None and s.iRebasing == True] + [s.sScope for s in self.instModule.ModuleVariables if s.sVarName != None and s.iCoverage == True and s.iRebasing == True]
        self.lCoverageUnrebased             = ['Exposure' + s.sSuffix for s in self.instModule.ModuleVariables if s.sVarName == None and s.iRebasing == False] + ['Uncovered_Issuer_' + s.sVarName for s in self.instModule.ModuleVariables if s.sVarName != None and s.iCoverage == True and s.iRebasing == False]
        self.lCoverageUnrebasedScope        = [s.sScope for s in self.instModule.ModuleVariables if s.sVarName == None and s.iRebasing == False] + [s.sScope for s in self.instModule.ModuleVariables if s.sVarName != None and s.iCoverage == True and s.iRebasing == False]

        self.lOutVars                       = self.lRebased + self.lUnrebased + self.lCoverageRebased + self.lCoverageUnrebased
       
        

        if len(self.instModule.sDate) == 1:
            self.lGroupByVariables = [s for s in lib_utils.lGroupByVariables if s != 'effective_date']
        else:
            self.lGroupByVariables = [s for s in lib_utils.lGroupByVariables]

    
    def calc_universes_new(self):
        self.dfUniverseMapping               = pd.read_excel(lib_utils.sWorkFolder + 'AMF_Config.xlsx')
        # save memory
        self.dfAllPortfolios = self.dfAllPortfolios[lib_utils.lVarListSec + ['portfolio_id',
                                                                             'eu_sfdr_sustainability_category', 
                                                                             'domicile', 
                                                                             'internal_asset_class', 
                                                                             'market_value_clean_firm',
                                                                             'issuer_market_cap_usd',
                                                                             'evic_usd',
                                                                             'market_cap_category',
                                                                             'evic_category',
                                                                             'ID_BB_PARENT_CO',
                                                                             'ID_BB_ULTIMATE_PARENT_CO',
                                                                             'Scope',
                                                                             'Scope_NAV',
                                                                             'country_code3',
                                                                             'Exposure_Sus',
                                                                             'sri_rating_final',
                                                                             'carbon_emissions_scope_12_inten',
                                                                             'Uncovered_Issuer_market_cap_category',
                                                                             'Uncovered_Issuer_evic_usd',
                                                                             'Uncovered_Issuer_evic_category',
                                                                             'Uncovered_Issuer_sri_rating_final', 
                                                                             'Uncovered_Issuer_carbon_emissions_scope_12_inten']]
        #self.dfAllPortfolios = self.dfAllPortfolios[lib_utils.lVarListSec + ['portfolio_id','eu_sfdr_sustainability_category', 'domicile', 'internal_asset_class', 'market_value_clean_firm','ID_BB_PARENT_CO','ID_BB_ULTIMATE_PARENT_CO','Scope','Scope_NAV','country_code3', 'Exposure_Sus','sri_rating_final','Uncovered_Issuer_sri_rating_final']]

        #dfISRExclusions = pd.concat([self.instModule.ModuleData.dfSusMinExclusions,self.instModule.ModuleData.dfExclusions.rename(columns = {'ID_BB_COMPANY':'id_bb_company'})])        
                    # Only covered
            #dfCovered                           = self.dfUniverse.dropna(subset = ['sri_rating_final'])[['ID_BB_COMPANY','sri_rating_final','market_cap_sector','weight']].drop_duplicates()
        
        
        for index, row in self.dfUniverseMapping.iterrows():
            # define exclusions
            if row['name'] == 'AMF':
                dfExclusions = self.instModule.ModuleData.dfSusMinExclusions.copy()
            elif row['name'] == 'TS':
                print('*************')
                print('merging AGI Sustainable Minimum Exclusions and custom exclusions')
                print('*************')
                #dfExclusions = pd.DataFrame()
                dfExclusions = self.instModule.ModuleData.dfSusMinExclusions.copy()
                lFileNames = ['AGI Label Towards Sustainability - Corporate_2025 March',
                              'AGI Label Towards Sustainability - Sovereigns_2025 March',
                              'Towards Sustainability - Urgewald 2024']
                for i in range(len(lFileNames)):
                    print(lFileNames[i])
                    dfTemp                                  = pd.read_excel(lib_utils.sWorkFolder + lFileNames[i] + '.xlsx')
                    dfTemp['ID']                            = lFileNames[i]
                    #dfTemp                                 = dfTemp.loc[dfTemp.ID_BB_COMPANY != 'No tradable securities']
                    dfTemp                                  = dfTemp[pd.to_numeric(dfTemp['ID_BB_COMPANY'], errors='coerce').notnull()]
                    dfTemp                                  = dfTemp[['ID','ID_BB_COMPANY']].drop_duplicates()
                    dfTemp['ID_BB_COMPANY']                 = dfTemp['ID_BB_COMPANY'].astype(int)
                    dfTemp['Excluded']                      = 1
                    dfTemp                                  = dfTemp.rename(columns = {'ID_BB_COMPANY':'id_bb_company'})
                    dfExclusions                            = pd.concat([dfExclusions,dfTemp])
                
            elif row['name'] == 'ISR':
                print('*************')
                print('merging AGI Sustainable Minimum Exclusions and custom exclusions')
                print('*************')
                
                dfExclusions = self.instModule.ModuleData.dfSusMinExclusions.copy()
                lFileNames = ['AGI Label ISR ex unconventional OG_Issuers_20250313',
                              'Label ISR_Urgewald 2024 Q4']
                if row['portfolio_id'] == 10004365:
                    lFileNames = lFileNames + ['AGI Custom Ircantec_Issuers_20250606']
                for i in range(len(lFileNames)):
                    print(lFileNames[i])
                    dfTemp                                  = pd.read_excel(lib_utils.sWorkFolder + lFileNames[i] + '.xlsx')
                    dfTemp['ID']                            = lFileNames[i]
                    #dfTemp                                 = dfTemp.loc[dfTemp.ID_BB_COMPANY != 'No tradable securities']
                    dfTemp                                  = dfTemp[pd.to_numeric(dfTemp['ID_BB_COMPANY'], errors='coerce').notnull()]
                    dfTemp                                  = dfTemp[['ID','ID_BB_COMPANY']].drop_duplicates()
                    dfTemp['ID_BB_COMPANY']                 = dfTemp['ID_BB_COMPANY'].astype(int)
                    dfTemp['Excluded']                      = 1
                    dfTemp                                  = dfTemp.rename(columns = {'ID_BB_COMPANY':'id_bb_company'})
                    dfExclusions                            = pd.concat([dfExclusions,dfTemp])
            elif row['name'] == 'TSISR':
                print('*************')
                print('merging AGI Sustainable Minimum Exclusions and custom exclusions')
                print('*************')
                
                dfExclusions = self.instModule.ModuleData.dfSusMinExclusions.copy()
                lFileNames = ['AGI Label Towards Sustainability - Corporate_2025 March',
                              'AGI Label Towards Sustainability - Sovereigns_2025 March',
                              'Towards Sustainability - Urgewald 2024',
                              'AGI Label ISR ex unconventional OG_Issuers_20250313',
                              'Label ISR_Urgewald 2024 Q4']
                for i in range(len(lFileNames)):
                    print(lFileNames[i])
                    dfTemp                                  = pd.read_excel(lib_utils.sWorkFolder + lFileNames[i] + '.xlsx')
                    dfTemp['ID']                            = lFileNames[i]
                    #dfTemp                                 = dfTemp.loc[dfTemp.ID_BB_COMPANY != 'No tradable securities']
                    dfTemp                                  = dfTemp[pd.to_numeric(dfTemp['ID_BB_COMPANY'], errors='coerce').notnull()]
                    dfTemp                                  = dfTemp[['ID','ID_BB_COMPANY']].drop_duplicates()
                    dfTemp['ID_BB_COMPANY']                 = dfTemp['ID_BB_COMPANY'].astype(int)
                    dfTemp['Excluded']                      = 1
                    dfTemp                                  = dfTemp.rename(columns = {'ID_BB_COMPANY':'id_bb_company'})
                    dfExclusions                            = pd.concat([dfExclusions,dfTemp])                
                
                
                
         
               

            
            
            ESG_Universe = Universe(self.dfAllPortfolios,
                                    dfExclusions,
                                    row['portfolio_id'],
                                    row['benchmark_id'],
                                    row['portfolio_type'],
                                    row['approach'],
                                    row['universe_reduction'],
                                    row['lower_bound'],
                                    row['name'],
                                    row['portfolio_name'])

            ESG_Universe.calc_eligible_universe()
            
            if row['export'] == 1:
                # append portfolios
                self.dfAllPortfolios                 = pd.concat([self.dfAllPortfolios,ESG_Universe.dfPortfolio])
                # append ESG universes
                self.dfAllPortfolios                 = pd.concat([self.dfAllPortfolios,ESG_Universe.dfEligibles])
                # append initial universes
                self.dfAllPortfolios                 = pd.concat([self.dfAllPortfolios,ESG_Universe.dfUniverse])

        self.dfAllPortfolios = self.dfAllPortfolios.loc[self.dfAllPortfolios.portfolio_type.isin(['Portfolio','Eligible Universe','Universe','Benchmark'])]
            
        self.lRebased                        = self.lRebased + ['Worst Rated Benchmark','Outside Benchmark','Exposure_Ex']
        self.lOutVars                        = self.lOutVars + ['Worst Rated Benchmark','Outside Benchmark','Exposure_Ex']


    def calc_impact(self):
        # portfolio level aggregation 
        print('Calculating portfolio level aggregates.')
        print(self.dfAllPortfolios.columns.to_list())
        self.dfOutAgg, dfOut, self.dfAllPortfolios = sma.calc_weighted_average_esg_new(self.dfAllPortfolios,
                                                                                       self.lRebased + self.lCoverageRebased,
                                                                                       self.lRebasedScope + self.lCoverageRebasedScope,
                                                                                       self.lUnrebased + self.lCoverageUnrebased,
                                                                                       self.lUnrebasedScope + self.lCoverageUnrebasedScope,
                                                                                       self.lGroupByVariables,
                                                                                       self.instModule.iLookthrough)

    def export_results(self,sWorkFolder,sName,lTabs):
        print('Export results...')
        #self.dfAllPortfolios = self.dfAllPortfolios.drop('Scope_PSR',axis = 1)
        #self.dfAllPortfolios = self.dfAllPortfolios.drop('ID_BB_COMPANY_PSR',axis = 1)

        
        lVarListOverview                = self.lOutVars
        lVarListPercentOverview         = [s.sVarName for s in self.instModule.ModuleVariables if s.sVarType != 'float' and s.sVarName is not None] + self.lCoverageRebased + self.lCoverageUnrebased + ['Scope']
        lVarListPercentDetails          = [s.sVarName for s in self.instModule.ModuleVariables if s.sVarType == 'perc' and s.sVarName is not None] + ['weight']
        lVarListBarsDetails             = [s.sVarName for s in self.instModule.ModuleVariables if s.sVarType != 'bool' and s.sVarName is not None] + ['weight']
        lVarListBoolDetails             = [s.sVarName for s in self.instModule.ModuleVariables if s.sVarType == 'bool' and s.sVarName is not None] + self.lCoverageRebased + self.lCoverageUnrebased + ['Worst Rated Benchmark','Outside Benchmark']


        lTabsOut = []
        print('')
        if 'Overview' in lTabs:
            print('******************************')
            print('Overview Tab')
            Exp = {     'data' :        self.dfOutAgg.reset_index(), 
                        'sSheetName' : 'Portfolio Overview',
                        'sPctCols':     lVarListPercentOverview + [x + '_ew' for x in lVarListPercentOverview],
                        'sBars':        lVarListOverview + [x + '_ew' for x in lVarListOverview],
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':False}
            lTabsOut.append(Exp)
            
        if 'Holdings' in lTabs:
            print('******************************')
            print('Holdings')
            if len(self.dfAllPortfolios)> 1500000:
                dfAllPortfoliosOut = self.dfAllPortfolios.loc[self.dfAllPortfolios.portfolio_type.isin(['Fund','Index'])]
            else:
                dfAllPortfoliosOut = self.dfAllPortfolios.copy()
            
            #dfAllPortfoliosOut = dfAllPortfoliosOut.loc[dfAllPortfoliosOut.benchmark_id.isin([-357])]
            
            Exp = {     'data' :        dfAllPortfoliosOut[lib_utils.lVarListSec + lVarListOverview].drop_duplicates(), 
                        'sSheetName' : 'Holdings View',
                        'sPctCols':     lVarListPercentDetails,
                        'sBars':        lVarListBarsDetails,
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   lVarListBoolDetails,
                        'sTrueFalse':   lib_utils.lVarListTrueFalse,
                        'sColsHighlight':False}
            lTabsOut.append(Exp)

        if 'Benchmark' in lTabs:
            print('******************************')
            print('Benchmark')
            dfPfBenchmark           = self.dfOutAgg.reset_index().loc[self.dfOutAgg.reset_index().portfolio_type.isin(['Fund','Benchmark'])]

            dfPfBenchmark           = sma.concat_portfolio_benchmark(dfPfBenchmark,self.dfBenchmarkBridge)
            # sort before pivot
            strcols                 = sma.sort_cols(lVarListOverview)

            dfPivot                 = dfPfBenchmark.groupby(['portfolio_benchmark_group_name','effective_date','portfolio_type'], sort=False)[['portfolio_name'] + strcols].first().unstack(['portfolio_type'])
            dfPivot.columns         = [s1 + str(s2) for (s1,s2) in dfPivot.columns.tolist()]
                  
            Exp = {     'data' :        dfPivot, 
                        'sSheetName' :  'Portfolio Overview Pivot',
                        'sPctCols':     [x + 'Fund' for x in lVarListPercentOverview] + [x + 'Benchmark' for x in lVarListPercentOverview],
                        'sBars':        [x + 'Fund' for x in lVarListOverview] + [x + 'Benchmark' for x in lVarListOverview],
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':False}
            lTabsOut.append(Exp)
            
        if 'Hist' in lTabs:
            print('******************************')
            print('Historical values')
            if len(self.instModule.sDate) > 1:
                dfPivot = self.dfOutAgg.reset_index().loc[self.dfOutAgg.reset_index().portfolio_type.isin(['Fund'])]
                dfPivot = dfPivot.groupby(['portfolio_id','effective_date'], sort=False)[['portfolio_name','eu_sfdr_sustainability_category','internal_asset_class','market_value_clean_firm'] + lVarListOverview].first().unstack('effective_date')
                dfPivot.columns =[s1 + str(s2) for (s1,s2) in dfPivot.columns.tolist()]
                
                Exp = {'data' :       dfPivot, 
                        'sSheetName' : 'Portfolio Dates Pivot',
                        'sPctCols':     False,
                        'sBars':        False,
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':False}
                lTabsOut.append(Exp)
                
        if 'Active_Weights' in lTabs:
            print('******************************')
            print('Active Weights')
            sVar                = 'carbon_emissions_scope_12_inten'
            dfPfBenchmark       = self.dfAllPortfolios.copy()
            #dfPfBenchmark.loc[dfPfBenchmark.portfolio_type == 'Portfolio','portfolio_type'] = 'Fund'
            #dfPfBenchmark.loc[dfPfBenchmark.portfolio_type == 'Index','portfolio_type'] = 'Benchmark'
            #dfPfBenchmark       = dfPfBenchmark.loc[dfPfBenchmark.portfolio_name.isin(['Euro-Aggregate: Corporates','Adjusted Model Portfolio for Sustainability Check_20240424_Holdings'])]
                                                                                               
            dfPfBenchmark       = dfPfBenchmark.loc[dfPfBenchmark.portfolio_type.isin(['Fund','Benchmark'])]
            
            dfPivot = sma.calc_active_weights(dfPfBenchmark,self.dfBenchmarkBridge,sVar,lib_utils.lVarListIssuer)
                                  
            Exp = {     'data':         dfPivot, 
                        'sSheetName':   'Active Weights',
                        'sPctCols':     [x + '_Fund' for x in lVarListPercentOverview + ['weight']] + [x + '_Benchmark' for x in lVarListPercentOverview+ ['weight']],
                        'sBars':        [x + '_Fund' for x in lVarListOverview + ['weight','contribution']] + [x + '_Benchmark' for x in lVarListOverview+ ['weight','contribution']] + ['diff'] + [sVar],
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':['diff']}
            lTabsOut.append(Exp)
           
            print('******************************')
            print('Top 10')
            dfTop                         = dfPivot.reset_index()
            dfTop                         = dfTop.loc[~dfTop.contribution_Fund.isna()]
            dfTop['pf_'+sVar]             = dfTop['contribution_Fund'].groupby(dfTop['portfolio_benchmark_group_name']).transform('sum')
            dfTop['contribution_FundPct'] = dfTop['contribution_Fund']/dfTop['pf_'+sVar]
            lVarlist                        = ['portfolio_benchmark_group_name',
                                               'issuer_long_name',
                                               'ID_BB_COMPANY']
                                               #'country_issue_name',
                                               #'gics_sector_description']
            dfTop1                           = dfTop.sort_values(sVar,ascending = False).head(10)[lVarlist + [sVar] + ['weight_Fund','contribution_FundPct']]
            dfBottom                        = dfTop.sort_values(sVar,ascending = False).tail(10)[lVarlist + [sVar] + ['weight_Fund','contribution_FundPct']]
                
            Exp = {     'data' :        dfTop1, 
                        'sSheetName' : 'Top 10 Carbon',
                        'sPctCols':     ['weight_Fund','contribution_FundPct'],
                        'sBars':        ['weight_Fund'] + [sVar] + ['contribution_FundPct'],
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':False}
            lTabsOut.append(Exp)            

            Exp = {     'data' :        dfBottom, 
                        'sSheetName' :  'Bottom 10 Carbon',
                        'sPctCols':     ['weight_Fund','contribution_FundPct'],
                        'sBars':        ['weight_Fund'] + [sVar] + ['contribution_FundPct'],
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':False}
            lTabsOut.append(Exp)    
            
            print('******************************')
            print('Sectors Benchmark')

            dfAll                                       = dfPivot.reset_index().copy()
            dfAll['BBG_l2_description']            = 'All'
            dfSectors                                   = pd.concat([dfPivot.reset_index(),dfAll])

            dfSectors = dfSectors.groupby(['portfolio_benchmark_group_name','BBG_l2_description']).agg({sVar:'mean','weight_Benchmark':'sum','weight_Fund':'sum','contribution_Benchmark':'sum','contribution_Fund':'sum'})            

            Exp = {     'data':         dfSectors, 
                        'sSheetName':   'Sectors vs Bmk',
                        'sPctCols':     [x + '_Fund' for x in lVarListPercentOverview + ['weight']] + [x + '_Benchmark' for x in lVarListPercentOverview+ ['weight']],
                        'sBars':        [x + '_Fund' for x in lVarListOverview + ['weight','contribution']] + [x + '_Benchmark' for x in lVarListOverview+ ['weight','contribution']] + ['diff'] + [sVar],
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':['diff']}
            lTabsOut.append(Exp)
        
        if 'Issuer' in lTabs:
            print('******************************')
            print('Issuer')
            dfPfBenchmark                               = self.dfAllPortfolios.loc[self.dfAllPortfolios['Scope_Inv'] == True]
            dfPfBenchmark                               = dfPfBenchmark.loc[dfPfBenchmark.portfolio_type.isin(['Fund','Benchmark','Index'])]
            dfPfBenchmark = sma.calc_group_average(dfPfBenchmark,
                                             self.lOutVars,
                                             'ID_BB_COMPANY')
            dfPfBenchmark.columns = [''.join(i) for i in dfPfBenchmark.columns.to_list()]
            Exp = {     'data' :        dfPfBenchmark, 
                        'sSheetName' : 'Issuer View',
                        'sPctCols':     [x for x in lVarListPercentOverview + ['weight','issuer_pct']],
                        'sBars':        [x for x in lVarListOverview + ['weight','issuer','issuer_pct']],
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':['diff']}
            lTabsOut.append(Exp)
        
        if 'Sectors_Absolute' in lTabs:
            print('******************************')
            print('Sectors')
            dfPfBenchmark                               = self.dfAllPortfolios.loc[self.dfAllPortfolios['Scope_Sov'] == True]
            dfPfBenchmark['weight']                               = dfPfBenchmark['weight'] / dfPfBenchmark.groupby('portfolio_name')['weight'].transform('sum')
            dfPfBenchmark                               = dfPfBenchmark.loc[dfPfBenchmark.portfolio_type.isin(['Fund','Benchmark','Index'])]
            dfPfBenchmark = sma.calc_group_average(dfPfBenchmark,
                                             self.lOutVars,
                                             'BBG_l2_description')
            dfPfBenchmark.columns = [''.join(i) for i in dfPfBenchmark.columns.to_list()]
            Exp = {     'data' :        dfPfBenchmark, 
                        'sSheetName' : 'Sectors',
                        'sPctCols':     [x for x in lVarListPercentOverview + ['weight','issuer_pct']],
                        'sBars':        [x for x in lVarListOverview + ['weight','issuer','issuer_pct']],
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':['diff']}
            lTabsOut.append(Exp)
            
        if 'Regions_Absolute' in lTabs:
            print('******************************')
            print('Regions')
            dfPfBenchmark                               = self.dfAllPortfolios.loc[self.dfAllPortfolios['Scope_Inv'] == True]
            dfPfBenchmark                               = dfPfBenchmark.loc[dfPfBenchmark.portfolio_type.isin(['Fund','Benchmark','Index'])]
            dfPfBenchmark = sma.calc_group_average(dfPfBenchmark,                                             self.lOutVars,
                                             'country_issue_name')
            dfPfBenchmark.columns = [''.join(i) for i in dfPfBenchmark.columns.to_list()]
            Exp = {     'data' :        dfPfBenchmark, 
                        'sSheetName' :  'Regions',
                        'sPctCols':     [x for x in lVarListPercentOverview + ['weight','issuer_pct']],
                        'sBars':        [x for x in lVarListOverview + ['weight','issuer','issuer_pct']],
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':['diff']}
            lTabsOut.append(Exp)  
        
        if 'MarketCapSectors_Absolute' in lTabs:
            print('******************************')
            print('MarketCapSectors')
            dfPfBenchmark                               = self.dfAllPortfolios.loc[self.dfAllPortfolios['Scope_Inv'] == True]
            dfPfBenchmark                               = dfPfBenchmark.loc[dfPfBenchmark.portfolio_type.isin(['Fund','Benchmark','Index'])]
            dfPfBenchmark = sma.calc_group_average(dfPfBenchmark,                                             self.lOutVars,
                                             'market_cap_sector')
            dfPfBenchmark.columns = [''.join(i) for i in dfPfBenchmark.columns.to_list()]
            Exp = {     'data' :        dfPfBenchmark, 
                        'sSheetName' : 'MarketCapSectors',
                        'sPctCols':     [x for x in lVarListPercentOverview + ['weight','issuer_pct']],
                        'sBars':        [x for x in lVarListOverview + ['weight','issuer','issuer_pct']],
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':['diff']}
            lTabsOut.append(Exp) 

        if 'Firm_View' in lTabs:
            print('******************************')
            print('Firm View')
            dfPfBenchmark                           = self.dfOutAgg.reset_index()
            dfPfBenchmark                           = dfPfBenchmark.loc[dfPfBenchmark.portfolio_type.isin(['Fund'])]
            dfPfBenchmark[self.lOutVars]            = dfPfBenchmark[self.lOutVars].fillna(0)
            dfAll                                   = dfPfBenchmark.copy()
            dfAll['internal_asset_class']           = 'All'
                        
            wavg                                    = lambda x: np.average(x, weights=dfPfBenchmark.loc[x.index, 'market_value_clean_firm'])
            d1                                      = dict.fromkeys(self.lOutVars, wavg)         
            d2                                      = dict.fromkeys(['market_value_clean_firm'],'sum')
            d                                       = {**d1, **d2}
            dfOut                                   = pd.concat([dfPfBenchmark.groupby('internal_asset_class').agg(d),dfAll.groupby('internal_asset_class').agg(d)])            
            
            Exp = {     'data' :        dfOut, 
                        'sSheetName' : 'Firm View',
                        'sPctCols':     lVarListPercentOverview,
                        'sBars':        lVarListOverview,
                        'sWide':        lib_utils.lVarListWide,
                        'sFalseTrue':   False,
                        'sTrueFalse':   False,
                        'sColsHighlight':False}
            lTabsOut.append(Exp)
      
        if 'Exclusions' in lTabs:
            print('******************************')
            print('Exclusions')

            lVars                       = ['Exposure_FW','Exposure_Sus','Exposure_Weap']
            lVarsContr                  = [s + '_Contr' for s in lVars]
            lVarsContrAdd               = [s + '_Add' for s in lVarsContr]
            lVarsContrDel               = [s + '_Del' for s in lVarsContr]
            
            
            
            dfExclusions                               = self.dfAllPortfolios.loc[self.dfAllPortfolios['Scope_Corp'] == True]
            dfExclusions                               = dfExclusions.loc[dfExclusions.portfolio_type.isin(['Fund','Benchmark','Index'])]
          
            dfExclusions[lVars]         = dfExclusions[lVars].astype(float)
            dfExclusions[lVarsContr]    = dfExclusions[lVars] - dfExclusions[lVars].shift(axis = 1).fillna(0)
            dfExclusions[lVarsContrAdd] = dfExclusions[lVarsContr].where(dfExclusions[lVarsContr]> 0).fillna(0)
            dfExclusions[lVarsContrDel] = dfExclusions[lVarsContr].where(dfExclusions[lVarsContr]<0).fillna(0)

            dfExclusions                = sma.calc_group_average(dfExclusions,
                                                         lVars + lVarsContr + lVarsContrAdd + lVarsContrDel,
                                                         'gics_sector_description')            
            Exp = {'data' : dfExclusions, 
            'sSheetName' : 'Exclusions',
            'sPctCols':     [x for x in lVarListPercentOverview + ['weight','issuer_pct']] + lVarsContr + lVarsContrAdd + lVarsContrDel,
            'sBars':        [x for x in lVarListOverview + ['weight','issuer','issuer_pct']] + lVarsContr + lVarsContrAdd + lVarsContrDel,
            'sWide':        lib_utils.lVarListWide,
            'sFalseTrue':   False,
            'sTrueFalse':   False,
            'sColsHighlight':['diff']}
            lTabsOut.append(Exp)
        
            
        if 'GB' in lTabs:
            lVars = ['country_issue_name',
             'country_name',
             'country_code3',
             'currency_traded',
             'ml_high_yield_sector',
             'agi_instrument_asset_type_description',
             'security_durable_key',
             'issuer_long_name',
             'long_name',
             'agi_instrument_type_description',
             'gics_sector_description',
             'bloomberg_security_typ',
             'id_isin',
             'id_cusip',
             'security_des',
             'issue_description',
             'ID_BB_COMPANY',
             'ID_BB_PARENT_CO',
             'ID_BB_ULTIMATE_PARENT_CO',
             'LONG_COMP_NAME',
             'ULT_PARENT_CNTRY_DOMICILE',
             'region',
             'ID_ISIN',
             'GREEN_BOND_LOAN_INDICATOR',
             'SOCIAL_BOND_IND',
             'SUSTAINABILITY_BOND_IND',
             #'SUSTNABILTY_LINKD_INDCTR'
             ]
            dfOut = self.dfAllPortfolios.loc[self.dfAllPortfolios.portfolio_type == 'Fund']
            dfOut = dfOut.loc[dfOut[['GREEN_BOND_LOAN_INDICATOR','SOCIAL_BOND_IND','SUSTAINABILITY_BOND_IND']].max(axis = 1) == 1]
            dfOut = dfOut[lVars].drop_duplicates()
            Exp = {'data' : dfOut, 
                        'sSheetName' : 'Green Bonds',
                        'sPctCols':False,
                        'sBars':['sri_rating_final'],
                        'sWide':['portfolio_name'],
                        'sFalseTrue': ['GREEN_BOND_LOAN_INDICATOR','SOCIAL_BOND_IND','SUSTAINABILITY_BOND_IND'],
                        'sTrueFalse':False,
                        'sColsHighlight':False}
            lTabsOut.append(Exp)

        buffer = BytesIO()
        sma.excel_export(pd.ExcelWriter(buffer, engine='xlsxwriter'),lTabsOut)

        # Step 2: Write buffer to Volume path
        volume_path = sWorkFolder + sName + '.xlsx'

        with open(volume_path, 'wb') as f:
            f.write(buffer.getvalue())
        
        if 'AMF' in lTabs:
            lVarlist                    = ['portfolio_id',
                                           'portfolio_name',
                                           'ID_BB_COMPANY',
                                           'issuer_long_name',
                                           'Worst Rated Benchmark',
                                           'Exposure_Sus',
                                           'Exposure_Ex',
                                           'sri_rating_final']

            lVarlistISR                    = ['portfolio_id',
                                           'portfolio_name',
                                           'ID_BB_COMPANY',
                                           'issuer_long_name',
                                           'issuer_market_cap_usd',
                                           'market_cap_category',
                                           'Worst Rated Benchmark',
                                           'Exposure_Sus',
                                           #'weight',
                                           'Exposure_Ex',
                                           'weights_adj',
                                           'sri_rating_final']

            lVarlistISREVIC                    = ['portfolio_id',
                                           'portfolio_name',
                                           'ID_BB_COMPANY',
                                           'issuer_long_name',
                                           'evic_usd',
                                           'evic_category',
                                           'Worst Rated Benchmark',
                                           'Exposure_Sus',
                                           #'weight',
                                           'Exposure_Ex',
                                           'weights_adj',
                                           'sri_rating_final']

            lVarlistISREVICSummary                    = ['portfolio_id',
                                           'portfolio_name',
                                           'evic_category',
                                           'Worst Rated Benchmark']

            lVarlistISRSummary                    = ['portfolio_id',
                                           'portfolio_name',
                                           'market_cap_category',
                                           'Worst Rated Benchmark']
            
            lVarlistCO2                    = ['portfolio_id',
                                              'portfolio_name',
                                            'ID_BB_COMPANY',
                                            'issuer_long_name',
                                            'Worst Rated Benchmark',
                                            'Exposure_Sus',
                                            #'AGI Label ISR_Issuers_20240207',
                                            'carbon_emissions_scope_12_inten']

            lVarlistEM                      = ['portfolio_id',
                                               'portfolio_type',	
                                               'portfolio_name',	
                                               'effective_date',	
                                               'issuer_long_name'	,
                                               'long_name'	,
                                               'id_isin'	,
                                               'ID_BB_COMPANY'	,
                                               'country_issue_name'	,
                                               'gics_sector_description'	,
                                               'agi_instrument_asset_type_description'	,
                                               'weight'	,
                                               'Worst Rated Benchmark']


            dfMapping = self.dfUniverseMapping.loc[self.dfUniverseMapping.export == 1]
            
            lSRI = dfMapping.loc[dfMapping.approach == 'sri_rating_final']['portfolio_id'].to_list()
            lCO2 = dfMapping.loc[dfMapping.approach == 'carbon_emissions_scope_12_inten']['portfolio_id'].to_list()
            lEM = dfMapping.loc[dfMapping.approach == 'em_debt']['portfolio_id'].to_list()
            lISR = dfMapping.loc[dfMapping.approach == 'sri_rating_final_mc']['portfolio_id'].to_list()
            lISREVIC = dfMapping.loc[dfMapping.approach == 'sri_rating_final_evic']['portfolio_id'].to_list()
            
            dfUniversesSRI = self.dfAllPortfolios.loc[(self.dfAllPortfolios.portfolio_type == 'Universe')
                                                           &(self.dfAllPortfolios.portfolio_id.isin(lSRI))]

            dfUniversesISR = self.dfAllPortfolios.loc[(self.dfAllPortfolios.portfolio_type == 'Universe')
                                                           &(self.dfAllPortfolios.portfolio_id.isin(lISR))]

            dfUniversesISREVIC = self.dfAllPortfolios.loc[(self.dfAllPortfolios.portfolio_type == 'Universe')
                                                           &(self.dfAllPortfolios.portfolio_id.isin(lISREVIC))]

            dfUniversesCO2 = self.dfAllPortfolios.loc[(self.dfAllPortfolios.portfolio_type == 'Universe')
                                                          &(self.dfAllPortfolios.portfolio_id.isin(lCO2))]

            dfUniversesEM = self.dfAllPortfolios.loc[(self.dfAllPortfolios.portfolio_type == 'Universe')
                                                           &(self.dfAllPortfolios.portfolio_id.isin(lEM))]
            
            dfThresholds                = self.dfAllPortfolios.loc[(self.dfAllPortfolios.portfolio_type == 'Universe')&(self.dfAllPortfolios['Worst Rated Benchmark'] == False)][['portfolio_name','portfolio_id','sri_rating_final']].groupby(['portfolio_name','portfolio_id'
                                                                                                                                                                              ]).min()['sri_rating_final'].reset_index()
            dfThresholds = dfThresholds.rename(columns = {'portfolio_name':'universe_name','sri_rating_final':'threshold'})
            dfThresholds = dfThresholds.merge(dfMapping[['portfolio_id','portfolio_name']],how = 'left',on = 'portfolio_id')
            
            dRes2 = {'data' : dfMapping, 
                        'sSheetName' : 'Mapping',
                        'sPctCols':False,
                        'sBars':False,
                        'sWide': ['portfolio_name','benchmark_name'],
                        'sFalseTrue': ['Worst Rated Benchmark'],
                        'sTrueFalse':False,
                        'sColsHighlight':False}
            
            dRes3 = {'data' : dfUniversesSRI[lVarlist].drop_duplicates(), 
                        'sSheetName' : 'Universes SRI',
                        'sPctCols':False,
                        'sBars':False,
                        'sWide': ['portfolio_name','issuer_long_name'],
                        'sFalseTrue': ['Worst Rated Benchmark'],
                        'sTrueFalse':False,
                        'sColsHighlight':False}

            dRes31 = {'data' : dfUniversesISR[lVarlistISR].drop_duplicates(), 
                        'sSheetName' : 'Universes ISR',
                        'sPctCols':False,
                        'sBars':False,
                        'sWide': ['portfolio_name','issuer_long_name'],
                        'sFalseTrue': ['Worst Rated Benchmark'],
                        'sTrueFalse':False,
                        'sColsHighlight':False}

            dRes32 = {'data' : dfUniversesISR[lVarlistISR].drop_duplicates().dropna(subset = ['sri_rating_final']).groupby(lVarlistISRSummary,as_index = False).agg({'sri_rating_final':'mean','ID_BB_COMPANY':'nunique'}).drop_duplicates(), 
                        'sSheetName' : 'Universes ISR Summary',
                        'sPctCols':False,
                        'sBars':False,
                        'sWide': ['portfolio_name','issuer_long_name'],
                        'sFalseTrue': ['Worst Rated Benchmark'],
                        'sTrueFalse':False,
                        'sColsHighlight':False}

            dRes33 = {'data' : dfUniversesISREVIC[lVarlistISREVIC].drop_duplicates(), 
                        'sSheetName' : 'Universes ISR EVIC',
                        'sPctCols':False,
                        'sBars':False,
                        'sWide': ['portfolio_name','issuer_long_name'],
                        'sFalseTrue': ['Worst Rated Benchmark'],
                        'sTrueFalse':False,
                        'sColsHighlight':False}

            dRes34 = {'data' : dfUniversesISREVIC[lVarlistISREVIC].drop_duplicates().dropna(subset = ['sri_rating_final']).groupby(lVarlistISREVICSummary,as_index = False).agg({'sri_rating_final':'mean','ID_BB_COMPANY':'nunique'}).drop_duplicates(), 
                        'sSheetName' : 'Universes ISR EVIC Summary',
                        'sPctCols':False,
                        'sBars':False,
                        'sWide': ['portfolio_name','issuer_long_name'],
                        'sFalseTrue': ['Worst Rated Benchmark'],
                        'sTrueFalse':False,
                        'sColsHighlight':False}                        
            
            dRes4 = {'data' : dfThresholds, 
                        'sSheetName' : 'SRI Thresholds',
                        'sPctCols':False,
                        'sBars':['sri_rating_final'],
                        'sWide':['portfolio_name'],
                        'sFalseTrue': 'Worst Rated Benchmark',
                        'sTrueFalse':False,
                        'sColsHighlight':False}

            dRes5 = {'data' : dfUniversesCO2[lVarlistCO2].drop_duplicates(), 
                        'sSheetName' : 'Universes CO2',
                        'sPctCols':False,
                        'sBars':False,
                        'sWide': ['portfolio_name','issuer_long_name'],
                        'sFalseTrue': ['Worst Rated Benchmark'],
                        'sTrueFalse':False,
                        'sColsHighlight':False}
            dRes6 = {'data' : dfUniversesEM[lVarlistEM].drop_duplicates(), 
                        'sSheetName' : 'Universe EM Debt',
                        'sPctCols':False,
                        'sBars':False,
                        'sWide': ['portfolio_name','issuer_long_name'],
                        'sFalseTrue': ['Worst Rated Benchmark'],
                        'sTrueFalse':False,
                        'sColsHighlight':False}
            buffer = BytesIO()
            sma.excel_export(pd.ExcelWriter(buffer,engine = 'xlsxwriter'),[dRes2,dRes3,dRes31,dRes32,dRes33, dRes34,dRes4,dRes5, dRes6])

            # Step 2: Write buffer to Volume path
            volume_path = sWorkFolder  + 'AMF_' + datetime.now().strftime('%Y-%m-%d %H%M') + '.xlsx'

            with open(volume_path, 'wb') as f:
                f.write(buffer.getvalue())
            
            

    def export_results_sql(self):
        
        lAMFVars = ['Exposure_Sus',
                    'Exposure_Ex',
                    'Uncovered_Issuer_sri_rating_final',
                    'sri_rating_final',
                    'Uncovered_Issuer_carbon_emissions_scope_12_inten',
                    'carbon_emissions_scope_12_inten',
                    'Worst Rated Benchmark',
                    'Outside Benchmark']
        
        df = self.dfAllPortfolios[lib_utils.lVarListSec + lAMFVars ]
        df = df.loc[df.portfolio_type == 'Portfolio']
        params = urllib.parse.quote_plus(
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=(localdb)\\MSSQLLocalDB;"
            "Database=PortfolioDB;"
            "Trusted_Connection=yes;"
        )
        
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        
        # Write DataFrame to SQL Server (will auto-create table if needed)
        df.to_sql("Impact", con=engine, if_exists="replace", index=False)
        
        df = self.dfOutAgg.reset_index()[self.lGroupByVariables  + lAMFVars  ]
        df = df.loc[df.portfolio_type == 'Portfolio']

        df.to_sql("Impact_Overview", con=engine, if_exists="replace", index=False)
            
class Data:
    """
    Object for ESG data load. Import dbx and external data sources.
    
    Can be initialized by:
        - Data set name
        
    Functions:
        - data_mastering: data manipulation and formatting
    """
    
    
    
    def __init__(self,lDataSetNames, lFileNames=None):
        
        # dremio imports
        sql = f"SELECT * FROM {lib_utils.dictDremio['dfLoadConfig']}"
        df_spark = lib_utils.spark.sql(sql)
        dfConfig = df_spark.toPandas()

        dfLoad = dfConfig[dfConfig['sDataSetName'].isin(lDataSetNames)]

        for _, row in dfLoad.iterrows():
            table_name = row['sDataSetName']

            # sGroupBy: nur wenn es ein nicht-leerer String ist
            sGroupBy = []
            if isinstance(row['sGroupBy'], str) and row['sGroupBy'].strip():
                sGroupBy = [row['sGroupBy'].strip()]

            # sDate: als String oder False
            if pd.notna(row['sDate']):
                sDate = str(int(row['sDate']))  # cast bigint to string safely
            else:
                sDate = False

            # sDateCol: leerer String, wenn nicht gesetzt
            sDateCol = row['sDateCol'].strip() if isinstance(row['sDateCol'], str) else ''

            # iHist: bool (wenn leer → False)
            iHist = bool(row['iHist']) if pd.notna(row['iHist']) else False

            # Dynamischer Load
            setattr(
                self,
                table_name,
                sma.load_dremio_data(
                    sDataset=table_name,
                    sGroupBy=sGroupBy,
                    sDate=sDate,
                    sDateCol=sDateCol,
                    iHist=iHist
                )
            )
        
        self.lExclusions = []
        if 'dfExclusions' in lDataSetNames:

            lFileNames = lFileNames

            self.dfExclusions                           = pd.DataFrame()
            for i in range(len(lFileNames)):
                print(lFileNames[i])
                dfTemp                                  = pd.read_excel(lib_utils.sWorkFolder + lFileNames[i] + '.xlsx')
                dfTemp['ID']                            = lFileNames[i]
                #dfTemp                                 = dfTemp.loc[dfTemp.ID_BB_COMPANY != 'No tradable securities']
                dfTemp                                  = dfTemp[pd.to_numeric(dfTemp['ID_BB_COMPANY'], errors='coerce').notnull()]
                dfTemp                                  = dfTemp[['ID','ID_BB_COMPANY']].drop_duplicates()
                dfTemp['ID_BB_COMPANY']                 = dfTemp['ID_BB_COMPANY'].astype(int)
                dfTemp['Excluded']                      = 1
                self.dfExclusions                       = pd.concat([self.dfExclusions,dfTemp])
            

                self.lExclusions.append(
                    Variable(
                        sDataSetName='dfExclusions',
                        sSuffix='_Ex',
                        sVarName=lFileNames[i],
                        sVarType='bool',
                        iCoverage = False,
                        iWaterfall = True,
                        sScope = 'Scope_NAV'
                    )
                )
                
            self.dfExclusions                           = self.dfExclusions.pivot_table(index = 'ID_BB_COMPANY',columns = 'ID',values = 'Excluded').fillna(0).astype(bool).reset_index()
            
        if 'dfExclusions_Susie' in lDataSetNames:
            # try:
            #     self.lExclusions
            # except NameError:
            #     self.lExclusions                        = []
          

            sql                                         = """
                                                            select *  from EXL_LIST
                                                          """
            dfListNames                                 = pd.read_sql(sql,lib_utils.conn)
            dfListNames                                 = dfListNames[['ID_EXL_LIST','OUT_VIEW_NAME']].drop_duplicates()
            # input list id
            lIDs                                        = [2]
            if len(lIDs) == 1:
                lIDs                                    = '(''{}'')'.format(str(lIDs[0]))
            else:
                lIDs                                    = str(tuple(lIDs))
            sql                                         = """
                                                            select *  from EXL_ISSUER_LIST_HISTO """" where ID_EXL_LIST in """ + lIDs + """ and insert_date = '2024-02-26' """
            dfISS                                       = pd.read_sql(sql,lib_utils.conn)            
            if len(dfISS) == 0:
                print('******************************')
                print('No data in prod. Trying dev...')
                print('******************************')

                dfISS                                   = pd.read_sql(sql,lib_utils.conndev)            
            if len(dfISS) == 0:
                raise Exception('No data in dev.')     
            
            dfISS                                       = pd.merge(dfISS,dfListNames,how = 'left',left_on = 'ID_EXL_LIST',right_on = 'ID_EXL_LIST',validate = 'm:1')   
            dfISS                                       = dfISS[['OUT_VIEW_NAME','ID_BB_COMPANY']].drop_duplicates()
            dfISS['Excluded']                           = 1
            
            for i in dfISS.OUT_VIEW_NAME.drop_duplicates().to_list():
                print(i)
                self.lExclusions.append(
                        Variable(
                            sDataSetName='dfExclusions_Susie',
                            sSuffix='_ExSusie',
                            sVarName=i,
                            sVarType='bool',
                            iCoverage = False
                        )
                    )
            self.dfExclusions_Susie                     = dfISS.pivot_table(index = 'ID_BB_COMPANY',
                                                                            columns = 'OUT_VIEW_NAME',
                                                                            values = 'Excluded').fillna(0).astype(bool).reset_index()

            # violations
            lVars                                       = ['issuerID',
                                                        'IssuerName',
                                                        'EDM_PTY_ID',
                                                        'ID_ISIN',
                                                        'SECTOR',
                                                        'COUNTRY']
            j = 0
            dfISS_Violations                            = pd.DataFrame()
            for i in dfISS.OUT_VIEW_NAME.drop_duplicates().to_list():
                sql                                     = """
                                                            select * from V_EXL_""" + str(i) + """_CALCULATED_OUT"""
                dfViolations                            = pd.read_sql(sql,lib_utils.conn)
                
                dfViolations                            = dfViolations.drop(lVars,axis = 1)
                
                if j == 0:
                    dfISS_Violations                    = dfViolations.copy()
                else:
                    dfISS_Violations                    = pd.merge(dfISS_Violations,dfViolations,how = 'outer',on = 'ID_BB_COMPANY',suffixes = ('',i))
                j = j + 1

            
            dfISS_Violations.replace({'GREEN': False, 'RED': True},inplace = True)
            dfISS_Violations                            = dfISS_Violations.fillna(False)
            self.dfExclusions_Susie_Violations          = dfISS_Violations.copy()
            
            for i in dfISS_Violations.drop('ID_BB_COMPANY',axis = 1).columns.to_list():
                print(i)
                self.lExclusions.append(
                        Variable(
                            sDataSetName='dfExclusions_Susie_Violations',
                            sSuffix='_ExSusie',
                            sVarName=i,
                            sVarType='bool',
                            iCoverage = False
                        )
                    )
  
        
        if 'dfMSCINZ' in lDataSetNames:
            self.dfMSCINZ = pd.read_excel(lib_utils.sWorkFolder + 'MSCIs NZIF dataset.xlsx')
            

        if 'dfEMDebt' in lDataSetNames:
            self.dfEMDebt                   = pd.read_excel(lib_utils.sWorkFolder + 'SMA_ESG_Framework_December2023_flat.xlsx')
            self.dfEMDebt = pd.merge(self.dfEMDebt,lib_utils.dfCountry.loc[lib_utils.dfCountry.trucost_is_primary == True][['country_code','country_code3']].drop_duplicates().dropna(),how = 'left',left_on = 'Country_Code',right_on = 'country_code3',validate = 'm:1')
            self.dfEMDebt['EM Debt Score < 5'] = False
            self.dfEMDebt.loc[self.dfEMDebt['ESG Score'] < 5,'EM Debt Score < 5'] = True
            self.dfEMDebt['FH Total Score < 35'] = False
            self.dfEMDebt.loc[self.dfEMDebt['Freedom House'] < 35,'FH Total Score < 35'] = True
            self.dfEMDebt = self.dfEMDebt.dropna(subset = ['country_code'])
    
             

        if 'dfSISn' in lDataSetNames:
            self.dfSISn = pd.read_excel(lib_utils.sWorkFolder + 'SIS_simulation_new_15022024.xlsx')
            lVarsPct = [s for s in self.dfSISn.columns.to_list() if s in lib_utils.lVarsDremioPct]
            if len(lVarsPct) > 0:
                self.dfSISn = sma.calc_decimal(self.dfSISn,lVarsPct)

        if 'dfSISSectors' in lDataSetNames:
            self.dfSISSectors = pd.read_sql("""SELECT a.[ID_BB_COMPANY]
                                                  ,[EDM_PTY_ID]
                                                  ,[ID_BB_REF_COMPANY]
                                                  ,a.[Trucost_Sector]
                                            	  ,b.[Trucost_Sector_Name]
                                                  ,a.[Trucost_Sector_Revenue_Percentage_PerCent]
                                                  ,[Positive_E_Contribution]
                                                  ,[Positive_S_Contribution]
                                              FROM [custom-ds].[SIS_TRUCOST_SECTOR_REVENUE_AND_ADJUSTMENT] a
                                              LEFT JOIN (SELECT DISTINCT(Trucost_Sector), Trucost_Sector_Name FROM dbo.TRUCOST_SECTOR_REVENUES) b
                                              on a.[Trucost_Sector] = b.[Trucost_Sector]""",lib_utils.conn)
            
                                              


        if 'dfMSCISovTS' in lDataSetNames:
            dfMSCISovTS = pd.read_excel(lib_utils.sWorkFolder + 'dfMSCISovTS.xlsx')
            dfMSCISovTS = dfMSCISovTS.dropna(subset = ['CARBON_GOVERNMENT_SCOPE_12_EXCL_LULUCF_TS'])
            dfMSCISovTS = dfMSCISovTS.pivot_table(index = ['ISSUER_CNTRY_DOMICILE','CARBON_GOVERNMENT_GHG_INTENSITY_GDP_TONPERMN'],columns = 'CARBON_GOVERNMENT_YEAR_TS', values = 'CARBON_GOVERNMENT_SCOPE_12_EXCL_LULUCF_TS')
            dfMSCISovTS.columns = [f'CARBON_GOVERNMENT_SCOPE_12_EXCL_LULUCF_TS_{col}' for col in dfMSCISovTS.columns]
            self.dfMSCISovTS = dfMSCISovTS.reset_index().copy()

        if 'dfTaxoAct' in lDataSetNames:
            
            if 1== 1:
                dfTaxo = pd.read_csv(lib_utils.sWorkFolder + 'EUTaxonomy_Allindicators_Issuers_20241231.csv')
                
                lEx = ['EUTaxElecProdGasExp',
                'EUTaxGasHEfCoGenExp',
                'EUTaxGasHeatCoolExp',
                'EUTaxNucConsNewExp',
                'EUTaxNucElecGenExp',
                'EUTaxNucPreComCycExp']
                
                lInput = [
                'EUTaxElecProdGas',
                'EUTaxGasHEfCoGen',
                'EUTaxGasHeatCool',
                'EUTaxNucConsNew',
                'EUTaxNucElecGen',
                'EUTaxNucPreComCyc']
                
                lSuffixes = ['CapEx','Rev']
                
                temp = [f"{item}{suffix}" for item in lInput for suffix in lSuffixes]
                
                lSuffixes1 = ['ENAA','ENAM','OvAlSA','OvAlSM','NonEligib']
                lSuffixes2 = ['ENAM','OvAlSM','NonEligib']
                
                lVarsCapEx = [f"{item}{suffix}" for item in temp if 'CapEx' in item for suffix in lSuffixes1]
                lVarsRev = [f"{item}{suffix}" for item in temp if 'Rev' in item for suffix in lSuffixes2]
                
                lAgg = ['EUTaxTotalEligibleActivityCapEx',
                        'EUTaxTotalEligibleActivityRev',
                        'EUTaxTotalAlignedCapEx',
                        'EUTaxTotalAlignedRevenues',
                        'EUTaxTotAlignedCapExM',
                        'EUTaxTotalAlignedRevenuesM',
                        'EUTaxTotAlignedCapExA',
                        'EUTaxTotalAlignedRevenuesA',
                        'EUTaxTotAlignedTransCapExM',
                        'EUTaxTotalAlignedTransitionRevM',
                        'EUTaxTotAlignedTransCapExA',
                        'EUTaxTotalAlignedTransitionRevA',
                        'EUTaxTotAlignedEnablCapExM',
                        'EUTaxTotalAlignedEnablingRevM',
                        'EUTaxTotAlignedEnablCapExA',
                        'EUTaxTotalAlignedEnablingRevA']
                
                dfTaxo[lVarsCapEx + lVarsRev + lAgg] = dfTaxo[lVarsCapEx + lVarsRev + lAgg].apply(pd.to_numeric, errors='coerce')
                dfTaxo[lEx] = dfTaxo[lEx].replace({"T": True, "F": False}).applymap(lambda x: x if isinstance(x, bool) else False)
                
                dfTaxo.to_pickle(lib_utils.sWorkFolder + 'dfTaxoAct.pkl')
                self.dfTaxoAct = dfTaxo
            else:
                self.dfTaxoAct = pd.read_pickle(lib_utils.sWorkFolder + 'dfTaxoAct.pkl')                

        if 'dfTaxoTest' in lDataSetNames:
            self.dfTaxoTest = pd.read_excel(lib_utils.sWorkFolder + 'Taxo_ISS.xlsx')
            self.dfTaxoTest = pd.merge(self.dfTaxoTest,lib_utils.dfSecurities,how = 'left',left_on = 'ISIN',right_on = 'ID_ISIN')
            self.dfTaxoTest = self.dfTaxoTest.rename(columns = {'ID_BB_COMPANY':'id_bb_company'})
            self.dfTaxoTest = self.dfTaxoTest[['id_bb_company','EUTaxTotalEligibleActivityRev','EUTaxTotalAlignedRevenues','EUTaxTotalLikelyAlignedRevenues']]
            self.dfTaxoTest = self.dfTaxoTest.drop_duplicates().dropna(subset = ['id_bb_company']).drop_duplicates(subset = ['id_bb_company'])
            


        if 'dfUI' in lDataSetNames:
            self.dfUI                   = pd.read_sql(""" select * from UI_ESG_COMPANY a left join UI_ESG_AGG_SCORE b on a.SECTOR_REF = b.SECTOR""",lib_utils.conn)



        if 'dfKPItest' in lDataSetNames:
            #sql = """select * from dal_prod_silver.msci_esg_api.esg_carbon_metrics where dal_effective_date  = '2025-06-19'"""            
            #dfTemp = pd.read_sql(sql,lib_utils.conndremio)
            #dfTemp.to_pickle(lib_utils.sWorkFolder + 'dfKPItest.pkl')
            
            self.dfKPItest = pd.merge(pd.read_pickle(lib_utils.sWorkFolder + 'dfKPItest.pkl'),lib_utils.dfSecurities,how = 'left',left_on= 'ISSUER_ISIN',right_on = 'ID_ISIN')
            self.dfKPItest = self.dfKPItest.drop_duplicates(subset = ['ID_BB_COMPANY'])                   
        if 'dfKPIProj' in lDataSetNames:
            # dfData              = pd.read_excel(lib_utils.sWorkFolder + 'Screen Results - 20240206 14_51_04.xlsx')
            # dfEVIC              = dfData[['ID_BB_COMPANY','EVIC_YEAR_TS','EVIC_USD_TS']].copy()
            # dfEVIC = dfEVIC.loc[dfEVIC['EVIC_YEAR_TS'].isin([2022,2023,2024,2025])]
            # dfEVIC = dfEVIC.pivot_table(index = 'ID_BB_COMPANY',columns = 'EVIC_YEAR_TS',values = 'EVIC_USD_TS')
            # dfEVIC = dfEVIC.ffill(axis = 1).reset_index()
            # dfEVIC = dfEVIC.rename(columns = {2023.0: 'EVIC'})

            # dfKPIHist = dfData.loc[dfData['PROJECTED_EMISSIONS_BUDGET_YEAR_TS'].isin([2022,2023,2024,2025])]
            # dfKPIHist['CARBON_EMISSIONS_SCOPE12'] = dfKPIHist['S1_ANNUAL_PROJECTED_EMISSIONS_TS'] + dfKPIHist['S2_ANNUAL_PROJECTED_EMISSIONS_TS']
            # dfKPIHist = dfKPIHist[['ID_BB_COMPANY','PROJECTED_EMISSIONS_BUDGET_YEAR_TS','CARBON_EMISSIONS_SCOPE12']].dropna().pivot_table(index = 'ID_BB_COMPANY',columns = 'PROJECTED_EMISSIONS_BUDGET_YEAR_TS',values = 'CARBON_EMISSIONS_SCOPE12')

            # dfOut = pd.merge(dfKPIHist.reset_index(),dfEVIC[['ID_BB_COMPANY','EVIC']],how = 'left',on = 'ID_BB_COMPANY').set_index('ID_BB_COMPANY')
            # dfOut = dfOut.div(dfOut['EVIC'], axis=0)
            # dfOut.columns = ['GHG_Intensity_EVIC_2022','GHG_Intensity_EVIC_2023','GHG_Intensity_EVIC_2024','GHG_Intensity_EVIC_2025','EVIC']
            # dfOut.reset_index().to_pickle(lib_utils.sWorkFolder + 'Projected_GHG_Intensity.pkl')
            self.dfKPIProj      = pd.read_pickle(lib_utils.sWorkFolder + 'Projected_GHG_Intensity.pkl')
            
        if 'dfKPIScope3Hist' in lDataSetNames:
            self.dfKPIScope3Hist                   = pd.read_excel(lib_utils.sWorkFolder + 'MSCI_carbon_data_historic.xlsx')
            self.dfKPIScope3Hist.drop_duplicates(subset = ['ID_BB_COMPANY'],inplace = True)
        if 'dfFinancials' in lDataSetNames: 
            dfRatings            = pd.read_sql(""" SELECT * FROM dal_prod_gold.dimensions.security_ratings_b where is_current = True and rating_type in ('RTG_FIT_LT', 'RTG_MDY_LT', 'RTG_SP_LT') """,lib_utils.conndremio)
            dfRatings           = pd.merge(dfRatings,pd.read_excel(lib_utils.sWorkFolder + 'ratings.xlsx'),how = 'left',on = ['rating_type','rating'])
            dfRatings            = dfRatings.pivot_table(index='security_durable_key', columns='rating_type', values=['rating', 'score'], aggfunc='first').reset_index()
            dfRatings.columns = [
                f"{col[1]}" if col[0] == 'rating' else
                f"{col[1]}_score" if col[0] == 'score' else col[0]
                for col in dfRatings.columns
            ]
            for col in [col for col in dfRatings.columns if '_score' in col]:
                dfRatings[f"{col}_bucket"] = np.where(
                dfRatings[col].between(1, 7), 'A',  # Bucket A for 1-6
                np.where(dfRatings[col].between(8, 22), 'B', np.nan)  # Bucket B for 7-25
            )
            dfRatings = dfRatings.drop_duplicates(subset = ['security_durable_key'])
            dfFinancials        = pd.read_sql(""" SELECT * FROM dal_prod_gold.dimensions.security_analytics_f where analytic_date = (select max(analytic_date) from dal_prod_gold.dimensions.security_analytics_f) and data_source = 'GIDP' and analytics_source_key = 17""",lib_utils.conndremio)
            self.dfFinancials        = pd.merge(dfRatings,dfFinancials,how = 'outer',on = 'security_durable_key')
            
        if 'dfEVIC_TS' in lDataSetNames:
            
            # sql = """ SELECT * FROM dal_prod_silver.msci_esg_api.issuer_timeseries where EVIC_YEAR_TS in (2023,2024) """

            # dfEVIC = pd.read_sql(sql,lib_utils.conndremio)
            
            # dfEVIC = pd.merge(dfEVIC,lib_utils.dfSecurities,how = 'left',left_on = 'ISSUER_ISIN',right_on = 'ID_ISIN')
            
            # dfOut = dfEVIC.pivot_table(values = 'EVIC_EUR_TS',columns = 'EVIC_YEAR_TS', index = 'ID_BB_COMPANY').reset_index()
            
            # dfOut.columns = ['ID_BB_COMPANY','EVIC_EUR_TS_base','EVIC_EUR_TS_T']
            self.dfEVIC_TS = pd.read_pickle(lib_utils.sWorkFolder + 'dfEVIC_TS.pkl')
        


 
        if 'dfCVAR' in lDataSetNames:
            self.dfCVAR          = pd.read_excel(lib_utils.sWorkFolder + 'Screen Results - 20241205 15_53_43.xlsx')
            self.dfCVAR          = pd.merge(self.dfCVAR,lib_utils.dfSecurities,how = 'left',left_on = 'ISSUER_ISIN',right_on = 'ID_ISIN')
 
 
        if 'dfMSCIProj' in lDataSetNames:
            dfMSCIProj             = pd.read_excel(lib_utils.sWorkFolder + 'Screen Results - 20240926 09_55_25.xlsx')
            dfMSCIProj             = dfMSCIProj.merge(lib_utils.dfSecurities, how = 'left',left_on = 'ISSUER_ISIN', right_on = 'ID_ISIN')
            dfMSCIProj = dfMSCIProj.pivot_table(index='ID_BB_COMPANY', columns='PROJECTED_EMISSIONS_BUDGET_YEAR_TS', values=[#'S1_ANNUAL_PROJECTED_CREDIBLE_INTENSITY_TS', 
                                                                                                                             #'S2_ANNUAL_PROJECTED_CREDIBLE_INTENSITY_TS',
                                                                                                                             #'S3_ANNUAL_PROJECTED_CREDIBLE_INTENSITY_TS',
                                                                                                                             'S1_ANNUAL_PROJECTED_INTENSITY_TARGET_FACE_VALUE_TS',
                                                                                                                             'S2_ANNUAL_PROJECTED_INTENSITY_TARGET_FACE_VALUE_TS',
                                                                                                                             'S3_ANNUAL_PROJECTED_INTENSITY_TARGET_FACE_VALUE_TS',
                                                                                                                             #'S1_ANNUAL_PROJECTED_EMISSIONS_TARGET_FACE_VALUE_TS',
                                                                                                                             #'S2_ANNUAL_PROJECTED_EMISSIONS_TARGET_FACE_VALUE_TS',
                                                                                                                             #'S3_ANNUAL_PROJECTED_EMISSIONS_TARGET_FACE_VALUE_TS',
                                                                                                                             #'S1_ANNUAL_PROJECTED_EMISSIONS_WITHOUT_TARGETS_TS',
                                                                                                                             #'S2_ANNUAL_PROJECTED_EMISSIONS_WITHOUT_TARGETS_TS',
                                                                                                                             #'S3_ANNUAL_PROJECTED_EMISSIONS_WITHOUT_TARGETS_TS'
                                                                                                                             ])
            dfMSCIProj.columns = ['{}_{}'.format(col[0], int(col[1])) for col in dfMSCIProj.columns]
            self.dfMSCIProj = dfMSCIProj.copy().reset_index()
            


        if 'dfNACE' in lDataSetNames:
            sql = """ select distinct         
                        ID_BB_COMPANY as id_bb_company,
                        NACE_SECTOR_NAME as nace_sector_name from dal_prod_gold.bbo.instrument_company"""
            self.dfNACE                 = pd.read_sql(sql,lib_utils.conndremio)


        # other imports    
        if 'dfSRIRaw' in lDataSetNames:
            self.dfSRIRaw               = sma.load_sri_data(False)

        if 'dfSustain' in lDataSetNames:
            self.dfSustain = pd.read_pickle(lib_utils.sWorkFolder + 'dfSustain.pkl')
            self.dfSustain['MSCI below 3'] = False
            self.dfSustain.loc[self.dfSustain.MSCI_INDUSTRY_ADJUSTED_SCORE < 3,'MSCI below 3'] = True
            self.dfSustain['Sustainalytics above 25'] = False
            self.dfSustain.loc[self.dfSustain.SUSTAINALYTICS_ESG_Risk_Score > 25,'Sustainalytics above 25'] = True
            self.dfSustain['SRI below 1'] = False
            self.dfSustain.loc[self.dfSustain.SRI_Rating_Final < 1,'SRI below 1'] = True
            
        if 'dfSRI_Sectors' in lDataSetNames:
            self.dfSRI_Sectors               = pd.read_sql(""" select * from V_RATING_FINAL_TOP_DOWN_OFFICIAL where id_universe = 9 """,lib_utils.conn)
           
                    
        if 'dfKPIhist' in lDataSetNames:
            self.dfKPIhist              = sma.load_kpi_data_hist()
            

        
        if 'dfC4F' in lDataSetNames:
            self.dfC4F                  = sma.load_c4f_data('2022-10-05-18-54-43_Bia_Corporates.xlsx')
    
        if 'dfGB' in lDataSetNames:           
            self.dfGB                   = sma.load_green_bonds()

        if 'dfMS' in lDataSetNames:           
            self.dfMS                   = pd.read_excel(lib_utils.sWorkFolder + 'MSP_df_december_23.xlsx')
            
        if 'dfMoody' in lDataSetNames:
            sql = """ SELECT a.*, a.dal_effective_date as effective_date, b.ID_BB_COMPANY as id_bb_company FROM dal_prod_silver.vigeo_eiris.ve_equitics_global_lir_additional_criteria a left join dal_prod_gold.bbo.instrument_company b on a.ISIN = b.ID_ISIN where a.dal_effective_date = '2024-12-31'"""
            self.dfMoody = pd.read_sql(sql,lib_utils.conndremio)

        # test data
    
        
        # if 'dfITR' in lDataSetNames:
        #     self.dfITR                  = pd.read_excel(lib_utils.sWorkFolder + 'MSCI ITR.xlsx').drop_duplicates(subset = ['ID_BB_COMPANY'])
        

        if 'dfSISUpdate' in lDataSetNames:
            self.dfSISUpdate                  = pd.read_excel(lib_utils.sWorkFolder + 'SIS_newDNSHandNZASboost_simulation_10042025.xlsx')
            self.dfSISUpdate[['SIS current','SIS new DNSH','SIS new DNSH + NZAS boost']]  = self.dfSISUpdate[['SIS current','SIS new DNSH','SIS new DNSH + NZAS boost']]/100

        if 'dfPSRBS' in lDataSetNames:
            self.dfPSRBS                  = pd.read_pickle(lib_utils.sWorkFolder + 'dfPSRBS.pkl')


        if 'dfPSRSov'in lDataSetNames:
            self.dfPSRSov = pd.read_excel(lib_utils.sWorkFolder + 'PSR_S_08_01_It_1.xlsx')
            dfIDs = pd.read_excel(lib_utils.sWorkFolder + 'Country_Names_ID.xlsx')
            self.dfPSRSov = pd.merge(self.dfPSRSov,dfIDs,how = 'left',left_on = 'Country_Code',right_on = 'ISO Code 3',validate = 'm:1')
            self.dfPSRSov = self.dfPSRSov.rename(columns = {'ID_BB_COMPANY':'id_bb_company'}).dropna(subset = ['id_bb_company']).drop_duplicates(subset = ['id_bb_company'])
            

            
        if 'dfLabel' in lDataSetNames:
            self.dfLabel                = sma.load_social_data()
        
        if 'dfPillar' in lDataSetNames:
            sql = """  select distinct ID_BB_COMPANY, ID_SECTOR_REF from V_RATING_FINAL_TOP_DOWN_OFFICIAL where id_universe = 9 and insert_date = '2024-11-29'"""
            dfSectors = pd.read_sql(sql,lib_utils.conn)
            sql = """  SELECT [ID_DOMAIN_WEIGHT],[ID_DOMAIN],[ID_SECTOR],[VALUE] FROM [dbo].[DOMAIN_WEIGHT]"""
            dfPillar = pd.read_sql(sql,lib_utils.conn)
            dfPillar = dfPillar.pivot_table(index = 'ID_SECTOR',columns = 'ID_DOMAIN',values = 'VALUE').reset_index()
            self.dfPillar = pd.merge(dfSectors,dfPillar,how = 'left', left_on = 'ID_SECTOR_REF', right_on = 'ID_SECTOR', validate = 'm:1')
            self.dfPillar = self.dfPillar.rename(columns = {'ID_BB_COMPANY':'id_bb_company'})
            
        if 'dfEquileap' in lDataSetNames:
            self.dfEquileap = pd.read_excel(lib_utils.sWorkFolder + 'bm_social kpi_equileap data.xlsx')
            lVars = ['chairperson_gender',	
                     'ceo_gender'	,
                     'cfo_gender'	,
                     'pct_women_board'	,
                     'pct_female_exec'	,
                     'pct_women_senior_mgt'	,
                     'pct_women_mgt'	,
                     'pct_women_emplys'	,
                     'prmtn_career_devl'	,
                     'living_wage_policy'	,
                     'eq_pay_pub'	,
                     'eq_pay_pub3'	,
                     'eq_pay_strat'	,
                     'eq_pay_gap'	,
                     'eq_pay_gap3'	,
                     'mean_unadjusted_nmbr'	,
                     'mean_adjusted_nmbr'	,
                     'median_unadjusted_nmbr',	
                     'median_adjusted_nmbr'	,
                     'pay_gap_bands_nmbr'	,
                     'parental_leave_primary_company'	,
                     'parental_leave_primary_num_weeks_company'	,
                     'parental_leave_primary_statutory'	,
                     'parental_leave_primary_num_weeks_statutory'	,
                     'parental_leave_primary_evaluation'	,
                     'parental_leave_primary_num_weeks_evaluation'	,
                     'parental_leave_secondary_company'	,
                     'parental_leave_secondary_num_weeks_company'	,
                     'parental_leave_secondary_statutory'	,
                     'parental_leave_secondary_num_weeks_statutory'	,
                     'parental_leave_secondary_evaluation'	,
                     'parental_leave_secondary_num_weeks_evaluation',	
                     'flex_hours'	,
                     'flex_loc'	,
                     'training_policy'	,
                     'equal_opportunity_policy'	,
                     'anti_abuse_policy'	,
                     'health_safety_policy'	,
                     'human_rights_policy'	,
                     'social_supply_chain_mgmt'	,
                     'sup_dvsty_program'	,
                     'emp_prot_whistle_blower_policy'	,
                     'wmns_empo_principle'	,
                     'certificate']
            lVarsPct = ['pct_women_board',
                        'pct_female_exec',
                        'pct_women_senior_mgt',
                        'pct_women_mgt',
                        'pct_women_emplys',
                        'prmtn_career_devl',                        
                        'mean_unadjusted_nmbr',
                        'mean_adjusted_nmbr',
                        'median_unadjusted_nmbr',
                        'median_adjusted_nmbr'
                        ]
            
            lVarsTrueFalse = [
                'living_wage_policy',
                'eq_pay_pub',
                'eq_pay_pub3',
                'eq_pay_strat',
                'eq_pay_gap',
                'eq_pay_gap3',
                'mean_unadjusted_nmbr',
                'mean_adjusted_nmbr',
                'median_unadjusted_nmbr',
                'median_adjusted_nmbr',
                'pay_gap_bands_nmbr',
                'parental_leave_primary_company',
                'parental_leave_primary_num_weeks_company',
                'parental_leave_primary_statutory',
                'parental_leave_primary_num_weeks_statutory',
                'parental_leave_primary_evaluation',
                'parental_leave_primary_num_weeks_evaluation',
                'parental_leave_secondary_company',
                'parental_leave_secondary_num_weeks_company',
                'parental_leave_secondary_statutory',
                'parental_leave_secondary_num_weeks_statutory',
                'parental_leave_secondary_evaluation',
                'parental_leave_secondary_num_weeks_evaluation',
                'flex_hours',
                'flex_loc',
                'training_policy',
                'equal_opportunity_policy',
                'anti_abuse_policy',
                'health_safety_policy',
                'human_rights_policy',
                'social_supply_chain_mgmt',
                'sup_dvsty_program',
                'emp_prot_whistle_blower_policy',
                'wmns_empo_principle'
                    ]
            lVarsGender = ['chairperson_gender','ceo_gender','cfo_gender']

            
            
            self.dfEquileap[lVars] = self.dfEquileap[lVars].replace('ND', np.nan)
            self.dfEquileap[lVars] = self.dfEquileap[lVars].replace('Not covered', np.nan)
            self.dfEquileap[lVarsPct] = self.dfEquileap[lVarsPct]/100
            self.dfEquileap[lVarsTrueFalse] = self.dfEquileap[lVarsTrueFalse].replace('Y', 1)
            self.dfEquileap[lVarsTrueFalse] = self.dfEquileap[lVarsTrueFalse].replace('N', 0)
            self.dfEquileap[lVarsGender] = self.dfEquileap[lVarsGender].replace('Female', 1)
            self.dfEquileap[lVarsGender] = self.dfEquileap[lVarsGender].replace('Male', 0)
     
        
        # apply mastering for some variables      
        self.data_mastering(lDataSetNames)


    def data_mastering(self,lDataSetNames):

        # test data
        if 'dfBVI' in lDataSetNames:
            self.dfBVI                  = self.dfBVI.loc[self.dfBVI.ID_BB_COMPANY != 'No tradable securities']
            self.dfBVI['BVI']           = True
            self.dfBVI                  = self.dfBVI[['ID_BB_COMPANY','BVI']].drop_duplicates()

        if 'dfDEKRA' in lDataSetNames:
            self.dfDEKRA                  = self.dfDEKRA.loc[self.dfDEKRA.ID_BB_COMPANY != 'No tradable securities']
            self.dfDEKRA['DEKRA']           = True
            self.dfDEKRA                  = self.dfDEKRA[['ID_BB_COMPANY','DEKRA']].drop_duplicates()


        if 'dfWeapons' in lDataSetNames:
            self.dfWeapons                  = self.dfWeapons.loc[self.dfWeapons.ID_BB_COMPANY != 'No tradable securities']
            self.dfWeapons['Weapons']           = True
            self.dfWeapons                  = self.dfWeapons[['ID_BB_COMPANY','Weapons']].drop_duplicates()


        if 'dfBVI_new' in lDataSetNames:
            self.dfBVI_new                 = self.dfBVI_new.loc[self.dfBVI_new.ID_BB_COMPANY != 'No tradable securities']
            self.dfBVI_new['BVI']           = True
            self.dfBVI_new                  = self.dfBVI_new[['ID_BB_COMPANY','BVI']].drop_duplicates()
        
       
        if 'dfNZ' in lDataSetNames:
            self.dfNZ['Coverage_SBTI_TPI_CDP'] = self.dfNZ[['sbti_covered','tpi_covered','cdp_covered']].max(axis = 1)
         
        if 'dfSusMinExclusions_D' in lDataSetNames:
            lVarList =  ['NBSOverallFlag',
                         'APMinesOverallFlag',
                         'BiologicalWeaponsOverallFlag',
                         'ChemicalWeaponsOverallFlag',
                         'ClusterMunitionsOverallFlag',
                         'DepletedUraniumOverallFlag',
                         'WhitePhosphorusOverallFlag',
                         'NuclearWeaponsNonNPTOverallFlag',
                         'NuclearWeaponsOverallFlag',
                         'MilitaryEqmtRevShareMax',
                         'CivFARevShareMax',
                         'CoalMiningRevShareMaxThermal',
                         'PowGenRevShareCoalMax',
                         'TobaccoInvolvement',
                         'TobaccoDistMaxRev']
            self.dfSusMinExclusions_D[lVarList] = self.dfSusMinExclusions_D[lVarList].replace({'GREEN':False,'RED':True})
            

dfVarDef = pd.read_sql(""" select * from """ + lib_utils.dictDremio['dfVarDef'],lib_utils.conndremio_dev)

lVariables = [
    Variable(
        row['sDataSetName'],
        row['sSuffix'],
        row['sVarName'],
        row['sVarType'],
        row['iCoverage'],
        row['iRebasing'],
        row['iWaterfall'],
        row['sLeftOn'],
        row['sRightOn'],
        row['sScope'],
        row['iCalculated']
    )
    for _, row in dfVarDef.iterrows()
]
