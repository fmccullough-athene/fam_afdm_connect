import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import configparser
import json as js
import math
import numpy as np

def load_csv_test(filepath_ail):

    # Testing to time csv load with no data types
    df_ail_data             = pd.read_csv(  filepath_ail,
                                            sep     = '\t' 
                                         )

    return(df_ail_data)

def load_us_deferred_ail_file(filepath_ail):

    ## define lists
    list_ail_fields     = [ 'ck.Plan','ck.IssAge','ck.Gender','ck.Class','ck.Char1','ck.Char2','ck.Char3','ck.Char4','ck.Char5','ck.Char6','ck.Char7',\
'ck.Char8','ck.IssYear','ck.IssMon','ck.NewBus','PolNo','Company','LegalEntity','DBRiderCodeYN','RiderCodeYN',\
'ReportingGroup','IssueYearCohort','CohortKey','ProductDescription','GroupIndiv','PremiumType','MultipleCohort','AG33apply',\
'AG43apply','DACApply','FAS133Apply','GAAPValMethod','ReportingLOB','ReportingSegment','SOPApply','StatValMethod',\
'BailoutInd','Clawback','DBAV','DBRiderCode','FixedLives','GPmode','IssueDate','LiquidityRider','ROPWaitPeriod',\
'ROPCharge','ROPAmt','NHRElected','AdminSystem','ICOSFlag','QualStatus','RiderCode','RiderCohort',  'State','AccumPrem',\
'AccumPW','AVIF','CYW','FPWRem','AccumStratCharges','AccumGenIntCred','AccumIdxIntCred','IncRiderAV','GMWBGuarBase','GMWBParBase',\
'InitGuarCSVwoBAV','InitGuarCSVwBAV','PremiumInforce','RemPrem','AnnGuarIntRate','AnnStartPeriod','BailGuarCap','BonusVest',\
'CertainPeriod','InitPeriod','ResetPeriod','CompSpreadAtIssue','ISP','NMRAtIssue','PourIn','PremBonusPct','PremFlag',\
'SAFALiborSpread','SAFAPeriodMonths','SAFASpread','SCPeriod','Segment','StatRes','TaxRes','TransferFlag','UseDBAV','GAVFloorRate',\
'GAVFloorValue','FixedSNFLRate','FixedSNFLVal','IndexedSNFLRate','IndexedSNFLVal','SNFLRate','SNFLVal','MVAAdd','MVAapply',\
'MVAMult','MVATreasury','D4DUtilTable','FRTable','FSTable','FreeWDTable','FreeWDPremPct','FPWType','MortalityTable',\
'PWTable','SCAVBonusTable','SCTable','SCTable_Admin','SCWindow','SCAVInputType','GMWBBonus','GMWBCharge','GMWBDBGuarPWRate',\
'GMWBDBPWRate','GMWBAccumCharges','GMWBIncType','GMWBMaxBenTable','GMWBParRate','GMWBParRate2','GMWBParRate3','GMWBParRate4',\
'GMWBParSpread','GMWBParCap','GMWBPayment','GMWBRollup','GMWBRollup2','GMWBRollup3','GMWBRollup4','IRAccumYrs1','IRAccumYrs2',\
'IRAccumYrs3','IRAccumYrs4','IRAccumYrsMax','EarningsIndexedPct','EarningsIndexedEIGPPct','IRRestart','GMWBInitAccumPeriod',\
'GMWBMaxAccumPeriod','GMWBMaxCharge','GMWBStartPeriod','GMWBWaitPer','GMDBCharge','GMDBFace','GMDBRollUpIRate','GMDBParRate',\
'DBBonusTbl','AnnuitzBonusTbl','D4DLimit','GMDBBenBasePct','GMDBPWRate','GMDBPWRateGuar','GMDBWaitPeriod','GMDBAccumCharges',\
'RiderCharge_DeductGtd','GenAV','GenBudgetOBCurr','GenBudgetUltOB','iCurr','iGuar','IdxBudgetIllustrated','ANXDeclaredAlloc',\
'ANXGuarFee','ANXIndexSpreadRateInp','ANXLockinIntRate','ANXStepUpPct','Idx1ANXStrat','Idx2ANXStrat','Idx3ANXStrat','Idx4ANXStrat',\
'Idx5ANXStrat','BCAEnhancement','LockIn','Idx2GIndexSpreadRate','WBRE_Cohort','ReinsDEpct','AAIAReins%','AADEReins%','ACRAReins%',\
'AAReReins%','AANYReins%','EABAmt','ALReExpAllow','SecondLifeAge','SecondLifeGender','MappedAV','F133AVIF','F133AccumPremIF',\
'F133AccumPWIF','F133DBAV','F133GMWBPayment','F133IncRiderAV','F133GMWBGuarBase','F133GMWBParBase','F133InitGuarCSVwoBAV',\
'F133InitGuarCSVwBAV','F133PremiumInforce','F133RemPremIF','F133Siz','F133GAVFloorValue','F133ROPAmt','F133AccumStratCharges',\
'NetFloor','Idx1AVIF','Idx2AVIF','Idx3AVIF','Idx4AVIF','Idx5AVIF','Idx1BudgetStrategyFee','Idx2BudgetStrategyFee','Idx3BudgetStrategyFee',\
'Idx4BudgetStrategyFee','Idx5BudgetStrategyFee','Idx1Term','Idx2Term','Idx3Term','Idx4Term','Idx5Term','Idx1TermStart','Idx2TermStart',\
'Idx3TermStart','Idx4TermStart','Idx5TermStart','Idx1BudgetOBCurr','Idx2BudgetOBCurr','Idx3BudgetOBCurr','Idx4BudgetOBCurr',\
'Idx5BudgetOBCurr','Idx1BudgetUltOB','Idx2BudgetUltOB','Idx3BudgetUltOB','Idx4BudgetUltOB','Idx5BudgetUltOB','Idx1BudgetVolAdjOB',\
'Idx2BudgetVolAdjOB','Idx3BudgetVolAdjOB','Idx4BudgetVolAdjOB','Idx5BudgetVolAdjOB','Idx1CredStrategy','Idx2CredStrategy',\
'Idx3CredStrategy','Idx4CredStrategy','Idx5CredStrategy','Idx1ParRate','Idx2ParRate','Idx3ParRate','Idx4ParRate','Idx5ParRate',\
'Idx1CapRate','Idx2CapRate','Idx3CapRate','Idx4CapRate','Idx5CapRate','Idx1SpreadRate','Idx2SpreadRate','Idx3SpreadRate',\
'Idx4SpreadRate','Idx5SpreadRate','Idx1TrigRate','Idx2TrigRate','Idx3TrigRate','Idx4TrigRate','Idx5TrigRate','Idx1GuarParRate',\
'Idx2GuarParRate','Idx3GuarParRate','Idx4GuarParRate','Idx5GuarParRate','Idx1GuarCapRate','Idx2GuarCapRate','Idx3GuarCapRate',\
'Idx4GuarCapRate','Idx5GuarCapRate','Idx1GuarSpreadRate','Idx2GuarSpreadRate','Idx3GuarSpreadRate','Idx4GuarSpreadRate',\
'Idx5GuarSpreadRate','Idx1GuarTrigRate','Idx2GuarTrigRate','Idx3GuarTrigRate','Idx4GuarTrigRate','Idx5GuarTrigRate','Idx1AOptNomMV',\
'Idx2AOptNomMV','Idx3AOptNomMV','Idx4AOptNomMV','Idx5AOptNomMV','Idx1AOptNomBV','Idx2AOptNomBV','Idx3AOptNomBV',\
'Idx4AOptNomBV','Idx5AOptNomBV','Idx1AOptNomFut','Idx2AOptNomFut','Idx3AOptNomFut','Idx4AOptNomFut','Idx5AOptNomFut',\
'Idx1IncepCost','Idx2IncepCost','Idx3IncepCost','Idx4IncepCost','Idx5IncepCost','Idx1RecLinkID','Idx2RecLinkID',\
'Idx3RecLinkID','Idx4RecLinkID','Idx5RecLinkID','Idx1Index','Idx2Index','Idx3Index','Idx4Index','Idx5Index',\
'Idx1DBInt','Idx2DBInt','Idx3DBInt','Idx4DBInt','Idx5DBInt','Idx5ExcessRecLinkID','LSASimpIntBase','F133LSASimpIntBase',\
'IRRestartNew','InitialRestartMonths','AdditionalRestartMonths','RestartCharge','CARVMAnnBenMode','CARVMAnnBenType','OrgIssYear',\
'Seed']

    list_ail_dtypes     = { 'ck.Plan':str, 'ck.IssAge':str,'ck.Gender':str,'ck.Class':str, 'ck.Char1':str, 'ck.Char2':str,'ck.Char3':str, 'ck.Char4':str, 'ck.Char5':str,\
'ck.Char6':str, 'ck.Char7':str,'ck.Char8':str, 'ck.IssYear':str, 'ck.IssMon':str, 'ck.NewBus':str, 'PolNo':str, 'Company':str, 'LegalEntity':str, 'DBRiderCodeYN':str, 'RiderCodeYN':str, \
'ReportingGroup':str, 'IssueYearCohort':str, 'CohortKey':str, 'ProductDescription':str, 'GroupIndiv':str, 'PremiumType':str, 'MultipleCohort':str, 'AG33apply':str,\
'AG43apply':str, 'DACApply':str, 'FAS133Apply':str, 'GAAPValMethod':str, 'ReportingLOB':str, 'ReportingSegment':str, 'SOPApply':str, 'StatValMethod':str,\
'BailoutInd':str, 'Clawback':str, 'DBAV':str, 'DBRiderCode':str, 'FixedLives':str, 'GPmode':str, 'IssueDate':str, 'LiquidityRider':str, 'ROPWaitPeriod':str,\
'ROPCharge':str, 'ROPAmt':str, 'NHRElected':str, 'AdminSystem':str, 'ICOSFlag':str, 'QualStatus':str, 'RiderCode':str, 'RiderCohort':str,  'State':str, 'AccumPrem':str,\
'AccumPW':str, 'AVIF':str, 'CYW':str, 'FPWRem':str, 'AccumStratCharges':str, 'AccumGenIntCred':str,'AccumIdxIntCred':str, 'IncRiderAV':str, 'GMWBGuarBase':str, 'GMWBParBase':str,\
'InitGuarCSVwoBAV':str, 'InitGuarCSVwBAV':str, 'PremiumInforce':str, 'RemPrem':str, 'AnnGuarIntRate':str, 'AnnStartPeriod':str, 'BailGuarCap':str, 'BonusVest':str,\
'CertainPeriod':str, 'InitPeriod':str, 'ResetPeriod':str, 'CompSpreadAtIssue':str, 'ISP':str, 'NMRAtIssue':str, 'PourIn':str, 'PremBonusPct':str, 'PremFlag':str,\
'SAFALiborSpread':str, 'SAFAPeriodMonths':str, 'SAFASpread':str, 'SCPeriod':str, 'Segment':str, 'StatRes':str, 'TaxRes':str, 'TransferFlag':str, 'UseDBAV':str, 'GAVFloorRate':str,\
'GAVFloorValue':str, 'FixedSNFLRate':str, 'FixedSNFLVal':str, 'IndexedSNFLRate':str, 'IndexedSNFLVal':str, 'SNFLRate':str, 'SNFLVal':str, 'MVAAdd':str, 'MVAapply':str,\
'MVAMult':str, 'MVATreasury':str, 'D4DUtilTable':str, 'FRTable':str, 'FSTable':str, 'FreeWDTable':str, 'FreeWDPremPct':str, 'FPWType':str, 'MortalityTable':str,\
'PWTable':str, 'SCAVBonusTable':str, 'SCTable':str, 'SCTable_Admin':str, 'SCWindow':str, 'SCAVInputType':str, 'GMWBBonus':str, 'GMWBCharge':str, 'GMWBDBGuarPWRate':str,\
'GMWBDBPWRate':str, 'GMWBAccumCharges':str, 'GMWBIncType':str, 'GMWBMaxBenTable':str, 'GMWBParRate':str, 'GMWBParRate2':str, 'GMWBParRate3':str, 'GMWBParRate4':str,\
'GMWBParSpread':str, 'GMWBParCap':str, 'GMWBPayment':str, 'GMWBRollup':str, 'GMWBRollup2':str, 'GMWBRollup3':str, 'GMWBRollup4':str, 'IRAccumYrs1':str, 'IRAccumYrs2':str,\
'IRAccumYrs3':str, 'IRAccumYrs4':str, 'IRAccumYrsMax':str, 'EarningsIndexedPct':str, 'EarningsIndexedEIGPPct':str, 'IRRestart':str, 'GMWBInitAccumPeriod':str,\
'GMWBMaxAccumPeriod':str, 'GMWBMaxCharge':str, 'GMWBStartPeriod':str, 'GMWBWaitPer':str, 'GMDBCharge':str, 'GMDBFace':str, 'GMDBRollUpIRate':str, 'GMDBParRate':str,\
'DBBonusTbl':str, 'AnnuitzBonusTbl':str, 'D4DLimit':str, 'GMDBBenBasePct':str, 'GMDBPWRate':str, 'GMDBPWRateGuar':str, 'GMDBWaitPeriod':str, 'GMDBAccumCharges':str,\
'RiderCharge_DeductGtd':str, 'GenAV':str, 'GenBudgetOBCurr':str, 'GenBudgetUltOB':str, 'iCurr':str, 'iGuar':str, 'IdxBudgetIllustrated':str, 'ANXDeclaredAlloc':str,\
'ANXGuarFee':str, 'ANXIndexSpreadRateInp':str, 'ANXLockinIntRate':str, 'ANXStepUpPct':str, 'Idx1ANXStrat':str, 'Idx2ANXStrat':str, 'Idx3ANXStrat':str, 'Idx4ANXStrat':str,\
'Idx5ANXStrat':str, 'BCAEnhancement':str, 'LockIn':str, 'Idx2GIndexSpreadRate':str, 'WBRE_Cohort':str, 'ReinsDEpct':str, 'AAIAReins%':str, 'AADEReins%':str, 'ACRAReins%':str,\
'AAReReins%':str, 'AANYReins%':str, 'EABAmt':str, 'ALReExpAllow':str, 'SecondLifeAge':str, 'SecondLifeGender':str, 'MappedAV':str, 'F133AVIF':str, 'F133AccumPremIF':str,\
'F133AccumPWIF':str, 'F133DBAV':str, 'F133GMWBPayment':str, 'F133IncRiderAV':str, 'F133GMWBGuarBase':str, 'F133GMWBParBase':str, 'F133InitGuarCSVwoBAV':str, \
'F133InitGuarCSVwBAV':str, 'F133PremiumInforce':str, 'F133RemPremIF':str, 'F133Siz':str, 'F133GAVFloorValue':str, 'F133ROPAmt':str, 'F133AccumStratCharges':str,\
'NetFloor':str, 'Idx1AVIF':str, 'Idx2AVIF':str, 'Idx3AVIF':str, 'Idx4AVIF':str, 'Idx5AVIF':str, 'Idx1BudgetStrategyFee':str, 'Idx2BudgetStrategyFee':str, 'Idx3BudgetStrategyFee':str,\
'Idx4BudgetStrategyFee':str, 'Idx5BudgetStrategyFee':str, 'Idx1Term':str, 'Idx2Term':str, 'Idx3Term':str, 'Idx4Term':str, 'Idx5Term':str, 'Idx1TermStart':str, 'Idx2TermStart':str,\
'Idx3TermStart':str, 'Idx4TermStart':str, 'Idx5TermStart':str, 'Idx1BudgetOBCurr':str, 'Idx2BudgetOBCurr':str, 'Idx3BudgetOBCurr':str, 'Idx4BudgetOBCurr':str,\
'Idx5BudgetOBCurr':str, 'Idx1BudgetUltOB':str, 'Idx2BudgetUltOB':str, 'Idx3BudgetUltOB':str, 'Idx4BudgetUltOB':str, 'Idx5BudgetUltOB':str, 'Idx1BudgetVolAdjOB':str,\
'Idx2BudgetVolAdjOB':str, 'Idx3BudgetVolAdjOB':str, 'Idx4BudgetVolAdjOB':str, 'Idx5BudgetVolAdjOB':str, 'Idx1CredStrategy':str, 'Idx2CredStrategy':str,\
'Idx3CredStrategy':str, 'Idx4CredStrategy':str, 'Idx5CredStrategy':str, 'Idx1ParRate':str, 'Idx2ParRate':str, 'Idx3ParRate':str, 'Idx4ParRate':str, 'Idx5ParRate':str,\
'Idx1CapRate':str, 'Idx2CapRate':str, 'Idx3CapRate':str, 'Idx4CapRate':str, 'Idx5CapRate':str, 'Idx1SpreadRate':str, 'Idx2SpreadRate':str, 'Idx3SpreadRate':str,\
'Idx4SpreadRate':str, 'Idx5SpreadRate':str, 'Idx1TrigRate':str, 'Idx2TrigRate':str, 'Idx3TrigRate':str, 'Idx4TrigRate':str, 'Idx5TrigRate':str, 'Idx1GuarParRate':str,\
'Idx2GuarParRate':str, 'Idx3GuarParRate':str, 'Idx4GuarParRate':str, 'Idx5GuarParRate':str, 'Idx1GuarCapRate':str, 'Idx2GuarCapRate':str, 'Idx3GuarCapRate':str,\
'Idx4GuarCapRate':str, 'Idx5GuarCapRate':str, 'Idx1GuarSpreadRate':str, 'Idx2GuarSpreadRate':str, 'Idx3GuarSpreadRate':str, 'Idx4GuarSpreadRate':str,\
'Idx5GuarSpreadRate':str, 'Idx1GuarTrigRate':str, 'Idx2GuarTrigRate':str, 'Idx3GuarTrigRate':str, 'Idx4GuarTrigRate':str, 'Idx5GuarTrigRate':str, 'Idx1AOptNomMV':str,\
'Idx2AOptNomMV':str, 'Idx3AOptNomMV':str, 'Idx4AOptNomMV':str, 'Idx5AOptNomMV':str, 'Idx1AOptNomBV':str, 'Idx2AOptNomBV':str, 'Idx3AOptNomBV':str,\
'Idx4AOptNomBV':str, 'Idx5AOptNomBV':str, 'Idx1AOptNomFut':str, 'Idx2AOptNomFut':str, 'Idx3AOptNomFut':str, 'Idx4AOptNomFut':str, 'Idx5AOptNomFut':str,\
'Idx1IncepCost':str, 'Idx2IncepCost':str, 'Idx3IncepCost':str, 'Idx4IncepCost':str, 'Idx5IncepCost':str, 'Idx1RecLinkID':str, 'Idx2RecLinkID':str,\
'Idx3RecLinkID':str, 'Idx4RecLinkID':str, 'Idx5RecLinkID':str, 'Idx1Index':str, 'Idx2Index':str, 'Idx3Index':str, 'Idx4Index':str, 'Idx5Index':str,\
'Idx1DBInt':str, 'Idx2DBInt':str, 'Idx3DBInt':str, 'Idx4DBInt':str, 'Idx5DBInt':str, 'Idx5ExcessRecLinkID':str, 'LSASimpIntBase':str, 'F133LSASimpIntBase':str,\
'IRRestartNew':str, 'InitialRestartMonths':str, 'AdditionalRestartMonths':str, 'RestartCharge':str, 'CARVMAnnBenMode':str, 'CARVMAnnBenType':str, 'OrgIssYear':str,\
'Seed':str }

    df_ail_data             = pd.read_csv(  filepath_ail,
                                            sep     = '\t',
                                            usecols = list_ail_fields,
                                            dtype   = list_ail_dtypes)

    return(df_ail_data)

def load_us_deferred_excel_ail_file(filepath_ail):

    print("Loading excel file ", filepath_ail)

    df_ail_data = pd.read_excel(filepath_ail)

    print("Done reading excel")

    return(df_ail_data)

def open_excel_file(connection_string):

    data_frame = pd.read_excel(connection_string)

    return(data_frame)

def write_excel_file(output_file_path, output_data_frame):
    Excelwriter = pd.ExcelWriter(output_file_path, engine="xlsxwriter")
    output_data_frame.to_excel(Excelwriter, sheet_name="Output" ,index=True)
    Excelwriter.close()
    return

def open_csv_file(connection_string):
    
    data_frame = pd.read_csv( connection_string)

    return data_frame

def get_sql_table(connection_string, query):

    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=ATHPRODBIDB01;'
                          'Database=AHLDW;'
                          'Trusted_Connection=yes;')

    #data_frame = pd.read_sql_query(str(query), pyodbc.connect(connection_string))
    data_frame = pd.read_sql_query(str(query), conn)

    return data_frame
    

def get_bda_ail(valuation_date, type, cedent, ae_type, connection_string, columntype):

    ail_type = type
    block = cedent
  
    if columntype == 'static':
        default_columnlist = "ClientShortName, ValuationDate, Entity, IssueAge, Gender, IssueYear, IssueMonth, FFileKey, ProductType, ProductName, PolicyNumber, ModelPlan, \
                          PlanCode, OrigIssueYear, OrigIssueMonth, PolicyCount, LineOfBusiness"
    elif columntype == 'variable':
        default_columnlist = "ClientShortName, NewOrSurviving, ValuationDate, Entity, ProductType, ProductName, PolicyNumber, ModelPlan, PlanCode, \
                          PolicyCount, AccountValueTotal, AccountValueFixed, AccountValueIndexTotal, CSVValue, GCSVValue, IAV, CreditRate, Idx1FV, Idx2FV, Idx3FV, Idx4FV, Idx5FV, Idx6FV"
    else:
        default_columnlist = "ClientShortName, NewOrSurviving, ValuationDate, Entity, IssueAge, Gender, Class, Char1, IssueYear, IssueMonth, FFileKey, ProductType, ProductName, PolicyNumber, ModelPlan, PlanCode, \
                          PolicyCount, AccountValueTotal, AccountValueFixed, AccountValueIndexTotal, CSVValue, GCSVValue, IAV, CreditRate, \
                          Idx1FV, Idx2FV, Idx3FV, Idx4FV, Idx5FV, Idx6FV"

    if ail_type == 'New':
        ailtype = " and NewOrSurviving = 'N\'"
    elif ail_type == 'Surviving':
        ailtype = " and NewOrSurviving = 'S\'"
    else:
        ailtype = " "

    if block == 'AEL':
        client = "\'AEL\'"
    elif block == 'EGL':
        client = "\'EGL\'"
    elif block == 'MNL':
        client = "\'MNL\'"
    else:
        client = "\'AEL\', \'EGL\', \'MNL\'"

    if ae_type == 'A':
       actorest = " and ActualOrEstimate = 'A\'"
    elif ae_type == 'E':
        actorest = " and ActualOrEstimate = 'E\'"
    else:
        print('No Actual or Estimate Value Provide')

    querystring = "select " + default_columnlist + " from AHLDW.rpt.AILPlus where ClientShortName in (" + client + ") and ValuationDate=\'" + valuation_date + "\'" + ailtype + actorest

     ## import AIL data   ProductType (Fixed or Indexed)   ProductName (FIA)
    ail_data = get_sql_table(connection_string, querystring)

    return(ail_data)


#    Currently not used.  The intention for this was to use todays date and return a dataframe with current and prior valuation dates
def get_valuation_dates():

    current_date = datetime.now()
    current_year = current_date.year
    current_quarter = math.floor((current_date.month - 1) / 3)
 
    if current_quarter == 0:
        current_quarter =4
        current_year = current_year - 1
 
    if current_quarter == 1:
        prior_quarter = 4
        prior_year = current_year - 1
    else:
        prior_quarter = current_quarter - 1
        prior_year = current_year
    
    if current_quarter == 4:
        Adj = 1
    else:
        Adj = 0
 
    first_date = datetime(current_year + Adj, 3 * current_quarter + 1 - (12*Adj), 1) + timedelta(days=-1)
    last_date = datetime(prior_year, 3 * prior_quarter + 1, 1) + timedelta(days=-1)

    data = { 'Current':[first_date],
             'Prior':[last_date] }
 
    # Create DataFrame
    valuationdate_df = pd.DataFrame(data)

    return(valuationdate_df)

def create_report(valuation_date, filename, outputdf, output_dir):

    vdate = datetime.strptime(valuation_date, '%Y/%m/%d').date()

    fulldate = vdate.strftime("%m%d%y")

    exceloutputfile = output_dir + "\\" + filename + "_" + fulldate + ".xlsx"

    write_excel_file(exceloutputfile, outputdf)

    return

def output_to_file(string):

  f = open('./output/Compare_Log.txt', 'a+')

  newstring = string + '\n'
  f.write(newstring) 
  f.close()

  return

def load_us_deferred_ail_db():

    config = configparser.RawConfigParser()
    #path of the config file
    configFilePath = 'C:\\temp\\postgres_config.ini'

    config.read(configFilePath)

    currenttime = datetime.now()
    print("Before DB get: ", str(currenttime))

    conn = psycopg2.connect(
            host=config.get('QA','host'),
            port=config.get('QA','port'),
            database=config.get('QA','database'),
            user=config.get('QA','user'),
            password=config.get('QA','password'))

    cur = conn.cursor()

    sql_statement = "SELECT ckPlan,ckIssAge,ckGender,ckClass,ckChar1,ckChar2,ckChar3,ckChar4,ckChar5,ckChar6,ckChar7,ckChar8,ckIssYear,ckIssMon,ckNewBus,\
json_data -> 'PolNo' as PolNo,Company,LegalEntity,DBRiderCodeYN,RiderCodeYN,ReportingGroup,IssueYearCohort,\
CohortKey,ProductDescription,GroupIndiv,PremiumType,MultipleCohort,AG33apply,AG43apply,DACApply,FAS133Apply,\
GAAPValMethod,ReportingLOB,ReportingSegment,SOPApply,StatValMethod,BailoutInd,Clawback,DBAV,DBRiderCode,FixedLives,GPmode,IssueDate,LiquidityRider,ROPWaitPeriod,\
ROPCharge,ROPAmt,NHRElected,AdminSystem,ICOSFlag,QualStatus,RiderCode,RiderCohort,State,AccumPrem,AccumPW,AVIF,CYW,FPWRem,\
AccumStratCharges,AccumGenIntCred,AccumIdxIntCred,IncRiderAV,GMWBGuarBase,GMWBParBase,\
json_data->'InitGuarCSVwoBAV' as InitGuarCSVwoBAV, json_data->'InitGuarCSVwBAV' as InitGuarCSVwBAV,PremiumInforce,RemPrem,AnnGuarIntRate,AnnStartPeriod,BailGuarCap,\
BonusVest,CertainPeriod,InitPeriod,ResetPeriod,CompSpreadAtIssue,ISP,NMRAtIssue,PourIn,PremBonusPct,PremFlag,SAFALiborSpread,SAFAPeriodMonths,SAFASpread,SCPeriod,Segment,StatRes,\
TaxRes,TransferFlag,UseDBAV,GAVFloorRate,GAVFloorValue,FixedSNFLRate,FixedSNFLVal,IndexedSNFLRate,IndexedSNFLVal,SNFLRate,SNFLVal,MVAAdd,MVAapply,MVAMult,MVATreasury,D4DUtilTable,\
FRTable,FSTable,FreeWDTable,FreeWDPremPct,FPWType,MortalityTable,PWTable,SCAVBonusTable,SCTable,SCTable_Admin,SCWindow,SCAVInputType,GMWBBonus,GMWBCharge,GMWBDBGuarPWRate,\
GMWBDBPWRate,GMWBAccumCharges,GMWBIncType,GMWBMaxBenTable,GMWBParRate,GMWBParRate2,GMWBParRate3,GMWBParRate4,\
GMWBParSpread,GMWBParCap,GMWBPayment,GMWBRollup,GMWBRollup2,GMWBRollup3,GMWBRollup4,IRAccumYrs1,IRAccumYrs2,IRAccumYrs3,IRAccumYrs4,IRAccumYrsMax,EarningsIndexedPct,\
EarningsIndexedEIGPPct,IRRestart,GMWBInitAccumPeriod,GMWBMaxAccumPeriod,GMWBMaxCharge,GMWBStartPeriod,GMWBWaitPer,GMDBCharge,GMDBFace,GMDBRollUpIRate,GMDBParRate,\
DBBonusTbl,AnnuitzBonusTbl,D4DLimit,GMDBBenBasePct,GMDBPWRate,GMDBPWRateGuar,GMDBWaitPeriod,GMDBAccumCharges,RiderCharge_DeductGtd,GenAV,GenBudgetOBCurr,GenBudgetUltOB,\
iCurr,iGuar,IdxBudgetIllustrated,ANXDeclaredAlloc,ANXGuarFee,ANXIndexSpreadRateInp,ANXLockinIntRate,ANXStepUpPct,Idx1ANXStrat,Idx2ANXStrat,Idx3ANXStrat,Idx4ANXStrat,\
Idx5ANXStrat,BCAEnhancement,LockIn,Idx2GIndexSpreadRate,WBRE_Cohort,ReinsDEpct,AAIAReinsPct,AADEReinsPct,ACRAReinsPct,AAReReinsPct,AANYReinsPct,EABAmt,\
ALReExpAllow,SecondLifeAge,SecondLifeGender,MappedAV,F133AVIF,F133AccumPremIF,F133AccumPWIF,F133DBAV,F133GMWBPayment,F133IncRiderAV,F133GMWBGuarBase,F133GMWBParBase,\
F133InitGuarCSVwoBAV,F133InitGuarCSVwBAV,F133PremiumInforce,F133RemPremIF,F133Siz,F133GAVFloorValue,F133ROPAmt,F133AccumStratCharges,NetFloor,Idx1AVIF,Idx2AVIF,Idx3AVIF,\
Idx4AVIF,Idx5AVIF,Idx1BudgetStrategyFee,Idx2BudgetStrategyFee,Idx3BudgetStrategyFee,Idx4BudgetStrategyFee,Idx5BudgetStrategyFee,Idx1Term,Idx2Term,Idx3Term,Idx4Term,Idx5Term,\
Idx1TermStart,Idx2TermStart,Idx3TermStart,Idx4TermStart,Idx5TermStart,Idx1BudgetOBCurr,\
Idx2BudgetOBCurr,Idx3BudgetOBCurr,Idx4BudgetOBCurr,Idx5BudgetOBCurr,Idx1BudgetUltOB,Idx2BudgetUltOB,Idx3BudgetUltOB,Idx4BudgetUltOB,Idx5BudgetUltOB,Idx1BudgetVolAdjOB,\
Idx2BudgetVolAdjOB,Idx3BudgetVolAdjOB,Idx4BudgetVolAdjOB,Idx5BudgetVolAdjOB,Idx1CredStrategy,Idx2CredStrategy,Idx3CredStrategy,Idx4CredStrategy,Idx5CredStrategy,\
Idx1ParRate,Idx2ParRate,Idx3ParRate,Idx4ParRate,Idx5ParRate,Idx1CapRate,Idx2CapRate,Idx3CapRate,Idx4CapRate,Idx5CapRate,Idx1SpreadRate,Idx2SpreadRate,\
Idx3SpreadRate,Idx4SpreadRate,Idx5SpreadRate,Idx1TrigRate,Idx2TrigRate,Idx3TrigRate,Idx4TrigRate,Idx5TrigRate,Idx1GuarParRate,Idx2GuarParRate,\
Idx3GuarParRate,Idx4GuarParRate,Idx5GuarParRate,Idx1GuarCapRate,Idx2GuarCapRate,Idx3GuarCapRate,Idx4GuarCapRate,Idx5GuarCapRate,Idx1GuarSpreadRate,\
Idx2GuarSpreadRate,Idx3GuarSpreadRate,Idx4GuarSpreadRate,Idx5GuarSpreadRate,Idx1GuarTrigRate,Idx2GuarTrigRate,Idx3GuarTrigRate,Idx4GuarTrigRate,Idx5GuarTrigRate,\
Idx1AOptNomMV,Idx2AOptNomMV,Idx3AOptNomMV,Idx4AOptNomMV,Idx5AOptNomMV,Idx1AOptNomBV,Idx2AOptNomBV,Idx3AOptNomBV,Idx4AOptNomBV,Idx5AOptNomBV,Idx1AOptNomFut,\
Idx2AOptNomFut,Idx3AOptNomFut,Idx4AOptNomFut,Idx5AOptNomFut,Idx1IncepCost,Idx2IncepCost,Idx3IncepCost,Idx4IncepCost,Idx5IncepCost,Idx1RecLinkID,\
Idx2RecLinkID,Idx3RecLinkID,Idx4RecLinkID,Idx5RecLinkID,Idx1Index,Idx2Index,Idx3Index,Idx4Index,Idx5Index,Idx1DBInt,Idx2DBInt,Idx3DBInt,Idx4DBInt,\
Idx5DBInt,Idx5ExcessRecLinkID,LSASimpIntBase,F133LSASimpIntBase,IRRestartNew,InitialRestartMonths,AdditionalRestartMonths,RestartCharge,\
CARVMAnnBenMode,CARVMAnnBenType,json_data->'OrgIssYear' as OrgIssYear, json_data->'Seed' as Seed \
from afdm.global_values_deferred_ail where file_name='fa_day3_alfa_feed_20210930.ail2';"

    #mysqlstatement = "\copy (SELECT ckPlan, ckIssAge, PolNo from afdm.global_values_deferred_ail where file_name='fa_day3_alfa_feed_20210930.ail2';) TO 'Desktop/mytest.csv' csv header"
    currenttime = datetime.now()
    print("Before SQL query: ", str(currenttime))

    # execute a statement
    try:
        cur.execute(sql_statement)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        #cur.close()

    result = cur.fetchall()
    
    # We just need to turn it into a pandas dataframe
    dbdf = pd.DataFrame(result)

    return dbdf

