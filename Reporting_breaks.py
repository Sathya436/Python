################################################################                 
### Job Name: shsmaq-reporting-breaks
### Job Desciption: Calculating Count Eligible and Diagnsois eligible from the exposure and mapping file data mart 
### Purpose: To calculate the Count Eligible and Diagnsois eligible getting required    
### 
### Author: Verinext                 
###
################################################################

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import split,col,count,expr,lower,udf,md5,concat_ws,lit,when,to_json,struct,collect_list,count,row_number,countDistinct,ceil,min,round,isnull
from awsglue.context import GlueContext
from pyspark.sql import SparkSession                    
from pyspark.sql import functions as f
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, DateType
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
from datetime import datetime, timedelta, date
import json
import os
from pyspark import SparkConf
from pyspark.sql.window import Window
import ast
import pyspark
from delta import *
from delta.tables import *
from delta.tables import DeltaTable
from pyspark.sql.functions import broadcast
from functools import reduce
from datetime import datetime
from pyspark.sql.functions import explode, split, col, udf
from pyspark.sql.types import StringType
import logging
import traceback
import math
import itertools

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read job parameters
args = getResolvedOptions(sys.argv, ['scheduleId','studyId','breakCombination','dataBreaks','condition','mediaType','expsoureMappingscheduleId','deliverableId','isHouseholdRequired','isEthnicityRequired','marketDefinition','isSpecialtyRequired','isStudyRulesRequired','studyRules','bucketName','fileProcessingMonth','runType','reportingPeriod','exposureMappingBreaks'])

#Parameters for StudyRules
isStudyRulesRequired = args["isStudyRulesRequired"]
studyRules = args["studyRules"]
studyRules = json.loads(studyRules)

#Parameters for standard report
studyID = args["studyId"]
scheduleId = args["scheduleId"]
expsoureMappingscheduleId = args["expsoureMappingscheduleId"]
deliverableId = args["deliverableId"]
isHouseholdRequired = args["isHouseholdRequired"]
breakCombination = args["breakCombination"]
breakCombination = json.loads(breakCombination)
breakCombination = list(dict.fromkeys(breakCombination))
dataBreaks = args["dataBreaks"]
dataBreaks = json.loads(dataBreaks)
Condition = args["condition"]
Condition = json.loads(Condition)
exposureMappingBreaks = args["exposureMappingBreaks"]
exposureMappingBreaks = json.loads(exposureMappingBreaks)
mediaType = args["mediaType"]
bucketName = args["bucketName"]
#bucketName ='shsmaq-datalake-dev'
runType = args["runType"]
reRunTime = args["fileProcessingMonth"]
reportingPeriod = args['reportingPeriod']

#Parameters for Ethnicity and Specialty
isEthnicityRequired = args["isEthnicityRequired"]
marketDefinition = args["marketDefinition"]
marketDefinition = json.loads(marketDefinition)
isSpecialtyRequired = args["isSpecialtyRequired"]

#Calculating date
# reRunTimePeriod = datetime.strptime(reRunTime, "%b-%Y").strftime("%-m-%Y")
if reportingPeriod == 'Monthly' and runType =='Rerun':
    strippedDate = datetime.strptime(reRunTime, "%B%Y")
    reRunTimePeriod = f"{strippedDate.month}-{strippedDate.year}"
    print("[+] Rerun for the monthly period:",reRunTimePeriod)

if reportingPeriod == 'Quarterly' and runType =='Rerun':
    # Create a mapping of month names to numbers
    month_mapping = {
        "January": "1", "February": "2", "March": "3", "April": "4",
        "May": "5", "June": "6", "July": "7", "August": "8",
        "September": "9", "October": "10", "November": "11", "December": "12"
    }
    
    # Create a UDF to convert month name to number
    @udf(returnType=StringType())
    def month_to_number(month_year):
        month, year = month_year[:len(month_year)-4], month_year[-4:]
        month_num = month_mapping.get(month, "00")
        return f"{month_num}-{year}"
    
    # Create a temporary DataFrame with the months to delete
    temp_df = spark.createDataFrame([(reRunTime,)], ["months"])
    
    # Split the string, explode it into rows, and apply the UDF
    formatted_months = temp_df.select(explode(split(col("months"), ",")).alias("month")) \
        .select(month_to_number(col("month")).alias("formatted_month"))
    
    # Collect the formatted months as a list
    months_list = [row.formatted_month for row in formatted_months.collect()]
    print("[+] Rerun for the quarterly period:",months_list)

########################################################################################################################
##################################### CUSTOM FUNCTIONS #################################################################
########################################################################################################################

def filter_df_by_dict(df,filter_dict,column_order_list):
    # Create an initial filter condition   
    condition = None        
    # Loop through the filter dictionary and create conditions dynamically 
    for key in column_order_list:    
        if condition is None:          
            condition = col(key) == filter_dict[key]      
        else:           
            condition = condition & (col(key) == filter_dict[key])          
    return df.filter(condition)
    
def move_to_front(lst, item):
    if item in lst:
        lst.remove(item)  
        lst.insert(0, item) 
    return lst
    

def walk_and_generate_combinations(items):   
    complete_combinations = []
    for i in range(len(items)):       
        for j in range(i + 1, len(items) + 1):       
            complete_combinations.append(items[i:j])
            
    return complete_combinations

def delta_table_exists(path):
    try:
        DeltaTable.forPath(spark, path)
        return True
    except:
        return False
            
# Function to check if a Delta table exists
def check_delta_table_exists(spark, path):
    return DeltaTable.isDeltaTable(spark, path)

def column_dropper(df, to_drop_column_list):
    drop_cols = []
    source_column_list = df.columns()
    for cols in source_column_list:
        if cols in to_drop_column_list:
            drop_cols.append(cols)

    df = df.drop(*drop_cols)
    return df
    
    
def quarterly_conversion(df): #correct quarter label
    # Convert Time_Period to Quarter-Year format (2023Q1,2023Q2, etc.)
    df = df.withColumn(
        'Time_Period', 
        f.concat(
            f.split(df['Time_Period'], '-').getItem(1), 
            f.when(f.split(df['Time_Period'], '-').getItem(0).cast('int').between(1, 3), 'Q1')
             .when(f.split(df['Time_Period'], '-').getItem(0).cast('int').between(4, 6), 'Q2')
             .when(f.split(df['Time_Period'], '-').getItem(0).cast('int').between(7, 9), 'Q3')
             .when(f.split(df['Time_Period'], '-').getItem(0).cast('int').between(10, 12), 'Q4')
        )
    )

    print("After quarterly conversion")
    df.select('Time_Period').distinct().show()
    
    return df

########################################################################################################################

# calling the total function
#Count eligible Household 
def count_eligible_break_household(DF_count_source_household):
    
    if 'hh_fact' in DF_count_source_household.columns:
        DF_count_source_household =  DF_count_source_household.drop('hh_fact')
    
    #Create Output DF
    DF_Count_Output = DF_count_source_household.select("*").where("1 = 0")
    DF_Count_Output = DF_Count_Output.withColumn("Count_eligible", lit(None)).drop("Min_HHFact").drop("patient_id")
    
    total_column_list = DF_Count_Output.columns
    print(f"[+] Count Eligible Source Columns: {total_column_list}")
    
    DF_count_source_household = DF_count_source_household.withColumn('Min_HHFact', f.col('Min_HHFact').cast('double'))
    print(f"[+] Count Eligible Household ROW COUNT: {DF_count_source_household.count()}")
    items_to_remove = ["patient_id","Min_HHFact","Count_eligible","hh_fact"]
    for item in items_to_remove:  
        if item in total_column_list:       
             total_column_list.remove(item)
    total_column_list = move_to_front(total_column_list,'Time_Period')
    breakcombination = walk_and_generate_combinations(total_column_list)
    DF_count_source_household = DF_count_source_household.withColumn('Min_HHFact', f.col('Min_HHFact').cast('double'))
    DF_count_source_household = DF_count_source_household.filter(f.col('Min_HHFact').isNotNull())
    
    df_total_total = DF_count_source_household

    print("[+] Count_eligible stage")
    for data_break in breakcombination:
        print(f"[+] Starting break:{data_break} ") 
        DF_temp_totals = DF_count_source_household
        if "patient_id" in data_break:
            data_break.remove("patient_id")
        duplicate_drop = data_break.copy()
        duplicate_drop.append('patient_id')
        print(f'[+] Drop Columns: {duplicate_drop}')
        #DF_temp_totals = DF_temp_totals.dropDuplicates(duplicate_drop).drop("patient_id")
        DF_temp_totals = DF_temp_totals.groupby(duplicate_drop).agg(f.min('Min_HHFact').alias('Min_HHFact'))
        DF_temp_totals = DF_temp_totals.groupby(*data_break).agg(f.ceil(f.sum('Min_HHFact')).alias("Count_eligible"))
        
        Excluded_column_base = DF_Count_Output.columns  
        if 'hh_fact' in Excluded_column_base:
            Excluded_column_base.remove('hh_fact')
        
        included_column =  list(DF_temp_totals.columns)
        excluded_column = [column for column in Excluded_column_base if column not in included_column]
        if "patient_id" in excluded_column:
            excluded_column.remove("patient_id")
        print("[+] Adding Missing Columns")
        for column in excluded_column:
            DF_temp_totals = DF_temp_totals.withColumn(column, lit("Total"))
        #print("[+] Casting Columns as String")
        DF_temp_totals = DF_temp_totals.select([col(c).cast("string").alias(c) for c in Excluded_column_base])
        print("[+] Joining Break Total to Output DF")
        DF_Count_Output = DF_Count_Output.unionByName(DF_temp_totals)
        print("[+] Finished Join to Output DF")
        #DF_Count_Output.persist()

    #Adding Total Total   
    df_total_total = df_total_total.groupby("patient_id").agg(f.min('Min_HHFact').alias('Min_HHFact'))
    #df_total_toal = df_total_total.dropDuplicates(['patient_id']).select(f.sum("Min_HHFact").alias("Count_eligible"))
    df_total_toal = df_total_total.select(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible"))
    
    total_total_column_list= Excluded_column_base.copy()
    total_total_column_list.remove("Count_eligible")
    for column in total_total_column_list:
        df_total_toal = df_total_toal.withColumn(column, lit("Total"))
    DF_Count_Output = DF_Count_Output.unionByName(df_total_toal)

    print("[+]  Finished Count Break Stages")
    DF_Count_Output = DF_Count_Output.dropDuplicates()
    print("[+]  Returning Break Totals DF From Count Eligible Household")
    return DF_Count_Output
################################################################################################################
#Count eligible
def count_eligible_break_standard(DF_count_source_standard):

    if 'hh_fact' in DF_count_source_standard.columns:
        DF_count_source_standard =  DF_count_source_standard.drop('hh_fact')

    if 'shs_patient_id' in DF_count_source_standard.columns:
        DF_count_source_standard =  DF_count_source_standard.drop('shs_patient_id')

    if 'Count_Eligible_Flag' in DF_count_source_standard.columns:
        DF_count_source_standard =  DF_count_source_standard.drop('Count_Eligible_Flag')

    DF_Count_Output = DF_count_source_standard.select("*").where("1 = 0")
    DF_Count_Output = DF_Count_Output.withColumn("Count_eligible", lit(None)).drop("patient_id")
    
    total_column_list = DF_Count_Output.columns
    
    print(f"[+] Count Eligible Source Columns: {total_column_list}")
        
    print(f"[+] Count Eligible Row Count: {DF_count_source_standard.count()}")
    items_to_remove = ["patient_id","shs_patient_id","Count_Eligible_Flag","Count_eligible"]
    for item in items_to_remove:  
        if item in total_column_list:       
             total_column_list.remove(item)
    total_column_list = move_to_front(total_column_list,'Time_Period')
    breakcombination = walk_and_generate_combinations(total_column_list)

    df_total_total = DF_count_source_standard
    for data_break in breakcombination:
        print(f"[+] Starting break:{data_break} ") 
        DF_temp_totals = DF_count_source_standard
        if "patient_id" in data_break:
            data_break.remove("patient_id")
        duplicate_drop = data_break.copy()
        duplicate_drop.append('patient_id')
        print(f'[+] Deduplicate Columns: {duplicate_drop}')
        DF_temp_totals = DF_temp_totals.groupby(*data_break).agg(f.countDistinct('patient_id').alias("Count_eligible"))
        
        Excluded_column_base = DF_Count_Output.columns
        
        if 'hh_fact' in Excluded_column_base:
            Excluded_column_base.remove('hh_fact')
        
        
        included_column =  list(DF_temp_totals.columns)
        excluded_column = [column for column in Excluded_column_base if column not in included_column]
        if "patient_id" in excluded_column:
            excluded_column.remove("patient_id")
        print("[+] Adding Missing Columns")
        for column in excluded_column:
            DF_temp_totals = DF_temp_totals.withColumn(column, lit("Total"))
        #print("[+] Casting Columns as String")
        DF_temp_totals = DF_temp_totals.select([col(c).cast("string").alias(c) for c in Excluded_column_base])
        print("[+] Joining Break Total to Output DF")
        DF_Count_Output = DF_Count_Output.unionByName(DF_temp_totals)
        print("[+] Finished Join to Output DF")
        #DF_Count_Output.persist()

    #Adding Total Total        
    df_total_toal = df_total_total.dropDuplicates(['patient_id']).select(f.countDistinct("patient_id").alias("Count_eligible"))
    total_total_column_list= Excluded_column_base.copy()
    total_total_column_list.remove("Count_eligible")
    for column in total_total_column_list:
        df_total_toal = df_total_toal.withColumn(column, lit("Total"))
    DF_Count_Output = DF_Count_Output.unionByName(df_total_toal)
    #
    print("[+]  Finished Count Break Stages")
    DF_Count_Output = DF_Count_Output.dropDuplicates()
    print("[+]  Returning Break Totals DF From Count Eligible Household")
    return DF_Count_Output

################################################################################################################
#Diagnosis eligible
def diagnosis_eligible_break(DF_diagnosis_source_standard):
    
    if 'hh_fact' in DF_diagnosis_source_standard.columns:
        DF_diagnosis_source_standard =  DF_diagnosis_source_standard.drop('hh_fact')
    
    if 'hh_number' in DF_diagnosis_source_standard.columns:
        DF_diagnosis_source_standard =  DF_diagnosis_source_standard.drop('hh_number')
        
    if 'shs_patient_id' in DF_diagnosis_source_standard.columns:
        DF_diagnosis_source_standard =  DF_diagnosis_source_standard.drop('shs_patient_id')
    
    if 'Count_Eligible_Flag' in DF_diagnosis_source_standard.columns:
        DF_diagnosis_source_standard =  DF_diagnosis_source_standard.drop('Count_Eligible_Flag')
        
    DF_Count_Output = DF_diagnosis_source_standard.select("*").where("1 = 0")
    total_column_list = DF_Count_Output.columns
    print(f"[+] Diagnosis Eligible Source Columns: {total_column_list}")
    DF_Count_Output = DF_Count_Output.withColumn("Diagnosis_eligible", lit(None)).drop("patient_id")
    print(f"[+] Diagnosis Eligible Row Count: {DF_diagnosis_source_standard.count()}")
    items_to_remove = ["patient_id","shs_patient_id","Count_Eligible_Flag","Diagnosis_eligible","hh_fact"]
    for item in items_to_remove:  
        if item in total_column_list:       
             total_column_list.remove(item)
    total_column_list = move_to_front(total_column_list,'diagnostic_indication')
    total_column_list = move_to_front(total_column_list,'Time_Period')
    breakcombination = walk_and_generate_combinations(total_column_list)
    #DF_diagnosis_source_standard = DF_diagnosis_source_standard.filter(f.col('Count_Eligible_Flag').isNotNull())

    for data_break in breakcombination:
        print(f"[+] Starting break:{data_break} ") 
        DF_temp_totals = DF_diagnosis_source_standard
        if "patient_id" in data_break:
            data_break.remove("patient_id")
        duplicate_drop = data_break.copy()
        duplicate_drop.append('patient_id')
        print(f'[+] Deduplicate Columns: {duplicate_drop}')
        DF_temp_totals = DF_temp_totals.groupby(*data_break).agg(f.countDistinct('patient_id').alias("Diagnosis_eligible"))

        Excluded_column_base = DF_Count_Output.columns

        if 'hh_fact' in Excluded_column_base:
            Excluded_column_base.remove('hh_fact')

        included_column =  list(DF_temp_totals.columns)
        excluded_column = [column for column in Excluded_column_base if column not in included_column]
        if "patient_id" in excluded_column:
            excluded_column.remove("patient_id")

        print("[+] Adding Missing Columns")
        for column in excluded_column:
            DF_temp_totals = DF_temp_totals.withColumn(column, lit("Total"))
        print("[+] Casting Columns as String")
        DF_temp_totals = DF_temp_totals.select([col(c).cast("string").alias(c) for c in Excluded_column_base])
        print("[+] Joining Break Total to Output")
        DF_Count_Output = DF_Count_Output.unionByName(DF_temp_totals)
        #DF_Count_Output.persist()

    #Adding Total Total        
    #DF_diagnosis_source_standard = DF_diagnosis_source_standard.filter(f.col('Min_HHFact').isNotNull())
    df_total_toal = DF_diagnosis_source_standard.dropDuplicates(['patient_id']).select(f.countDistinct('patient_id').alias("Diagnosis_eligible"))
    total_total_column_list = Excluded_column_base.copy()
    total_total_column_list.remove("Diagnosis_eligible")
    for column in total_total_column_list:
        df_total_toal = df_total_toal.withColumn(column, lit("Total"))
    DF_Count_Output = DF_Count_Output.unionByName(df_total_toal)

    print("[+]  Finished Diagnosis break stages")
    DF_Count_Output=  DF_Count_Output.dropDuplicates()
    print("[+]  Returning Break Totals DF From Diagnosis Eligible")
    return DF_Count_Output

###################################################################################
################### Transformation Functions ######################################
###################################################################################

def get_filtered_data(df, condition, gid, gid_type, age_qualifier, age, ethnicities, codes, occurrence, occurrence_count, timeFrame, timeFrame_count):

    if condition is not None:
        df = df.filter(col('diagnostic_indication') == condition)

    if gid and gid_type:
        if gid_type == 'PX':
            df = df.filter(col('Proc_mkt_def_gid') == gid)
        if gid_type == 'RX':
            df = df.filter(col('Prod_mkt_def_gid') == gid)
        if gid_type == 'DX':
            df = df.filter(col('Diag_mkt_def_gid') == gid)
        if gid_type == 'SX':
            df = df.filter(col('Surg_mkt_def_gid') == gid)

    if age_qualifier and age != 0:
        Filtered_year = current_year - int(age)
        df = df.filter(f'patient_birth_year {age_qualifier} {Filtered_year}')

    if ethnicities:
        df = df.filter(col('patient_ethnicity').isin(ethnicities))

    if codes:
        code_list = [code['code'].strip() for code in codes]
        df = df.filter(col('Diag_diag_code').isin(code_list))

    if timeFrame and timeFrame_count != 0:
        timeFrame_year = current_date - timedelta(days=int(timeFrame_count))
        if timeFrame =='DaysApart':
            con = '<'
            df = df.filter(f"service_date {con} '{timeFrame_year}'")

        if timeFrame =='DaysBetween':
            df = df.filter(f"service_date is between {current_year}")

    if occurrence and occurrence_count != 0:
        window = Window.partitionBy('shs_patient_id')
        df = df.withColumn('occurrence_count', count('shs_patient_id').over(window))

        if occurrence == '<':
            df = df.filter(col('occurrence_count') < occurrence_count)
        elif occurrence == '>':
            df = df.filter(col('occurrence_count') > occurrence_count)
        elif occurrence == '<=':
            df = df.filter(col('occurrence_count') <= occurrence_count)
        elif occurrence == '>=':
            df = df.filter(col('occurrence_count') >= occurrence_count)
        elif occurrence == '==':
            df = df.filter(col('occurrence_count') == occurrence_count)

        df = df.drop('occurrence_count')
        
    return df

def apply_rules(df, rules):
    internal_dfs = []
    final_result = None
    
    #levelwise calcualtion
    for level in rules:
        include_dfs = []
        exclude_df = None
        include_result = None
        
        #Applying filter 
        for item in level['items']:
            item_df = get_filtered_data(
                df,
                item.get('condition'),
                item.get('gid'),
                item.get('gidType'),
                item.get('ageQualifier'),
                item.get('age'),
                item.get('ethnicity'),
                item.get('codes'),
                item.get('occurrence'),
                item.get('occurrenceCount'),
                item.get('timeFrame'),
                item.get('timeFrameCount')
            )
            if item['option'] == 'INCLUDE':
                include_dfs.append(item_df)
            elif item['option'] == 'EXCLUDE':
                exclude_df = df.subtract(item_df)
                include_dfs.append(exclude_df)

        # INTERNAL'AND'/'OR' for loop
        for item in level['items']:
            for dfs in include_dfs:
                if include_result is None:
                    include_result = dfs
                else:
                    if item.get('logic') == 'AND':
                        include_result = include_result.intersect(dfs)
                    elif item.get('logic') == 'OR':
                        include_result = include_result.unionByName(dfs).distinct()
        internal_dfs.append(include_result)
            

    # EXTERNAL 'AND' / 'OR' for loop
    for level in rules:
        for level_result in internal_dfs:
            if final_result is None:
                final_result = level_result
            else:
                if level['logic'] == 'AND':
                    final_result = final_result.intersect(level_result)
                elif level['logic'] == 'OR':
                    final_result = final_result.unionByName(level_result).distinct()
                elif level['logic'] == 'OFF':
                    break

    return final_result if final_result is not None else df

#Applying transformation if any of my column has null assign the total to the dataframe(standard)
def replace_null_with_total_standard(df):
    return df.select([
        f.when(f.col(c).isNull(), f.lit('Total')).otherwise(f.col(c)).alias(c) 
        if c not in ['Time_Period', 'Count_eligible', 'Diagnosis_eligible'] 
        else f.col(c).alias(c) 
        for c in df.columns
    ])
    
#Applying transformation if any of my column has null assign the total to the dataframe(specailty)
def replace_null_with_total_specialty(df):
    return df.select([
        f.when(f.col(c).isNull(), f.lit('Total')).otherwise(f.col(c)).alias(c) 
        if c not in ['Time_Period', 'Count_eligible', 'Diagnosis_eligible','SSPEC','NSPEC'] 
        else f.col(c).alias(c) 
        for c in df.columns
    ])

# def get_total_rows(df, breakCombination):
#     break_df = None
#     for combination in breakCombination:
#         # Convert break combination to a list
#         break_cols = [col.strip().replace(' ', '_') for col in combination.split(',')]

#         # Create conditions for total rows
#         conditions = [col(c) == 'Total' for c in break_cols]

#         # Filter the dataframe to get only total rows for this combination
#         total_df = df.filter(reduce(lambda a, b: a | b, conditions))

#         # Union with previous results
#         if break_df is None:
#             break_df = total_df
#         else:
#             break_df = break_df.unionByName(total_df)

#     return break_df


########################################################################################################################
##################################### IMPORTS ##########################################################################
########################################################################################################################

#Path definition
s3_inputpath_count = f's3://{bucketName}/silver/{studyID}/{expsoureMappingscheduleId}/break_datamart/{mediaType}/count/'
s3_inputpath_diagnosis = f's3://{bucketName}/silver/{studyID}/{expsoureMappingscheduleId}/break_datamart/{mediaType}/diagnosis/'
output_filepath = f's3://{bucketName}/silver/{studyID}/{scheduleId}/{deliverableId}/reporting_break/'
output_filepath_qp = f's3://{bucketName}/silver/{studyID}/{scheduleId}/{deliverableId}/quarterly/reporting_break/'

#Ethnicity File Path
Ethnicity_output_filepath = f's3://{bucketName}/silver/{studyID}/{scheduleId}/{deliverableId}/Ethnicity'
Ethnicity_output_filepath_qp = f's3://{bucketName}/silver/{studyID}/{scheduleId}/{deliverableId}/quarterly/Ethnicity'

#Specialty File Path
Specialty_output_filepath = f's3://{bucketName}/silver/{studyID}/{scheduleId}/{deliverableId}/Specialty'
Specialty_output_filepath_qp = f's3://{bucketName}/silver/{studyID}/{scheduleId}/{deliverableId}/quarterly/Specialty'

isRerunRequired = 'false'

########################################################################################################################
########################################################################################################################
##################################### Rerun and Monthly ################################################################
########################################################################################################################
########################################################################################################################

# Rerun calculation Monthly
if runType =='Rerun' and reportingPeriod == 'Monthly':
    print("[+] Starting Rerun calculation for Monthly")
    #standrad
    Age_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Age_drop/' 
    count_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_count_drop/'
    
    #Ethnicity
    Con_cumulative_filepath_ethnicity = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Con_Ethnicity/'
    Lot_cumulative_filepath_ethnicity = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Lot_Ethnicity/'
    
    #Specialty
    Con_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Con_Specialty/'
    Lot_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Lot_Specialty/'
    SSPEC_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_SSPEC_Specialty/'
    NSPEC_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_NSPEC_Specialty/'
    
    
    # Check Age and Count files
    print(f"Age standard Delta table exists: {check_delta_table_exists(spark, Age_cumulative_filepath)}")
    print(f"Count standard Delta table exists: {check_delta_table_exists(spark, count_cumulative_filepath)}")
    
    StandardRunStatusAge = check_delta_table_exists(spark, Age_cumulative_filepath)
    StandardRunStatusLot = check_delta_table_exists(spark, count_cumulative_filepath)
    
    # Check Ethnicity files if required
    if isEthnicityRequired == 'true':
        print(f"Con Ethnicity Delta table exists: {check_delta_table_exists(spark, Con_cumulative_filepath_ethnicity)}")
        print(f"Lot Ethnicity Delta table exists: {check_delta_table_exists(spark, Lot_cumulative_filepath_ethnicity)}")
        EthnicityRunStatusCon = check_delta_table_exists(spark, Con_cumulative_filepath_ethnicity)
        EthnicityRunStatusLot = check_delta_table_exists(spark, Lot_cumulative_filepath_ethnicity)
    
    # Check Specialty files if required
    if isSpecialtyRequired == 'true':
        print(f"Con Specialty Delta table exists: {check_delta_table_exists(spark, Con_cumulative_filepath_Specialty)}")
        print(f"Lot Specialty Delta table exists: {check_delta_table_exists(spark, Lot_cumulative_filepath_Specialty)}")
        print(f"SSPEC Specialty Delta table exists: {check_delta_table_exists(spark, SSPEC_cumulative_filepath_Specialty)}")
        print(f"NSPEC Specialty Delta table exists: {check_delta_table_exists(spark, NSPEC_cumulative_filepath_Specialty)}")
        SpecialtyRunStatusCon = check_delta_table_exists(spark, Con_cumulative_filepath_Specialty)
        SpecialtyRunStatusLot = check_delta_table_exists(spark, Lot_cumulative_filepath_Specialty)
        SpecialtyRunStatusSSPEC = check_delta_table_exists(spark, SSPEC_cumulative_filepath_Specialty)
        SpecialtyRunStatusNSPEC = check_delta_table_exists(spark, NSPEC_cumulative_filepath_Specialty)
     
    #Rerun Check condition    
    if isSpecialtyRequired =='true'  and isEthnicityRequired =='true':
        
        if EthnicityRunStatusCon and EthnicityRunStatusLot and StandardRunStatusAge and StandardRunStatusLot and SpecialtyRunStatusCon and SpecialtyRunStatusLot and SpecialtyRunStatusSSPEC and SpecialtyRunStatusNSPEC:
            isRerunRequired = 'true'
            print("[+] ethnicity and specialty Rerun ")
        else:
            print("[+] Ethnicity and Specialty Rerun false")
            isRerunRequired = 'false'
        
    elif isEthnicityRequired =='true':
        if EthnicityRunStatusCon and EthnicityRunStatusLot and StandardRunStatusAge and StandardRunStatusLot:
            isRerunRequired = 'true'
            print("[+] ethnicity Rerun")
        else:
            print("[+] ethnicity Rerun false")
            isRerunRequired = 'false'
            
    elif isSpecialtyRequired=='true':
        
        if StandardRunStatusAge and StandardRunStatusLot and SpecialtyRunStatusCon and SpecialtyRunStatusLot and SpecialtyRunStatusSSPEC and SpecialtyRunStatusNSPEC:
            isRerunRequired = 'true'
            print("[+] specialty Rerun")
        else:
            print("[+] specialty Rerun false")
            isRerunRequired = 'false'
    else:
        if StandardRunStatusAge and StandardRunStatusLot:
            print("[+] standard Rerun")
            isRerunRequired = 'true'
        else:
            print("[+] standard Rerun false")
            isRerunRequired = 'false'


########################################################################################################################
##################################### Rerun and Quarterly ##############################################################
########################################################################################################################

# Rerun calculation Quarterly 
if runType =='Rerun' and reportingPeriod == 'Quarterly':
    print("[+] Starting Rerun calculation for Quarterly")
    #standrad
    Age_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/quarterly_cumulative/DF_Age_drop/' 
    count_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/quarterly_cumulative/DF_count_drop/'
    
    #Ethnicity
    Con_cumulative_filepath_ethnicity = f's3://{bucketName}/silver/{studyID}/{deliverableId}/quarterly_cumulative/DF_Con_Ethnicity/'
    Lot_cumulative_filepath_ethnicity = f's3://{bucketName}/silver/{studyID}/{deliverableId}/quarterly_cumulative/DF_Lot_Ethnicity/'
    
    #Specialty
    Con_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/quarterly/DF_Con_Specialty/'
    Lot_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/quarterly/DF_Lot_Specialty/'
    SSPEC_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/quarterly/DF_SSPEC_Specialty/'
    NSPEC_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/quarterly/DF_NSPEC_Specialty/'
    

    # Check Age and Count files
    print(f"Age standard Delta table exists: {check_delta_table_exists(spark, Age_cumulative_filepath)}")
    print(f"Count standard Delta table exists: {check_delta_table_exists(spark, count_cumulative_filepath)}")
    
    StandardRunStatusAge = check_delta_table_exists(spark, Age_cumulative_filepath)
    StandardRunStatusLot = check_delta_table_exists(spark, count_cumulative_filepath)
    
    # Check Ethnicity files if required
    if isEthnicityRequired == 'true':
        print(f"Con Ethnicity Delta table exists: {check_delta_table_exists(spark, Con_cumulative_filepath_ethnicity)}")
        print(f"Lot Ethnicity Delta table exists: {check_delta_table_exists(spark, Lot_cumulative_filepath_ethnicity)}")
        EthnicityRunStatusCon = check_delta_table_exists(spark, Con_cumulative_filepath_ethnicity)
        EthnicityRunStatusLot = check_delta_table_exists(spark, Lot_cumulative_filepath_ethnicity)
    
    # Check Specialty files if required
    if isSpecialtyRequired == 'true':
        print(f"Con Specialty Delta table exists: {check_delta_table_exists(spark, Con_cumulative_filepath_Specialty)}")
        print(f"Lot Specialty Delta table exists: {check_delta_table_exists(spark, Lot_cumulative_filepath_Specialty)}")
        print(f"SSPEC Specialty Delta table exists: {check_delta_table_exists(spark, SSPEC_cumulative_filepath_Specialty)}")
        print(f"NSPEC Specialty Delta table exists: {check_delta_table_exists(spark, NSPEC_cumulative_filepath_Specialty)}")
        SpecialtyRunStatusCon = check_delta_table_exists(spark, Con_cumulative_filepath_Specialty)
        SpecialtyRunStatusLot = check_delta_table_exists(spark, Lot_cumulative_filepath_Specialty)
        SpecialtyRunStatusSSPEC = check_delta_table_exists(spark, SSPEC_cumulative_filepath_Specialty)
        SpecialtyRunStatusNSPEC = check_delta_table_exists(spark, NSPEC_cumulative_filepath_Specialty)
     
    #Rerun Check condition    
    if isSpecialtyRequired =='true'  and isEthnicityRequired =='true':
        
        if EthnicityRunStatusCon and EthnicityRunStatusLot and StandardRunStatusAge and StandardRunStatusLot and SpecialtyRunStatusCon and SpecialtyRunStatusLot and SpecialtyRunStatusSSPEC and SpecialtyRunStatusNSPEC:
            isRerunRequired = 'true'
            print("[+] ethnicity and specialty Rerun ")
        else:
            print("[+] Ethnicity and Specialty Rerun false")
            isRerunRequired = 'false'
        
    elif isEthnicityRequired =='true':
        if EthnicityRunStatusCon and EthnicityRunStatusLot and StandardRunStatusAge and StandardRunStatusLot:
            isRerunRequired = 'true'
            print("[+] ethnicity Rerun")
        else:
            print("[+] ethnicity Rerun false")
            isRerunRequired = 'false'
            
    elif isSpecialtyRequired=='true':
        
        if StandardRunStatusAge and StandardRunStatusLot and SpecialtyRunStatusCon and SpecialtyRunStatusLot and SpecialtyRunStatusSSPEC and SpecialtyRunStatusNSPEC:
            isRerunRequired = 'true'
            print("[+] specialty Rerun")
        else:
            print("[+] specialty Rerun false")
            isRerunRequired = 'false'
    else:
        if StandardRunStatusAge and StandardRunStatusLot:
            print("[+] standard Rerun")
            isRerunRequired = 'true'
        else:
            print("[+] standard Rerun false")
            isRerunRequired = 'false'


print("[+] The isRerunRequired value is ", isRerunRequired)


#Reading the applied exposure count eligible data into dataframe 
DF_count = spark.read.format("delta").load(s3_inputpath_count)
DF_count = DF_count.filter((col("Count_Eligible_Flag") == 1) & (col("media_type") == mediaType))

#Reading the applied exposure diagnosis eligible data into dataframe 
DF_diagnosis = spark.read.format("delta").load(s3_inputpath_diagnosis)
DF_diagnosis = DF_diagnosis.filter((col("Count_Eligible_Flag") == 1) & (col("media_type") == mediaType))
DF_diagnosis = DF_diagnosis.filter('diagnostic_indication is not null')

try:
    for exMpBreak in exposureMappingBreaks:
        columnName = exMpBreak["breaks"]
        value = exMpBreak["distinctValue"]
        filterName = exMpBreak["filter"]
        required = exMpBreak["isReportRequired"]

        if required and filterName and value is not None:
            if filterName == 'Equal To':
                con = '==' if isinstance(columnName, str) else '='
            elif filterName == 'Not Equal To':
                con = '!='
            elif filterName == 'Greater Than':
                con = '>='
            elif filterName == 'Less Than':
                con = '<='
            else:
                raise ValueError(f"Unsupported filter: {filterName}")

            if isinstance(value, str):
                DF_count = DF_count.filter(f"{columnName} {con} '{value}'")
                DF_diagnosis = DF_diagnosis.filter(f"{columnName} {con} '{value}'")
            else:
                DF_diagnosis = DF_diagnosis.filter(f"{columnName} {con} '{value}'")
                DF_count = DF_count.filter(f"{columnName} {con} '{value}'")
                
except Exception as e:
    print(f'exposureMappingBreaks filter has failed: {str(e)}')


#Filtering media type for parsing the ethnicity/specailty
try:
    media_platforms = []
    for con in Condition:
        for platform in con['mediaPlatform']:
            media_platforms.append({
                'mediaKey': platform['mediaKey'],
                'mediaValue': platform['mediaValue']
            })

    # Remove duplicates (if any)
    media_platforms = [dict(t) for t in {tuple(d.items()) for d in media_platforms}]
except:
    print("[+] No platform values are found")

########################################################################################################################
########################################################################################################################
##################################### Study Rule Required ##############################################################
########################################################################################################################
########################################################################################################################

if isStudyRulesRequired == 'true':
    print("[+] Starting Study Rules Required IF Run")

    current_date = datetime.now().date()
    current_year = current_date.year
    
    
    final_df_diagnosis = apply_rules(DF_diagnosis, studyRules)
    DF_diagnosis  = final_df_diagnosis
    print("[+] studyRules calcualtion has been completed")
    

DF_count.show(5)
DF_diagnosis.show(5)

########################################################################################################################
########################################################################################################################
############################################ Monthly ###################################################################
########################################################################################################################
########################################################################################################################

if reportingPeriod == 'Monthly':
    print("[+] Starting Monthly Run")

    for con in Condition:
        conditionName = con["conditionName"]
        # contractStart = con["contractStart"]
        # contractStart = datetime(contractStart[0], contractStart[1], contractStart[2])
        # contractStart = datetime.strftime(contractStart, '%Y-%m-%d')
        # contractEnd = con["contractEnd"]
        # contractEnd = datetime(contractEnd[0], contractEnd[1], contractEnd[2])
        # contractEnd = datetime.strftime(contractEnd, '%Y-%m-%d') 
        DF_Contract_Date_count = DF_count
        DF_Contract_Date_diagnosis = DF_diagnosis
    
        DF_LOT_count = DF_Contract_Date_count.withColumn("Device", lit(None))
        DF_LOT_diagnosis = DF_Contract_Date_diagnosis.withColumn("Device", lit(None))
        
        if con["mediaPlatform"] is not None:
            for media in con["mediaPlatform"]:
                Media_Value = media["mediaValue"]
                Media_Key = media["mediaKey"]
    
                # Filter and add Device column for count dataframe
                DF_filtered_count = DF_Contract_Date_count.filter(col("mediaPlatform") == Media_Key) \
                    .withColumn("Device", lit(Media_Value))
    
                # Filter and add Device column for diagnosis dataframe
                DF_filtered_diagnosis = DF_Contract_Date_diagnosis.filter(col("mediaPlatform") == Media_Key) \
                    .withColumn("Device", lit(Media_Value))
                DF_LOT_count = DF_LOT_count.unionByName(DF_filtered_count)
                DF_LOT_diagnosis = DF_LOT_diagnosis.unionByName(DF_filtered_diagnosis)
        else:
            print("[+] handled null values")
            DF_LOT_count = DF_Contract_Date_count.withColumn("Device",col("mediaPlatform") )
            DF_LOT_diagnosis = DF_Contract_Date_diagnosis.withColumn("Device",col("mediaPlatform"))
    
    DF_LOT_count = DF_LOT_count.filter(col("Device").isNotNull())
    DF_LOT_diagnosis = DF_LOT_diagnosis.filter(col("Device").isNotNull())
    
    #distinct_platforms = list(set(media['mediaKey'] for media in Condition[0]['mediaPlatform']))
    
    try:
        if len(Condition[0]['mediaPlatform'])==2 :
            # Get distinct values
            # distinct_values = DF_LOT_count.select('device').distinct()
    
            # # Create a string concatenation of all distinct values
            # concat_expr = f.concat_ws('+', *[f.collect_list('device')[i] for i in range(distinct_count)])
    
            # concatenated_value = distinct_values.agg(concat_expr.alias('concatenated')).collect()[0]['concatenated']

            distinct_values = DF_LOT_count.select('device').distinct()

            # Collect the distinct values as a Python list
            distinct_list = distinct_values.rdd.flatMap(lambda x: x).collect()

            # Concatenate the list values with a '+' separator
            concatenated_value = '+'.join(distinct_list)
    
            # Add new column with concatenated value
            DF_LOT_count_break = DF_LOT_count.withColumn('device', f.lit(concatenated_value))
            DF_LOT_diagnosis_break = DF_LOT_diagnosis.withColumn('device', f.lit(concatenated_value))
            DF_LOT_count = DF_LOT_count.unionByName(DF_LOT_count_break)
            DF_LOT_diagnosis = DF_LOT_diagnosis.unionByName(DF_LOT_diagnosis_break)
    
            
        if len(Condition[0]['mediaPlatform'])>=3:
            # Get distinct values
            
            distinct_values = DF_LOT_count.select("device").distinct().rdd.flatMap(lambda x: x).collect()
    
            values_to_check = {'Mobile','Desktop','Tablet'}
            is_present = values_to_check.issubset(distinct_values)
            
            if is_present:
                # Create a string concatenation of all distinct values
                #concat_expr = f.concat_ws('+', *[f.collect_list('device')[i] for i in range(distinct_count)])
                #concatenated_value = distinct_values.agg(concat_expr.alias('concatenated')).collect()[0]['concatenated']
    
                concatenated_value = "TOTAL DIGITAL"
                # Add new column with concatenated value
                
                DF_LOT_count_break = DF_LOT_count.filter(col('device').isin(values_to_check))
                DF_LOT_diagnosis_break = DF_LOT_diagnosis.filter(col('device').isin(values_to_check))
                
                DF_LOT_count_break = DF_LOT_count_break.withColumn('device', f.lit(concatenated_value))
                DF_LOT_diagnosis_break = DF_LOT_diagnosis_break.withColumn('device', f.lit(concatenated_value))
                DF_LOT_count = DF_LOT_count.unionByName(DF_LOT_count_break)
                DF_LOT_diagnosis = DF_LOT_diagnosis.unionByName(DF_LOT_diagnosis_break)  
            
        if len(Condition[0]['mediaPlatform'])>3:
            
            # Get distinct values
            distinct_values = DF_LOT_count.select('device').distinct()
    
            # Create a string concatenation of all distinct values
            #concat_expr = f.concat_ws('+', *[f.collect_list('device')[i] for i in range(distinct_count)])
            #concatenated_value = distinct_values.agg(concat_expr.alias('concatenated')).collect()[0]['concatenated']
    
            concatenated_value = "TOTAL PLATFORM"
            # Add new column with concatenated value
            DF_LOT_count_break = DF_LOT_count.withColumn('device', f.lit(concatenated_value))
            DF_LOT_diagnosis_break = DF_LOT_diagnosis.withColumn('device', f.lit(concatenated_value))
            DF_LOT_count = DF_LOT_count.unionByName(DF_LOT_count_break)
            DF_LOT_diagnosis = DF_LOT_diagnosis.unionByName(DF_LOT_diagnosis_break)  
    
    except Exception as e:
        print(f"No platform break applied. Error")
        
    print("[+] Applying dataBreaks calculation")
    current_date = datetime.now().date()
    year = current_date.year
    
    DF_LOT = DF_LOT_count
    DF_Age = DF_LOT_diagnosis
    
    for sym_break in dataBreaks:
        break_name = sym_break["breakName"]
        filter_condition = sym_break["filter"]
        distinct_value = sym_break["distinctValue"]
    
        if filter_condition == 'Equal To':
            con = '='
        elif filter_condition == 'Not Equal To':
            con = '!='
        elif filter_condition == 'Greater Than':
            con = '>='
        elif filter_condition == 'Less Than':
            con = '<='
    
        if break_name == 'AGE':
            DF_Age = DF_Age.filter('patient_age_group is not null')
            DF_LOT = DF_LOT.filter('patient_age_group is not null')
            DF_Age = DF_Age.withColumnRenamed("patient_age_group" , "AGE")
            DF_LOT = DF_LOT.withColumnRenamed("patient_age_group" , "AGE")
            
            # DF_Age = DF_Age.withColumn("AGE", year - f.col("patient_birth_year"))
            # DF_LOT = DF_LOT.withColumn("AGE", year - f.col("patient_birth_year"))
            # if distinct_value is None:
            #     print("[+] Warning: No distinct value provided for AGE filter")
            #     continue
            # try:
            #     YearOfBirth = year - int(distinct_value)
            #     DF_Age = DF_Age.filter(f'patient_birth_year {con} {YearOfBirth}')
            #     DF_LOT = DF_LOT.filter(f'patient_birth_year {con} {YearOfBirth}')
            # except ValueError:
            #     print(f"[-] Warning: Invalid distinct value '{distinct_value}' for AGE filter")
            #     continue
            print("[+] Apply the age breaks")
    
        elif break_name == 'ETHNICITY':
            DF_Age = DF_Age.filter('patient_ethnicity is not null')
            DF_LOT = DF_LOT.filter('patient_ethnicity is not null')

            DF_Age = DF_Age.withColumn("ETHNICITY",f.col("patient_ethnicity"))
            DF_LOT = DF_LOT.withColumn("ETHNICITY",f.col("patient_ethnicity"))
            if con == '=':
                DF_Age = DF_Age.filter(f"patient_ethnicity == '{distinct_value}'")
                DF_LOT = DF_LOT.filter(f"patient_ethnicity == '{distinct_value}'")
            elif con == '!=':
                DF_Age = DF_Age.filter(f"patient_ethnicity != '{distinct_value}'")
                DF_LOT = DF_LOT.filter(f"patient_ethnicity != '{distinct_value}'")
            else:
                print(f"[-] Warning: '{filter_condition}' filter may not be applicable for ETHNICITY")
    
        elif break_name == 'RACE':
            DF_Age = DF_Age.filter('patient_ethnicity is not null')
            DF_LOT = DF_LOT.filter('patient_ethnicity is not null')
            DF_Age = DF_Age.withColumn("RACE",f.col("patient_ethnicity"))
            DF_LOT = DF_LOT.withColumn("RACE",f.col("patient_ethnicity"))
            if con == '=':
                DF_Age = DF_Age.filter(f"patient_ethnicity == '{distinct_value}'")
                DF_LOT = DF_LOT.filter(f"patient_ethnicity == '{distinct_value}'")
            elif con == '!=':
                DF_Age = DF_Age.filter(f"patient_ethnicity != '{distinct_value}'")
                DF_LOT = DF_LOT.filter(f"patient_ethnicity != '{distinct_value}'")
            else:
                print(f"[-] Warning: '{filter_condition}' filter may not be applicable for ETHNICITY")
    
        elif break_name == 'LOT':
            DF_Age = DF_Age.filter('proc_group is not null')
            DF_Age = DF_Age.withColumn("Line_of_Therapy",f.col("proc_group"))
            #DF_LOT = DF_LOT.withColumn("Line_of_Therapy",f.col("proc_group")) 
            if con == '=':
                DF_Age = DF_Age.filter(f"proc_group == '{distinct_value}'")
                #DF_LOT = DF_LOT.filter(f"proc_group == '{distinct_value}'")
            elif con == '!=':
                DF_Age = DF_Age.filter(f"proc_group != '{distinct_value}'")
                #DF_LOT = DF_LOT.filter(f"proc_group != '{distinct_value}'")
            else:
                print(f"[-] Warning: '{filter_condition}' filter may not be applicable for SPECIALTY")
    
        else:
            print(f"[-] Warning: Unknown break name '{break_name}'")
    
    DF_Age.createOrReplaceTempView("Age")
    DF_Age = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Age''')
    DF_Age = DF_Age.drop(DF_Age.Time)
    
    DF_LOT.createOrReplaceTempView("Lot")
    DF_LOT = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Lot''')
    DF_LOT = DF_LOT.drop(DF_LOT.Time)
    
    print("[+] The filter condition, dataBreaks has been calculated for the standard report or the household report calculation")

########################################################################################################################
####################################### Household NOT Required #########################################################
########################################################################################################################

    if isHouseholdRequired != 'true':
        print("[+] Starting Household NOT Required Run")

        col_age = DF_Age.columns
        count_col = DF_LOT.columns
    
        Drop_Columns = ["_c0","Viewable_Flag","_c2","_c3","_c4","_c5","Total_Impression","hh_number","hh_fact","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_ethnicity","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_practitioner_role_code","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","proc_group","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","ethniPublisher","Surg_practitioner_id","Surg_practitioner_role_code","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET8","BUCKET9","BUCKET10","Time","mediaPlatform","patient_age_group"]
    
        #Dropping unnessary Columns
        Drop_Cols = []
        for cols in col_age:
            if cols in Drop_Columns:
                Drop_Cols.append(cols)
    
        #Dropping unnessary Columns        
        Count_Drop_Cols = []
        for cols in count_col:
            if cols in Drop_Columns:
                Count_Drop_Cols.append(cols)

    
    
        DF_Age_drop = DF_Age.drop(*Drop_Cols)
        DF_count_drop = DF_LOT.drop(*Count_Drop_Cols)

        DF_Age_drop = DF_Age_drop.dropDuplicates()
        DF_count_drop = DF_count_drop.dropDuplicates()

        #cumulative
        DF_Age_drop_month = DF_Age_drop
        DF_count_drop_month = DF_count_drop
        
    
        #Declaring required aggregated columns
        Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
        Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        #Getting Group By Column
        group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
    
        count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
    
        #Calculating Count Eligible
        DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
        DF_total_source_count = DF_count_drop
        count_eligible_df = DF_Count_Elig
        
        DF_count_drop_month = DF_count_drop
        
        #Calculating Diagnosis Eligible
        DF_Diag = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
        DF_total_source_diagnosis = DF_Age_drop 
        diagnosis_eligible_df = DF_Diag
        
        DF_Age_drop_month = DF_Age_drop
        
        DF_Count_Elig.write.format('delta').mode("overwrite").save(output_filepath)
        
        print("[+] The filter condition, dataBreaks and breakCombination has been applied to the Standard report")
        
        #Standard report 
        Age_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Age_drop/' 
        count_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_count_drop/'

        Age_cumulative_filepath_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/prev_cumulative/DF_Age_drop/'
        count_cumulative_filepath_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/prev_cumulative/DF_count_drop/'

        count_cumulative_filepath_temp_std = f's3://{bucketName}/temp/{studyID}/{deliverableId}/standard_cumulative/DF_count_drop/'
        Age_cumulative_filepath_temp_std = f's3://{bucketName}/temp/{studyID}/{deliverableId}/standard_cumulative/DF_Age_drop/'
        
        if isRerunRequired != 'true':
            try:
                if isHouseholdRequired != 'true':
                    # For Age data
                #     delta_table_age = DeltaTable.forPath(spark, Age_cumulative_filepath)
                    
                    
                #     DF_Age_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_age")

                #    # If you need to read the updated data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    
                #     # delta_table_age.alias("old").merge(
                #     #     DF_Age_drop_month.alias("new"),
                #     #     'old.patient_id = new.patient_id'
                #     # ).whenNotMatchedInsertAll().execute()
        

                #     # For Lot data
                #     delta_table_lot = DeltaTable.forPath(spark, count_cumulative_filepath)
                #     # delta_table_lot.alias("old").merge(
                #     #     DF_count_drop_month.alias("new"),
                #     #     'old.patient_id = new.patient_id'
                #     # ).whenNotMatchedInsertAll().execute()
        
                    
                #     DF_count_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_age")

                #     # If you need to read the updated data
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   
                    
                    #Reading cumulative data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop = DF_Age_drop.dropDuplicates()
                    DF_count_drop = DF_count_drop.dropDuplicates()
                    
                    #std report without household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()

                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE')   

                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                
                    #Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop_std.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_total_source_count_std = DF_count_drop_std
                    count_eligible_df_std = DF_Count_Elig
                    
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_total_source_diagnosis_std = DF_Age_drop_std
                    diagnosis_eligible_df_std = DF_Diag

                    #Count eligible hippa

                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
        
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
        
                    #Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_Count_Elig_total_cumulative = DF_count_drop

                    DF_total_source_count = DF_Count_Elig_total_cumulative
                    count_eligible_df  = DF_Count_Elig
        
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_Diag_total_cumulative = DF_Age_drop
                    
                    DF_total_source_diagnosis = DF_Diag_total_cumulative                
                    diagnosis_eligible_df = DF_Diag

                    #Count eligible

                    DF_total_count_eligible = count_eligible_break_standard(DF_total_source_count)
                    
                    #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)
                    
                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_standard(DF_total_source_count_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)

                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')
 
                    Cumulative_DF = Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()
                    
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break/'

                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break_std/'
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)
                                        
                    print("[+] The cumulative report for the subsequent month of the Standard report has been completed.")
        
            #except:
            except Exception as e:
                logger.error(f"Exception occurred: {str(e)}")
                logger.error(f"Stack Trace: {traceback.format_exc()}")
                
                if isHouseholdRequired != 'true':        
                    # Raises an exception if the path already exists
                    if delta_table_exists(Age_cumulative_filepath) or delta_table_exists(count_cumulative_filepath):
                        raise Exception("One or more tables already exist,insert is forbidden in the except statement after delta table creation.")  
        
                    # DF_Age_drop_month.write.format('delta').mode("overwrite").save(Age_cumulative_filepath)
                    # DF_count_drop_month.write.format('delta').mode("overwrite").save(count_cumulative_filepath)

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)

                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   

                   #std report without household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()

                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE')   

                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                
                    #Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop_std.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_total_source_count_std = DF_count_drop_std
                    count_eligible_df_std = DF_Count_Elig
                    
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_total_source_diagnosis_std = DF_Age_drop_std
                    diagnosis_eligible_df_std = DF_Diag

                    #Count eligible

                    DF_total_count_eligible = count_eligible_break_standard(DF_total_source_count)
                    
                    #diagnosis eligible

                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)
                     
                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_standard(DF_total_source_count_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()


                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)

                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                    
                
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')

                    count_eligible_df_std   = count_eligible_df_std.dropDuplicates()
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')
                                
                    print("[+] Joined the both data frames")
                    Cumulative_DF =  Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break/'
                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break_std/'
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)
                    
                    print("[+] The cumulative report for the 1st month of the Standard report has been completed.")
        
        if isRerunRequired == 'true':
            try:
                if isHouseholdRequired != 'true':
                    # For Age data
                    # delta_table_age = DeltaTable.forPath(spark, Age_cumulative_filepath)
                    
                    # #perfroming the given month deletion
                    # delta_table_age.delete(col("Time_Period") == reRunTimePeriod)
                    
                    # DF_Age_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_age")
        
                    # # If you need to read the updated data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    
                    # #DF_Age = spark.read.parquet(Age_cumulative_filepath)
                    # DF_Age_drop = DF_Age_drop.dropDuplicates()
        
                    # # For Lot data
                    # delta_table_lot = DeltaTable.forPath(spark, count_cumulative_filepath)
                    
                    # #perfroming the given month deletion
                    # delta_table_lot.delete(col("Time_Period") == reRunTimePeriod)
                    
                    # DF_count_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_lot")
        
                    # # If you need to read the updated data
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)
                    # DF_count_drop = DF_count_drop.dropDuplicates()

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   
                    
                    #Reading cumulative data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop = DF_Age_drop.dropDuplicates()
                    DF_count_drop = DF_count_drop.dropDuplicates()

                    #std report without household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()

                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE')   

                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                
                    #Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop_std.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_total_source_count_std = DF_count_drop_std
                    count_eligible_df_std = DF_Count_Elig
                    
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_total_source_diagnosis_std = DF_Age_drop_std
                    diagnosis_eligible_df_std = DF_Diag

                    #Count eligible hippa

                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
        
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
        
                    #Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_Count_Elig_total_cumulative = DF_count_drop

                    DF_total_source_count = DF_Count_Elig_total_cumulative
                    count_eligible_df  = DF_Count_Elig
        
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_Diag_total_cumulative = DF_Age_drop
                    
                    DF_total_source_diagnosis = DF_Diag_total_cumulative                
                    diagnosis_eligible_df = DF_Diag

                    #Count eligible

                    DF_total_count_eligible = count_eligible_break_standard(DF_total_source_count)
                    
                    #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)
                    
                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_standard(DF_total_source_count_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)

                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')
 
                    Cumulative_DF = Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()
                    
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break/'

                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break_std/'
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)
                    
                    print("[+] Rerun process has been Succeeded for the standard report")
                    
            except Exception as e:
                print(f"[-] Rerun process has been failed for the standard report: {e}")
              
########################################################################################################################
####################################### Household Required #############################################################
########################################################################################################################

    if isHouseholdRequired == 'true':
        print("[+] Starting Household Required IF Run")

        col_age = DF_Age.columns
        count_col = DF_LOT.columns    
    
        Drop_Columns = ["_c0","Viewable_Flag","_c3","_c4","_c5","Total_Impression","Count_Eligible_Flag","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_ethnicity","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_practitioner_role_code","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","proc_group","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","ethniPublisher","Surg_practitioner_id","Surg_practitioner_role_code","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET8","BUCKET9","BUCKET10","Time","mediaPlatform","patient_age_group"]
    
        #Dropping unnessary Columns
        Drop_Cols = []
        for cols in col_age:
            if cols in Drop_Columns:
                Drop_Cols.append(cols)
    
        #Dropping unnessary Columns        
        Count_Drop_Cols = []
        for cols in count_col:
            if cols in Drop_Columns:
                Count_Drop_Cols.append(cols)
    
    
        DF_Age_drop = DF_Age.drop(*Drop_Cols)
        DF_Age_drop = DF_Age_drop.drop('hh_fact')
        DF_count_drop = DF_LOT.drop(*Count_Drop_Cols)
    
        DF_Age_drop = DF_Age_drop.dropDuplicates()
        DF_count_drop = DF_count_drop.dropDuplicates()
    
        #cumulative
        DF_Age_drop_month = DF_Age_drop
        DF_count_drop_month = DF_count_drop
    
        #Declaring required aggregated columns
        Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
        Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        #Getting Group By Column
        group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
        group_by_Cols.remove('hh_number')
    
        #Getting Group By Column
        count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
        count_group_by_Cols.remove('hh_number')
        columns = ','.join(str(c) for c in group_by_Cols)
        DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
        hhgrouping = count_group_by_Cols.copy()
        hhgrouping.append('patient_id')
        DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
    
        hhgrouping = count_group_by_Cols.copy()
        hhgrouping.append('patient_id')
    
        #Getting Minimum HHFact aginst required breaks
        DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
        DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
        DF_Claims_Group_total = DF_Claims_Group
        DF_total_source_claims = DF_Claims_Group
        #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
        DF_Claims_Group = DF_Claims_Group.drop('patient_id')
        DF_Claims_Group.createOrReplaceTempView('Claims_Group')    
        
        DF_Claims_CE = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
        
        count_eligible_df = DF_Claims_CE
            
        print("diag columns",DF_Age_drop.columns)
        #Calculating Diagnosis Eligible
        DF_Diag = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
        
        DF_total_source_diagnosis =  DF_Age_drop
    
        diagnosis_eligible_df = DF_Diag
            
        #Writing the data into S3 as Delta format
        count_eligible_df.write.format('delta').mode("overwrite").save(output_filepath)
        
        print("[+] The filter condition, dataBreaks and breakCombination has been applied to the household standard report")
    
        #standard report household
        Age_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Age_drop/'
        count_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_count_drop/'
        print(f"[+] Age_cumulative_filepath: {Age_cumulative_filepath}")
        print(f"[+] count_cumulative_filepath: {count_cumulative_filepath}")

        Age_cumulative_filepath_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/prev_cumulative/DF_Age_drop/'
        count_cumulative_filepath_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/prev_cumulative/DF_count_drop/'

        count_cumulative_filepath_temp_std = f's3://{bucketName}/temp/{studyID}/{deliverableId}/standard_cumulative/DF_count_drop/'
        Age_cumulative_filepath_temp_std = f's3://{bucketName}/temp/{studyID}/{deliverableId}/standard_cumulative/DF_Age_drop/'

        if isRerunRequired != 'true':
            try:
                if isHouseholdRequired == 'true':
                    print("Try block is executing")
                    #Reading the previous data
                    # # For Age data
                    # delta_table_age = DeltaTable.forPath(spark, Age_cumulative_filepath)
                    
                    
                    # # delta_table_age.alias("old").merge(
                    # #     DF_Age_drop_month.alias("new"),
                    # #     'old.patient_id = new.patient_id'
                    # # ).whenNotMatchedInsertAll().execute()
                    
                    # DF_Age_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_age")

                    # # If you need to read the updated data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    
                    # # For Lot data
                    # delta_table_lot = DeltaTable.forPath(spark, count_cumulative_filepath)
                    # # delta_table_lot.alias("old").merge(
                    # #     DF_count_drop_month.alias("new"),
                    # #     'old.patient_id = new.patient_id'
                    # # ).whenNotMatchedInsertAll().execute()

                    # DF_count_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_lot")

                    # # If you need to read the updated data
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   
                    
                    #Reading cumulative data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop = DF_Age_drop.dropDuplicates()
                    DF_count_drop = DF_count_drop.dropDuplicates()
                    
                    #Declaring required aggregated columns
                    Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
                    group_by_Cols.remove('hh_number')
                
                    #Getting Group By Column
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
                    count_group_by_Cols.remove('hh_number')
                    columns = ','.join(str(c) for c in group_by_Cols)
                    DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                    DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
                
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')

                    #Getting Minimum HHFact aginst required breaks
                    DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
                    DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
                    DF_Claims_Group_total = DF_Claims_Group
                    DF_total_source_claims = DF_Claims_Group
                    #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
                    DF_Claims_Group = DF_Claims_Group.drop('patient_id')
                    DF_Claims_Group.createOrReplaceTempView('Claims_Group')
                    
                    DF_Claims_CE_cumulative = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
                    
                    count_eligible_df_cumulative = DF_Claims_CE_cumulative
                    
                    #Calculating Diagnosis Eligible Calculating current_month data
                    DF_Diag_cumulative = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_total_source_diagnosis = DF_Age_drop
                    diagnosis_eligible_df_cumulative = DF_Diag_cumulative

                    #std report household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()
                    
                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE') 

                    #Declaring required aggregated columns
                    Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                    group_by_Cols.remove('hh_number')
                
                    #Getting Group By Column
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                    count_group_by_Cols.remove('hh_number')
                    columns = ','.join(str(c) for c in group_by_Cols)
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                
                    #Getting Minimum HHFact aginst required breaks
                    DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
                    DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
                    DF_Claims_Group_total = DF_Claims_Group
                    DF_total_source_claims_std = DF_Claims_Group
                    #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
                    DF_Claims_Group = DF_Claims_Group.drop('patient_id')
                    DF_Claims_Group.createOrReplaceTempView('Claims_Group')    
                    
                    DF_Claims_CE = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
                    
                    count_eligible_df_std = DF_Claims_CE
                        
                    print("diag columns",DF_Age_drop.columns)
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    diagnosis_eligible_df_std = DF_Diag
                    DF_total_source_diagnosis_std =  DF_Age_drop_std
                    
                    #Read the newly updated months data
                    
                    count_eligible_df = count_eligible_df_cumulative
                    count_eligible_df= count_eligible_df.dropDuplicates()
                    
                    diagnosis_eligible_df = diagnosis_eligible_df_cumulative
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates()
                    
                    #Count eligible
                    DF_total_count_eligible = count_eligible_break_household(DF_total_source_claims)
                   
                    #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)

                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_household(DF_total_source_claims_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)

                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)
                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')
                    
                    print("[+] Joined the both data frames")
            
                    Cumulative_DF = Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    count_eligible_df_std   = count_eligible_df_std.dropDuplicates()
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')

                    print("[+] Joined the both data frames standrad")

                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()
                    
                    print(f"[+] Building Filepath: s3://{bucketName}/silver/{studyID}/{deliverableId}")
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break/'

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break_std/'
                    
                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)

            except Exception as e:
                print("Except block is executing")
                logger.error(f"Exception occurred: {str(e)}")
                logger.error(f"Stack Trace: {traceback.format_exc()}")
                
                if isHouseholdRequired == 'true':
                    # Raises an exception if the path already exists
                    if delta_table_exists(Age_cumulative_filepath) or delta_table_exists(count_cumulative_filepath):
                        raise Exception("One or more tables already exist,insert is forbidden in the except statement after delta table creation.")  
                    
                    # DF_Age_drop.write.format('delta').mode("overwrite").save(Age_cumulative_filepath)
                    # DF_count_drop.write.format('delta').mode("overwrite").save(count_cumulative_filepath)

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   

                    #std report household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()

                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE')    
                
                    #Declaring required aggregated columns
                    Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                    group_by_Cols.remove('hh_number')
                
                    #Getting Group By Column
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                    count_group_by_Cols.remove('hh_number')
                    columns = ','.join(str(c) for c in group_by_Cols)
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                
                    #Getting Minimum HHFact aginst required breaks
                    DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
                    DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
                    DF_Claims_Group_total = DF_Claims_Group
                    DF_total_source_claims_std = DF_Claims_Group
                    #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
                    DF_Claims_Group = DF_Claims_Group.drop('patient_id')
                    DF_Claims_Group.createOrReplaceTempView('Claims_Group')    
                    
                    DF_Claims_CE = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
                    
                    count_eligible_df_std = DF_Claims_CE
                        
                    print("diag columns",DF_Age_drop.columns)
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    diagnosis_eligible_df_std = DF_Diag
                    DF_total_source_diagnosis_std =  DF_Age_drop_std
                    
                    #Count eligible

                    DF_total_count_eligible = count_eligible_break_household(DF_total_source_claims)
                    
                    print("DF_total_count_eligible columns",DF_total_count_eligible.columns)
                    
                    if 'hh_fact' in DF_total_count_eligible.columns:
                        DF_total_count_eligible =  DF_total_count_eligible.drop('hh_fact')
                    if 'HHFact' in DF_total_count_eligible.columns:
                        DF_total_count_eligible =  DF_total_count_eligible.drop('HHFact')
                    print("count columns",DF_total_count_eligible.columns)

                     #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)
                    
                    print("DF_total_diagnosis columns",DF_total_diagnosis.columns)
                    
                    if 'hh_fact' in count_eligible_df.columns:
                        count_eligible_df =  count_eligible_df.drop('hh_fact')
                    if 'HHFact' in count_eligible_df.columns:
                        count_eligible_df =  count_eligible_df.drop('HHFact')
                    print("count columns",count_eligible_df.columns)

                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    
                    print("count_eligible_df columns",count_eligible_df.columns)
                    
                    if 'hh_fact' in diagnosis_eligible_df.columns:
                        diagnosis_eligible_df =  diagnosis_eligible_df.drop('hh_fact')
                    if 'HHFact' in diagnosis_eligible_df.columns:
                        diagnosis_eligible_df =  diagnosis_eligible_df.drop('HHFact')
                    print("count columns",diagnosis_eligible_df.columns)
                    
                    if 'hh_fact' in DF_total_diagnosis.columns:
                        DF_total_diagnosis =  DF_total_diagnosis.drop('hh_fact')
                    if 'HHFact' in DF_total_diagnosis.columns:
                        DF_total_diagnosis =  DF_total_diagnosis.drop('HHFact')
                    print("count columns",DF_total_diagnosis.columns)
                    
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 
                    
                    print("diagnosis_eligible_df columns",diagnosis_eligible_df.columns)

                       
                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_household(DF_total_source_claims_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)
        
                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)
                    
                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                          
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')

                    count_eligible_df_std   = count_eligible_df_std.dropDuplicates()
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')

                    print("[+] Joined the both data frames hippa")
            
                    Cumulative_DF = Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    print("[+] Joined the both data frames standrad")
            
                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()
                    
                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break/'
                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break_std/'
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)
                    
                    print("[+] The cumulative report for the 1st month of the Standard household report has been completed.")
                    
        if isRerunRequired == 'true':
            try:
                if isHouseholdRequired == 'true':
                    #Reading the previous data
                    print("Rerun block is executing")

                    print(f"[+] Checking Delta Table For Path: {Age_cumulative_filepath}")
                    delta_table_age = DeltaTable.forPath(spark, Age_cumulative_filepath)
                    
                    # #perfroming the given month deletion
                    # delta_table_age.delete(col("Time_Period") == reRunTimePeriod)
                    
                    # DF_Age_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_age")
                    
                    # # delta_table_age.alias("old").merge(
                    # #     DF_Age_drop_month.alias("new"),
                    # #     'old.patient_id = new.patient_id'
                    # # ).whenNotMatchedInsertAll().execute()
            
                    # # If you need to read the updated data
                    # print(f"[+] Reading Delta Table For Path: {Age_cumulative_filepath}")
                    # DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    # #DF_Age = spark.read.parquet(Age_cumulative_filepath)
            
                    # # For Lot data
                    # delta_table_lot = DeltaTable.forPath(spark, count_cumulative_filepath)
                    
                    # #perfroming the given month deletion
                    # delta_table_lot.delete(col("Time_Period") == reRunTimePeriod)
                    
                    # DF_count_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_lot")
                    
                    # # delta_table_lot.alias("old").merge(
                    # #     DF_count_drop_month.alias("new"),
                    # #     'old.patient_id = new.patient_id'
                    # # ).whenNotMatchedInsertAll().execute()
            
                    # # If you need to read the updated data
                    # DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   
                    
                    #Reading cumulative data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop = DF_Age_drop.dropDuplicates()
                    DF_count_drop = DF_count_drop.dropDuplicates()
                    
                    #Declaring required aggregated columns
                    Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
                    group_by_Cols.remove('hh_number')
                
                    #Getting Group By Column
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
                    count_group_by_Cols.remove('hh_number')
                    columns = ','.join(str(c) for c in group_by_Cols)
                    DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                    DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
                
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')

                    #Getting Minimum HHFact aginst required breaks
                    DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
                    DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
                    DF_Claims_Group_total = DF_Claims_Group
                    DF_total_source_claims = DF_Claims_Group
                    #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
                    DF_Claims_Group = DF_Claims_Group.drop('patient_id')
                    DF_Claims_Group.createOrReplaceTempView('Claims_Group')
                    
                    DF_Claims_CE_cumulative = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
                    
                    count_eligible_df_cumulative = DF_Claims_CE_cumulative
                    
                    #Calculating Diagnosis Eligible Calculating current_month data
                    DF_Diag_cumulative = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_total_source_diagnosis = DF_Age_drop
                    diagnosis_eligible_df_cumulative = DF_Diag_cumulative

                    #std report household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()
                    
                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE') 

                    #Declaring required aggregated columns
                    Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                    group_by_Cols.remove('hh_number')
                
                    #Getting Group By Column
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                    count_group_by_Cols.remove('hh_number')
                    columns = ','.join(str(c) for c in group_by_Cols)
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                
                    #Getting Minimum HHFact aginst required breaks
                    DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
                    DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
                    DF_Claims_Group_total = DF_Claims_Group
                    DF_total_source_claims_std = DF_Claims_Group
                    #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
                    DF_Claims_Group = DF_Claims_Group.drop('patient_id')
                    DF_Claims_Group.createOrReplaceTempView('Claims_Group')    
                    
                    DF_Claims_CE = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
                    
                    count_eligible_df_std = DF_Claims_CE
                        
                    print("diag columns",DF_Age_drop.columns)
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    diagnosis_eligible_df_std = DF_Diag
                    DF_total_source_diagnosis_std =  DF_Age_drop_std
                    
                    #Read the newly updated months data
                    
                    count_eligible_df = count_eligible_df_cumulative
                    count_eligible_df= count_eligible_df.dropDuplicates()
                    
                    diagnosis_eligible_df = diagnosis_eligible_df_cumulative
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates()
                    
                    #Count eligible

                    DF_total_count_eligible = count_eligible_break_household(DF_total_source_claims)
                    
                    #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)

                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_household(DF_total_source_claims_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)

                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)
                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')
                    
                    print("[+] Joined the both data frames")
            
                    Cumulative_DF = Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    count_eligible_df_std   = count_eligible_df_std.dropDuplicates()
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')

                    print("[+] Joined the both data frames standrad")

                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()
                    
                    print(f"[+] Building Filepath: s3://{bucketName}/silver/{studyID}/{deliverableId}")
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break/'

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/reporting_break_std/'
                    
                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)
                    
                    print("[+] Rerun process has been Succeeded for the Household Standard report")
                    
            except Exception as e:
                if isHouseholdRequired == 'true':
                    print(f"[-] Rerun process has been failed for the Household Standard report: {e}")

########################################################################################################################
####################################### Ethnicity Required #############################################################
########################################################################################################################

    if isEthnicityRequired == 'true':
        print("[+] Starting isEthnicityRequired IF Run")
        # Initialize the result DataFrame
        DF_CON_count = spark.createDataFrame([], DF_count.schema)
        DF_CON_diagnosis = spark.createDataFrame([], DF_diagnosis.schema)
    
        i=0
        for mar in marketDefinition:
            conditionName = mar["conditionName"]
            studyCondition=mar["studyCondition"]
            # contractStart = studyCondition["contractStart"]
            # contractStart = datetime(contractStart[0], contractStart[1], contractStart[2])
            # contractStart = datetime.strftime(contractStart,'%Y-%m-%d')
            # contractEnd = studyCondition["contractEnd"]
            # contractEnd = datetime(contractEnd[0], contractEnd[1], contractEnd[2])
            # contractEnd = datetime.strftime(contractEnd,'%Y-%m-%d')
            specialtyCode = studyCondition["specialtyCode"]
    
            # #Applying the filters
            # DF_Contract_Date = DF.filter(f'(Time between "{contractStart}" and "{contractEnd}")')
            DF_Contract_Date_diagnosis = DF_diagnosis
            
            DF_CON_2_diagnois = DF_Contract_Date_diagnosis.filter(col("diagnostic_indication")==conditionName)
            
            if i == 0:
                DF_CON_diagnosis = DF_CON_2_diagnois
            else: 
                DF_CON_diagnosis = DF_CON_diagnosis.unionByName(DF_CON_2_diagnois)
            i=+1
    
        #Creating the date_column
        DF_count.createOrReplaceTempView("Lot")
        DF_LOT = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Lot''')
        DF_LOT_P = DF_LOT.drop(DF_LOT.Time)
    
        DF_CON_diagnosis.createOrReplaceTempView("Con")
        DF_CON = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Con''')
        DF_CON_P = DF_CON.drop(DF_CON.Time)
    
        DF_LOT = None
        DF_CON = None
        
        if media_platforms and len(media_platforms) > 0:
            for media in media_platforms:
                Media_Value = media["mediaValue"]
                Media_Key = media["mediaKey"]
        
                DF_LOT_D = DF_LOT_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
                DF_CON_D = DF_CON_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
        
                DF_LOT = DF_LOT_D if DF_LOT is None else DF_LOT.unionByName(DF_LOT_D)
                DF_CON = DF_CON_D if DF_CON is None else DF_CON.unionByName(DF_CON_D)
                print("[+] Platform breaks has been applied")
        else:
            DF_LOT = DF_LOT_P
            DF_CON = DF_CON_P
            print("[+] Platform breaks is not applied")
            
    
        #Selecting the columns
        Col_con = DF_CON.columns
        count_col = DF_LOT.columns
        Drop_Columns = ["_c0","Viewable_Flag","_c2","_c3","_c4","_c5","Total_Impression","hh_number","hh_fact","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_practitioner_role_code","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","proc_group","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","Surg_practitioner_id","Surg_practitioner_role_code","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET9","BUCKET10","BUCKET8","Line_of_Therapy","CREATIVE_TYPE","PUBLISHER_ID","Time","mediaPlatform","publisher_name","PUBLISHER_NAME"]
    
        #Dropping unnessary Columns        
        Drop_Cols = []
        for cols in Col_con:
            if cols in Drop_Columns:
                Drop_Cols.append(cols)
    
        #Dropping unnessary Columns        
        Count_Drop_Cols = []
        for cols in count_col:
            if cols in Drop_Columns:
                Count_Drop_Cols.append(cols)
    
        DF_con_drop = DF_CON.drop(*Drop_Cols)
        DF_count_drop = DF_LOT.drop(*Count_Drop_Cols)

        select_columns_con = ['patient_id', 'shs_patient_id', 'Count_Eligible_Flag', 'patient_ethnicity', 'diagnostic_indication', 'ethniPublisher','Time_Period','patient_age_group']
        select_columns_count = ['patient_id', 'shs_patient_id', 'Count_Eligible_Flag', 'patient_ethnicity', 'ethniPublisher','Time_Period','patient_age_group']
        DF_con_drop = DF_con_drop.select(*select_columns_con)
        DF_count_drop = DF_count_drop.select(*select_columns_count) 

        #cumulative
        DF_con_drop_month = DF_con_drop
        DF_count_drop_month = DF_count_drop   
    
        #Declaring required aggregated columns
        Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
        Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        #Getting Group By Column for diagnosis_eligible
        group_by_Cols = [col for col in DF_con_drop.columns if col not in Agg_Columns]
    
        #Getting Group By Column for count_eligible
        count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
    
        # Calculating Count Eligible
        DF_Count_Elig_Total = DF_count_drop.groupBy(*count_group_by_Cols).agg(f.countDistinct('patient_id').alias('Count_eligible'))
        DF_total_source_ethnicity_count = DF_count_drop
        count_eligible_ethnicity_df = DF_Count_Elig_Total

        # Calculating Diagnosis Eligible
        DF_Diag_Total = DF_con_drop.groupBy(*group_by_Cols).agg(f.countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter(f.col('diagnostic_indication').isNotNull())
        DF_total_source_ethnicity_diagnosis = DF_con_drop 
        diagnosis_eligible_ethnicity_df = DF_Diag_Total

        # Finding common columns for join
        Join_Columns = list(set(count_eligible_ethnicity_df.columns) & set(diagnosis_eligible_ethnicity_df.columns))

        # Joining DataFrames
        Joined_DF = diagnosis_eligible_ethnicity_df.join(count_eligible_ethnicity_df, Join_Columns, 'inner')
        Joined_DF = Joined_DF.withColumnRenamed('patient_ethnicity','ETHNICITY')\
            .withColumnRenamed('diagnostic_indication','DIAGNOSIS')\
            .withColumnRenamed('ethniPublisher','PUBLISHER_NAME')\
            .withColumnRenamed('patient_age_group','AGE')
        
        Final_DF =  Joined_DF.dropDuplicates()
        
        print("[+] The filter condition, dataBreaks and breakCombination has been applied to the Ethnicity report")
        
        #Writing as Delta File
        Final_DF.write.format('delta').mode("overwrite").save(Ethnicity_output_filepath)
        
        Con_cumulative_filepath_ethnicity = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Con_Ethnicity/'
        Lot_cumulative_filepath_ethnicity = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Lot_Ethnicity/'

        Con_cumulative_filepath_ethnicity_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/prev_cumulative/DF_Con_Ethnicity/'
        Lot_cumulative_filepath_ethnicity_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/prev_cumulative/DF_Lot_Ethnicity/'
        
        if isRerunRequired !='true':
            if isEthnicityRequired == 'true':
                try:
                #if DeltaTable.isDeltaTable(spark,Con_cumulative_filepath_ethnicity)==True and DeltaTable.isDeltaTable(spark,Lot_cumulative_filepath_ethnicity)==True: 
                    #Reading the previous data
                    # delta_table_con = DeltaTable.forPath(spark, Con_cumulative_filepath_ethnicity)
                    # delta_table_con.alias("old").merge(
                    #     DF_CON.alias("new"),
                    #     'old.patient_id = new.patient_id'
                    # ).whenNotMatchedInsertAll().execute()
        
                    # # For Lot data
                    # delta_table_lot = DeltaTable.forPath(spark, Lot_cumulative_filepath_ethnicity)
                    # delta_table_lot.alias("old").merge(
                    #     DF_LOT.alias("new"),
                    #     'old.patient_id = new.patient_id'
                    # ).whenNotMatchedInsertAll().execute()

                    DF_con_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Con_cumulative_filepath_ethnicity)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Lot_cumulative_filepath_ethnicity)                      
        
                    #Reading the merged data present in the respective path
                    DF_con_drop = spark.read.format("delta").load(Con_cumulative_filepath_ethnicity)
                    DF_count_drop = spark.read.format("delta").load(Lot_cumulative_filepath_ethnicity)

                    DF_con_drop = DF_con_drop.dropDuplicates()
                    DF_count_drop = DF_count_drop.dropDuplicates()

                
                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
        
                    #Getting Group By Column for diagnosis_eligible
                    group_by_Cols= [col for col in DF_con_drop.columns if col not in Agg_Columns]
        
                    #Getting Group By Column for count_eligible
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
        
                    # Calculating Count Eligible
                    DF_Count_Elig_Total = DF_count_drop.groupBy(*count_group_by_Cols).agg(f.countDistinct('patient_id').alias('Count_eligible'))
                    DF_total_source_ethnicity_count_cumulative = DF_count_drop
                    count_eligible_ethnicity_cumulative_df = DF_Count_Elig_Total

                    # Calculating Diagnosis Eligible
                    DF_Diag_Total = DF_con_drop.groupBy(*group_by_Cols).agg(f.countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter(f.col('diagnostic_indication').isNotNull())
                    DF_total_source_ethnicity_diagnosis_cumulative = DF_con_drop
                    diagnosis_eligible_ethnicity_cumulative_df = DF_Diag_Total

                    #Count eligible
                    count_eligible_ethnicity_df = count_eligible_ethnicity_cumulative_df
                    DF_total_count_eligible = count_eligible_break_standard(DF_total_source_ethnicity_count_cumulative)
                    count_eligible_ethnicity_df = count_eligible_ethnicity_df.unionByName(DF_total_count_eligible)
                    count_eligible_ethnicity_df = count_eligible_ethnicity_df.dropDuplicates()

                    #diagnosis eligible
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_cumulative_df
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_ethnicity_diagnosis_cumulative)
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.unionByName(DF_total_diagnosis)
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.dropDuplicates()

                    #temp 
                    count_eligible_ethnicity_df.write.format('parquet').mode('overwrite').save(Lot_cumulative_filepath_ethnicity_temp)
                    diagnosis_eligible_ethnicity_df.write.format('parquet').mode('overwrite').save(Con_cumulative_filepath_ethnicity_temp)

                    count_eligible_ethnicity_df = spark.read.parquet(Lot_cumulative_filepath_ethnicity_temp)
                    diagnosis_eligible_ethnicity_df = spark.read.parquet(Con_cumulative_filepath_ethnicity_temp)

                    count_eligible_ethnicity_df = count_eligible_ethnicity_df.dropDuplicates()
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.dropDuplicates()

                    # Finding common columns for join
                    Join_Columns = list(set(count_eligible_ethnicity_df.columns) & set(diagnosis_eligible_ethnicity_df.columns))

                    # Joining DataFrames
                    Joined_DF = diagnosis_eligible_ethnicity_df.join(count_eligible_ethnicity_df, Join_Columns, 'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('patient_ethnicity','ETHNICITY')\
                        .withColumnRenamed('diagnostic_indication','DIAGNOSIS')\
                        .withColumnRenamed('ethniPublisher','PUBLISHER_NAME')\
                        .withColumnRenamed('patient_age_group','AGE')

                    Cumulative_ethnicity_df = Joined_DF.dropDuplicates() 
     
                    Ethnicity_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/Ethnicity/'

                    Cumulative_ethnicity_df.write.format('delta').mode("overwrite").save(Ethnicity_cumlative_output_filepath)
                    
                    print("[+] The cumulative report for the subsequent month of the Ethnicity report has been written and completed.")
        
                #except:
                except Exception as e:
                    logger.error(f"Exception occurred: {str(e)}")
                    logger.error(f"Stack Trace: {traceback.format_exc()}")
                #else:
        
                    # Raises an exception if the path already exists
                    if delta_table_exists(Con_cumulative_filepath_ethnicity) or delta_table_exists(Lot_cumulative_filepath_ethnicity):
                        raise Exception("One or more tables already exist,insert is forbidden in the except statement after delta table creation.")                

                    DF_con_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Con_cumulative_filepath_ethnicity)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Lot_cumulative_filepath_ethnicity)   

                    #Count eligible
                    DF_total_count_eligible = count_eligible_break_standard(DF_total_source_ethnicity_count)
                    count_eligible_ethnicity_df = count_eligible_ethnicity_df.unionByName(DF_total_count_eligible)

                    #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_ethnicity_diagnosis)
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.unionByName(DF_total_diagnosis)

                    count_eligible_ethnicity_df = count_eligible_ethnicity_df.dropDuplicates()
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.dropDuplicates()

                    # Finding common columns for join
                    Join_Columns = list(set(count_eligible_ethnicity_df.columns) & set(diagnosis_eligible_ethnicity_df.columns))

                    # Joining DataFrames
                    Joined_DF = diagnosis_eligible_ethnicity_df.join(count_eligible_ethnicity_df, Join_Columns, 'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('patient_ethnicity','ETHNICITY')\
                        .withColumnRenamed('diagnostic_indication','DIAGNOSIS')\
                        .withColumnRenamed('ethniPublisher','PUBLISHER_NAME')\
                        .withColumnRenamed('patient_age_group','AGE')

                    Cumulative_ethnicity_df = Joined_DF.dropDuplicates() 
        
                    #Writing as Delta File
                    Ethnicity_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/Ethnicity/'
                    Cumulative_ethnicity_df.write.format('delta').mode("overwrite").save(Ethnicity_cumlative_output_filepath)
                    print("[+] The cumulative report for the 1st month of the Ethnicity report has been completed.")
                            
        if isRerunRequired =='true':
                if isEthnicityRequired == 'true': 
                    try:
                    # #if DeltaTable.isDeltaTable(spark,Con_cumulative_filepath_ethnicity)==True and DeltaTable.isDeltaTable(spark,Lot_cumulative_filepath_ethnicity)==True: 
                    #     #Reading the previous data
                    #     delta_table_con = DeltaTable.forPath(spark, Con_cumulative_filepath_ethnicity)
                        
                    #     #perfroming the given month deletion
                    #     delta_table_con.delete(col("Time_Period") == reRunTimePeriod)
                        
                    #     delta_table_con.alias("old").merge(
                    #         DF_CON.alias("new"),
                    #         'old.patient_id = new.patient_id'
                    #     ).whenNotMatchedInsertAll().execute()
        
                    #     # For Lot data
                    #     delta_table_lot = DeltaTable.forPath(spark, Lot_cumulative_filepath_ethnicity)
                        
                    #     #perfroming the given month deletion
                    #     delta_table_lot.delete(col("Time_Period") == reRunTimePeriod)
                        
                    #     delta_table_lot.alias("old").merge(
                    #         DF_LOT.alias("new"),
                    #         'old.patient_id = new.patient_id'
                    #     ).whenNotMatchedInsertAll().execute()

                        DF_con_drop_month.write \
                            .partitionBy("Time_Period") \
                            .mode("overwrite") \
                            .option("partitionOverwriteMode", "dynamic") \
                            .format("delta") \
                            .save(Con_cumulative_filepath_ethnicity)
                        
                        DF_count_drop_month.write \
                            .partitionBy("Time_Period") \
                            .mode("overwrite") \
                            .option("partitionOverwriteMode", "dynamic") \
                            .format("delta") \
                            .save(Lot_cumulative_filepath_ethnicity)   
        
                        #Reading the merged data present in the respective path
                        DF_con_drop = spark.read.format("delta").load(Con_cumulative_filepath_ethnicity)
                        DF_count_drop = spark.read.format("delta").load(Lot_cumulative_filepath_ethnicity)

                        DF_con_drop = DF_con_drop.dropDuplicates()
                        DF_count_drop = DF_count_drop.dropDuplicates()
                
                        #Declaring required aggregated columns
                        Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                        Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
        
                        #Getting Group By Column for diagnosis_eligible
                        group_by_Cols = [col for col in DF_con_drop.columns if col not in Agg_Columns]
        
                        #Getting Group By Column for count_eligible
                        count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]

                        # Calculating Count Eligible
                        DF_Count_Elig_Total = DF_count_drop.groupBy(*count_group_by_Cols).agg(f.countDistinct('patient_id').alias('Count_eligible'))
                        DF_total_source_ethnicity_count_cumulative = DF_count_drop
                        count_eligible_ethnicity_cumulative_df = DF_Count_Elig_Total

                        # Calculating Diagnosis Eligible
                        DF_Diag_Total = DF_con_drop.groupBy(*group_by_Cols).agg(f.countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter(f.col('diagnostic_indication').isNotNull())
                        DF_total_source_ethnicity_diagnosis_cumulative = DF_con_drop
                        diagnosis_eligible_ethnicity_cumulative_df = DF_Diag_Total

                        #Count eligible
                        count_eligible_ethnicity_df = count_eligible_ethnicity_cumulative_df
                        DF_total_count_eligible = count_eligible_break_standard(DF_total_source_ethnicity_count_cumulative)
                        count_eligible_ethnicity_df = count_eligible_ethnicity_df.unionByName(DF_total_count_eligible)
                        count_eligible_ethnicity_df = count_eligible_ethnicity_df.dropDuplicates()

                        #diagnosis eligible
                        diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_cumulative_df
                        DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_ethnicity_diagnosis_cumulative)
                        diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.unionByName(DF_total_diagnosis)
                        diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.dropDuplicates()

                        #temp 
                        count_eligible_ethnicity_df.write.format('parquet').mode('overwrite').save(Lot_cumulative_filepath_ethnicity_temp)
                        diagnosis_eligible_ethnicity_df.write.format('parquet').mode('overwrite').save(Con_cumulative_filepath_ethnicity_temp)

                        count_eligible_ethnicity_df = spark.read.parquet(Lot_cumulative_filepath_ethnicity_temp)
                        diagnosis_eligible_ethnicity_df = spark.read.parquet(Con_cumulative_filepath_ethnicity_temp)

                        count_eligible_ethnicity_df = count_eligible_ethnicity_df.dropDuplicates()
                        diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.dropDuplicates()

                        # Finding common columns for join
                        Join_Columns = list(set(count_eligible_ethnicity_df.columns) & set(diagnosis_eligible_ethnicity_df.columns))

                        # Joining DataFrames
                        Joined_DF = diagnosis_eligible_ethnicity_df.join(count_eligible_ethnicity_df, Join_Columns, 'inner')
                        Joined_DF = Joined_DF.withColumnRenamed('patient_ethnicity','ETHNICITY')\
                            .withColumnRenamed('diagnostic_indication','DIAGNOSIS')\
                            .withColumnRenamed('ethniPublisher','PUBLISHER_NAME')\
                            .withColumnRenamed('patient_age_group','AGE')

                        Cumulative_ethnicity_df = Joined_DF.dropDuplicates()       
               
                        Ethnicity_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/Ethnicity/'
                        Cumulative_ethnicity_df.write.format('delta').mode("overwrite").save(Ethnicity_cumlative_output_filepath)
                        
                        print("[+] Rerun process has been Succeeded for the Ethnicity Report")
                        
                    except Exception as e:
                        print(f"[-]Rerun process has been failed: {e}")


########################################################################################################################
####################################### Specialty Required #############################################################
########################################################################################################################

    if isSpecialtyRequired == 'true':
        DF_CON = spark.createDataFrame([], DF_diagnosis.schema)
        DF_SSPEC = spark.createDataFrame([], DF_diagnosis.schema)
        DF_NSPEC = spark.createDataFrame([], DF_diagnosis.schema)
    
        i=0    
        for mar in marketDefinition:
            conditionName = mar["conditionName"]
            studyCondition = mar["studyCondition"]
            # contractStart = studyCondition["contractStart"]
            # contractStart = datetime(contractStart[0], contractStart[1], contractStart[2])
            # contractStart = datetime.strftime(contractStart,'%Y-%m-%d')
            # contractEnd = studyCondition["contractEnd"]
            # contractEnd = datetime(contractEnd[0], contractEnd[1], contractEnd[2])
            # contractEnd = datetime.strftime(contractEnd,'%Y-%m-%d')
            specialtyCode = studyCondition["specialtyCode"]
    
            #DF_Contract_Date = DF.filter(f'(Time between "{contractStart}" and "{contractEnd}")')
            DF_Contract_Date = DF_diagnosis
            DF_CON_2 = DF_Contract_Date.filter(col("diagnostic_indication")==conditionName)
    
            DF_SSPEC_2 = DF_CON_2.filter(DF_Contract_Date["Proc_practitioner_role_code"].isin(specialtyCode))
    
            DF_NSPEC_2 = DF_CON_2.filter(~DF_Contract_Date["Proc_practitioner_role_code"].isin(specialtyCode))
    
            if i == 0:
                DF_CON = DF_CON_2
                DF_SSPEC = DF_SSPEC_2
                DF_NSPEC = DF_NSPEC_2
    
            else: 
                DF_CON = DF_CON.unionByName(DF_CON_2)
                DF_SSPEC = DF_SSPEC.unionByName(DF_SSPEC_2)
                DF_NSPEC = DF_NSPEC.unionByName(DF_NSPEC_2)
    
            i=+1    
            
        DF_count.createOrReplaceTempView("Lot")
        DF_LOT = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Lot''')
        DF_LOT_P = DF_LOT.drop(DF_LOT.Time)
    
        DF_CON.createOrReplaceTempView("Con")
        DF_CON = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Con''')
        DF_CON_P = DF_CON.drop(DF_CON.Time)
    
        DF_SSPEC.createOrReplaceTempView("Sspec")
        DF_SSPEC = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Sspec''')
        DF_SSPEC_P = DF_SSPEC.drop(DF_SSPEC.Time)
    
        DF_NSPEC.createOrReplaceTempView("Nspec")
        DF_NSPEC = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Nspec''')
        DF_NSPEC_P = DF_NSPEC.drop(DF_NSPEC.Time)
    
        DF_LOT = None
        DF_CON = None
        DF_SSPEC = None
        DF_NSPEC = None
        
        if media_platforms and len(media_platforms) > 0:
            for media in media_platforms:
                Media_Value = media["mediaValue"]
                Media_Key = media["mediaKey"]
        
                DF_LOT_D = DF_LOT_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
                DF_CON_D = DF_CON_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
        
                DF_SSPEC_D = DF_SSPEC_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
                DF_NSPEC_D = DF_NSPEC_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
        
                
                DF_LOT = DF_LOT_D if DF_LOT is None else DF_LOT.unionByName(DF_LOT_D)
                DF_CON = DF_CON_D if DF_CON is None else DF_CON.unionByName(DF_CON_D)
                DF_SSPEC = DF_SSPEC_D if DF_SSPEC is None else DF_SSPEC.unionByName(DF_SSPEC_D)
                DF_NSPEC = DF_NSPEC_D if DF_NSPEC is None else DF_NSPEC.unionByName(DF_NSPEC_D)
                print("[+] Platform breaks has been applied")
        else:
            DF_LOT = DF_LOT_P
            DF_CON = DF_CON_P
            DF_SSPEC = DF_SSPEC_P
            DF_NSPEC = DF_NSPEC_P
            print("[+] Platform breaks is not applied")
            
        num_platforms = len(media_platforms)
        try:
            if num_platforms == 2:
                # Get distinct values
                distinct_values = DF_LOT.select('mediaPlatform').distinct()

                # Create a string concatenation of all distinct values
                concat_expr = f.concat_ws('+', *[f.collect_list('mediaPlatform')[i] for i in range(num_platforms)])
                concatenated_value = distinct_values.agg(concat_expr.alias('concatenated')).collect()[0]['concatenated']

                # Add new rows with concatenated value
                DF_LOT_break = DF_LOT.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_CON_break = DF_CON.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_SSPEC_break = DF_SSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_NSPEC_break = DF_NSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))

                # Union the new rows with the original DataFrames
                DF_LOT = DF_LOT.unionByName(DF_LOT_break)
                DF_CON = DF_CON.unionByName(DF_CON_break)
                DF_SSPEC = DF_SSPEC.unionByName(DF_SSPEC_break)
                DF_NSPEC = DF_NSPEC.unionByName(DF_NSPEC_break)

            if num_platforms >=3:
                distinct_values = DF_LOT.select("mediaPlatform").distinct().rdd.flatMap(lambda x: x).collect()
                values_to_check = {'Mobile','Desktop','Tablet'}
                is_present = values_to_check.issubset(distinct_values)
                
                if is_present:
                    concatenated_value = "TOTAL DIGITAL"
                    # Add new rows with "TOTAL DIGITAL" value
                    DF_LOT_break = DF_LOT.withColumn('mediaPlatform', f.lit(concatenated_value))
                    DF_CON_break = DF_CON.withColumn('mediaPlatform', f.lit(concatenated_value))
                    DF_SSPEC_break = DF_SSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))
                    DF_NSPEC_break = DF_NSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))

                    # Union the new rows with the original DataFrames
                    DF_LOT = DF_LOT.unionByName(DF_LOT_break)
                    DF_CON = DF_CON.unionByName(DF_CON_break)
                    DF_SSPEC = DF_SSPEC.unionByName(DF_SSPEC_break)
                    DF_NSPEC = DF_NSPEC.unionByName(DF_NSPEC_break)

            if num_platforms >3:
                concatenated_value = "TOTAL PLATFORM"
                # Add new rows with "TOTAL DIGITAL" value
                DF_LOT_break = DF_LOT.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_CON_break = DF_CON.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_SSPEC_break = DF_SSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_NSPEC_break = DF_NSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))

                # Union the new rows with the original DataFrames
                DF_LOT = DF_LOT.unionByName(DF_LOT_break)
                DF_CON = DF_CON.unionByName(DF_CON_break)
                DF_SSPEC = DF_SSPEC.unionByName(DF_SSPEC_break)
                DF_NSPEC = DF_NSPEC.unionByName(DF_NSPEC_break)

        except Exception as e:
            print(f"[-] No platform break applied. Error: {str(e)}")
            
        #Getting all the columns
        col_con = DF_CON.columns
        count_col = DF_LOT.columns
        col_sspec = DF_SSPEC.columns
        col_nspec = DF_NSPEC.columns    
    
        Drop_Columns = ["_c0","Viewable_Flag","_c2","_c3","_c4","_c5","Total_Impression","hh_number","hh_fact","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_ethnicity","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","Surg_practitioner_id","Surg_practitioner_role_code","ethniPublisher","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET8","BUCKET9","BUCKET10","Proc_practitioner_role_code","Time","Device"]
    
        #Dropping unnessary Columns
        Drop_Cols = [col for col in col_con if col in Drop_Columns]
        #Dropping unnessary Columns        
        Count_Drop_Cols = [col for col in count_col if col in Drop_Columns]
        #Dropping unnessary Columns        
        Sspec_Drop_Cols = [col for col in col_sspec if col in Drop_Columns]
        #Dropping unnessary Columns        
        Nspec_Drop_Cols = [col for col in col_nspec if col in Drop_Columns]

    
        DF_Con_drop = DF_CON.drop(*Drop_Cols)
        DF_count_drop = DF_LOT.drop(*Count_Drop_Cols)
        DF_sspec_drop = DF_SSPEC.drop(*Sspec_Drop_Cols)
        DF_nspec_drop = DF_NSPEC.drop(*Nspec_Drop_Cols)
    
        #Declaring required aggregated columns
    
        Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        Sspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        Nspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        #Getting Group By Column to get columns for count eligible
        count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
    
        #Getting Group By Column to get columns for diagnosis eligible
        group_by_Cols = [col for col in DF_con_drop.columns if col not in Agg_Columns]
    
        #Getting Group By Column to get columns for SSPEC
        count_group_by_Sspec= [col for col in DF_sspec_drop.columns if col not in Sspec_Columns]
    
        #Getting Group By Column to get columns for NSPEC
        count_group_by_Nspec = [col for col in DF_nspec_drop.columns if col not in Nspec_Columns]
    
        # Calculating Count Eligible
        DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
        
        DF_Count_Elig_total = DF_count_drop
        
        # Calculating Diagnosis Eligible
        DF_Diag = DF_Con_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
        
        DF_Diag_total = DF_Con_drop
        
        # Calculating SSPEC Eligible
        DF_Sspec = DF_sspec_drop.groupBy(*count_group_by_Sspec).agg(countDistinct('shs_patient_id').alias('SSPEC')).filter('diagnostic_indication is not null')
    
        DF_Sspec_total = DF_sspec_drop
    
        # Calculating NSPEC Eligible
        DF_Nspec = DF_nspec_drop.groupBy(*count_group_by_Nspec).agg(countDistinct('shs_patient_id').alias('NSPEC')).filter('diagnostic_indication is not null')
        
        DF_Nspec_total  = DF_nspec_drop
        
       
        # Creating the common columns betweeen the dataframe
        Join_key = list(set(DF_Count_Elig.columns) & set(DF_Diag.columns))
    
        Join_key1 = list(set(DF_Diag.columns) & set(DF_Sspec.columns))
    
        Join_key2 = list(set(DF_Diag.columns) & set(DF_Nspec.columns))
    
        #Joining all the created dataframes
        joined_df = (DF_Diag.join(DF_Count_Elig, Join_key, how = "inner")\
                        .join(DF_Sspec, Join_key1 , how = "inner")\
                        .join(DF_Nspec, Join_key2, how = "inner"))
                        
        column_list = joined_df.columns
        columns_to_drop = [ "Diagnosis_eligible", "Count_eligible"]
        column_list = [col for col in column_list if col not in columns_to_drop]
        group_list = []
        
        for i in range(1, len(column_list)):
            for j in range(len(column_list) - i + 1):
                group = column_list[j:j+i]
                
                if i == len(column_list) - 1 and 'Device' in group:
                    continue
                
                group_list.append(','.join(group))
                
        breakCombination  = group_list        
    
        total_df = None
        for pair in breakCombination:
            print(f"Applying break combination for Specialty report: {pair}")
            A = []
            columns = pair.split(',')
            for column_value in columns:
                column_value = column_value.replace(' ', '_')
                A.append(column_value)
            print(f"Columns to be set for Specialty report to 'Total': {A}")
    
            # Create a temporary DataFrame with 'Total' for the current combination
            temp_df = joined_df
            for Total_Col in A:
                temp_df = temp_df.withColumn(Total_Col, lit('Total'))
    
            # Union with the total_df
            if total_df is None:
                total_df = temp_df
            else:
                total_df = total_df.unionByName(temp_df)
            print(f"[+] Finished applying break combination for Specialty report: {pair}")
            print("[+] --------------------")
    
        # Union the total_df with the original Final_df
        result_df = joined_df.unionByName(total_df)
        print("[+] All break combinations applied for the Specialty report.")
    
        #Renaming the columns                
        result_df = result_df.withColumnRenamed('diagnostic_indication','Condition')\
                             .withColumnRenamed('mediaPlatform','Device')\
                             .withColumnRenamed('proc_group','Line_Of_Therapy')
        
        #Each break aggregation
        sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
        dynamic_columns = [col for col in result_df.columns if col not in sum_columns]
    
        result_df = result_df.groupBy(dynamic_columns).agg(
            f.sum("Count_eligible").alias("Count_eligible"),
            f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
            f.sum("SSPEC").alias("SSPEC"),
            f.sum("NSPEC").alias("NSPEC")
        )
        
        #Each break aggregation
        sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
        dynamic_columns = [col for col in result_df.columns if col not in sum_columns]
    
        result_df = result_df.groupBy(dynamic_columns).agg(
            f.sum("Count_eligible").alias("Count_eligible"),
            f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
            f.sum("SSPEC").alias("SSPEC"),
            f.sum("NSPEC").alias("NSPEC")
        )
    
        specialty_cumulative_df=result_df
        
        print("[+] The filter condition, dataBreaks and breakCombination has been applied to the Specialty report")
    
        #Writing as Delta File
        result_df.write.format('delta').mode("overwrite").save(Specialty_output_filepath)
    
        #Specialty 
        Con_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Con_Specialty/'
        Lot_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_Lot_Specialty/'
        SSPEC_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_SSPEC_Specialty/'
        NSPEC_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/DF_NSPEC_Specialty/'
    
        if isRerunRequired !='true':
            if isSpecialtyRequired == 'true':
                try:
                #if DeltaTable.isDeltaTable(spark,Con_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,Lot_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,SSPEC_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,NSPEC_cumulative_filepath_Specialty)==True:
                    delta_table_con = DeltaTable.forPath(spark, Con_cumulative_filepath_Specialty)
                    delta_table_con.alias("old").merge(
                        DF_CON.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    # For Lot data
                    delta_table_lot = DeltaTable.forPath(spark, Lot_cumulative_filepath_Specialty)
                    delta_table_lot.alias("old").merge(
                        DF_LOT.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    delta_table_sspec = DeltaTable.forPath(spark, SSPEC_cumulative_filepath_Specialty)
                    delta_table_sspec.alias("old").merge(
                        DF_SSPEC.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    # For Lot data
                    delta_table_nspec = DeltaTable.forPath(spark, NSPEC_cumulative_filepath_Specialty)
                    delta_table_nspec.alias("old").merge(
                        DF_NSPEC.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()        
        
                    #To read the updated data
                    DF_CON = spark.read.format("delta").load(Con_cumulative_filepath_Specialty)
                    DF_LOT = spark.read.format("delta").load(Lot_cumulative_filepath_Specialty)
                    DF_SSPEC = spark.read.format("delta").load(SSPEC_cumulative_filepath_Specialty)
                    DF_NSPEC = spark.read.format("delta").load(NSPEC_cumulative_filepath_Specialty)
                    
                    #Getting all the columns
                    col_con = DF_CON.columns
                    count_col = DF_LOT.columns
                    col_sspec = DF_SSPEC.columns
                    col_nspec = DF_NSPEC.columns
                
                    Drop_Columns = ["_c0","Viewable_Flag","_c2","_c3","_c4","_c5","Total_Impression","hh_number","hh_fact","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_ethnicity","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","ethniPublisher","Surg_practitioner_id","Surg_practitioner_role_code","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET9","BUCKET8","BUCKET10","Proc_practitioner_role_code","Time","Device"]
                
                    DF_Con_drop = column_dropper(DF_CON, Drop_Columns)
                    DF_count_drop = column_dropper(DF_LOT, Drop_Columns)
                    DF_sspec_drop = column_dropper(DF_SSPEC, Drop_Columns)
                    DF_nspec_drop = column_dropper(DF_NSPEC, Drop_Columns)
                
                    #Declaring required aggregated columns
                
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Sspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Nspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column to get columns for count eligible
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
                
                    #Getting Group By Column to get columns for diagnosis eligible
                    group_by_Cols= [col for col in DF_con_drop.columns if col not in Agg_Columns]
                
                    #Getting Group By Column to get columns for SSPEC
                    count_group_by_Sspec = [col for col in DF_sspec_drop.columns if col not in Sspec_Columns]
                    #Getting Group By Column to get columns for NSPEC
                    count_group_by_Nspec = [col for col in DF_nspec_drop.columns if col not in Nspec_Columns]
                
                    # Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    
                    DF_Count_Elig_total_cumulative = DF_count_drop
                    
                    # Calculating Diagnosis Eligible
                    DF_Diag = DF_Con_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                
                    DF_Diag_total_cumulative = DF_Con_drop
                
                    # Calculating SSPEC Eligible
                    DF_Sspec = DF_sspec_drop.groupBy(*count_group_by_Sspec).agg(countDistinct('shs_patient_id').alias('SSPEC')).filter('diagnostic_indication is not null')
                    
                    DF_Sspec_total_cumulative = DF_sspec_drop
                
                    # Calculating NSPEC Eligible
                    DF_Nspec = DF_nspec_drop.groupBy(*count_group_by_Nspec).agg(countDistinct('shs_patient_id').alias('NSPEC')).filter('diagnostic_indication is not null')
                   
                    DF_Nspe_total_cumulative = DF_nspec_drop
                    
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(DF_Count_Elig.columns) & set(DF_Diag.columns))
                
                    Join_key1 = list(set(DF_Diag.columns) & set(DF_Sspec.columns))
                
                    Join_key2 = list(set(DF_Diag.columns) & set(DF_Nspec.columns))
                
                    #Joining all the created dataframes
                    joined_df = (DF_Diag.join(DF_Count_Elig, Join_key, how = "inner")\
                                    .join(DF_Sspec, Join_key1 , how = "inner")\
                                    .join(DF_Nspec, Join_key2, how = "inner"))
                                    
                    column_list = joined_df.columns
                    columns_to_drop = [ "Diagnosis_eligible", "Count_eligible"]
                    column_list = [col for col in column_list if col not in columns_to_drop]
                    group_list = []
                    
                    for i in range(1, len(column_list)):
                        for j in range(len(column_list) - i + 1):
                            group = column_list[j:j+i]
                            
                            if i == len(column_list) - 1 and 'Device' in group:
                                continue
                            
                            group_list.append(','.join(group))
                            
                    breakCombination  = group_list
                
                    total_df = None
                    for pair in breakCombination:
                        A = []
                        columns = pair.split(',')
                        for column_value in columns:
                            column_value = column_value.replace(' ', '_')
                            A.append(column_value)
                
                        # Create a temporary DataFrame with 'Total' for the current combination
                        temp_df = joined_df
                        for Total_Col in A:
                            temp_df = temp_df.withColumn(Total_Col, lit('Total'))
                
                        # Union with the total_df
                        if total_df is None:
                            total_df = temp_df
                        else:
                            total_df = total_df.unionByName(temp_df)
                
                    # Union the total_df with the original Final_df
                    result_df = joined_df.unionByName(total_df)
                
                    #Renaming the columns                
                    result_df = result_df.withColumnRenamed('diagnostic_indication','Condition')\
                                         .withColumnRenamed('mediaPlatform','Device')\
                                         .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Each break aggregation
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    dynamic_columns = [col for col in result_df.columns if col not in sum_columns]
                
                    result_df = result_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
                    
                    #campaign to date by each break
                    
                    # break_df = get_total_rows(result_df, breakCombination)
                    break_df = result_df.withColumn("Time_Period",lit("Campaign to Date"))
                    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    dynamic_columns = [col for col in break_df.columns if col not in sum_columns]
                    
                    campaign_by_break = break_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
        
                    # aggregation total for the time_df
                    #Count eligible total calculation
                    DF_Count_Elig_total = DF_Count_Elig_total.drop('shs_patient_id','Count_Eligible_Flag')
                    Total_df_count = DF_Count_Elig_total.dropDuplicates(['patient_id'])
                    
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_count.columns if col not in sum_columns]
                    
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Count_eligible"))
                    
                    time_df = Total_df_count.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df = time_df.withColumnRenamed('diagnostic_indication','Condition')\
                                     .withColumnRenamed('mediaPlatform','Device')\
                                     .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Diagnosis total calculation
                    DF_Diag_total = DF_Diag_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_diag = DF_Diag_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_diag.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Diagnosis_eligible"))
                    
                    # Perform the groupBy and aggregation
                    time_df_diag = Total_df_diag.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_diag = time_df_diag.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #SSPEC total calculation
                    DF_Sspec_total = DF_Sspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Sspec = DF_Sspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Sspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("SSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Sspec = Total_df_Sspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Sspec = time_df_Sspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #NSPEC total calculation
                    DF_Nspec_total = DF_Nspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Nspec = DF_Nspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Nspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("NSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Nspec = Total_df_Nspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Nspec = time_df_Nspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(time_df.columns) & set(time_df_diag.columns))
                    
                    Join_key1 = list(set(time_df_diag.columns) & set(time_df_Sspec.columns))
                    
                    Join_key2 = list(set(time_df_diag.columns) & set(time_df_Nspec.columns))
                    
                    #Joining all the created dataframes
                    joined_df = (time_df_diag.join(time_df, Join_key, how = "inner")\
                                    .join(time_df_Sspec, Join_key1 , how = "inner")\
                                    .join(time_df_Nspec, Join_key2, how = "inner"))
                    
                    time_df = joined_df        
                    
                    # aggregation total for the cumlative_df
                    #Count eligible total calculation
                    DF_Count_Elig_total_cumulative = DF_Count_Elig_total_cumulative.drop('shs_patient_id','Count_Eligible_Flag')
                    Total_df_count_cumulative = DF_Count_Elig_total_cumulative.dropDuplicates(['patient_id'])
                    
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_count_cumulative.columns if col not in sum_columns]
                    
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Count_eligible"))
                    
                    time_df_cumulative = Total_df_count_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_cumulative = time_df_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                     .withColumnRenamed('mediaPlatform','Device')\
                                     .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Diagnosis total calculation
                    DF_Diag_total_cumulative = DF_Diag_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    
                    Total_df_diag_cumulative = DF_Diag_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_diag_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Diagnosis_eligible"))
                    
                    # Perform the groupBy and aggregation
                    time_df_diag_cumulative = Total_df_diag_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_diag_cumulative = time_df_diag_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #SSPEC total calculation
                    DF_Sspec_total_cumulative = DF_Sspec_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Sspec_cumulative = DF_Sspec_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Sspec_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("SSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Sspec_cumulative = Total_df_Sspec_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Sspec_cumulative = time_df_Sspec_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #NSPEC total calculation
                    DF_Nspe_total_cumulative = DF_Nspe_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Nspec_cumulative = DF_Nspe_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Nspec_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("NSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Nspec_cumulative = Total_df_Nspec_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Nspec_cumulative = time_df_Nspec_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(time_df_cumulative.columns) & set(time_df_diag_cumulative.columns))
                    
                    Join_key1 = list(set(time_df_diag_cumulative.columns) & set(time_df_Sspec_cumulative.columns))
                    
                    Join_key2 = list(set(time_df_diag_cumulative.columns) & set(time_df_Nspec_cumulative.columns))
                    
                    #Joining all the created dataframes
                    joined_df_cumulative = (time_df_diag_cumulative.join(time_df_cumulative, Join_key, how = "inner")\
                                    .join(time_df_Sspec_cumulative, Join_key1 , how = "inner")\
                                    .join(time_df_Nspec_cumulative, Join_key2, how = "inner"))
                    
                    campaign_df = joined_df_cumulative
                    
                    campaign_df = campaign_df.withColumn("Time_Period",lit("Campaign to Date"))
                    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    
                    dynamic_columns = [col for col in campaign_df.columns if col not in sum_columns]
                    campaign_df = campaign_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
        
                    campaign_by_break = campaign_by_break.unionByName(campaign_df)
                    
                    #current month aggregated values
                    current_month = specialty_cumulative_df.unionByName(time_df)
                    Specality_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/Specality/'
                    prev_month = spark.read.format("delta").load(Specality_cumlative_output_filepath)
        
                    #previous month aggregated values
                    prev_month = prev_month.filter(col("Time_Period") != "Campaign to Date")
                    prev_month = prev_month.dropDuplicates()
                    current_month = current_month.unionByName(campaign_by_break)
                    Final_agg = prev_month.unionByName(current_month)
                    
                    #Writing as Delta File
                    Final_agg.write.format('delta').mode("overwrite").save(Specality_cumlative_output_filepath)
                    
                    print("[+] The cumulative report for the subsequent month of the Specality report has been written and completed.")
        
                #except:
                except Exception as e:
                    logger.error(f"Exception occurred: {str(e)}")
                    logger.error(f"Stack Trace: {traceback.format_exc()}")
                #else:
                    
                    # Raises an exception if the path already exists
                    if delta_table_exists(Con_cumulative_filepath_Specialty) or delta_table_exists(Lot_cumulative_filepath_Specialty)or delta_table_exists(SSPEC_cumulative_filepath_Specialty)or delta_table_exists(NSPEC_cumulative_filepath_Specialty):
                        raise Exception("One or more tables already exist,insert is forbidden in the except statement after delta table creation.")  
                    
                    #Writing into the delta path
                    DF_CON.write.format('delta').mode("overwrite").save(Con_cumulative_filepath_Specialty)
                    DF_LOT.write.format('delta').mode("overwrite").save(Lot_cumulative_filepath_Specialty)
                    DF_SSPEC.write.format('delta').mode("overwrite").save(SSPEC_cumulative_filepath_Specialty)
                    DF_NSPEC.write.format('delta').mode("overwrite").save(NSPEC_cumulative_filepath_Specialty)
                    
                    # #campaign to date by each break
                    
                    # break_df = get_total_rows(specialty_cumulative_df, breakCombination)
                    break_df = specialty_cumulative_df.withColumn("Time_Period",lit("Campaign to Date"))
                    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    dynamic_columns = [col for col in break_df.columns if col not in sum_columns]
                    
                    campaign_by_break = break_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
                    
                    #time_df calculation
                    # aggregation total for the time_df
                    #Count eligible total calculation
                    DF_Count_Elig_total = DF_Count_Elig_total.drop('shs_patient_id','Count_Eligible_Flag')
                    Total_df_count = DF_Count_Elig_total.dropDuplicates(['patient_id'])
                    
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_count.columns if col not in sum_columns]
                    
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Count_eligible"))
                    
                    time_df = Total_df_count.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df = time_df.withColumnRenamed('diagnostic_indication','Condition')\
                                     .withColumnRenamed('mediaPlatform','Device')\
                                     .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Diagnosis total calculation
                    DF_Diag_total = DF_Diag_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_diag = DF_Diag_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_diag.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Diagnosis_eligible"))
                    
                    # Perform the groupBy and aggregation
                    time_df_diag = Total_df_diag.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_diag = time_df_diag.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #SSPEC total calculation
                    DF_Sspec_total = DF_Sspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Sspec = DF_Sspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Sspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("SSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Sspec = Total_df_Sspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Sspec = time_df_Sspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #NSPEC total calculation
                    DF_Nspec_total = DF_Nspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Nspec = DF_Nspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Nspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("NSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Nspec = Total_df_Nspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Nspec = time_df_Nspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(time_df.columns) & set(time_df_diag.columns))
                    
                    Join_key1 = list(set(time_df_diag.columns) & set(time_df_Sspec.columns))
                    
                    Join_key2 = list(set(time_df_diag.columns) & set(time_df_Nspec.columns))
                    
                    #Joining all the created dataframes
                    joined_df = (time_df_diag.join(time_df, Join_key, how = "inner")\
                                    .join(time_df_Sspec, Join_key1 , how = "inner")\
                                    .join(time_df_Nspec, Join_key2, how = "inner"))
                    
                    time_df = joined_df
                    campaign_df = time_df
                    campaign_df = campaign_df.withColumn("Time_Period",lit("Campaign to Date"))
                    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    
                    dynamic_columns = [col for col in campaign_df.columns if col not in sum_columns]
                    campaign_df = campaign_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
        
                    total_df=campaign_by_break.unionByName(time_df)
                    total_df = total_df.unionByName(campaign_df)
                    # Union the aggregated data with the total
                    specialty_cumulative_df = specialty_cumulative_df.unionByName(total_df)
                    
                    #Writing as Delta File
                    Specality_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/Specality/'
                    specialty_cumulative_df.write.format('delta').mode("overwrite").save(Specality_cumlative_output_filepath)
                    
                    print("[+] The cumulative report for the 1st month of the Specality report has been completed.")
        
        if isRerunRequired =='true':
            if isSpecialtyRequired == 'true':
                try:
                #if DeltaTable.isDeltaTable(spark,Con_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,Lot_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,SSPEC_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,NSPEC_cumulative_filepath_Specialty)==True:
                    delta_table_con = DeltaTable.forPath(spark, Con_cumulative_filepath_Specialty)
                    
                    #perfroming the given month deletion
                    delta_table_con.delete(col("Time_Period") == reRunTimePeriod)
                    
                    delta_table_con.alias("old").merge(
                        DF_CON.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    # For Lot data
                    delta_table_lot = DeltaTable.forPath(spark, Lot_cumulative_filepath_Specialty)
                    
                    #perfroming the given month deletion
                    delta_table_lot.delete(col("Time_Period") == reRunTimePeriod)
                    
                    delta_table_lot.alias("old").merge(
                        DF_LOT.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    delta_table_sspec = DeltaTable.forPath(spark, SSPEC_cumulative_filepath_Specialty)
                    
                    #perfroming the given month deletion
                    delta_table_sspec.delete(col("Time_Period") == reRunTimePeriod)
                    
                    
                    delta_table_sspec.alias("old").merge(
                        DF_SSPEC.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    # For Lot data
                    delta_table_nspec = DeltaTable.forPath(spark, NSPEC_cumulative_filepath_Specialty)
                    
                    #perfroming the given month deletion
                    delta_table_nspec.delete(col("Time_Period") == reRunTimePeriod)            
                    
                    delta_table_nspec.alias("old").merge(
                        DF_NSPEC.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()        
        
                    #To read the updated data
                    DF_CON = spark.read.format("delta").load(Con_cumulative_filepath_Specialty)
                    DF_LOT = spark.read.format("delta").load(Lot_cumulative_filepath_Specialty)
                    DF_SSPEC = spark.read.format("delta").load(SSPEC_cumulative_filepath_Specialty)
                    DF_NSPEC = spark.read.format("delta").load(NSPEC_cumulative_filepath_Specialty)
                    
                    #Getting all the columns
                    col_con = DF_CON.columns
                    count_col = DF_LOT.columns
                    col_sspec = DF_SSPEC.columns
                    col_nspec = DF_NSPEC.columns
                
                    Drop_Columns = ["_c0","Viewable_Flag","_c2","_c3","_c4","_c5","Total_Impression","hh_number","hh_fact","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_ethnicity","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","ethniPublisher","Surg_practitioner_id","Surg_practitioner_role_code","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET9","BUCKET8","BUCKET10","Proc_practitioner_role_code","Time","Device"]
                
                    DF_Con_drop = column_dropper(DF_CON, Drop_Columns)
                    DF_count_drop = column_dropper(DF_LOT, Drop_Columns)
                    DF_sspec_drop = column_dropper(DF_SSPEC, Drop_Columns)
                    DF_nspec_drop = column_dropper(DF_NSPEC, Drop_Columns)
                
                    #Declaring required aggregated columns
                
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Sspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Nspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column to get columns for count eligible
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
                
                    #Getting Group By Column to get columns for diagnosis eligible
                    group_by_Cols= [col for col in DF_con_drop.columns if col not in Agg_Columns]
                
                    #Getting Group By Column to get columns for SSPEC
                    count_group_by_Sspec = [col for col in DF_sspec_drop.columns if col not in Sspec_Columns]
                    #Getting Group By Column to get columns for NSPEC
                    count_group_by_Nspec = [col for col in DF_nspec_drop.columns if col not in Nspec_Columns]
                
                    # Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_Count_Elig_total_cumulative = DF_count_drop
                
                    # Calculating Diagnosis Eligible
                    DF_Diag = DF_Con_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_Diag_total_cumulative = DF_Con_drop
                
                    # Calculating SSPEC Eligible
                    DF_Sspec = DF_sspec_drop.groupBy(*count_group_by_Sspec).agg(countDistinct('shs_patient_id').alias('SSPEC')).filter('diagnostic_indication is not null')
                    DF_Sspec_total_cumulative = DF_sspec_drop
                
                    # Calculating NSPEC Eligible
                    DF_Nspec = DF_nspec_drop.groupBy(*count_group_by_Nspec).agg(countDistinct('shs_patient_id').alias('NSPEC')).filter('diagnostic_indication is not null')
                    DF_Nspe_total_cumulative = DF_nspec_drop
                    
                
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(DF_Count_Elig.columns) & set(DF_Diag.columns))
                
                    Join_key1 = list(set(DF_Diag.columns) & set(DF_Sspec.columns))
                
                    Join_key2 = list(set(DF_Diag.columns) & set(DF_Nspec.columns))
                
                    #Joining all the created dataframes
                    joined_df = (DF_Diag.join(DF_Count_Elig, Join_key, how = "inner")\
                                    .join(DF_Sspec, Join_key1 , how = "inner")\
                                    .join(DF_Nspec, Join_key2, how = "inner"))
                                    
                    column_list = joined_df.columns
                    columns_to_drop = ["Diagnosis_eligible", "Count_eligible"]
                    column_list = [col for col in column_list if col not in columns_to_drop]
                    group_list = []
                    
                    for i in range(1, len(column_list)):
                        for j in range(len(column_list) - i + 1):
                            group = column_list[j:j+i]
                            
                            if i == len(column_list) - 1 and 'Device' in group:
                                continue
                            
                            group_list.append(','.join(group))
                            
                    breakCombination  = group_list
                
                    total_df = None
                    for pair in breakCombination:
                        A = []
                        columns = pair.split(',')
                        for column_value in columns:
                            column_value = column_value.replace(' ', '_')
                            A.append(column_value)
                
                        # Create a temporary DataFrame with 'Total' for the current combination
                        temp_df = joined_df
                        for Total_Col in A:
                            temp_df = temp_df.withColumn(Total_Col, lit('Total'))
                
                        # Union with the total_df
                        if total_df is None:
                            total_df = temp_df
                        else:
                            total_df = total_df.unionByName(temp_df)
                
                    # Union the total_df with the original Final_df
                    result_df = joined_df.unionByName(total_df)
                
                    #Renaming the columns                
                    result_df = result_df.withColumnRenamed('diagnostic_indication','Condition')\
                                         .withColumnRenamed('mediaPlatform','Device')\
                                         .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Each break aggregation
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    dynamic_columns = [col for col in result_df.columns if col not in sum_columns]
                
                    result_df = result_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
                    
                    #campaign to date by each break
                    
                    # break_df = get_total_rows(result_df, breakCombination)
                    break_df = result_df.withColumn("Time_Period",lit("Campaign to Date"))
                    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    dynamic_columns = [col for col in break_df.columns if col not in sum_columns]
                    
                    campaign_by_break = break_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
        
                    # aggregation total for the time_df
                    #Count eligible total calculation
                    DF_Count_Elig_total = DF_Count_Elig_total.drop('shs_patient_id','Count_Eligible_Flag')
                    Total_df_count = DF_Count_Elig_total.dropDuplicates(['patient_id'])
                    
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_count.columns if col not in sum_columns]
                    
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Count_eligible"))
                    
                    time_df = Total_df_count.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df = time_df.withColumnRenamed('diagnostic_indication','Condition')\
                                     .withColumnRenamed('mediaPlatform','Device')\
                                     .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Diagnosis total calculation
                    DF_Diag_total = DF_Diag_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_diag = DF_Diag_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_diag.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Diagnosis_eligible"))
                    
                    # Perform the groupBy and aggregation
                    time_df_diag = Total_df_diag.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_diag = time_df_diag.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #SSPEC total calculation
                    DF_Sspec_total = DF_Sspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Sspec = DF_Sspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Sspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("SSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Sspec = Total_df_Sspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Sspec = time_df_Sspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #NSPEC total calculation
                    DF_Nspec_total = DF_Nspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Nspec = DF_Nspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Nspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("NSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Nspec = Total_df_Nspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Nspec = time_df_Nspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(time_df.columns) & set(time_df_diag.columns))
                    
                    Join_key1 = list(set(time_df_diag.columns) & set(time_df_Sspec.columns))
                    
                    Join_key2 = list(set(time_df_diag.columns) & set(time_df_Nspec.columns))
                    
                    #Joining all the created dataframes
                    joined_df = (time_df_diag.join(time_df, Join_key, how = "inner")\
                                    .join(time_df_Sspec, Join_key1 , how = "inner")\
                                    .join(time_df_Nspec, Join_key2, how = "inner"))
                    
                    time_df = joined_df        
                    
                    # aggregation total for the campaign_df
                    #Count eligible total calculation
                    DF_Count_Elig_total_cumulative = DF_Count_Elig_total_cumulative.drop('shs_patient_id','Count_Eligible_Flag')
                    Total_df_count_cumulative = DF_Count_Elig_total_cumulative.dropDuplicates(['patient_id'])
                    
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_count_cumulative.columns if col not in sum_columns]
                    
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Count_eligible"))
                    
                    time_df_cumulative = Total_df_count_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_cumulative = time_df_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                     .withColumnRenamed('mediaPlatform','Device')\
                                     .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Diagnosis total calculation
                    DF_Diag_total_cumulative = DF_Diag_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    
                    Total_df_diag_cumulative = DF_Diag_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_diag_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Diagnosis_eligible"))
                    
                    # Perform the groupBy and aggregation
                    time_df_diag_cumulative = Total_df_diag_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_diag_cumulative = time_df_diag_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #SSPEC total calculation
                    DF_Sspec_total_cumulative = DF_Sspec_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Sspec_cumulative = DF_Sspec_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Sspec_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("SSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Sspec_cumulative = Total_df_Sspec_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Sspec_cumulative = time_df_Sspec_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #NSPEC total calculation
                    DF_Nspe_total_cumulative = DF_Nspe_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Nspec_cumulative = DF_Nspe_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Nspec_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("NSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Nspec_cumulative = Total_df_Nspec_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Nspec_cumulative = time_df_Nspec_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(time_df_cumulative.columns) & set(time_df_diag_cumulative.columns))
                    
                    Join_key1 = list(set(time_df_diag_cumulative.columns) & set(time_df_Sspec_cumulative.columns))
                    
                    Join_key2 = list(set(time_df_diag_cumulative.columns) & set(time_df_Nspec_cumulative.columns))
                    
                    #Joining all the created dataframes
                    joined_df_cumulative = (time_df_diag_cumulative.join(time_df_cumulative, Join_key, how = "inner")\
                                    .join(time_df_Sspec_cumulative, Join_key1 , how = "inner")\
                                    .join(time_df_Nspec_cumulative, Join_key2, how = "inner"))
                    
                    campaign_df = joined_df_cumulative
                    
                    campaign_df = campaign_df.withColumn("Time_Period",lit("Campaign to Date"))
    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    
                    dynamic_columns = [col for col in campaign_df.columns if col not in sum_columns]
                    campaign_df = campaign_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
                    
                    campaign_by_break = campaign_by_break.unionByName(campaign_df)
                    
                    #current month aggregated values
                    current_month = specialty_cumulative_df.unionByName(time_df)
                    Specality_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/Specality/'
                    prev_month = spark.read.format("delta").load(Specality_cumlative_output_filepath)
        
                    #previous month aggregated values
                    prev_month = prev_month.filter((col("Time_Period") != "Campaign to Date") & (col("Time_Period") != reRunTimePeriod ))
                    
                    current_month = current_month.unionByName(campaign_by_break)
                    Final_agg = prev_month.unionByName(current_month)
                    
                    #Writing as Delta File
                    Final_agg.write.format('delta').mode("overwrite").save(Specality_cumlative_output_filepath)
                    
                    print("[+] Rerun process has been Succeeded for the Specality report")
                
                except Exception as e:
                    print(f"[-] Rerun process has been failed for the Specality report: {e}")

########################################################################################################################
########################################################################################################################
############################################ Quarterly #################################################################
########################################################################################################################
########################################################################################################################

if reportingPeriod == 'Quarterly':
    print("[+] Starting Quarterly Run")

    for con in Condition:
        conditionName = con["conditionName"]
        # contractStart = con["contractStart"]
        # contractStart = datetime(contractStart[0], contractStart[1], contractStart[2])
        # contractStart = datetime.strftime(contractStart, '%Y-%m-%d')
        # contractEnd = con["contractEnd"]
        # contractEnd = datetime(contractEnd[0], contractEnd[1], contractEnd[2])
        # contractEnd = datetime.strftime(contractEnd, '%Y-%m-%d') 
        DF_Contract_Date_count = DF_count
        DF_Contract_Date_diagnosis = DF_diagnosis
    
        DF_LOT_count = DF_Contract_Date_count.withColumn("Device", lit(None))
        DF_LOT_diagnosis = DF_Contract_Date_diagnosis.withColumn("Device", lit(None))
        
        if con["mediaPlatform"] is not None:
            for media in con["mediaPlatform"]:
                Media_Value = media["mediaValue"]
                Media_Key = media["mediaKey"]
    
                # Filter and add Device column for count dataframe
                DF_filtered_count = DF_Contract_Date_count.filter(col("mediaPlatform") == Media_Key) \
                    .withColumn("Device", lit(Media_Value))
    
                # Filter and add Device column for diagnosis dataframe
                DF_filtered_diagnosis = DF_Contract_Date_diagnosis.filter(col("mediaPlatform") == Media_Key) \
                    .withColumn("Device", lit(Media_Value))
                DF_LOT_count = DF_LOT_count.unionByName(DF_filtered_count)
                DF_LOT_diagnosis = DF_LOT_diagnosis.unionByName(DF_filtered_diagnosis)
        else:
            print("[+] handled null values")
            DF_LOT_count = DF_Contract_Date_count.withColumn("Device",col("mediaPlatform") )
            DF_LOT_diagnosis = DF_Contract_Date_diagnosis.withColumn("Device",col("mediaPlatform"))
    
    DF_LOT_count = DF_LOT_count.filter(col("Device").isNotNull())
    DF_LOT_diagnosis = DF_LOT_diagnosis.filter(col("Device").isNotNull())
    
    #distinct_platforms = list(set(media['mediaKey'] for media in Condition[0]['mediaPlatform']))
    
    try:
        if len(Condition[0]['mediaPlatform'])==2 :
            # Get distinct values
            # distinct_values = DF_LOT_count.select('device').distinct()
    
            # # Create a string concatenation of all distinct values
            # concat_expr = f.concat_ws('+', *[f.collect_list('device')[i] for i in range(distinct_count)])
    
            # concatenated_value = distinct_values.agg(concat_expr.alias('concatenated')).collect()[0]['concatenated']

            distinct_values = DF_LOT_count.select('device').distinct()

            # Collect the distinct values as a Python list
            distinct_list = distinct_values.rdd.flatMap(lambda x: x).collect()

            # Concatenate the list values with a '+' separator
            concatenated_value = '+'.join(distinct_list)
    
            # Add new column with concatenated value
            DF_LOT_count_break = DF_LOT_count.withColumn('device', f.lit(concatenated_value))
            DF_LOT_diagnosis_break = DF_LOT_diagnosis.withColumn('device', f.lit(concatenated_value))
            DF_LOT_count = DF_LOT_count.unionByName(DF_LOT_count_break)
            DF_LOT_diagnosis = DF_LOT_diagnosis.unionByName(DF_LOT_diagnosis_break)
    
            
        if len(Condition[0]['mediaPlatform'])>=3:
            # Get distinct values
            
            distinct_values = DF_LOT_count.select("device").distinct().rdd.flatMap(lambda x: x).collect()
    
            values_to_check = {'Mobile','Desktop','Tablet'}
            is_present = values_to_check.issubset(distinct_values)
            
            if is_present:
                # Create a string concatenation of all distinct values
                #concat_expr = f.concat_ws('+', *[f.collect_list('device')[i] for i in range(distinct_count)])
                #concatenated_value = distinct_values.agg(concat_expr.alias('concatenated')).collect()[0]['concatenated']
    
                concatenated_value = "TOTAL DIGITAL"
                # Add new column with concatenated value
                DF_LOT_count_break = DF_LOT_count.filter(col('device').isin(values_to_check))
                DF_LOT_diagnosis_break = DF_LOT_diagnosis.filter(col('device').isin(values_to_check))

                DF_LOT_count_break = DF_LOT_count_break.withColumn('device', f.lit(concatenated_value))
                DF_LOT_diagnosis_break = DF_LOT_diagnosis_break.withColumn('device', f.lit(concatenated_value))
                DF_LOT_count = DF_LOT_count.unionByName(DF_LOT_count_break)
                DF_LOT_diagnosis = DF_LOT_diagnosis.unionByName(DF_LOT_diagnosis_break)  
            
        if len(Condition[0]['mediaPlatform'])>3:
            
            # Get distinct values
            distinct_values = DF_LOT_count.select('device').distinct()
    
            # Create a string concatenation of all distinct values
            #concat_expr = f.concat_ws('+', *[f.collect_list('device')[i] for i in range(distinct_count)])
            #concatenated_value = distinct_values.agg(concat_expr.alias('concatenated')).collect()[0]['concatenated']
    
            concatenated_value = "TOTAL PLATFORM"
            # Add new column with concatenated value
            DF_LOT_count_break = DF_LOT_count.withColumn('device', f.lit(concatenated_value))
            DF_LOT_diagnosis_break = DF_LOT_diagnosis.withColumn('device', f.lit(concatenated_value))
            DF_LOT_count = DF_LOT_count.unionByName(DF_LOT_count_break)
            DF_LOT_diagnosis = DF_LOT_diagnosis.unionByName(DF_LOT_diagnosis_break)  
    
    except Exception as e:
        print(f"No platform break applied. Error")
        
    print("[+] Applying dataBreaks calculation")
    current_date = datetime.now().date()
    year = current_date.year
    
    DF_LOT = DF_LOT_count
    DF_Age = DF_LOT_diagnosis
    
    for sym_break in dataBreaks:
        break_name = sym_break["breakName"]
        filter_condition = sym_break["filter"]
        distinct_value = sym_break["distinctValue"]
    
        if filter_condition == 'Equal To':
            con = '='
        elif filter_condition == 'Not Equal To':
            con = '!='
        elif filter_condition == 'Greater Than':
            con = '>='
        elif filter_condition == 'Less Than':
            con = '<='
    
        if break_name == 'AGE':
            DF_Age = DF_Age.filter('patient_age_group is not null')
            DF_LOT = DF_LOT.filter('patient_age_group is not null')
            DF_Age = DF_Age.withColumnRenamed("patient_age_group" , "AGE")
            DF_LOT = DF_LOT.withColumnRenamed("patient_age_group" , "AGE")
            
            # DF_Age = DF_Age.withColumn("AGE", year - f.col("patient_birth_year"))
            # DF_LOT = DF_LOT.withColumn("AGE", year - f.col("patient_birth_year"))
            # if distinct_value is None:
            #     print("[+] Warning: No distinct value provided for AGE filter")
            #     continue
            # try:
            #     YearOfBirth = year - int(distinct_value)
            #     DF_Age = DF_Age.filter(f'patient_birth_year {con} {YearOfBirth}')
            #     DF_LOT = DF_LOT.filter(f'patient_birth_year {con} {YearOfBirth}')
            # except ValueError:
            #     print(f"[-] Warning: Invalid distinct value '{distinct_value}' for AGE filter")
            #     continue
            print("[+] Apply the age breaks")
    
        elif break_name == 'ETHNICITY':
            DF_Age = DF_Age.filter('patient_ethnicity is not null')
            DF_LOT = DF_LOT.filter('patient_ethnicity is not null') 
            DF_Age = DF_Age.withColumn("ETHNICITY",f.col("patient_ethnicity"))
            DF_LOT = DF_LOT.withColumn("ETHNICITY",f.col("patient_ethnicity"))
            if con == '=':
                DF_Age = DF_Age.filter(f"patient_ethnicity == '{distinct_value}'")
                DF_LOT = DF_LOT.filter(f"patient_ethnicity == '{distinct_value}'")
            elif con == '!=':
                DF_Age = DF_Age.filter(f"patient_ethnicity != '{distinct_value}'")
                DF_LOT = DF_LOT.filter(f"patient_ethnicity != '{distinct_value}'")
            else:
                print(f"[-] Warning: '{filter_condition}' filter may not be applicable for ETHNICITY")
    
        elif break_name == 'RACE':
            DF_Age = DF_Age.filter('patient_ethnicity is not null')
            DF_LOT = DF_LOT.filter('patient_ethnicity is not null')     
            DF_Age = DF_Age.withColumn("RACE",f.col("patient_ethnicity"))
            DF_LOT = DF_LOT.withColumn("RACE",f.col("patient_ethnicity"))
            if con == '=':
                DF_Age = DF_Age.filter(f"patient_ethnicity == '{distinct_value}'")
                DF_LOT = DF_LOT.filter(f"patient_ethnicity == '{distinct_value}'")
            elif con == '!=':
                DF_Age = DF_Age.filter(f"patient_ethnicity != '{distinct_value}'")
                DF_LOT = DF_LOT.filter(f"patient_ethnicity != '{distinct_value}'")
            else:
                print(f"[-] Warning: '{filter_condition}' filter may not be applicable for ETHNICITY")
    
        elif break_name == 'LOT':
            DF_Age = DF_Age.filter('proc_group is not null')
            DF_Age = DF_Age.withColumn("Line_of_Therapy",f.col("proc_group"))
            #DF_LOT = DF_LOT.withColumn("Line_of_Therapy",f.col("proc_group")) 
            if con == '=':
                DF_Age = DF_Age.filter(f"proc_group == '{distinct_value}'")
                #DF_LOT = DF_LOT.filter(f"proc_group == '{distinct_value}'")
            elif con == '!=':
                DF_Age = DF_Age.filter(f"proc_group != '{distinct_value}'")
                #DF_LOT = DF_LOT.filter(f"proc_group != '{distinct_value}'")
            else:
                print(f"[-] Warning: '{filter_condition}' filter may not be applicable for SPECIALTY")
    
        else:
            print(f"[-] Warning: Unknown break name '{break_name}'")
    
    DF_Age.createOrReplaceTempView("Age")
    DF_Age = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Age''')
    DF_Age = DF_Age.drop(DF_Age.Time)
    
    DF_LOT.createOrReplaceTempView("Lot")
    DF_LOT = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Lot''')
    DF_LOT = DF_LOT.drop(DF_LOT.Time)
    
    print("[+] The filter condition, dataBreaks has been calculated for the standard report or the household report calculation")

########################################################################################################################
####################################### Household NOT Required #########################################################
########################################################################################################################

    if isHouseholdRequired != 'true':
        print("[+] Starting Household NOT Required Run")

        col_age = DF_Age.columns
        count_col = DF_LOT.columns
    
        Drop_Columns = ["_c0","Viewable_Flag","_c2","_c3","_c4","_c5","Total_Impression","hh_number","hh_fact","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_ethnicity","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_practitioner_role_code","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","proc_group","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","ethniPublisher","Surg_practitioner_id","Surg_practitioner_role_code","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET8","BUCKET9","BUCKET10","Time","mediaPlatform","patient_age_group"]
    
        #Dropping unnessary Columns
        Drop_Cols = []
        for cols in col_age:
            if cols in Drop_Columns:
                Drop_Cols.append(cols)
    
        #Dropping unnessary Columns        
        Count_Drop_Cols = []
        for cols in count_col:
            if cols in Drop_Columns:
                Count_Drop_Cols.append(cols)

    
    
        DF_Age_drop = DF_Age.drop(*Drop_Cols)
        DF_count_drop = DF_LOT.drop(*Count_Drop_Cols)

        DF_Age_drop = DF_Age_drop.dropDuplicates()
        DF_count_drop = DF_count_drop.dropDuplicates()

        #cumulative
        DF_Age_drop_month = DF_Age_drop
        DF_count_drop_month = DF_count_drop

        DF_Age_drop = quarterly_conversion(DF_Age_drop)
        DF_count_drop = quarterly_conversion(DF_count_drop)
        
        #Declaring required aggregated columns
        Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
        Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        #Getting Group By Column
        group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
    
        count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
    
        #Calculating Count Eligible
        DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
        DF_total_source_count = DF_count_drop
        count_eligible_df = DF_Count_Elig
        
        DF_count_drop_month = DF_count_drop
        
        #Calculating Diagnosis Eligible
        DF_Diag = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
        DF_total_source_diagnosis = DF_Age_drop 
        diagnosis_eligible_df = DF_Diag
        
        DF_Age_drop_month = DF_Age_drop
        
        DF_Count_Elig.write.format('delta').mode("overwrite").save(output_filepath)
        
        print("[+] The filter condition, dataBreaks and breakCombination has been applied to the Standard report")

        #Standard report 
        Age_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/quarterly_cumulative/DF_Age_drop/' 
        count_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/quarterly_cumulative/DF_count_drop/' 

        Age_cumulative_filepath_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/quarterly_cumulative/DF_Age_drop/'
        count_cumulative_filepath_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/quarterly_cumulative/DF_count_drop/'

        count_cumulative_filepath_temp_std = f's3://{bucketName}/temp/{studyID}/{deliverableId}/quarterly_standard_cumulative/DF_count_drop/'
        Age_cumulative_filepath_temp_std = f's3://{bucketName}/temp/{studyID}/{deliverableId}/quarterly_standard_cumulative/DF_Age_drop/'
       
        if isRerunRequired != 'true':
            try:
                if isHouseholdRequired != 'true':
                    # For Age data
                #     delta_table_age = DeltaTable.forPath(spark, Age_cumulative_filepath)
                    
                #     DF_Age_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_age")

                #    # If you need to read the updated data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    
                #     # delta_table_age.alias("old").merge(
                #     #     DF_Age_drop_month.alias("new"),
                #     #     'old.patient_id = new.patient_id'
                #     # ).whenNotMatchedInsertAll().execute()
        

                #     # For Lot data
                #     delta_table_lot = DeltaTable.forPath(spark, count_cumulative_filepath)
                #     # delta_table_lot.alias("old").merge(
                #     #     DF_count_drop_month.alias("new"),
                #     #     'old.patient_id = new.patient_id'
                #     # ).whenNotMatchedInsertAll().execute()
        
                    
                #     DF_count_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_age")

                #     # If you need to read the updated data
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   
                    
                    #Reading cumulative data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop = DF_Age_drop.dropDuplicates()
                    DF_count_drop = DF_count_drop.dropDuplicates()

                    DF_Age_drop = quarterly_conversion(DF_Age_drop)
                    DF_count_drop = quarterly_conversion(DF_count_drop)
                    
                    #std report without household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()

                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE')   

                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                
                    #Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop_std.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_total_source_count_std = DF_count_drop_std
                    count_eligible_df_std = DF_Count_Elig
                    
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_total_source_diagnosis_std = DF_Age_drop_std
                    diagnosis_eligible_df_std = DF_Diag

                    #Count eligible hippa

                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
        
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
        
                    #Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_Count_Elig_total_cumulative = DF_count_drop

                    DF_total_source_count = DF_Count_Elig_total_cumulative
                    count_eligible_df  = DF_Count_Elig
        
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_Diag_total_cumulative = DF_Age_drop
                    
                    DF_total_source_diagnosis = DF_Diag_total_cumulative                
                    diagnosis_eligible_df = DF_Diag

                    #Count eligible

                    DF_total_count_eligible = count_eligible_break_standard(DF_total_source_count)
                    
                    #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)
                    
                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_standard(DF_total_source_count_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)

                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')
 
                    Cumulative_DF = Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()
                    
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break/'

                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break_std/'
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)
                                        
                    print("[+] The cumulative report for the subsequent month of the Standard report has been completed.")
        
            #except:
            except Exception as e:
                logger.error(f"Exception occurred: {str(e)}")
                logger.error(f"Stack Trace: {traceback.format_exc()}")
                
                if isHouseholdRequired != 'true':        
                    # Raises an exception if the path already exists
                    if delta_table_exists(Age_cumulative_filepath) or delta_table_exists(count_cumulative_filepath):
                        raise Exception("One or more tables already exist,insert is forbidden in the except statement after delta table creation.")  
        
                    # DF_Age_drop_month.write.format('delta').mode("overwrite").save(Age_cumulative_filepath)
                    # DF_count_drop_month.write.format('delta').mode("overwrite").save(count_cumulative_filepath)

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)

                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   

                   #std report without household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()

                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE')   

                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                
                    #Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop_std.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_total_source_count_std = DF_count_drop_std
                    count_eligible_df_std = DF_Count_Elig
                    
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_total_source_diagnosis_std = DF_Age_drop_std
                    diagnosis_eligible_df_std = DF_Diag

                    #Count eligible

                    DF_total_count_eligible = count_eligible_break_standard(DF_total_source_count)
                    
                    #diagnosis eligible

                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)
                     
                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_standard(DF_total_source_count_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()


                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)

                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                    
                
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')

                    count_eligible_df_std   = count_eligible_df_std.dropDuplicates()
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')
                                
                    print("[+] Joined the both data frames")
                    Cumulative_DF =  Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break/'
                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break_std/'
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)
                    
                    print("[+] The cumulative report for the 1st month of the Standard report has been completed.")
        
        if isRerunRequired == 'true':
            try:
                if isHouseholdRequired != 'true':
                    # For Age data
                    # delta_table_age = DeltaTable.forPath(spark, Age_cumulative_filepath)
                    
                    # #perfroming the given month deletion
                    # delta_table_age.delete(col("Time_Period") == reRunTimePeriod)
                    
                    # DF_Age_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_age")
        
                    # # If you need to read the updated data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    
                    # #DF_Age = spark.read.parquet(Age_cumulative_filepath)
                    # DF_Age_drop = DF_Age_drop.dropDuplicates()
        
                    # # For Lot data
                    # delta_table_lot = DeltaTable.forPath(spark, count_cumulative_filepath)
                    
                    # #perfroming the given month deletion
                    # delta_table_lot.delete(col("Time_Period") == reRunTimePeriod)
                    
                    # DF_count_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_lot")
        
                    # # If you need to read the updated data
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)
                    # DF_count_drop = DF_count_drop.dropDuplicates()

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   
                    
                    #Reading cumulative data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop = DF_Age_drop.dropDuplicates()
                    DF_count_drop = DF_count_drop.dropDuplicates()

                    DF_Age_drop = quarterly_conversion(DF_Age_drop)
                    DF_count_drop = quarterly_conversion(DF_count_drop)

                    #std report without household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()

                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE')   

                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                
                    #Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop_std.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_total_source_count_std = DF_count_drop_std
                    count_eligible_df_std = DF_Count_Elig
                    
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_total_source_diagnosis_std = DF_Age_drop_std
                    diagnosis_eligible_df_std = DF_Diag

                    #Count eligible hippa

                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
        
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
        
                    #Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_Count_Elig_total_cumulative = DF_count_drop

                    DF_total_source_count = DF_Count_Elig_total_cumulative
                    count_eligible_df  = DF_Count_Elig
        
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_Diag_total_cumulative = DF_Age_drop
                    
                    DF_total_source_diagnosis = DF_Diag_total_cumulative                
                    diagnosis_eligible_df = DF_Diag

                    #Count eligible

                    DF_total_count_eligible = count_eligible_break_standard(DF_total_source_count)
                    
                    #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)
                    
                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_standard(DF_total_source_count_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)

                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')
 
                    Cumulative_DF = Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()
                    
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break/'

                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break_std/'
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)
                    
                    print("[+] Rerun process has been Succeeded for the standard report")
                    
            except Exception as e:
                print(f"[-] Rerun process has been failed for the standard report: {e}")
              
########################################################################################################################
####################################### Household Required #############################################################
########################################################################################################################

    if isHouseholdRequired == 'true':
        print("[+] Starting Household Required IF Run")

        col_age = DF_Age.columns
        count_col = DF_LOT.columns    
    
        Drop_Columns = ["_c0","Viewable_Flag","_c3","_c4","_c5","Total_Impression","Count_Eligible_Flag","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_ethnicity","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_practitioner_role_code","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","proc_group","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","ethniPublisher","Surg_practitioner_id","Surg_practitioner_role_code","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET8","BUCKET9","BUCKET10","Time","mediaPlatform","patient_age_group"]
    
        #Dropping unnessary Columns
        Drop_Cols = []
        for cols in col_age:
            if cols in Drop_Columns:
                Drop_Cols.append(cols)
    
        #Dropping unnessary Columns        
        Count_Drop_Cols = []
        for cols in count_col:
            if cols in Drop_Columns:
                Count_Drop_Cols.append(cols)
    
    
        DF_Age_drop = DF_Age.drop(*Drop_Cols)
        DF_Age_drop = DF_Age_drop.drop('hh_fact')
        DF_count_drop = DF_LOT.drop(*Count_Drop_Cols)
    
        DF_Age_drop = DF_Age_drop.dropDuplicates()
        DF_count_drop = DF_count_drop.dropDuplicates()
    
        #cumulative
        DF_Age_drop_month = DF_Age_drop
        DF_count_drop_month = DF_count_drop

        DF_Age_drop = quarterly_conversion(DF_Age_drop)
        DF_count_drop = quarterly_conversion(DF_count_drop)
    
        #Declaring required aggregated columns
        Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
        Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        #Getting Group By Column
        group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
        group_by_Cols.remove('hh_number')
    
        #Getting Group By Column
        count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
        count_group_by_Cols.remove('hh_number')
        columns = ','.join(str(c) for c in group_by_Cols)
        DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
        hhgrouping = count_group_by_Cols.copy()
        hhgrouping.append('patient_id')
        DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
    
        hhgrouping = count_group_by_Cols.copy()
        hhgrouping.append('patient_id')
    
        #Getting Minimum HHFact aginst required breaks
        DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
        DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
        DF_Claims_Group_total = DF_Claims_Group
        DF_total_source_claims = DF_Claims_Group
        #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
        DF_Claims_Group = DF_Claims_Group.drop('patient_id')
        DF_Claims_Group.createOrReplaceTempView('Claims_Group')    
        
        DF_Claims_CE = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
        
        count_eligible_df = DF_Claims_CE
            
        print("diag columns",DF_Age_drop.columns)
        #Calculating Diagnosis Eligible
        DF_Diag = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
        
        DF_total_source_diagnosis =  DF_Age_drop
    
        diagnosis_eligible_df = DF_Diag
            
        #Writing the data into S3 as Delta format
        count_eligible_df.write.format('delta').mode("overwrite").save(output_filepath)
        
        print("[+] The filter condition, dataBreaks and breakCombination has been applied to the household standard report")
    
        #standard report household
        Age_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/quarterly_cumulative/DF_Age_drop/' 
        count_cumulative_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/quarterly_cumulative/DF_count_drop/'
        print(f"[+] Age_cumulative_filepath: {Age_cumulative_filepath}")
        print(f"[+] count_cumulative_filepath: {count_cumulative_filepath}")

        Age_cumulative_filepath_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/quarterly_cumulative/DF_Age_drop/'
        count_cumulative_filepath_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/quarterly_cumulative/DF_count_drop/'

        count_cumulative_filepath_temp_std = f's3://{bucketName}/temp/{studyID}/{deliverableId}/quarterly_standard_cumulative/DF_count_drop/'
        Age_cumulative_filepath_temp_std = f's3://{bucketName}/temp/{studyID}/{deliverableId}/quarterly_standard_cumulative/DF_Age_drop/'

        if isRerunRequired != 'true':
            try:
                if isHouseholdRequired == 'true':
                    print("Try block is executing")
                    #Reading the previous data
                    # # For Age data
                    # delta_table_age = DeltaTable.forPath(spark, Age_cumulative_filepath)
                    
                    
                    # # delta_table_age.alias("old").merge(
                    # #     DF_Age_drop_month.alias("new"),
                    # #     'old.patient_id = new.patient_id'
                    # # ).whenNotMatchedInsertAll().execute()
                    
                    # DF_Age_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_age")

                    # # If you need to read the updated data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    
                    # # For Lot data
                    # delta_table_lot = DeltaTable.forPath(spark, count_cumulative_filepath)
                    # # delta_table_lot.alias("old").merge(
                    # #     DF_count_drop_month.alias("new"),
                    # #     'old.patient_id = new.patient_id'
                    # # ).whenNotMatchedInsertAll().execute()

                    # DF_count_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_lot")

                    # # If you need to read the updated data
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   
                    
                    #Reading cumulative data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop = DF_Age_drop.dropDuplicates()
                    DF_count_drop = DF_count_drop.dropDuplicates()

                    DF_Age_drop = quarterly_conversion(DF_Age_drop)
                    DF_count_drop = quarterly_conversion(DF_count_drop)
                    
                    #Declaring required aggregated columns
                    Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
                    group_by_Cols.remove('hh_number')
                
                    #Getting Group By Column
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
                    count_group_by_Cols.remove('hh_number')
                    columns = ','.join(str(c) for c in group_by_Cols)
                    DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                    DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
                
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')

                    #Getting Minimum HHFact aginst required breaks
                    DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
                    DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
                    DF_Claims_Group_total = DF_Claims_Group
                    DF_total_source_claims = DF_Claims_Group
                    #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
                    DF_Claims_Group = DF_Claims_Group.drop('patient_id')
                    DF_Claims_Group.createOrReplaceTempView('Claims_Group')
                    
                    DF_Claims_CE_cumulative = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
                    
                    count_eligible_df_cumulative = DF_Claims_CE_cumulative
                    
                    #Calculating Diagnosis Eligible Calculating current_month data
                    DF_Diag_cumulative = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_total_source_diagnosis = DF_Age_drop
                    diagnosis_eligible_df_cumulative = DF_Diag_cumulative

                    #std report household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()
                    
                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE') 

                    #Declaring required aggregated columns
                    Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                    group_by_Cols.remove('hh_number')
                
                    #Getting Group By Column
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                    count_group_by_Cols.remove('hh_number')
                    columns = ','.join(str(c) for c in group_by_Cols)
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                
                    #Getting Minimum HHFact aginst required breaks
                    DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
                    DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
                    DF_Claims_Group_total = DF_Claims_Group
                    DF_total_source_claims_std = DF_Claims_Group
                    #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
                    DF_Claims_Group = DF_Claims_Group.drop('patient_id')
                    DF_Claims_Group.createOrReplaceTempView('Claims_Group')    
                    
                    DF_Claims_CE = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
                    
                    count_eligible_df_std = DF_Claims_CE
                        
                    print("diag columns",DF_Age_drop.columns)
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    diagnosis_eligible_df_std = DF_Diag
                    DF_total_source_diagnosis_std =  DF_Age_drop_std
                    
                    #Read the newly updated months data
                    
                    count_eligible_df = count_eligible_df_cumulative
                    count_eligible_df= count_eligible_df.dropDuplicates()
                    
                    diagnosis_eligible_df = diagnosis_eligible_df_cumulative
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates()
                    
                    #Count eligible
                    DF_total_count_eligible = count_eligible_break_household(DF_total_source_claims)
                   
                    #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)

                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_household(DF_total_source_claims_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)

                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)
                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')
                    
                    print("[+] Joined the both data frames")
            
                    Cumulative_DF = Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    count_eligible_df_std   = count_eligible_df_std.dropDuplicates()
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')

                    print("[+] Joined the both data frames standrad")

                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()
                    
                    print(f"[+] Building Filepath: s3://{bucketName}/silver/{studyID}/{deliverableId}")
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break/'

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break_std/'
                    
                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)

            except Exception as e:
                print("Except block is executing")
                logger.error(f"Exception occurred: {str(e)}")
                logger.error(f"Stack Trace: {traceback.format_exc()}")
                
                if isHouseholdRequired == 'true':
                    # Raises an exception if the path already exists
                    if delta_table_exists(Age_cumulative_filepath) or delta_table_exists(count_cumulative_filepath):
                        raise Exception("One or more tables already exist,insert is forbidden in the except statement after delta table creation.")  
                    
                    # DF_Age_drop.write.format('delta').mode("overwrite").save(Age_cumulative_filepath)
                    # DF_count_drop.write.format('delta').mode("overwrite").save(count_cumulative_filepath)

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   

                    #std report household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()

                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE')    
                
                    #Declaring required aggregated columns
                    Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                    group_by_Cols.remove('hh_number')
                
                    #Getting Group By Column
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                    count_group_by_Cols.remove('hh_number')
                    columns = ','.join(str(c) for c in group_by_Cols)
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                
                    #Getting Minimum HHFact aginst required breaks
                    DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
                    DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
                    DF_Claims_Group_total = DF_Claims_Group
                    DF_total_source_claims_std = DF_Claims_Group
                    #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
                    DF_Claims_Group = DF_Claims_Group.drop('patient_id')
                    DF_Claims_Group.createOrReplaceTempView('Claims_Group')    
                    
                    DF_Claims_CE = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
                    
                    count_eligible_df_std = DF_Claims_CE
                        
                    print("diag columns",DF_Age_drop.columns)
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    diagnosis_eligible_df_std = DF_Diag
                    DF_total_source_diagnosis_std =  DF_Age_drop_std
                    
                    #Count eligible

                    DF_total_count_eligible = count_eligible_break_household(DF_total_source_claims)
                    
                    print("DF_total_count_eligible columns",DF_total_count_eligible.columns)
                    
                    if 'hh_fact' in DF_total_count_eligible.columns:
                        DF_total_count_eligible =  DF_total_count_eligible.drop('hh_fact')
                    if 'HHFact' in DF_total_count_eligible.columns:
                        DF_total_count_eligible =  DF_total_count_eligible.drop('HHFact')
                    print("count columns",DF_total_count_eligible.columns)

                     #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)
                    
                    print("DF_total_diagnosis columns",DF_total_diagnosis.columns)
                    
                    if 'hh_fact' in count_eligible_df.columns:
                        count_eligible_df =  count_eligible_df.drop('hh_fact')
                    if 'HHFact' in count_eligible_df.columns:
                        count_eligible_df =  count_eligible_df.drop('HHFact')
                    print("count columns",count_eligible_df.columns)

                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    
                    print("count_eligible_df columns",count_eligible_df.columns)
                    
                    if 'hh_fact' in diagnosis_eligible_df.columns:
                        diagnosis_eligible_df =  diagnosis_eligible_df.drop('hh_fact')
                    if 'HHFact' in diagnosis_eligible_df.columns:
                        diagnosis_eligible_df =  diagnosis_eligible_df.drop('HHFact')
                    print("count columns",diagnosis_eligible_df.columns)
                    
                    if 'hh_fact' in DF_total_diagnosis.columns:
                        DF_total_diagnosis =  DF_total_diagnosis.drop('hh_fact')
                    if 'HHFact' in DF_total_diagnosis.columns:
                        DF_total_diagnosis =  DF_total_diagnosis.drop('HHFact')
                    print("count columns",DF_total_diagnosis.columns)
                    
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 
                    
                    print("diagnosis_eligible_df columns",diagnosis_eligible_df.columns)

                       
                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_household(DF_total_source_claims_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)
        
                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)
                    
                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                          
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')

                    count_eligible_df_std   = count_eligible_df_std.dropDuplicates()
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')

                    print("[+] Joined the both data frames hippa")
            
                    Cumulative_DF = Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    print("[+] Joined the both data frames standrad")
            
                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()
                    
                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break/'
                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break_std/'
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)
                    
                    print("[+] The cumulative report for the 1st month of the Standard household report has been completed.")
                    
        if isRerunRequired == 'true':
            try:
                if isHouseholdRequired == 'true':
                    #Reading the previous data
                    print("Rerun block is executing")

                    print(f"[+] Checking Delta Table For Path: {Age_cumulative_filepath}")
                    delta_table_age = DeltaTable.forPath(spark, Age_cumulative_filepath)
                    
                    # #perfroming the given month deletion
                    # delta_table_age.delete(col("Time_Period") == reRunTimePeriod)
                    
                    # DF_Age_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_age")
                    
                    # # delta_table_age.alias("old").merge(
                    # #     DF_Age_drop_month.alias("new"),
                    # #     'old.patient_id = new.patient_id'
                    # # ).whenNotMatchedInsertAll().execute()
            
                    # # If you need to read the updated data
                    # print(f"[+] Reading Delta Table For Path: {Age_cumulative_filepath}")
                    # DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    # #DF_Age = spark.read.parquet(Age_cumulative_filepath)
            
                    # # For Lot data
                    # delta_table_lot = DeltaTable.forPath(spark, count_cumulative_filepath)
                    
                    # #perfroming the given month deletion
                    # delta_table_lot.delete(col("Time_Period") == reRunTimePeriod)
                    
                    # DF_count_drop_month.write.format("delta").mode("append").saveAsTable("delta_table_lot")
                    
                    # # delta_table_lot.alias("old").merge(
                    # #     DF_count_drop_month.alias("new"),
                    # #     'old.patient_id = new.patient_id'
                    # # ).whenNotMatchedInsertAll().execute()
            
                    # # If you need to read the updated data
                    # DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Age_cumulative_filepath)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(count_cumulative_filepath)   
                    
                    #Reading cumulative data
                    DF_Age_drop = spark.read.format("delta").load(Age_cumulative_filepath)
                    DF_count_drop = spark.read.format("delta").load(count_cumulative_filepath)

                    DF_Age_drop = DF_Age_drop.dropDuplicates()
                    DF_count_drop = DF_count_drop.dropDuplicates()

                    DF_Age_drop = quarterly_conversion(DF_Age_drop)
                    DF_count_drop = quarterly_conversion(DF_count_drop)
                    
                    #Declaring required aggregated columns
                    Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop.columns if col not in Agg_Columns]
                    group_by_Cols.remove('hh_number')
                
                    #Getting Group By Column
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
                    count_group_by_Cols.remove('hh_number')
                    columns = ','.join(str(c) for c in group_by_Cols)
                    DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                    DF_Claims_HH_Fact = DF_count_drop.drop('hh_number')
                
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')

                    #Getting Minimum HHFact aginst required breaks
                    DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
                    DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
                    DF_Claims_Group_total = DF_Claims_Group
                    DF_total_source_claims = DF_Claims_Group
                    #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
                    DF_Claims_Group = DF_Claims_Group.drop('patient_id')
                    DF_Claims_Group.createOrReplaceTempView('Claims_Group')
                    
                    DF_Claims_CE_cumulative = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
                    
                    count_eligible_df_cumulative = DF_Claims_CE_cumulative
                    
                    #Calculating Diagnosis Eligible Calculating current_month data
                    DF_Diag_cumulative = DF_Age_drop.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_total_source_diagnosis = DF_Age_drop
                    diagnosis_eligible_df_cumulative = DF_Diag_cumulative

                    #std report household
                    DF_Age_drop_std = DF_Age_drop
                    DF_count_drop_std = DF_count_drop

                    DF_Age_drop_std = DF_Age_drop_std.dropDuplicates()
                    DF_count_drop_std = DF_count_drop_std.dropDuplicates()
                    
                    if 'AGE' in DF_Age_drop_std.columns:
                        DF_Age_drop_std = DF_Age_drop_std.drop('AGE')
                        DF_count_drop_std = DF_count_drop_std.drop('AGE') 

                    #Declaring required aggregated columns
                    Count_Agg_Columns = ["patient_id","diagnostic_indication","HHFact","shs_patient_id","hh_fact"]
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column
                    group_by_Cols = [col for col in DF_Age_drop_std.columns if col not in Agg_Columns]
                    group_by_Cols.remove('hh_number')
                
                    #Getting Group By Column
                    count_group_by_Cols = [col for col in DF_count_drop_std.columns if col not in Count_Agg_Columns]
                    count_group_by_Cols.remove('hh_number')
                    columns = ','.join(str(c) for c in group_by_Cols)
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                    DF_Claims_HH_Fact = DF_count_drop_std.drop('hh_number')
                
                    hhgrouping = count_group_by_Cols.copy()
                    hhgrouping.append('patient_id')
                
                    #Getting Minimum HHFact aginst required breaks
                    DF_Claims_Group = DF_Claims_HH_Fact.groupBy(*hhgrouping).agg(min("hh_fact").alias("Min_HHFact"))
                    DF_Claims_Group = DF_Claims_Group.withColumn("Min_HHFact", DF_Claims_Group.Min_HHFact.cast('double'))
                    DF_Claims_Group_total = DF_Claims_Group
                    DF_total_source_claims_std = DF_Claims_Group
                    #DF_Claims_Group = DF_Claims_Group.dropDuplicates(['patient_id'])
                    DF_Claims_Group = DF_Claims_Group.drop('patient_id')
                    DF_Claims_Group.createOrReplaceTempView('Claims_Group')    
                    
                    DF_Claims_CE = (DF_Claims_Group.groupBy(*count_group_by_Cols).agg(f.ceil(f.sum("Min_HHFact")).alias("Count_eligible")))
                    
                    count_eligible_df_std = DF_Claims_CE
                        
                    print("diag columns",DF_Age_drop.columns)
                    #Calculating Diagnosis Eligible
                    DF_Diag = DF_Age_drop_std.groupBy(*group_by_Cols).agg(countDistinct('patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    diagnosis_eligible_df_std = DF_Diag
                    DF_total_source_diagnosis_std =  DF_Age_drop_std
                    
                    #Read the newly updated months data
                    
                    count_eligible_df = count_eligible_df_cumulative
                    count_eligible_df= count_eligible_df.dropDuplicates()
                    
                    diagnosis_eligible_df = diagnosis_eligible_df_cumulative
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates()
                    
                    #Count eligible

                    DF_total_count_eligible = count_eligible_break_household(DF_total_source_claims)
                    
                    #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_diagnosis)

                    count_eligible_df = count_eligible_df.unionByName(DF_total_count_eligible)
                    
                    diagnosis_eligible_df = diagnosis_eligible_df.unionByName(DF_total_diagnosis)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    #adjusted age calculation can go here in if condition if needed
                    DF_total_count_eligible_standard_std = count_eligible_break_household(DF_total_source_claims_std)
                    DF_total_diagnosis_standard_std =  diagnosis_eligible_break(DF_total_source_diagnosis_std)

                    count_eligible_df_std = count_eligible_df_std.unionByName(DF_total_count_eligible_standard_std)
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.unionByName(DF_total_diagnosis_standard_std)

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    count_eligible_df.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp)
                    diagnosis_eligible_df.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp)

                    count_eligible_df = spark.read.parquet(count_cumulative_filepath_temp)
                    diagnosis_eligible_df = spark.read.parquet(Age_cumulative_filepath_temp)

                    count_eligible_df_std.write.format('parquet').mode('overwrite').save(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std.write.format('parquet').mode('overwrite').save(Age_cumulative_filepath_temp_std)
                    count_eligible_df_std = spark.read.parquet(count_cumulative_filepath_temp_std)
                    diagnosis_eligible_df_std = spark.read.parquet(Age_cumulative_filepath_temp_std)

                    count_eligible_df= count_eligible_df.dropDuplicates()
                    diagnosis_eligible_df = diagnosis_eligible_df.dropDuplicates() 

                    count_eligible_df_std= count_eligible_df_std.dropDuplicates()   
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    print("[+]  Diagnosis has been Finished")
                    Join_Columns = []
                    for Join_Col in count_eligible_df.columns:
                        if Join_Col in diagnosis_eligible_df.columns:
                            Join_Columns.append(Join_Col)
                
                    Joined_DF = diagnosis_eligible_df.join(count_eligible_df,[c for c in Join_Columns],'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('diagnostic_indication','Condition')
                    
                    print("[+] Joined the both data frames")
            
                    Cumulative_DF = Joined_DF.dropDuplicates()
                    Cumulative_DF = Cumulative_DF.dropDuplicates()

                    count_eligible_df_std   = count_eligible_df_std.dropDuplicates()
                    diagnosis_eligible_df_std = diagnosis_eligible_df_std.dropDuplicates()

                    Join_Columns = []                           
                    for Join_Col in count_eligible_df_std.columns:
                        if Join_Col in diagnosis_eligible_df_std.columns:
                            Join_Columns.append(Join_Col)

                    Joined_DF_std = diagnosis_eligible_df_std.join(count_eligible_df_std,[c for c in Join_Columns],'inner')
                    Joined_DF_std = Joined_DF_std.withColumnRenamed('diagnostic_indication','Condition')

                    print("[+] Joined the both data frames standrad")

                    Cumulative_DF_std = Joined_DF_std.dropDuplicates()
                    Cumulative_DF_std = Cumulative_DF_std.dropDuplicates()
                    
                    print(f"[+] Building Filepath: s3://{bucketName}/silver/{studyID}/{deliverableId}")
                    cumulative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break/'

                    #Writing the data into S3 as Delta format
                    cumulative_output_filepath_std = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/reporting_break_std/'
                    
                    Cumulative_DF.write.format('delta').mode("overwrite").save(cumulative_output_filepath)
                    Cumulative_DF_std.write.format('delta').mode("overwrite").save(cumulative_output_filepath_std)
                    
                    print("[+] Rerun process has been Succeeded for the Household Standard report")
                    
            except Exception as e:
                if isHouseholdRequired == 'true':
                    print(f"[-] Rerun process has been failed for the Household Standard report: {e}")


########################################################################################################################
####################################### Ethnicity Required #############################################################
########################################################################################################################

    if isEthnicityRequired == 'true':
        print("[+] Starting isEthnicityRequired IF Run")
        # Initialize the result DataFrame
        DF_CON_count = spark.createDataFrame([], DF_count.schema)
        DF_CON_diagnosis = spark.createDataFrame([], DF_diagnosis.schema)
    
        i=0
        for mar in marketDefinition:
            conditionName = mar["conditionName"]
            studyCondition=mar["studyCondition"]
            # contractStart = studyCondition["contractStart"]
            # contractStart = datetime(contractStart[0], contractStart[1], contractStart[2])
            # contractStart = datetime.strftime(contractStart,'%Y-%m-%d')
            # contractEnd = studyCondition["contractEnd"]
            # contractEnd = datetime(contractEnd[0], contractEnd[1], contractEnd[2])
            # contractEnd = datetime.strftime(contractEnd,'%Y-%m-%d')
            specialtyCode = studyCondition["specialtyCode"]
    
            # #Applying the filters
            # DF_Contract_Date = DF.filter(f'(Time between "{contractStart}" and "{contractEnd}")')
            DF_Contract_Date_diagnosis = DF_diagnosis
            
            DF_CON_2_diagnois = DF_Contract_Date_diagnosis.filter(col("diagnostic_indication")==conditionName)
            
            if i == 0:
                DF_CON_diagnosis = DF_CON_2_diagnois
            else: 
                DF_CON_diagnosis = DF_CON_diagnosis.unionByName(DF_CON_2_diagnois)
            i=+1
    
        #Creating the date_column
        DF_count.createOrReplaceTempView("Lot")
        DF_LOT = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Lot''')
        DF_LOT_P = DF_LOT.drop(DF_LOT.Time)
    
        DF_CON_diagnosis.createOrReplaceTempView("Con")
        DF_CON = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Con''')
        DF_CON_P = DF_CON.drop(DF_CON.Time)
    
        DF_LOT = None
        DF_CON = None
        
        if media_platforms and len(media_platforms) > 0:
            for media in media_platforms:
                Media_Value = media["mediaValue"]
                Media_Key = media["mediaKey"]
        
                DF_LOT_D = DF_LOT_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
                DF_CON_D = DF_CON_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
        
                DF_LOT = DF_LOT_D if DF_LOT is None else DF_LOT.unionByName(DF_LOT_D)
                DF_CON = DF_CON_D if DF_CON is None else DF_CON.unionByName(DF_CON_D)
                print("[+] Platform breaks has been applied")
        else:
            DF_LOT = DF_LOT_P
            DF_CON = DF_CON_P
            print("[+] Platform breaks is not applied")
            
    
        #Selecting the columns
        Col_con = DF_CON.columns
        count_col = DF_LOT.columns
        Drop_Columns = ["_c0","Viewable_Flag","_c2","_c3","_c4","_c5","Total_Impression","hh_number","hh_fact","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_practitioner_role_code","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","proc_group","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","Surg_practitioner_id","Surg_practitioner_role_code","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET9","BUCKET10","BUCKET8","Line_of_Therapy","CREATIVE_TYPE","PUBLISHER_ID","Time","mediaPlatform","publisher_name","PUBLISHER_NAME"]
    
        #Dropping unnessary Columns        
        Drop_Cols = []
        for cols in Col_con:
            if cols in Drop_Columns:
                Drop_Cols.append(cols)
    
        #Dropping unnessary Columns        
        Count_Drop_Cols = []
        for cols in count_col:
            if cols in Drop_Columns:
                Count_Drop_Cols.append(cols)
    
        DF_con_drop = DF_CON.drop(*Drop_Cols)
        DF_count_drop = DF_LOT.drop(*Count_Drop_Cols)

        select_columns_con = ['patient_id', 'shs_patient_id', 'Count_Eligible_Flag', 'patient_ethnicity', 'diagnostic_indication', 'ethniPublisher','Time_Period','patient_age_group']
        select_columns_count = ['patient_id', 'shs_patient_id', 'Count_Eligible_Flag', 'patient_ethnicity', 'ethniPublisher','Time_Period','patient_age_group']
        DF_con_drop = DF_con_drop.select(*select_columns_con)
        DF_count_drop = DF_count_drop.select(*select_columns_count) 

        #cumulative
        DF_con_drop_month = DF_con_drop
        DF_count_drop_month = DF_count_drop  

        DF_con_drop = quarterly_conversion(DF_con_drop)
        DF_count_drop = quarterly_conversion(DF_count_drop) 
    
        #Declaring required aggregated columns
        Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
        Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        #Getting Group By Column for diagnosis_eligible
        group_by_Cols = [col for col in DF_con_drop.columns if col not in Agg_Columns]
    
        #Getting Group By Column for count_eligible
        count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
    
        # Calculating Count Eligible
        DF_Count_Elig_Total = DF_count_drop.groupBy(*count_group_by_Cols).agg(f.countDistinct('patient_id').alias('Count_eligible'))
        DF_total_source_ethnicity_count = DF_count_drop
        count_eligible_ethnicity_df = DF_Count_Elig_Total

        # Calculating Diagnosis Eligible
        DF_Diag_Total = DF_con_drop.groupBy(*group_by_Cols).agg(f.countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter(f.col('diagnostic_indication').isNotNull())
        DF_total_source_ethnicity_diagnosis = DF_con_drop 
        diagnosis_eligible_ethnicity_df = DF_Diag_Total

        # Finding common columns for join
        Join_Columns = list(set(count_eligible_ethnicity_df.columns) & set(diagnosis_eligible_ethnicity_df.columns))

        # Joining DataFrames
        Joined_DF = diagnosis_eligible_ethnicity_df.join(count_eligible_ethnicity_df, Join_Columns, 'inner')
        Joined_DF = Joined_DF.withColumnRenamed('patient_ethnicity','ETHNICITY')\
            .withColumnRenamed('diagnostic_indication','DIAGNOSIS')\
            .withColumnRenamed('ethniPublisher','PUBLISHER_NAME')\
            .withColumnRenamed('patient_age_group','AGE')
        
        Final_DF =  Joined_DF.dropDuplicates()
        
        print("[+] The filter condition, dataBreaks and breakCombination has been applied to the Ethnicity report")
        
        #Writing as Delta File
        Final_DF.write.format('delta').mode("overwrite").save(Ethnicity_output_filepath_qp)
        
        Con_cumulative_filepath_ethnicity_qp = f's3://{bucketName}/silver/{studyID}/{deliverableId}/quarterly_cumulative/DF_Con_Ethnicity/'
        Lot_cumulative_filepath_ethnicity_qp = f's3://{bucketName}/silver/{studyID}/{deliverableId}/quarterly_cumulative/DF_Lot_Ethnicity/'
        
        Con_cumulative_filepath_ethnicity_qp_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/quarterly_cumulative/DF_Con_Ethnicity/'
        Lot_cumulative_filepath_ethnicity_qp_temp = f's3://{bucketName}/temp/{studyID}/{deliverableId}/quarterly_cumulative/DF_Lot_Ethnicity/'

        
        if isRerunRequired !='true':
            if isEthnicityRequired == 'true':
                try:
                # #if DeltaTable.isDeltaTable(spark,Con_cumulative_filepath_ethnicity_qp)==True and DeltaTable.isDeltaTable(spark,Lot_cumulative_filepath_ethnicity_qp)==True: 
                #     #Reading the previous data
                #     delta_table_con = DeltaTable.forPath(spark, Con_cumulative_filepath_ethnicity_qp)
                #     delta_table_con.alias("old").merge(
                #         DF_CON.alias("new"),
                #         'old.patient_id = new.patient_id'
                #     ).whenNotMatchedInsertAll().execute()
        
                #     # For Lot data
                #     delta_table_lot = DeltaTable.forPath(spark, Lot_cumulative_filepath_ethnicity_qp)
                #     delta_table_lot.alias("old").merge(
                #         DF_LOT.alias("new"),
                #         'old.patient_id = new.patient_id'
                #     ).whenNotMatchedInsertAll().execute()

                    DF_con_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Con_cumulative_filepath_ethnicity_qp)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Lot_cumulative_filepath_ethnicity_qp)   
    
                    #Reading the merged data present in the respective path
                    DF_con_drop = spark.read.format("delta").load(Con_cumulative_filepath_ethnicity_qp)
                    DF_count_drop = spark.read.format("delta").load(Lot_cumulative_filepath_ethnicity_qp)

                    DF_con_drop = DF_con_drop.dropDuplicates()
                    DF_count_drop = DF_count_drop.dropDuplicates()

                    DF_con_drop = quarterly_conversion(DF_con_drop)
                    DF_count_drop = quarterly_conversion(DF_count_drop) 
    
                    #Declaring required aggregated columns
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
        
                    #Getting Group By Column for diagnosis_eligible
                    group_by_Cols= [col for col in DF_con_drop.columns if col not in Agg_Columns]
        
                    #Getting Group By Column for count_eligible
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
        
                    # Calculating Count Eligible
                    DF_Count_Elig_Total = DF_count_drop.groupBy(*count_group_by_Cols).agg(f.countDistinct('patient_id').alias('Count_eligible'))
                    DF_total_source_ethnicity_count_cumulative = DF_count_drop
                    count_eligible_ethnicity_cumulative_df = DF_Count_Elig_Total

                    # Calculating Diagnosis Eligible
                    DF_Diag_Total = DF_con_drop.groupBy(*group_by_Cols).agg(f.countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter(f.col('diagnostic_indication').isNotNull())
                    DF_total_source_ethnicity_diagnosis_cumulative = DF_con_drop
                    diagnosis_eligible_ethnicity_cumulative_df = DF_Diag_Total

                    #Count eligible
                    count_eligible_ethnicity_df = count_eligible_ethnicity_cumulative_df
                    DF_total_count_eligible = count_eligible_break_standard(DF_total_source_ethnicity_count_cumulative)
                    count_eligible_ethnicity_df = count_eligible_ethnicity_df.unionByName(DF_total_count_eligible)
                    count_eligible_ethnicity_df = count_eligible_ethnicity_df.dropDuplicates()

                    #diagnosis eligible
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_cumulative_df
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_ethnicity_diagnosis_cumulative)
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.unionByName(DF_total_diagnosis)
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.dropDuplicates()

                    #temp 
                    count_eligible_ethnicity_df.write.format('parquet').mode('overwrite').save(Lot_cumulative_filepath_ethnicity_qp_temp)
                    diagnosis_eligible_ethnicity_df.write.format('parquet').mode('overwrite').save(Con_cumulative_filepath_ethnicity_qp_temp)

                    count_eligible_ethnicity_df = spark.read.parquet(Lot_cumulative_filepath_ethnicity_qp_temp)
                    diagnosis_eligible_ethnicity_df = spark.read.parquet(Con_cumulative_filepath_ethnicity_qp_temp)

                    count_eligible_ethnicity_df = count_eligible_ethnicity_df.dropDuplicates()
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.dropDuplicates()

                    # Finding common columns for join
                    Join_Columns = list(set(count_eligible_ethnicity_df.columns) & set(diagnosis_eligible_ethnicity_df.columns))

                    # Joining DataFrames
                    Joined_DF = diagnosis_eligible_ethnicity_df.join(count_eligible_ethnicity_df, Join_Columns, 'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('patient_ethnicity','ETHNICITY')\
                        .withColumnRenamed('diagnostic_indication','DIAGNOSIS')\
                        .withColumnRenamed('ethniPublisher','PUBLISHER_NAME')\
                        .withColumnRenamed('patient_age_group','AGE')

                    Cumulative_ethnicity_df = Joined_DF.dropDuplicates()      
                    
                    Ethnicity_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/Ethnicity/'
                    Cumulative_ethnicity_df.write.format('delta').mode("overwrite").save(Ethnicity_cumlative_output_filepath)
                    
                    print("[+] The cumulative report for the subsequent month of the Ethnicity report has been written and completed.")
        
                #except:
                except Exception as e:
                    logger.error(f"Exception occurred: {str(e)}")
                    logger.error(f"Stack Trace: {traceback.format_exc()}")
                #else:
        
                    # Raises an exception if the path already exists
                    if delta_table_exists(Con_cumulative_filepath_ethnicity_qp) or delta_table_exists(Lot_cumulative_filepath_ethnicity_qp):
                        raise Exception("One or more tables already exist,insert is forbidden in the except statement after delta table creation.")                

                    DF_con_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Con_cumulative_filepath_ethnicity_qp)
                    
                    DF_count_drop_month.write \
                        .partitionBy("Time_Period") \
                        .mode("overwrite") \
                        .option("partitionOverwriteMode", "dynamic") \
                        .format("delta") \
                        .save(Lot_cumulative_filepath_ethnicity_qp)   
                    
                    #Count eligible
                    DF_total_count_eligible = count_eligible_break_standard(DF_total_source_ethnicity_count)
                    count_eligible_ethnicity_df = count_eligible_ethnicity_df.unionByName(DF_total_count_eligible)

                    #diagnosis eligible
                    DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_ethnicity_diagnosis)
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.unionByName(DF_total_diagnosis)

                    count_eligible_ethnicity_df = count_eligible_ethnicity_df.dropDuplicates()
                    diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.dropDuplicates()

                    # Finding common columns for join
                    Join_Columns = list(set(count_eligible_ethnicity_df.columns) & set(diagnosis_eligible_ethnicity_df.columns))

                    # Joining DataFrames
                    Joined_DF = diagnosis_eligible_ethnicity_df.join(count_eligible_ethnicity_df, Join_Columns, 'inner')
                    Joined_DF = Joined_DF.withColumnRenamed('patient_ethnicity','ETHNICITY')\
                        .withColumnRenamed('diagnostic_indication','DIAGNOSIS')\
                        .withColumnRenamed('ethniPublisher','PUBLISHER_NAME')\
                        .withColumnRenamed('patient_age_group','AGE')

                    Cumulative_ethnicity_df = Joined_DF.dropDuplicates() 

                    #Writing as Delta File
                    Ethnicity_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/Ethnicity/'
                    Cumulative_ethnicity_df.write.format('delta').mode("overwrite").save(Ethnicity_cumlative_output_filepath)
                    print("[+] The cumulative report for the 1st month of the Ethnicity report has been completed.")
                            
        if isRerunRequired =='true':
                if isEthnicityRequired == 'true': 
                    try:
                    #if DeltaTable.isDeltaTable(spark,Con_cumulative_filepath_ethnicity_qp)==True and DeltaTable.isDeltaTable(spark,Lot_cumulative_filepath_ethnicity_qp)==True: 
                        #Reading the previous data
                        # delta_table_con = DeltaTable.forPath(spark, Con_cumulative_filepath_ethnicity_qp)
                        
                        # #perfroming the given month deletion
                        # delta_table_con.delete(col("Time_Period").isin(months_list))
                        
                        # delta_table_con.alias("old").merge(
                        #     DF_CON.alias("new"),
                        #     'old.patient_id = new.patient_id'
                        # ).whenNotMatchedInsertAll().execute()
        
                        # # For Lot data
                        # delta_table_lot = DeltaTable.forPath(spark, Lot_cumulative_filepath_ethnicity_qp)
                        
                        # #perfroming the given month deletion
                        # delta_table_lot.delete(col("Time_Period").isin(months_list))
                        
                        # delta_table_lot.alias("old").merge(
                        #     DF_LOT.alias("new"),
                        #     'old.patient_id = new.patient_id'
                        # ).whenNotMatchedInsertAll().execute()
       
                        DF_con_drop_month.write \
                            .partitionBy("Time_Period") \
                            .mode("overwrite") \
                            .option("partitionOverwriteMode", "dynamic") \
                            .format("delta") \
                            .save(Con_cumulative_filepath_ethnicity_qp)
                        
                        DF_count_drop_month.write \
                            .partitionBy("Time_Period") \
                            .mode("overwrite") \
                            .option("partitionOverwriteMode", "dynamic") \
                            .format("delta") \
                            .save(Lot_cumulative_filepath_ethnicity_qp)   
        
                        #Reading the merged data present in the respective path
                        DF_con_drop = spark.read.format("delta").load(Con_cumulative_filepath_ethnicity_qp)
                        DF_count_drop = spark.read.format("delta").load(Lot_cumulative_filepath_ethnicity_qp)

                        DF_con_drop = DF_con_drop.dropDuplicates()
                        DF_count_drop = DF_count_drop.dropDuplicates()

                        DF_con_drop = quarterly_conversion(DF_con_drop)
                        DF_count_drop = quarterly_conversion(DF_count_drop) 
                
                        #Declaring required aggregated columns
                        Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                        Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
        
                        #Getting Group By Column for diagnosis_eligible
                        group_by_Cols = [col for col in DF_con_drop.columns if col not in Agg_Columns]
        
                        #Getting Group By Column for count_eligible
                        count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]

                        # Calculating Count Eligible
                        DF_Count_Elig_Total = DF_count_drop.groupBy(*count_group_by_Cols).agg(f.countDistinct('patient_id').alias('Count_eligible'))
                        DF_total_source_ethnicity_count_cumulative = DF_count_drop
                        count_eligible_ethnicity_cumulative_df = DF_Count_Elig_Total

                        # Calculating Diagnosis Eligible
                        DF_Diag_Total = DF_con_drop.groupBy(*group_by_Cols).agg(f.countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter(f.col('diagnostic_indication').isNotNull())
                        DF_total_source_ethnicity_diagnosis_cumulative = DF_con_drop
                        diagnosis_eligible_ethnicity_cumulative_df = DF_Diag_Total

                        #Count eligible
                        count_eligible_ethnicity_df = count_eligible_ethnicity_cumulative_df
                        DF_total_count_eligible = count_eligible_break_standard(DF_total_source_ethnicity_count_cumulative)
                        count_eligible_ethnicity_df = count_eligible_ethnicity_df.unionByName(DF_total_count_eligible)
                        count_eligible_ethnicity_df = count_eligible_ethnicity_df.dropDuplicates()

                        #diagnosis eligible
                        diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_cumulative_df
                        DF_total_diagnosis =  diagnosis_eligible_break(DF_total_source_ethnicity_diagnosis_cumulative)
                        diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.unionByName(DF_total_diagnosis)
                        diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.dropDuplicates()

                        #temp 
                        count_eligible_ethnicity_df.write.format('parquet').mode('overwrite').save(Lot_cumulative_filepath_ethnicity_qp_temp)
                        diagnosis_eligible_ethnicity_df.write.format('parquet').mode('overwrite').save(Con_cumulative_filepath_ethnicity_qp_temp)

                        count_eligible_ethnicity_df = spark.read.parquet(Lot_cumulative_filepath_ethnicity_qp_temp)
                        diagnosis_eligible_ethnicity_df = spark.read.parquet(Con_cumulative_filepath_ethnicity_qp_temp)

                        count_eligible_ethnicity_df = count_eligible_ethnicity_df.dropDuplicates()
                        diagnosis_eligible_ethnicity_df = diagnosis_eligible_ethnicity_df.dropDuplicates()

                        # Finding common columns for join
                        Join_Columns = list(set(count_eligible_ethnicity_df.columns) & set(diagnosis_eligible_ethnicity_df.columns))

                        # Joining DataFrames
                        Joined_DF = diagnosis_eligible_ethnicity_df.join(count_eligible_ethnicity_df, Join_Columns, 'inner')
                        Joined_DF = Joined_DF.withColumnRenamed('patient_ethnicity','ETHNICITY')\
                            .withColumnRenamed('diagnostic_indication','DIAGNOSIS')\
                            .withColumnRenamed('ethniPublisher','PUBLISHER_NAME')\
                            .withColumnRenamed('patient_age_group','AGE')

                        Cumulative_ethnicity_df = Joined_DF.dropDuplicates()        

                        Ethnicity_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/Ethnicity/'
                        
                        Cumulative_ethnicity_df.write.format('delta').mode("overwrite").save(Ethnicity_cumlative_output_filepath)
                        
                        print("[+] Rerun process has been Succeeded for the Ethnicity Report")
                        
                    except Exception as e:
                        print(f"Rerun process has been failed: {e}")


########################################################################################################################
####################################### Specialty Required #############################################################
########################################################################################################################


    if isSpecialtyRequired == 'true':
        print("[+] Starting isSpecialtyRequired")
        DF_CON = spark.createDataFrame([], DF_diagnosis.schema)
        DF_SSPEC = spark.createDataFrame([], DF_diagnosis.schema)
        DF_NSPEC = spark.createDataFrame([], DF_diagnosis.schema)
    
        i=0    
        for mar in marketDefinition:
            conditionName = mar["conditionName"]
            studyCondition = mar["studyCondition"]
            # contractStart = studyCondition["contractStart"]
            # contractStart = datetime(contractStart[0], contractStart[1], contractStart[2])
            # contractStart = datetime.strftime(contractStart,'%Y-%m-%d')
            # contractEnd = studyCondition["contractEnd"]
            # contractEnd = datetime(contractEnd[0], contractEnd[1], contractEnd[2])
            # contractEnd = datetime.strftime(contractEnd,'%Y-%m-%d')
            specialtyCode = studyCondition["specialtyCode"]
    
            #DF_Contract_Date = DF.filter(f'(Time between "{contractStart}" and "{contractEnd}")')
            DF_Contract_Date = DF_diagnosis
            DF_CON_2 = DF_Contract_Date.filter(col("diagnostic_indication")==conditionName)
    
            DF_SSPEC_2 = DF_CON_2.filter(DF_Contract_Date["Proc_practitioner_role_code"].isin(specialtyCode))
    
            DF_NSPEC_2 = DF_CON_2.filter(~DF_Contract_Date["Proc_practitioner_role_code"].isin(specialtyCode))
    
            if i == 0:
                DF_CON = DF_CON_2
                DF_SSPEC = DF_SSPEC_2
                DF_NSPEC = DF_NSPEC_2
    
            else: 
                DF_CON = DF_CON.unionByName(DF_CON_2)
                DF_SSPEC = DF_SSPEC.unionByName(DF_SSPEC_2)
                DF_NSPEC = DF_NSPEC.unionByName(DF_NSPEC_2)
    
            i=+1    
            
        DF_count.createOrReplaceTempView("Lot")
        DF_LOT = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Lot''')
        DF_LOT_P = DF_LOT.drop(DF_LOT.Time)
    
        DF_CON.createOrReplaceTempView("Con")
        DF_CON = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Con''')
        DF_CON_P = DF_CON.drop(DF_CON.Time)
    
        DF_SSPEC.createOrReplaceTempView("Sspec")
        DF_SSPEC = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Sspec''')
        DF_SSPEC_P = DF_SSPEC.drop(DF_SSPEC.Time)
    
        DF_NSPEC.createOrReplaceTempView("Nspec")
        DF_NSPEC = spark.sql('''select *, concat(month(Time),'-',year(Time)) as Time_Period from Nspec''')
        DF_NSPEC_P = DF_NSPEC.drop(DF_NSPEC.Time)
    
        DF_LOT = None
        DF_CON = None
        DF_SSPEC = None
        DF_NSPEC = None
        
        if media_platforms and len(media_platforms) > 0:
            for media in media_platforms:
                Media_Value = media["mediaValue"]
                Media_Key = media["mediaKey"]
        
                DF_LOT_D = DF_LOT_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
                DF_CON_D = DF_CON_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
        
                DF_SSPEC_D = DF_SSPEC_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
                DF_NSPEC_D = DF_NSPEC_P.filter(col("mediaPlatform") == Media_Key).withColumn("mediaPlatform", lit(Media_Value))
        
                
                DF_LOT = DF_LOT_D if DF_LOT is None else DF_LOT.unionByName(DF_LOT_D)
                DF_CON = DF_CON_D if DF_CON is None else DF_CON.unionByName(DF_CON_D)
                DF_SSPEC = DF_SSPEC_D if DF_SSPEC is None else DF_SSPEC.unionByName(DF_SSPEC_D)
                DF_NSPEC = DF_NSPEC_D if DF_NSPEC is None else DF_NSPEC.unionByName(DF_NSPEC_D)
                print("[+] Platform breaks has been applied")
        else:
            DF_LOT = DF_LOT_P
            DF_CON = DF_CON_P
            DF_SSPEC = DF_SSPEC_P
            DF_NSPEC = DF_NSPEC_P
            print("[+] Platform breaks is not applied")
            
        num_platforms = len(media_platforms)
        try:
            if num_platforms == 2:
                # Get distinct values
                distinct_values = DF_LOT.select('mediaPlatform').distinct()

                # Create a string concatenation of all distinct values
                concat_expr = f.concat_ws('+', *[f.collect_list('mediaPlatform')[i] for i in range(num_platforms)])
                concatenated_value = distinct_values.agg(concat_expr.alias('concatenated')).collect()[0]['concatenated']

                # Add new rows with concatenated value
                DF_LOT_break = DF_LOT.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_CON_break = DF_CON.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_SSPEC_break = DF_SSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_NSPEC_break = DF_NSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))

                # Union the new rows with the original DataFrames
                DF_LOT = DF_LOT.unionByName(DF_LOT_break)
                DF_CON = DF_CON.unionByName(DF_CON_break)
                DF_SSPEC = DF_SSPEC.unionByName(DF_SSPEC_break)
                DF_NSPEC = DF_NSPEC.unionByName(DF_NSPEC_break)

            if num_platforms >=3:
                distinct_values = DF_LOT.select("mediaPlatform").distinct().rdd.flatMap(lambda x: x).collect()
                values_to_check = {'Mobile','Desktop','Tablet'}
                is_present = values_to_check.issubset(distinct_values)
                
                if is_present:
                    concatenated_value = "TOTAL DIGITAL"
                    # Add new rows with "TOTAL DIGITAL" value
                    DF_LOT_break = DF_LOT.withColumn('mediaPlatform', f.lit(concatenated_value))
                    DF_CON_break = DF_CON.withColumn('mediaPlatform', f.lit(concatenated_value))
                    DF_SSPEC_break = DF_SSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))
                    DF_NSPEC_break = DF_NSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))

                    # Union the new rows with the original DataFrames
                    DF_LOT = DF_LOT.unionByName(DF_LOT_break)
                    DF_CON = DF_CON.unionByName(DF_CON_break)
                    DF_SSPEC = DF_SSPEC.unionByName(DF_SSPEC_break)
                    DF_NSPEC = DF_NSPEC.unionByName(DF_NSPEC_break)

            if num_platforms >3:
                concatenated_value = "TOTAL PLATFORM"
                # Add new rows with "TOTAL DIGITAL" value
                DF_LOT_break = DF_LOT.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_CON_break = DF_CON.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_SSPEC_break = DF_SSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))
                DF_NSPEC_break = DF_NSPEC.withColumn('mediaPlatform', f.lit(concatenated_value))

                # Union the new rows with the original DataFrames
                DF_LOT = DF_LOT.unionByName(DF_LOT_break)
                DF_CON = DF_CON.unionByName(DF_CON_break)
                DF_SSPEC = DF_SSPEC.unionByName(DF_SSPEC_break)
                DF_NSPEC = DF_NSPEC.unionByName(DF_NSPEC_break)

        except Exception as e:
            print(f"[-] No platform break applied. Error: {str(e)}")
            
        #Getting all the columns
        col_con = DF_CON.columns
        count_col = DF_LOT.columns
        col_sspec = DF_SSPEC.columns
        col_nspec = DF_NSPEC.columns    
    
        Drop_Columns = ["_c0","Viewable_Flag","_c2","_c3","_c4","_c5","Total_Impression","hh_number","hh_fact","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_ethnicity","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","Surg_practitioner_id","Surg_practitioner_role_code","ethniPublisher","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET8","BUCKET9","BUCKET10","Proc_practitioner_role_code","Time","Device"]
    
        #Dropping unnessary Columns
        Drop_Cols = [col for col in col_con if col in Drop_Columns]
        #Dropping unnessary Columns        
        Count_Drop_Cols = [col for col in count_col if col in Drop_Columns]
        #Dropping unnessary Columns        
        Sspec_Drop_Cols = [col for col in col_sspec if col in Drop_Columns]
        #Dropping unnessary Columns        
        Nspec_Drop_Cols = [col for col in col_nspec if col in Drop_Columns]

    
        DF_Con_drop = DF_CON.drop(*Drop_Cols)
        DF_count_drop = DF_LOT.drop(*Count_Drop_Cols)
        DF_sspec_drop = DF_SSPEC.drop(*Sspec_Drop_Cols)
        DF_nspec_drop = DF_NSPEC.drop(*Nspec_Drop_Cols)
    
        #Declaring required aggregated columns
    
        Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        Sspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        Nspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
    
        #Getting Group By Column to get columns for count eligible
        count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
    
        #Getting Group By Column to get columns for diagnosis eligible
        group_by_Cols = [col for col in DF_con_drop.columns if col not in Agg_Columns]
    
        #Getting Group By Column to get columns for SSPEC
        count_group_by_Sspec= [col for col in DF_sspec_drop.columns if col not in Sspec_Columns]
    
        #Getting Group By Column to get columns for NSPEC
        count_group_by_Nspec = [col for col in DF_nspec_drop.columns if col not in Nspec_Columns]
    
        # Calculating Count Eligible
        DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
        
        DF_Count_Elig_total = DF_count_drop
        
        # Calculating Diagnosis Eligible
        DF_Diag = DF_Con_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
        
        DF_Diag_total = DF_Con_drop
        
        # Calculating SSPEC Eligible
        DF_Sspec = DF_sspec_drop.groupBy(*count_group_by_Sspec).agg(countDistinct('shs_patient_id').alias('SSPEC')).filter('diagnostic_indication is not null')
    
        DF_Sspec_total = DF_sspec_drop
    
        # Calculating NSPEC Eligible
        DF_Nspec = DF_nspec_drop.groupBy(*count_group_by_Nspec).agg(countDistinct('shs_patient_id').alias('NSPEC')).filter('diagnostic_indication is not null')
        
        DF_Nspec_total  = DF_nspec_drop
        
       
        # Creating the common columns betweeen the dataframe
        Join_key = list(set(DF_Count_Elig.columns) & set(DF_Diag.columns))
    
        Join_key1 = list(set(DF_Diag.columns) & set(DF_Sspec.columns))
    
        Join_key2 = list(set(DF_Diag.columns) & set(DF_Nspec.columns))
    
        #Joining all the created dataframes
        joined_df = (DF_Diag.join(DF_Count_Elig, Join_key, how = "inner")\
                        .join(DF_Sspec, Join_key1 , how = "inner")\
                        .join(DF_Nspec, Join_key2, how = "inner"))
                        
        column_list = joined_df.columns
        columns_to_drop = ["Diagnosis_eligible", "Count_eligible"]
        column_list = [col for col in column_list if col not in columns_to_drop]
        group_list = []
        
        for i in range(1, len(column_list)):
            for j in range(len(column_list) - i + 1):
                group = column_list[j:j+i]
                
                if i == len(column_list) - 1 and 'Device' in group:
                    continue
                
                group_list.append(','.join(group))
                
        breakCombination  = group_list
    
        total_df = None
        for pair in breakCombination:
            print(f"Applying break combination for Specialty report: {pair}")
            A = []
            columns = pair.split(',')
            for column_value in columns:
                column_value = column_value.replace(' ', '_')
                A.append(column_value)
            print(f"Columns to be set for Specialty report to 'Total': {A}")
    
            # Create a temporary DataFrame with 'Total' for the current combination
            temp_df = joined_df
            for Total_Col in A:
                temp_df = temp_df.withColumn(Total_Col, lit('Total'))
    
            # Union with the total_df
            if total_df is None:
                total_df = temp_df
            else:
                total_df = total_df.unionByName(temp_df)
            print(f"Finished applying break combination for Specialty report: {pair}")
            print("[+] --------------------")
    
        # Union the total_df with the original Final_df
        result_df = joined_df.unionByName(total_df)
        print("[+] All break combinations applied for the Specialty report.")
    
        #Renaming the columns                
        result_df = result_df.withColumnRenamed('diagnostic_indication','Condition')\
                             .withColumnRenamed('mediaPlatform','Device')\
                             .withColumnRenamed('proc_group','Line_Of_Therapy')
        
        #Each break aggregation
        sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
        dynamic_columns = [col for col in result_df.columns if col not in sum_columns]
    
        result_df = result_df.groupBy(dynamic_columns).agg(
            f.sum("Count_eligible").alias("Count_eligible"),
            f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
            f.sum("SSPEC").alias("SSPEC"),
            f.sum("NSPEC").alias("NSPEC")
        )
        
        #Each break aggregation
        sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
        dynamic_columns = [col for col in result_df.columns if col not in sum_columns]
    
        result_df = result_df.groupBy(dynamic_columns).agg(
            f.sum("Count_eligible").alias("Count_eligible"),
            f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
            f.sum("SSPEC").alias("SSPEC"),
            f.sum("NSPEC").alias("NSPEC")
        )
    
        specialty_cumulative_df=result_df
        
        print("[+] The filter condition, dataBreaks and breakCombination has been applied to the Specialty report")
    
        #Writing as Delta File
        result_df.write.format('delta').mode("overwrite").save(Specialty_output_filepath_qp)
    
        #Specialty 
        Con_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/quarterly/DF_Con_Specialty/'
        Lot_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/quarterly/DF_Lot_Specialty/'
        SSPEC_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/quarterly/DF_SSPEC_Specialty/'
        NSPEC_cumulative_filepath_Specialty = f's3://{bucketName}/silver/{studyID}/{deliverableId}/prev_cumulative/quarterly/DF_NSPEC_Specialty/'
    
        if isRerunRequired !='true':
            if isSpecialtyRequired == 'true':
                try:
                #if DeltaTable.isDeltaTable(spark,Con_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,Lot_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,SSPEC_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,NSPEC_cumulative_filepath_Specialty)==True:
                    delta_table_con = DeltaTable.forPath(spark, Con_cumulative_filepath_Specialty)
                    delta_table_con.alias("old").merge(
                        DF_CON.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    # For Lot data
                    delta_table_lot = DeltaTable.forPath(spark, Lot_cumulative_filepath_Specialty)
                    delta_table_lot.alias("old").merge(
                        DF_LOT.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    delta_table_sspec = DeltaTable.forPath(spark, SSPEC_cumulative_filepath_Specialty)
                    delta_table_sspec.alias("old").merge(
                        DF_SSPEC.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    # For Lot data
                    delta_table_nspec = DeltaTable.forPath(spark, NSPEC_cumulative_filepath_Specialty)
                    delta_table_nspec.alias("old").merge(
                        DF_NSPEC.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()        
        
                    #To read the updated data
                    DF_CON = spark.read.format("delta").load(Con_cumulative_filepath_Specialty)
                    DF_LOT = spark.read.format("delta").load(Lot_cumulative_filepath_Specialty)
                    DF_SSPEC = spark.read.format("delta").load(SSPEC_cumulative_filepath_Specialty)
                    DF_NSPEC = spark.read.format("delta").load(NSPEC_cumulative_filepath_Specialty)
                    
                    #Getting all the columns
                    col_con = DF_CON.columns
                    count_col = DF_LOT.columns
                    col_sspec = DF_SSPEC.columns
                    col_nspec = DF_NSPEC.columns
                
                    Drop_Columns = ["_c0","Viewable_Flag","_c2","_c3","_c4","_c5","Total_Impression","hh_number","hh_fact","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_ethnicity","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","ethniPublisher","Surg_practitioner_id","Surg_practitioner_role_code","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET9","BUCKET8","BUCKET10","Proc_practitioner_role_code","Time","Device"]
                
                
                    DF_Con_drop = column_dropper(DF_CON, Drop_Columns)
                    DF_count_drop = column_dropper(DF_LOT, Drop_Columns)
                    DF_sspec_drop = column_dropper(DF_SSPEC, Drop_Columns)
                    DF_nspec_drop = column_dropper(DF_NSPEC, Drop_Columns)
                
                    #Declaring required aggregated columns
                
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Sspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Nspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column to get columns for count eligible
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
                
                    #Getting Group By Column to get columns for diagnosis eligible
                    group_by_Cols= [col for col in DF_con_drop.columns if col not in Agg_Columns]
                
                    #Getting Group By Column to get columns for SSPEC
                    count_group_by_Sspec = [col for col in DF_sspec_drop.columns if col not in Sspec_Columns]
                    #Getting Group By Column to get columns for NSPEC
                    count_group_by_Nspec = [col for col in DF_nspec_drop.columns if col not in Nspec_Columns]
                
                    # Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    
                    DF_Count_Elig_total_cumulative = DF_count_drop
                    
                    # Calculating Diagnosis Eligible
                    DF_Diag = DF_Con_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                
                    DF_Diag_total_cumulative = DF_Con_drop
                
                    # Calculating SSPEC Eligible
                    DF_Sspec = DF_sspec_drop.groupBy(*count_group_by_Sspec).agg(countDistinct('shs_patient_id').alias('SSPEC')).filter('diagnostic_indication is not null')
                    
                    DF_Sspec_total_cumulative = DF_sspec_drop
                
                    # Calculating NSPEC Eligible
                    DF_Nspec = DF_nspec_drop.groupBy(*count_group_by_Nspec).agg(countDistinct('shs_patient_id').alias('NSPEC')).filter('diagnostic_indication is not null')
                   
                    DF_Nspe_total_cumulative = DF_nspec_drop
                    
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(DF_Count_Elig.columns) & set(DF_Diag.columns))
                
                    Join_key1 = list(set(DF_Diag.columns) & set(DF_Sspec.columns))
                
                    Join_key2 = list(set(DF_Diag.columns) & set(DF_Nspec.columns))
                
                    #Joining all the created dataframes
                    joined_df = (DF_Diag.join(DF_Count_Elig, Join_key, how = "inner")\
                                    .join(DF_Sspec, Join_key1 , how = "inner")\
                                    .join(DF_Nspec, Join_key2, how = "inner"))
                                    
                    column_list = joined_df.columns
                    columns_to_drop = ["Diagnosis_eligible", "Count_eligible"]
                    column_list = [col for col in column_list if col not in columns_to_drop]
                    group_list = []
                    
                    for i in range(1, len(column_list)):
                        for j in range(len(column_list) - i + 1):
                            group = column_list[j:j+i]
                            
                            if i == len(column_list) - 1 and 'Device' in group:
                                continue
                            
                            group_list.append(','.join(group))
                            
                    breakCombination  = group_list
                
                    total_df = None
                    for pair in breakCombination:
                        A = []
                        columns = pair.split(',')
                        for column_value in columns:
                            column_value = column_value.replace(' ', '_')
                            A.append(column_value)
                
                        # Create a temporary DataFrame with 'Total' for the current combination
                        temp_df = joined_df
                        for Total_Col in A:
                            temp_df = temp_df.withColumn(Total_Col, lit('Total'))
                
                        # Union with the total_df
                        if total_df is None:
                            total_df = temp_df
                        else:
                            total_df = total_df.unionByName(temp_df)
                
                    # Union the total_df with the original Final_df
                    result_df = joined_df.unionByName(total_df)
                
                    #Renaming the columns                
                    result_df = result_df.withColumnRenamed('diagnostic_indication','Condition')\
                                         .withColumnRenamed('mediaPlatform','Device')\
                                         .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Each break aggregation
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    dynamic_columns = [col for col in result_df.columns if col not in sum_columns]
                
                    result_df = result_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
                    


                    
                    # break_df = get_total_rows(result_df, breakCombination)
                    break_df = result_df.withColumn("Time_Period",lit("Campaign to Date"))
                    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    dynamic_columns = [col for col in break_df.columns if col not in sum_columns]
                    
                    campaign_by_break = break_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
        
                    # aggregation total for the time_df
                    #Count eligible total calculation
                    DF_Count_Elig_total = DF_Count_Elig_total.drop('shs_patient_id','Count_Eligible_Flag')
                    Total_df_count = DF_Count_Elig_total.dropDuplicates(['patient_id'])
                    
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_count.columns if col not in sum_columns]
                    
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Count_eligible"))
                    
                    time_df = Total_df_count.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df = time_df.withColumnRenamed('diagnostic_indication','Condition')\
                                     .withColumnRenamed('mediaPlatform','Device')\
                                     .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Diagnosis total calculation
                    DF_Diag_total = DF_Diag_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_diag = DF_Diag_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_diag.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Diagnosis_eligible"))
                    
                    # Perform the groupBy and aggregation
                    time_df_diag = Total_df_diag.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_diag = time_df_diag.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #SSPEC total calculation
                    DF_Sspec_total = DF_Sspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Sspec = DF_Sspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Sspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("SSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Sspec = Total_df_Sspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Sspec = time_df_Sspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #NSPEC total calculation
                    DF_Nspec_total = DF_Nspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Nspec = DF_Nspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Nspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("NSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Nspec = Total_df_Nspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Nspec = time_df_Nspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(time_df.columns) & set(time_df_diag.columns))
                    
                    Join_key1 = list(set(time_df_diag.columns) & set(time_df_Sspec.columns))
                    
                    Join_key2 = list(set(time_df_diag.columns) & set(time_df_Nspec.columns))
                    
                    #Joining all the created dataframes
                    joined_df = (time_df_diag.join(time_df, Join_key, how = "inner")\
                                    .join(time_df_Sspec, Join_key1, how = "inner")\
                                    .join(time_df_Nspec, Join_key2, how = "inner"))
                    
                    time_df = joined_df        
                    
                    # aggregation total for the cumlative_df
                    #Count eligible total calculation
                    DF_Count_Elig_total_cumulative = DF_Count_Elig_total_cumulative.drop('shs_patient_id','Count_Eligible_Flag')
                    Total_df_count_cumulative = DF_Count_Elig_total_cumulative.dropDuplicates(['patient_id'])
                    
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_count_cumulative.columns if col not in sum_columns]
                    
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Count_eligible"))
                    
                    time_df_cumulative = Total_df_count_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_cumulative = time_df_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                     .withColumnRenamed('mediaPlatform','Device')\
                                     .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Diagnosis total calculation
                    DF_Diag_total_cumulative = DF_Diag_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    
                    Total_df_diag_cumulative = DF_Diag_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_diag_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Diagnosis_eligible"))
                    
                    # Perform the groupBy and aggregation
                    time_df_diag_cumulative = Total_df_diag_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_diag_cumulative = time_df_diag_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #SSPEC total calculation
                    DF_Sspec_total_cumulative = DF_Sspec_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Sspec_cumulative = DF_Sspec_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Sspec_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("SSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Sspec_cumulative = Total_df_Sspec_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Sspec_cumulative = time_df_Sspec_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #NSPEC total calculation
                    DF_Nspe_total_cumulative = DF_Nspe_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Nspec_cumulative = DF_Nspe_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Nspec_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("NSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Nspec_cumulative = Total_df_Nspec_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Nspec_cumulative = time_df_Nspec_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(time_df_cumulative.columns) & set(time_df_diag_cumulative.columns))
                    
                    Join_key1 = list(set(time_df_diag_cumulative.columns) & set(time_df_Sspec_cumulative.columns))
                    
                    Join_key2 = list(set(time_df_diag_cumulative.columns) & set(time_df_Nspec_cumulative.columns))
                    
                    #Joining all the created dataframes
                    joined_df_cumulative = (time_df_diag_cumulative.join(time_df_cumulative, Join_key, how = "inner")\
                                    .join(time_df_Sspec_cumulative, Join_key1 , how = "inner")\
                                    .join(time_df_Nspec_cumulative, Join_key2, how = "inner"))
                    
                    campaign_df = joined_df_cumulative
                    
                    campaign_df = campaign_df.withColumn("Time_Period",lit("Campaign to Date"))
                    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    
                    dynamic_columns = [col for col in campaign_df.columns if col not in sum_columns]
                    campaign_df = campaign_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
        
                    campaign_by_break = campaign_by_break.unionByName(campaign_df)
                    
                    #current month aggregated values
                    current_month = specialty_cumulative_df.unionByName(time_df)
                    Specality_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/Specality/'
                    prev_month = spark.read.format("delta").load(Specality_cumlative_output_filepath)
        
                    #previous month aggregated values
                    prev_month = prev_month.filter(col("Time_Period") != "Campaign to Date")
                    prev_month = prev_month.dropDuplicates()
                    current_month = current_month.unionByName(campaign_by_break)
                    Final_agg = prev_month.unionByName(current_month)
                    
                    #Writing as Delta File
                    Final_agg.write.format('delta').mode("overwrite").save(Specality_cumlative_output_filepath)
                    
                    print("[+] The cumulative report for the subsequent month of the Specality report has been written and completed.")
        
                #except:
                except Exception as e:
                    logger.error(f"Exception occurred: {str(e)}")
                    logger.error(f"Stack Trace: {traceback.format_exc()}")
                #else:
                    # Raises an exception if the path already exists
                    if delta_table_exists(Con_cumulative_filepath_Specialty) or delta_table_exists(Lot_cumulative_filepath_Specialty)or delta_table_exists(SSPEC_cumulative_filepath_Specialty)or delta_table_exists(NSPEC_cumulative_filepath_Specialty):
                        raise Exception("One or more tables already exist,insert is forbidden in the except statement after delta table creation.")  
                    
                    #Writing into the delta path
                    DF_CON.write.format('delta').mode("overwrite").save(Con_cumulative_filepath_Specialty)
                    DF_LOT.write.format('delta').mode("overwrite").save(Lot_cumulative_filepath_Specialty)
                    DF_SSPEC.write.format('delta').mode("overwrite").save(SSPEC_cumulative_filepath_Specialty)
                    DF_NSPEC.write.format('delta').mode("overwrite").save(NSPEC_cumulative_filepath_Specialty)
                    

                    # break_df = get_total_rows(specialty_cumulative_df, breakCombination)
                    break_df = specialty_cumulative_df.withColumn("Time_Period",lit("Campaign to Date"))
                    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    dynamic_columns = [col for col in break_df.columns if col not in sum_columns]
                    
                    campaign_by_break = break_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
                    
                    #time_df calculation
                    # aggregation total for the time_df
                    #Count eligible total calculation
                    DF_Count_Elig_total = DF_Count_Elig_total.drop('shs_patient_id','Count_Eligible_Flag')
                    Total_df_count = DF_Count_Elig_total.dropDuplicates(['patient_id'])
                    
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_count.columns if col not in sum_columns]
                    
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Count_eligible"))
                    
                    time_df = Total_df_count.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df = time_df.withColumnRenamed('diagnostic_indication','Condition')\
                                     .withColumnRenamed('mediaPlatform','Device')\
                                     .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Diagnosis total calculation
                    DF_Diag_total = DF_Diag_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_diag = DF_Diag_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_diag.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Diagnosis_eligible"))
                    
                    # Perform the groupBy and aggregation
                    time_df_diag = Total_df_diag.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_diag = time_df_diag.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #SSPEC total calculation
                    DF_Sspec_total = DF_Sspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Sspec = DF_Sspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Sspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("SSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Sspec = Total_df_Sspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Sspec = time_df_Sspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #NSPEC total calculation
                    DF_Nspec_total = DF_Nspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Nspec = DF_Nspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Nspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("NSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Nspec = Total_df_Nspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Nspec = time_df_Nspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(time_df.columns) & set(time_df_diag.columns))
                    
                    Join_key1 = list(set(time_df_diag.columns) & set(time_df_Sspec.columns))
                    
                    Join_key2 = list(set(time_df_diag.columns) & set(time_df_Nspec.columns))
                    
                    #Joining all the created dataframes
                    joined_df = (time_df_diag.join(time_df, Join_key, how = "inner")\
                                    .join(time_df_Sspec, Join_key1, how = "inner")\
                                    .join(time_df_Nspec, Join_key2, how = "inner"))
                    
                    time_df = joined_df
                    campaign_df = time_df
                    campaign_df = campaign_df.withColumn("Time_Period",lit("Campaign to Date"))
                    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    
                    dynamic_columns = [col for col in campaign_df.columns if col not in sum_columns]
                    campaign_df = campaign_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
        
                    total_df=campaign_by_break.unionByName(time_df)
                    total_df = total_df.unionByName(campaign_df)
                    # Union the aggregated data with the total
                    specialty_cumulative_df = specialty_cumulative_df.unionByName(total_df)
                    
                    #Writing as Delta File
                    Specality_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/Specality/'
                    specialty_cumulative_df.write.format('delta').mode("overwrite").save(Specality_cumlative_output_filepath)
                    
                    print("[+] The cumulative report for the 1st month of the Specality report has been completed.")
        
        if isRerunRequired =='true':
            if isSpecialtyRequired == 'true':
                try:
                #if DeltaTable.isDeltaTable(spark,Con_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,Lot_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,SSPEC_cumulative_filepath_Specialty)==True and DeltaTable.isDeltaTable(spark,NSPEC_cumulative_filepath_Specialty)==True:
                    delta_table_con = DeltaTable.forPath(spark, Con_cumulative_filepath_Specialty)
                    
                    #perfroming the given month deletion
                    delta_table_con.delete(col("Time_Period").isin(months_list))
                    
                    delta_table_con.alias("old").merge(
                        DF_CON.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    # For Lot data
                    delta_table_lot = DeltaTable.forPath(spark, Lot_cumulative_filepath_Specialty)
                    
                    #perfroming the given month deletion
                    delta_table_lot.delete(col("Time_Period").isin(months_list))
                    
                    delta_table_lot.alias("old").merge(
                        DF_LOT.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    delta_table_sspec = DeltaTable.forPath(spark, SSPEC_cumulative_filepath_Specialty)
                    
                    #perfroming the given month deletion
                    delta_table_sspec.delete(col("Time_Period").isin(months_list))
                    
                    
                    delta_table_sspec.alias("old").merge(
                        DF_SSPEC.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()
                    
                    # For Lot data
                    delta_table_nspec = DeltaTable.forPath(spark, NSPEC_cumulative_filepath_Specialty)
                    
                    #perfroming the given month deletion
                    delta_table_nspec.delete(col("Time_Period").isin(months_list))            
                    
                    delta_table_nspec.alias("old").merge(
                        DF_NSPEC.alias("new"),
                        'old.patient_id = new.patient_id'
                    ).whenNotMatchedInsertAll().execute()        
        
                    #To read the updated data
                    DF_CON = spark.read.format("delta").load(Con_cumulative_filepath_Specialty)
                    DF_LOT = spark.read.format("delta").load(Lot_cumulative_filepath_Specialty)
                    DF_SSPEC = spark.read.format("delta").load(SSPEC_cumulative_filepath_Specialty)
                    DF_NSPEC = spark.read.format("delta").load(NSPEC_cumulative_filepath_Specialty)
                    
                    #Getting all the columns
                    col_con = DF_CON.columns
                    count_col = DF_LOT.columns
                    col_sspec = DF_SSPEC.columns
                    col_nspec = DF_NSPEC.columns
                
                    Drop_Columns = ["_c0","Viewable_Flag","_c2","_c3","_c4","_c5","Total_Impression","hh_number","hh_fact","hshld_id","media_type","file_name","file_process_month_date","run_Date","patient_birth_year","patient_gender","patient_ethnicity","patient_income","cbsa","csa","education","state","msa","dma","patient_zip2","Prod_claim_id","claim_rel_gid","Prod_patient_id","Prod_drug_id","Prod_practitioner_id","Prod_mkt_def_gid","plan_id","patient_pay","plan_pay","refill_code","quantity","days_supply","rx_fill_date","pharmacy_id","claim_arrvl_tstmp","claim_status","daw_code","reject_code","paid_dspng_fee_amt","incnt_srvcs_fee_paid_amt","plhld_emplr_grp_id","refill_athrz_nbr","rx_origin_code","rx_wrtn_dte","scrbd_rx_nbr","final_claim_ind","lifecycle_flag","Prod_h_prtn_key","Proc_patient_id","Proc_claim_id","claim_line_item","Proc_claim_type","procedure_code","procedure_date","Proc_practitioner_id","units_administered","charge_amount","Proc_mkt_def_gid","attending_practitioner_id","operating_practitioner_id","ordering_practitioner_id","referring_practitioner_id","rendering_practitioner_id","Proc_drug_id","Proc_srvc_from_dte","Proc_place_of_service_code","Proc_plan_type_code","procedure_modifier_1_code","procedure_modifier_2_code","Proc_h_prtn_key","Diag_patient_id","Diag_claim_id","Diag_claim_type","service_date","service_to_date","diagnosis_code","Diag_practitioner_id","diagnosis_type_code","Diag_practitioner_role_code","Diag_h_prtn_key","Diag_diag_code","Diag_mkt_def_gid","Surg_patient_id","Surg_claim_id","procedure_type_code","Surg_claim_type","surgical_procedure_code","surgical_procedure_date","ethniPublisher","Surg_practitioner_id","Surg_practitioner_role_code","Surg_srvc_from_dte","Surg_place_of_service_code","Surg_mkt_def_gid","Surg_plan_type_code","Surg_h_prtn_key","C3_HASH","C4_HASH","C5_HASH","BUCKET1","BUCKET2","BUCKET9","BUCKET8","BUCKET10","Proc_practitioner_role_code","Time","Device"]
                
                    #Dropping unnessary Columns
                    Drop_Cols = [col for col in col_con if col in Drop_Columns]                
                    #Dropping unnessary Columns        
                    Count_Drop_Cols = [col for col in count_col if col in Drop_Columns]                
                    #Dropping unnessary Columns        
                    Sspec_Drop_Cols = [col for col in col_sspec if col in Drop_Columns]                
                    #Dropping unnessary Columns        
                    Nspec_Drop_Cols = [col for col in col_nspec if col in Drop_Columns]

                
                    DF_Con_drop = column_dropper(DF_CON, Drop_Columns)
                    DF_count_drop = column_dropper(DF_LOT, Drop_Columns)
                    DF_sspec_drop = column_dropper(DF_SSPEC, Drop_Columns)
                    DF_nspec_drop = column_dropper(DF_NSPEC, Drop_Columns)
                
                    #Declaring required aggregated columns
                
                    Count_Agg_Columns = ["diagnostic_indication","patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Agg_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Sspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    Nspec_Columns = ["patient_id","shs_patient_id","Count_Eligible_Flag"]
                
                    #Getting Group By Column to get columns for count eligible
                    count_group_by_Cols = [col for col in DF_count_drop.columns if col not in Count_Agg_Columns]
                
                    #Getting Group By Column to get columns for diagnosis eligible
                    group_by_Cols = [col for col in DF_Con_drop.columns if col not in Agg_Columns]
                
                    #Getting Group By Column to get columns for SSPEC
                    count_group_by_Sspec = [col for col in DF_sspec_drop.columns if col not in Sspec_Columns]
                
                    #Getting Group By Column to get columns for NSPEC
                    count_group_by_Nspec = [col for col in DF_nspec_drop.columns if col not in Nspec_Columns]
                
                    # Calculating Count Eligible
                    DF_Count_Elig = DF_count_drop.groupBy(*count_group_by_Cols).agg(countDistinct('patient_id').alias('Count_eligible'))
                    DF_Count_Elig_total_cumulative = DF_count_drop
                
                    # Calculating Diagnosis Eligible
                    DF_Diag = DF_Con_drop.groupBy(*group_by_Cols).agg(countDistinct('shs_patient_id').alias('Diagnosis_eligible')).filter('diagnostic_indication is not null')
                    DF_Diag_total_cumulative = DF_Con_drop
                
                    # Calculating SSPEC Eligible
                    DF_Sspec = DF_sspec_drop.groupBy(*count_group_by_Sspec).agg(countDistinct('shs_patient_id').alias('SSPEC')).filter('diagnostic_indication is not null')
                    DF_Sspec_total_cumulative = DF_sspec_drop
                
                    # Calculating NSPEC Eligible
                    DF_Nspec = DF_nspec_drop.groupBy(*count_group_by_Nspec).agg(countDistinct('shs_patient_id').alias('NSPEC')).filter('diagnostic_indication is not null')
                    DF_Nspe_total_cumulative = DF_nspec_drop
                    
                
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(DF_Count_Elig.columns) & set(DF_Diag.columns))
                
                    Join_key1 = list(set(DF_Diag.columns) & set(DF_Sspec.columns))
                
                    Join_key2 = list(set(DF_Diag.columns) & set(DF_Nspec.columns))
                
                    #Joining all the created dataframes
                    joined_df = (DF_Diag.join(DF_Count_Elig, Join_key, how = "inner")\
                                    .join(DF_Sspec, Join_key1 , how = "inner")\
                                    .join(DF_Nspec, Join_key2, how = "inner"))
                                    
                    column_list = joined_df.columns
                    columns_to_drop = ["Diagnosis_eligible", "Count_eligible"]
                    column_list = [col for col in column_list if col not in columns_to_drop]
                    group_list = []
                    
                    for i in range(1, len(column_list)):
                        for j in range(len(column_list) - i + 1):
                            group = column_list[j:j+i]
                            
                            if i == len(column_list) - 1 and 'Device' in group:
                                continue
                            
                            group_list.append(','.join(group))
                            
                    breakCombination  = group_list
                
                    total_df = None
                    for pair in breakCombination:
                        A = []
                        columns = pair.split(',')
                        for column_value in columns:
                            column_value = column_value.replace(' ', '_')
                            A.append(column_value)
                
                        # Create a temporary DataFrame with 'Total' for the current combination
                        temp_df = joined_df
                        for Total_Col in A:
                            temp_df = temp_df.withColumn(Total_Col, lit('Total'))
                
                        # Union with the total_df
                        if total_df is None:
                            total_df = temp_df
                        else:
                            total_df = total_df.unionByName(temp_df)
                
                    # Union the total_df with the original Final_df
                    result_df = joined_df.unionByName(total_df)
                
                    #Renaming the columns                
                    result_df = result_df.withColumnRenamed('diagnostic_indication','Condition')\
                                         .withColumnRenamed('mediaPlatform','Device')\
                                         .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Each break aggregation
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    dynamic_columns = [col for col in result_df.columns if col not in sum_columns]
                
                    result_df = result_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
                    
                    
                    # break_df = get_total_rows(result_df, breakCombination)
                    break_df = result_df.withColumn("Time_Period",lit("Campaign to Date"))
                    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    dynamic_columns = [col for col in break_df.columns if col not in sum_columns]
                    
                    campaign_by_break = break_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
        
                    # aggregation total for the time_df
                    #Count eligible total calculation
                    DF_Count_Elig_total = DF_Count_Elig_total.drop('shs_patient_id','Count_Eligible_Flag')
                    Total_df_count = DF_Count_Elig_total.dropDuplicates(['patient_id'])
                    
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_count.columns if col not in sum_columns]
                    
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Count_eligible"))
                    
                    time_df = Total_df_count.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df = time_df.withColumnRenamed('diagnostic_indication','Condition')\
                                     .withColumnRenamed('mediaPlatform','Device')\
                                     .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Diagnosis total calculation
                    DF_Diag_total = DF_Diag_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_diag = DF_Diag_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_diag.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Diagnosis_eligible"))
                    
                    # Perform the groupBy and aggregation
                    time_df_diag = Total_df_diag.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_diag = time_df_diag.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #SSPEC total calculation
                    DF_Sspec_total = DF_Sspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Sspec = DF_Sspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Sspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("SSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Sspec = Total_df_Sspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Sspec = time_df_Sspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #NSPEC total calculation
                    DF_Nspec_total = DF_Nspec_total.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Nspec = DF_Nspec_total.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Nspec.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("NSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Nspec = Total_df_Nspec.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Nspec = time_df_Nspec.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(time_df.columns) & set(time_df_diag.columns))
                    
                    Join_key1 = list(set(time_df_diag.columns) & set(time_df_Sspec.columns))
                    
                    Join_key2 = list(set(time_df_diag.columns) & set(time_df_Nspec.columns))
                    
                    #Joining all the created dataframes
                    joined_df = (time_df_diag.join(time_df, Join_key, how = "inner")\
                                    .join(time_df_Sspec, Join_key1 , how = "inner")\
                                    .join(time_df_Nspec, Join_key2, how = "inner"))
                    
                    time_df = joined_df        
                    
                    # aggregation total for the campaign_df
                    #Count eligible total calculation
                    DF_Count_Elig_total_cumulative = DF_Count_Elig_total_cumulative.drop('shs_patient_id','Count_Eligible_Flag')
                    Total_df_count_cumulative = DF_Count_Elig_total_cumulative.dropDuplicates(['patient_id'])
                    
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_count_cumulative.columns if col not in sum_columns]
                    
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Count_eligible"))
                    
                    time_df_cumulative = Total_df_count_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_cumulative = time_df_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                     .withColumnRenamed('mediaPlatform','Device')\
                                     .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #Diagnosis total calculation
                    DF_Diag_total_cumulative = DF_Diag_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    
                    Total_df_diag_cumulative = DF_Diag_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_diag_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("Diagnosis_eligible"))
                    
                    # Perform the groupBy and aggregation
                    time_df_diag_cumulative = Total_df_diag_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_diag_cumulative = time_df_diag_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #SSPEC total calculation
                    DF_Sspec_total_cumulative = DF_Sspec_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Sspec_cumulative = DF_Sspec_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Sspec_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("SSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Sspec_cumulative = Total_df_Sspec_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Sspec_cumulative = time_df_Sspec_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    #NSPEC total calculation
                    DF_Nspe_total_cumulative = DF_Nspe_total_cumulative.drop('Count_Eligible_Flag','shs_patient_id')
                    Total_df_Nspec_cumulative = DF_Nspe_total_cumulative.dropDuplicates(['patient_id'])
                    
                    # Define columns to sum and dynamic columns
                    sum_columns = ["Time_Period", "patient_id"]
                    dynamic_columns = [col for col in Total_df_Nspec_cumulative.columns if col not in sum_columns]
                    
                    # Create a list of aggregations for the dynamic columns
                    agg_exprs = [f.lit("Total").alias(col) for col in dynamic_columns]
                    
                    # Add the count patient_id for  as Count_eligible
                    agg_exprs.append(f.count("patient_id").cast(IntegerType()).alias("NSPEC"))
                    
                    # Perform the groupBy and aggregation
                    time_df_Nspec_cumulative = Total_df_Nspec_cumulative.groupBy("Time_Period").agg(*agg_exprs)
                    
                    time_df_Nspec_cumulative = time_df_Nspec_cumulative.withColumnRenamed('diagnostic_indication','Condition')\
                                               .withColumnRenamed('mediaPlatform','Device')\
                                               .withColumnRenamed('proc_group','Line_Of_Therapy')
                    
                    # Creating the common columns betweeen the dataframe
                    Join_key = list(set(time_df_cumulative.columns) & set(time_df_diag_cumulative.columns))
                    
                    Join_key1 = list(set(time_df_diag_cumulative.columns) & set(time_df_Sspec_cumulative.columns))
                    
                    Join_key2 = list(set(time_df_diag_cumulative.columns) & set(time_df_Nspec_cumulative.columns))
                    
                    #Joining all the created dataframes
                    joined_df_cumulative = (time_df_diag_cumulative.join(time_df_cumulative, Join_key, how = "inner")\
                                    .join(time_df_Sspec_cumulative, Join_key1 , how = "inner")\
                                    .join(time_df_Nspec_cumulative, Join_key2, how = "inner"))
                    
                    campaign_df = joined_df_cumulative
                    
                    campaign_df = campaign_df.withColumn("Time_Period",lit("Campaign to Date"))
    
                    sum_columns = ["Diagnosis_eligible","Count_eligible","SSPEC","NSPEC"]
                    
                    dynamic_columns = [col for col in campaign_df.columns if col not in sum_columns]
                    campaign_df = campaign_df.groupBy(dynamic_columns).agg(
                        f.sum("Count_eligible").alias("Count_eligible"),
                        f.sum("Diagnosis_eligible").alias("Diagnosis_eligible"),
                        f.sum("SSPEC").alias("SSPEC"),
                        f.sum("NSPEC").alias("NSPEC")
                    )
                    
                    campaign_by_break = campaign_by_break.unionByName(campaign_df)
                    
                    #current month aggregated values
                    current_month = specialty_cumulative_df.unionByName(time_df)
                    Specality_cumlative_output_filepath = f's3://{bucketName}/silver/{studyID}/{deliverableId}/cumulative/quarterly/Specality/'
                    prev_month = spark.read.format("delta").load(Specality_cumlative_output_filepath)
        
                    #previous month aggregated values
                    prev_month = prev_month.filter((col("Time_Period") != "Campaign to Date") & (~col("Time_Period").isin(months_list)))
                    #prev_month = prev_month.filter((col("Time_Period") != "Campaign to Date") & (col("Time_Period") != reRunTimePeriod ))
                    
                    current_month = current_month.unionByName(campaign_by_break)
                    Final_agg = prev_month.unionByName(current_month)
                    
                    #Writing as Delta File
                    Final_agg.write.format('delta').mode("overwrite").save(Specality_cumlative_output_filepath)
                    
                    print("[+] Rerun process has been Succeeded for the Specality report")
                
                except Exception as e:
                    print(f"[-] Rerun process has been failed for the Specality report: {e}")
            
job.commit()