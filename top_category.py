import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
from datetime import datetime,date
from snowflake.snowpark.functions import expr, regexp_replace
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, FloatType
import snowflake.snowpark as sp
import pandas as pd
import csv
import time
import dateutil.relativedelta

deal_id = '12025'
 
def intersection(l1, l2):
    return list(set(l1) & set(l2))
 
def fix_datatypes(df_sales, server_table):
    cols = list(df_sales.columns)
    dict_server = {}
    NameofCol = server_table.select('NAME').collect()
    TypeofValue =  server_table.select('TYPE').collect()
    for i in range(0,len(NameofCol)):
        dict_server[NameofCol[i]['NAME']] = TypeofValue[i]['TYPE']
 
    for coll in cols:
        if coll not in dict_server:
            print("****!!warning!!****")
            print(f"Encountered new column : {coll}")
            continue
        if dict_server[coll] == "int":
            try:
                df_sales = df_sales.withColumn(coll, col(coll).cast(IntegerType()))
            except:
                df_sales = df_sales.na.fill(0)
        elif dict_server[coll] == "str":
            df_sales = df_sales.withColumn(coll, col(coll).cast(StringType()))
        elif dict_server[coll] == "float":
            df_sales = df_sales.withColumn(coll, col(coll).cast(FloatType()))   
            # df_sales = df_sales.withColumn(coll, col(coll).cast(DoubleType()))  #try if floating point issue 
        elif dict_server[coll] == "datetime":
            df_sales = df_sales.withColumn(coll, to_date(col(coll)))
            df_sales = df_sales.withColumn(coll,when(col(coll).isNull(), date(1900, 1, 1)).otherwise(col(coll)))
            # df_sales = df_sales.withColumn(coll, col(coll).cast(DateType()))
        else:
            print(f"Unknow data type!! : {dict_server[coll]}")
 
    return df_sales

def main(session: snowpark.Session, deal_id):
    start_time = datetime.now()
    df_inv_map = session.sql(f"select * from TEMP_MO_INVENTORY where DEAL_ID={deal_id}")
    df_sales_map = session.sql(f"select * from TEMP_MO_SALES where DEAL_ID={deal_id}")
         
    # Dropping all columns having null values
    null_counts = df_inv_map.select([count(when(col(c).isNull(), c)).alias(c) for c in df_inv_map.columns]).first().asDict()
    to_drop = [k for k, v in null_counts.items() if v == df_inv_map.count()] 
    df_inv_map = df_inv_map.drop(to_drop)
    null_counts = df_sales_map.select([count(when(col(c).isNull(), c)).alias(c) for c in df_sales_map.columns]).first().asDict()
    to_drop = [k for k, v in null_counts.items() if v == df_sales_map.count()]
    df_sales_map = df_sales_map.drop(to_drop)
    server_table = session.table("FIX_DATATYPES_APR")
    df_inv_map = fix_datatypes(df_inv_map,server_table)
    df_sales_map = fix_datatypes(df_sales_map,server_table)
    df_inv_map = df_inv_map.select('MAJOR_FRAME','MINOR_FRAME','SUB_MINOR_FRAME','LOCATION','CATEGORY_1','TOTAL_COST_EXTENDED','BOM_INV_DATE')
    # window_spec = Window.partitionBy('MAJOR_FRAME', 'MINOR_FRAME', 'SUB_MINOR_FRAME', 'LOCATION', 'CATEGORY_1').orderBy(desc('BOM_INV_DATE'))
    # df_inv_map = df_inv_map.withColumn('latest_date', max('BOM_INV_DATE'))

    # Calculate the maximum BOM_INV_DATE in the entire dataframe
    latest_date = df_inv_map.agg(max('BOM_INV_DATE')).collect()[0][0]
    # Filter the dataframe to keep only the rows with the latest date
    df_latest = df_inv_map.filter(col('BOM_INV_DATE') == latest_date)
    # df_latest = df_inv_map.filter(col('BOM_INV_DATE') == col('latest_date'))
    df_inv_map = df_latest.groupBy('MAJOR_FRAME', 'MINOR_FRAME', 'SUB_MINOR_FRAME', 'LOCATION', 'CATEGORY_1', 'BOM_INV_DATE')\
        .agg(sum('TOTAL_COST_EXTENDED').alias('TOTAL_COST_SUM'))
    # df_inv_map = df_inv_map.groupBy('MAJOR_FRAME', 'MINOR_FRAME', 'SUB_MINOR_FRAME', 'LOCATION', 'CATEGORY_1').agg(sum('TOTAL_COST_EXTENDED').alias('TOTAL_COST_SUM'))
    df_inv_map = df_inv_map.orderBy('MAJOR_FRAME', 'MINOR_FRAME', 'SUB_MINOR_FRAME', 'LOCATION', df_inv_map['TOTAL_COST_SUM'].desc())
    # Define the window specification
    window_spec = Window.partitionBy('MAJOR_FRAME', 'MINOR_FRAME', 'SUB_MINOR_FRAME', 'LOCATION').orderBy(desc('TOTAL_COST_SUM'))
    
    # Add a row number to each row within the partition
    df_with_row_num = df_inv_map.withColumn('row_num', row_number().over(window_spec))
    
    # Filter the rows to take only the latest 11 rows for each set of columns
    df_inv_map_latest_11 = df_with_row_num.filter(df_with_row_num.row_num <= 11)
    
    # Change the 11th row CATEGORY_1 to "Others"
    df_inv_map_latest_11 = df_inv_map_latest_11.withColumn(
        'CATEGORY_1', 
        when(df_inv_map_latest_11.row_num == 11, 'Others').otherwise(df_inv_map_latest_11.CATEGORY_1)
    )
    df_inv_map_latest_11 = df_inv_map_latest_11.withColumnRenamed("row_num", "RANK")
    df_inv_map_latest_11 = df_inv_map_latest_11.withColumn(
        'RANK_CRITERIA',lit('Inventory'))

    df_sales_map = df_sales_map.select('MAJOR_FRAME','MINOR_FRAME','SUB_MINOR_FRAME','LOCATION','CATEGORY_1','NET_SALES_$')
    df_sales_map = df_sales_map.groupBy('MAJOR_FRAME', 'MINOR_FRAME', 'SUB_MINOR_FRAME', 'LOCATION', 'CATEGORY_1').agg(sum('NET_SALES_$').alias('TOTAL_SALES_SUM'))
    df_sales_map = df_sales_map.orderBy('MAJOR_FRAME', 'MINOR_FRAME', 'SUB_MINOR_FRAME', 'LOCATION', df_sales_map['TOTAL_SALES_SUM'].desc())

    # Define the window specification
    window_spec = Window.partitionBy('MAJOR_FRAME', 'MINOR_FRAME', 'SUB_MINOR_FRAME', 'LOCATION').orderBy(desc('TOTAL_SALES_SUM'))
    
    # Add a row number to each row within the partition
    df_with_row_num = df_sales_map.withColumn('row_num', row_number().over(window_spec))
    
    # Filter the rows to take only the latest 11 rows for each set of columns
    df_sales_map_latest_11 = df_with_row_num.filter(df_with_row_num.row_num <= 11)
    
    # Change the 11th row CATEGORY_1 to "Others"
    df_sales_map_latest_11 = df_sales_map_latest_11.withColumn(
        'CATEGORY_1', 
        when(df_sales_map_latest_11.row_num == 11, 'Others').otherwise(df_sales_map_latest_11.CATEGORY_1)
    )

    df_sales_map_latest_11 = df_sales_map_latest_11.withColumnRenamed("row_num", "RANK")
    df_sales_map_latest_11 = df_sales_map_latest_11.withColumn(
        'RANK_CRITERIA',lit('Sales'))
    # Drop the 'row_num' column as it's no longer needed
    # df_sales_map_latest_11 = df_sales_map_latest_11.drop('row_num')
    df_inv_map_latest_11 = df_inv_map_latest_11.drop('TOTAL_COST_SUM','BOM_INV_DATE')
    df_sales_map_latest_11 = df_sales_map_latest_11.drop('TOTAL_SALES_SUM','BEG_MO')
    # return df_sales_map_latest_11
    df_final = df_inv_map_latest_11.unionAllByName(df_sales_map_latest_11)
    df_final = df_final.withColumn("DEAL_ID", lit(deal_id))
    df_final = df_final.withColumnRenamed("CATEGORY_1", "TOP_CATEGORIES")
    # final_df = df_inv_map_latest_11.join(df_sales_map_latest_11, on=['MAJOR_FRAME','MINOR_FRAME','SUB_MINOR_FRAME','LOCATION'], how='left')
    return df_final
    # df_final.write.mode('append').save_as_table('TOP_CATEGORY', column_order = 'name')
    #completion of SELL_THRU         
    # session.sql(f"UPDATE SNOWPARK_DEAL_STATUS_TRACKER SET STATUS = 'COMPLETED' WHERE DEAL_ID IN (deal_id) AND ANALYSIS IN ('SELL_THRU')").collect()
    # end_time = datetime.now()
    # total_time = end_time - start_time
    # total_seconds = total_time.total_seconds()
    # session.sql(f"UPDATE SNOWPARK_DEAL_STATUS_TRACKER SET START_TIME = '{start_time}', END_TIME = '{end_time}', TOTAL_TIME = {total_seconds} WHERE DEAL_ID IN (deal_id) AND ANALYSIS IN ('SELL_THRU')").collect()
    # return 'DATA PUSHED'

def test_main(session: snowpark.Session):
    return main(session, deal_id)
if __name__ == "__main__":
    result_dataframe = test_main(session)
    session.close()
