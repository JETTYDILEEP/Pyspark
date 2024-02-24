 # -*- coding: utf-8 -*-
import os
import shutil
import traceback 
import pyspark
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import * 
from pyspark.sql.types import  StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, TimestampType

def read_data (spark,input_file,Schema):
    ''' 
    spark_session : spark 
    for input_file : input_file 
    for schema : Schema
    '''

     #replace this line with your  actual code
    
    # Write your code
    df = spark.read.format('csv').option("header", True).schema(Schema).load('inputfile/Employment_data.csv')
    # print(df.count())
    return df  # return the final dataframe


def clean_data(input_df):
    '''
    for input file: input_df # The Final dataframe of the read_data function.
    '''
    df = input_df
    df = df.na.drop('all')
    df = df.dropDuplicates()
    # print(df.count())
    return df  # return the final dataframe


def latest_empolyees(input_df):
    '''
    for input file: input_df // The final dataframe of clean_data funtion.
    '''
    print("-------------------------")
    print("Starting latest_empolyees")
    print("-------------------------")
    df = input_df
    df = df.withColumn("Year",year(col('Date')))\
           .withColumn("Month",month(col('Date')))
    df = df.orderBy(col('Year').desc(),col('Month').desc())
    df = df.withColumn('Month_Name',date_format('Date','MMMM'))
    df = df.drop('Date','Month')
    df = df.select('Variable','Sex','Alberta','British_Columbia','Manitoba','Brunswick','Newfoundland_and_Labrador','Nova_Scotia','Ontario','Prince_Edward_Island','Quebec','Saskatchewan','Year','Month_Name')
    df = df.limit(1000)
    # df.show()
    return df     #return the final dataframe


def Gender_Wise(input_df):
    '''
    for input file: input_df // The final dataframe of latest_empolyees funtion.
    '''
    print("-------------------------")
    print("Starting Gender_Wise")
    print("-------------------------")

    df = input_df   
    #Write your code
    df = df.groupBy("Sex").sum('Alberta','British_Columbia','Manitoba','Brunswick','Newfoundland_and_Labrador','Nova_Scotia','Ontario','Prince_Edward_Island','Quebec','Saskatchewan')
    df = df.select("Sex",
                   round('sum(Alberta)',2).alias('Sum_Alberta'),
                   round('sum(British_Columbia)', 2).alias('Sum_British_Columbia'),
                   round('sum(Manitoba)', 2).alias('Sum_Manitoba'),
                   round('sum(Brunswick)', 2).alias('Sum_Brunswick'),
                   round('sum(Newfoundland_and_Labrador)', 2).alias('Sum_Newfoundland_and_Labrador'),
                   round('sum(Nova_Scotia)', 2).alias('Sum_Nova_Scotia'),
                   round('sum(Ontario)', 2).alias('Sum_Ontario'),
                   round('sum(Prince_Edward_Island)', 2).alias('Sum_Prince_Edward_Island'),
                   round('sum(Quebec)', 2).alias('Sum_Quebec'),
                   round('sum(Saskatchewan)', 2).alias('Sum_Saskatchewan'),
                   )
    # df.show()
    return df     #return the final dataframe


def load_data(data,output_path):
    '''
    The following are the parameters :

        data : All the tasks output data frame.

        outputpath: Location where the file needs to be saved 

    '''

    if (data.count() != 0):
        print("Loading the data",output_path)
        data.write.option('header',True).format('csv').save(output_path)
        #Write your code above this line
    else:
        print("Empty dataframe, hence cannot save the data",output_path)



def main():

    """ Main driver program to control the flow of execution.
        Please DO NOT change anything here.
    """
    #Clean the output files for fresh execution
    outputfile_cleanup()
    #Get a new spark session
    spark = (SparkSession.builder
                         .appName("Employment data Analysis")
                         .master("local")
                         .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")


            
    Schema = StructType([ \
        StructField("Date",StringType(),True), \
        StructField("Variable",StringType(),True), \
        StructField("Sex",StringType(),True), \
        StructField("Alberta",DoubleType(),True),  
        StructField("British_Columbia",DoubleType(),True),\
        StructField("Manitoba",DoubleType(),True),\
        StructField("Brunswick",DoubleType(),True),\
        StructField("Newfoundland_and_Labrador",DoubleType(),True),\
        StructField("Nova_Scotia",DoubleType(),True),\
        StructField("Ontario",DoubleType(),True),\
        StructField("Prince_Edward_Island",DoubleType(),True),\
        StructField("Quebec",DoubleType(),True),\
        StructField("Saskatchewan",DoubleType(),True)
        ])

    cwd = os.getcwd()
    dirname = os.path.dirname(cwd)
    input_file = "file:\\"+ dirname + "\inputfile\Employment_data.csv"
    output_path = "file:\\"+ dirname + "\\frescoplay\output"
    result_1_path = output_path + "\latest_empolyees"
    result_2_path = output_path + "\Gender_Wise"
   

    try:
        task_1 = read_data(spark,input_file,Schema)
    except Exception as e:
        print("Getting error in the read_data function",e)
    try:
        task_2 = clean_data(task_1)
    except Exception as e:
        print("Getting error in the task_2 function",e)
        traceback.print_exc()
    try:
        task_3 = latest_empolyees(task_2)
    except Exception as e:
        print("Getting error in the task_3 function",e)
        traceback.print_exc()
    try:
        task_4 = Gender_Wise(task_3)
    except Exception as e:
        print("Getting error in the task_4 function",e)
        traceback.print_exc()

    #
    try:
        load_data(task_3,result_1_path)
    except Exception as e:
        print("Getting error while loading latest_empolyees",e)
    try:
        load_data(task_4,result_2_path)
    except Exception as e:
        print("Getting error while loading Gender_Wise",e)
    #

    spark.stop()

def outputfile_cleanup():

    """ Clean up the output files for a fresh execution.
        This is executed every time a job is run. 
        Please DO NOT change anything here.
    """

    cwd = os.getcwd()
    dirname = os.path.dirname(cwd)
    path = dirname + "\\frescoplay\output\\"
    if (os.path.isdir(path)):
        try:
            shutil.rmtree(path)  
            print("% s removed successfully" % path)
            os.mkdir(path)  
        except OSError as error:  
            print(error)  
    else:
        print("The directory does not exist. Creating..% s", path)
        os.mkdir(path)

if __name__ == "__main__":
	main()