from pyspark import SparkContext
from pyspark.streaming import StreamingContext,DStream
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row
import json
from collections import OrderedDict
import re
import ast
import pyspark
from itertools import chain
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,isnan,when,count,create_map,lit
import pyspark.sql.functions as fn
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import FeatureHasher
from datetime import datetime



conf=SparkContext("local[2]","NetworkWordCount")
sc=StreamingContext(conf,1)
#sqc=SQLContext(conf)
spark=SparkSession.builder.appName('CrimeReport').getOrCreate()

data_dict = {'FRAUD':1, 'SUICIDE':2, 'SEX OFFENSES FORCIBLE':3, 'LIQUOR LAWS':4, 
'SECONDARY CODES':5, 'FAMILY OFFENSES':6, 'MISSING PERSON':7, 'OTHER OFFENSES':8, 
'DRIVING UNDER THE INFLUENCE':9, 'WARRANTS':10, 'ARSON':11, 'SEX OFFENSES NON FORCIBLE':12,
'FORGERY/COUNTERFEITING':13, 'GAMBLING':14, 'BRIBERY':15, 'ASSAULT':16, 'DRUNKENNESS':17,
'EXTORTION':18, 'TREA':19, 'WEAPON LAWS':20, 'LOITERING':21, 'SUSPICIOUS OCC':22, 
'ROBBERY':23, 'PROSTITUTION':24, 'EMBEZZLEMENT':25, 'BAD CHECKS':26, 'DISORDERLY CONDUCT':27,
'RUNAWAY':28, 'RECOVERED VEHICLE':29, 'VANDALISM':30,'DRUG/NARCOTIC':31, 
'PORNOGRAPHY/OBSCENE MAT':32, 'TRESPASS':33,'VEHICLE THEFT':34, 'NON-CRIMINAL':35, 
'STOLEN PROPERTY':36, 'LARCENY/THEFT':37, 'KIDNAPPING':38,'BURGLARY':39}

dof={'Monday':1,'Tuesday':2,'Wednesday':3,'Thursday':4,'Friday':5,'Saturday':6,'Sunday':7}


pddis={'MISSION':1,'BAYVIEW':2,'CENTRAL':3,'TARAVAL':4, 'TENDERLOIN':5,'INGLESIDE':6, 'PARK':7,'SOUTHERN':8, 'RICHMOND':9,'NORTHERN':10}



def stringToNum(df1):
	mapping_expr1 = create_map([lit(x) for x in chain(*data_dict.items())])
	mapping_expr2 = create_map([lit(x) for x in chain(*pddis.items())])
	#mapping_expr3 = create_map([lit(x) for x in chain(*dof.items())])
	df1=df1.withColumn("Category", mapping_expr1.getItem(col("Category")))
	df1=df1.withColumn("PdDistrict", mapping_expr2.getItem(col("PdDistrict")))
	#df1=df1.withcolumn("DayOfWeek", mapping_expr3.getItem(col("DayOfWeek")))
	
	return df1
	

	
def preprocess(df1):
	
	#df1=df1.drop("Resolution","address","Description")

	'''
	
	#convert nonnumerical data to numerical
	indexer = StringIndexer(inputCol="category", outputCol="Category")
	df1 = indexer.fit(df1).transform(df1)
	indexer = StringIndexer(inputCol="dayOfweek", outputCol="DayOfWeek")
	df1 = indexer.fit(df1).transform(df1)
	indexer = StringIndexer(inputCol="district", outputCol="District")
	df1 = indexer.fit(df1).transform(df1)
	'''

	df1=stringToNum(df1)
	
	#changing type of latitude and longitude
	#df1=df1.withColumn("X",df1.X.cast(DoubleType()))
	#df1=df1.withColumn("Y",df1.Y.cast(DoubleType()))
	#df1.show(10)
	#print(df1.dtypes)
	
	#split the date field into date,time, year, hour, min,seconds
	
	#df1.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df1.columns])
	'''
	date_split=pyspark.sql.functions.split(df1['Date'],' ')
	df1=df1.withColumn('Dates',date_split.getItem(0))
	df1=df1.withColumn('Time',date_split.getItem(1))
	
	
	date_split=pyspark.sql.functions.split(df1['Dates'],'-')
	df1=df1.withColumn('Year',date_split.getItem(0))
	df1=df1.withColumn('Month',date_split.getItem(1))
	df1=df1.withColumn('Day',date_split.getItem(2))
	
	date_split=pyspark.sql.functions.split(df1['Time'],':')
	df1=df1.withColumn('Hour',date_split.getItem(0))
	df1=df1.withColumn('Minute',date_split.getItem(1))
	df1=df1.withColumn('Second',date_split.getItem(2))
	
	df1=df1.withColumn("Year",df1.Year.cast(IntegerType()))
	df1=df1.withColumn("Month",df1.Month.cast(IntegerType()))
	df1=df1.withColumn("Day",df1.Day.cast(IntegerType()))
	df1=df1.withColumn("Hour",df1.Hour.cast(IntegerType()))
	df1=df1.withColumn("Minute",df1.Minute.cast(IntegerType()))
	df1=df1.withColumn("Second",df1.Second.cast(IntegerType()))
	df1=df1.drop("Date","Dates","Time")
	'''
	#df1.show(10)
	
	#vector Assembler
	

	vecAssembler = VectorAssembler(inputCols=['X','Y'],outputCol="Features")
	combine_XY=vecAssembler.transform(df1)
	cols=['X','Y']
	remove_XY=combine_XY.drop(*cols)
	minmax_scaler=MinMaxScaler(inputCol="Features",outputCol="scaled")
	scaled=minmax_scaler.fit(remove_XY).transform(remove_XY)
	
	'''
	new_df = (remove_XY
		.withColumn('Day', dayofmonth(col('Dates'))) 
    	.withColumn('Month', month(col('Dates'))) 
    	.withColumn('Year', year(col('Dates'))) 
    	.withColumn('Hour', hour(col('Dates')))
    	.withColumn('Minutes', minute(col('Dates'))) 
    	.withColumn('Seconds', second(col('Dates')))
    	)
	#new_df.show(5)

	minmax_scaler=MinMaxScaler(inputCol="Features",outputCol="Scaled_XY")
	scaled=minmax_scaler.fit(remove_XY).transform(remove_XY)
 	
	scaled.show(20)

	'''

	#splitting date

	#transformed = (scaled.withColumn("day", dayofmonth(col("Dates"))).withColumn("month", date_format(col("Dates"), "MM")).withColumn("year", year(col("Dates"))).withColumn('second',second(df.Dates)).withColumn('minute',minute(df.Dates)).withColumn('hour',hour(df.Dates)))
    
	transformed = (scaled
        .withColumn("day", dayofmonth(col("Dates")))
        .withColumn("month", date_format(col("Dates"), "MM"))
        .withColumn("year", year(col("Dates")))
        .withColumn('second',second(df1.Dates))
        .withColumn('minute',minute(df1.Dates))
        .withColumn('hour',hour(df1.Dates))
        )

	data_df = transformed.withColumn("month", transformed["month"].cast(IntegerType()))

	data_df=data_df.drop('Dates','Resolution','Descript','DayOfWeek','Address')

	data_df.show(20)
	

	#df1.show(10)

	
	#print(df1.dtypes)






def jsonToDf(rdd):

	if not rdd.isEmpty():
		cols=["Dates","Category","Descript","DayOfWeek","PdDistrict","Resolution","Address","X","Y"]
		'''
		cols = StructType([StructField("Date", StringType(), True),\
                                      StructField("Category", StringType(), True),\
                                      StructField("Description", StringType(), True),\
                                      StructField("DayOfWeek", StringType(), True),\
                                      StructField("District", StringType(), True),\
                                      StructField("Resolution", StringType(), True),\
                                      StructField("Address", StringType(), True),\
                                      StructField("X", StringType(), True),\
                                      StructField("Y", StringType(), True)
                                     ])
        '''
		df=spark.read.json(rdd)
		for row in df.rdd.toLocalIterator():
			'''
			r1=".".join(row)
			print(type(r1))
			r2=eval(r1)
			print(type(r2))
			#r2=r1.split("\\n")
			#print(type(r2))
			#result = [k.split(",") for k in r2]
			#print(result)
			'''
			for r in row:

				res=ast.literal_eval(r)
				row1=[]
				for line in res:
					line=re.sub('\\n','',line)
					line=re.sub(r',(?=[^"]*"(?:[^"]*"[^"]*")*[^"]*$)',"",line)
					line=re.sub('"',"",line)
					rowList=line.split(',')
					if not "Dates" in rowList:
						row1.append(rowList)
			#newrdd = conf.parallelize(r2)
			df1=spark.createDataFrame(row1,schema=cols)
			#df1.show(5)
			
			df1=(df1
				.withColumn("Dates",col("Dates").cast(TimestampType()))
            	.withColumn("X",col("X").cast(FloatType()))
            	.withColumn("Y",col("Y").cast(FloatType()))
            	)

            
			preprocess(df1)
			#df1.show(20)
			


lines = sc.socketTextStream("localhost", 6100)
lines.foreachRDD(lambda rdd: jsonToDf(rdd))
#print('##################################################### printing words..')
#print(lines)
#print(type(lines))
sc.start()
sc.awaitTermination()
sc.stop(stopSparkContext=False)





	


