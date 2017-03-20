# Set the HDFS (WASB) location of example data
bigDataDirRoot <- "/example/data"

# create a local folder for storaging data temporarily
source <- "/tmp/AirOnTimeCSV2012"
dir.create(source)

# Download data to the tmp folder
remoteDir <- "http://packages.revolutionanalytics.com/datasets/AirOnTimeCSV2012"
download.file(file.path(remoteDir, "airOT201201.csv"), file.path(source, "airOT201201.csv"))
download.file(file.path(remoteDir, "airOT201202.csv"), file.path(source, "airOT201202.csv"))
download.file(file.path(remoteDir, "airOT201203.csv"), file.path(source, "airOT201203.csv"))
download.file(file.path(remoteDir, "airOT201204.csv"), file.path(source, "airOT201204.csv"))
download.file(file.path(remoteDir, "airOT201205.csv"), file.path(source, "airOT201205.csv"))
download.file(file.path(remoteDir, "airOT201206.csv"), file.path(source, "airOT201206.csv"))
download.file(file.path(remoteDir, "airOT201207.csv"), file.path(source, "airOT201207.csv"))
download.file(file.path(remoteDir, "airOT201208.csv"), file.path(source, "airOT201208.csv"))
download.file(file.path(remoteDir, "airOT201209.csv"), file.path(source, "airOT201209.csv"))
download.file(file.path(remoteDir, "airOT201210.csv"), file.path(source, "airOT201210.csv"))
download.file(file.path(remoteDir, "airOT201211.csv"), file.path(source, "airOT201211.csv"))
download.file(file.path(remoteDir, "airOT201212.csv"), file.path(source, "airOT201212.csv"))

# Set directory in bigDataDirRoot to load the data into
inputDir <- file.path(bigDataDirRoot,"AirOnTimeCSV2012") 

# Make the directory
rxHadoopMakeDir(inputDir)

# Copy the data from source to input
rxHadoopCopyFromLocal(source, bigDataDirRoot)

# Define the HDFS (WASB) file system
hdfsFS <- RxHdfsFileSystem()

# Create info list for the airline data
airlineColInfo <- list(
  DAY_OF_WEEK = list(type = "factor"),
  ORIGIN = list(type = "factor"),
  DEST = list(type = "factor"),
  DEP_TIME = list(type = "integer"),
  ARR_DEL15 = list(type = "logical"),
  ARR_DELAY = list(type = "integer"))

# get all the column names
varNames <- names(airlineColInfo)

# Define the text data source in hdfs
airOnTimeData <- RxTextData(inputDir, colInfo = airlineColInfo, varsToKeep = varNames, fileSystem = hdfsFS)

# Define the text data source in local system edge node
airOnTimeDataLocal <- RxTextData(source, colInfo = airlineColInfo, varsToKeep = varNames)

# Import csv into a data frame; calculate processing time
flightsDf <-   read.csv(file.path(source, "airOT201201.csv"), nrows=100000)

# formula to use
formula = "ARR_DEL15 ~ DAY_OF_WEEK + DEP_TIME"
formulalinearreg = "ARR_DELAY ~ DAY_OF_WEEK + DEP_TIME + ORIGIN"


###############################################################################
# RUNNING ON LOCAL COMPUTE CONTEXT - WITH DATA FRAME and CRAN R PACKAGE       #
###############################################################################

head(flightsDf)

# Running logistic regression
system.time(
  modelGLM <- glm(formula, family=binomial(logit), data = flightsDf) #148 secs for 100,000 rows
)

# Generate model summary
summary(modelGLM)

# Running linear regression
system.time(
  modelLM01 <- lm(formulalinearreg, data = flightsDf01) # 25 secs for 1 month data
)

# Generate model summary
summary(modelLM01)

###############################################################################
# RUNNING ON LOCAL COMPUTE CONTEXT - WITH DATA STORED ON LOCAL FILE SYSTEM    #
###############################################################################


# convert all 12 months csv to XDF on edge node local drive
airOnTimeDataLocalXdf <- "airOnTimeDataLocalXdf.xdf"
rxImport(inData=airOnTimeDataLocal, outFile = airOnTimeDataLocalXdf, overwrite = TRUE) #48 seconds to convert


# Check the summary statistics of the data
rxSummary(~., airOnTimeDataLocalXdf)

rxSummary( ~ ARR_DELAY : DAY_OF_WEEK + ARR_DELAY : ORIGIN, data = airOnTimeDataLocalXdf)

rxHistogram(~F(ARR_DELAY), title="HISTOGRAM: Arrival Delay", xAxisMinMax = (1:200), yAxisMinMax = (0:80000), data = airOnTimeDataLocalXdf)


# Run a logistic regression
system.time(
  modelLocalXdf <- rxLogit(formula, data = airOnTimeDataLocalXdf) #10 seconds on 12 months
)

# Display a summary 
summary(modelLocalXdf)


###############################################################################
# RUNNING ON SPARK COMPUTE CONTEXT - WITH DATA STORED ON HDFS FILE SYSTEM     #
###############################################################################

# Define the Spark compute context 
mySparkCluster <- RxSpark()

# Set the compute context 
rxSetComputeContext(mySparkCluster)

rxGetInfo(airOnTimeData, numRows = 10)

# Run a logistic regression 
system.time(  
  modelSpark <- rxLogit(formula, data = airOnTimeData)  #94 seconds on 12 months
)

# Display a summary
summary(modelSpark)

####################################
# Convert to composite XDF         #
####################################

bigAirXdfName <-file.path(bigDataDirRoot,"AirlineOnTime2012") 
airData <- RxXdfData( bigAirXdfName,
                      fileSystem = hdfsFS )


blockSize <- 250000
numRowsToRead = -1

rxImport(inData = airOnTimeData,
         outFile = airData,
         rowsPerRead = blockSize,
         overwrite = TRUE,
         numRows = numRowsToRead )   # 60 seconds to convert


system.time(  
  modelSparkCompositeXdf <- rxLogit(formula, data = airData)  #53 seconds on 12 months
)

summary(modelSparkCompositeXdf)



###############################################
# READ HIVE AND PARQUET                       #
###############################################


#..create a Spark compute context

myHadoopCluster <- rxSparkConnect(reset = TRUE)

#..retrieve some sample data from Hive and run a model 

hiveData <- RxHiveData("select * from hivesampletable", 
                       colInfo = list(devicemake = list(type = "factor")))
rxGetInfo(hiveData, getVarInfo = TRUE)

rxLinMod(querydwelltime ~ devicemake, data=hiveData)

#..retrieve some sample data from Parquet and run a model 

rxHadoopMakeDir('/share')
rxHadoopCopyFromLocal(file.path(rxGetOption('sampleDataDir'), 'claimsParquet/'), '/share/')
pqData <- RxParquetData('/share/claimsParquet',
                        colInfo = list(
                          age    = list(type = "factor"),
                          car.age = list(type = "factor"),
                          type = list(type = "factor")
                        ) )
rxGetInfo(pqData, getVarInfo = TRUE)

rxNaiveBayes(type ~ age + cost, data = pqData)

#..check on Spark data objects, cleanup, and close the Spark session 

lsObj <- rxSparkListData() # two data objs are cached
lsObj
rxSparkRemoveData(lsObj)
rxSparkListData() # it should show empty list
rxSparkDisconnect(myHadoopCluster)
