##
# [v1.21]
# desc: to use 'translate_data-v1_21.r'
# date: 05/03/2018
# by: David Lim
#
# [v1.1]
# desc: add the feature reading summary_stat data
# date: 03/29/2018
# by: David Lim
##

library(data.table)
library(DBI)
library(RPostgreSQL)
library(plyr)


# sets your current working directory where your .R files are
setwd("./R/d2cm/data_utility/translate_data (wafer_data & summary_stat)")

# imports the library source
source("./translate_data-v1_21.r")


##
# [1] PART I : wafer_data
#

## testing the connection
uFunc_SQL("query", "SELECT * FROM tool_list;")

## gets a single record for testing
waferDataRaw <- uFunc_SQL("query", "SELECT wdid, sn, encode(data, 'escape') as data, wid FROM wafer_data WHERE wdid = 268621;")

##
# example 1.
# - An old way using translateData()

# gets the tranlated data using translateData() function
waferDataFrame <- apply(waferDataRaw, 1, function(x) {
	waferData <- NULL
	waferData <- translateData(x[3]) # for columns [sensor_name : value] & msec
	waferData$wdid <- x[1] # for column of wdid
	waferData$sensor_name <- x[2] # for column of sn
	waferData$wid <- x[4] # for column of wid

	return(waferData)
})

waferDataFrame <- ldply(waferDataFrame, rbind)

# removes unnecessary data from memory
rm(waferDataRaw)
gc()

##
# example 2.
# - using translateWaferData(waferDataRaw, numOfCores)
# - using multicores
# - params :
#           1. waferDataRaw : result set from database query = [wdid, sn, encode(data, 'escape') as data, wid]
#           2. numOfCores : number of cores you use (for windows OS, must be 1)

waferDataFrame <- translateWaferData(waferDataRaw, 1)

# removes unnecessary data from memory
rm(waferDataRaw)
gc()


#####

##
# [2] PART II : summary_stat
#


##
# example 1.
#

# sets the DCP ID which collects summary_stat data to be used
dcp_list <- uFunc_SQL("query","SELECT dcpid, recipe, step_param, encode(params, 'escape') as params, stats, activate, step_sub_param, recipe_filter FROM dcp_list order by dcpid;")

dcp_list$dcpid

# gets recipe where the dcpid == 1
recipe <- dcp_list[dcp_list$dcpid == 1, ]$recipe

# gets the list of recipes
recipe <- unlist(strsplit(recipe,":"))

# gets summary stat 'raw' data from database
# using dcpid, date range, and recipe
sumStatRaw <- uFunc_SQL("query", paste0("SELECT A.wid, dcpid, process_id, encode(data, 'escape') as data FROM summary_stat as A LEFT JOIN (SELECT wid, value as process_id FROM wafer_header WHERE header = 'Process Job ID') as B ON A.wid = B.wid WHERE dcpid = ", dcp, " AND A.wid IN (SELECT wid FROM wafer_summary WHERE process_datetime BETWEEN \'", paste0(fromToDate[1], " 00:00:00"), "\' AND \'", paste0(fromToDate[2], " 23:59:59"),"\' AND recipe IN (", paste("\'", recipe, "\'", sep = "", collapse=", "), "))"))

# translates summary stat 'raw' data to summary stat list
# - translateSummaryStatData(sumStatRaw, numOfCores)
# - params :
#           1. sumStatRaw : result set from database query = [wid, dcpid, process_id, encode(data, 'escape') as data]
#           2. numOfCores : number of cores you use (for windows OS, must be 1)
sumStatList <- translateSummaryStatData(sumStatRaw, 1)

# removes unnecessary data from memory
rm(sumStatRaw)
gc()

##
# example 2.
#

# gets summary stat 'raw' data from database
# using limit 10, dcpid = 1
sumStatRaw <- uFunc_SQL("query", paste0("SELECT A.wid, dcpid, pmid, encode(data, 'escape') as data, recipe, process_datetime
                                         FROM summary_stat as A
										JOIN (
										SELECT wid, pmid, recipe, process_datetime
										FROM wafer_summary
										WHERE 1=1
										)
										as B ON A.wid = B.wid
										WHERE dcpid = 1
										limit 10"))

# translates summary stat 'raw' data to summary stat list
# - translateSummaryStatData(sumStatRaw, numOfCores)
# - params :
#           1. sumStatRaw : result set from database query = [wid, dcpid, process_id, encode(data, 'escape') as data]
#           2. numOfCores : number of cores you use (for windows OS, must be 1)
sumStatList <- translateSummaryStatData(sumStatRaw, 1)

# removes unnecessary data from memory
rm(sumStatRaw)
gc()

