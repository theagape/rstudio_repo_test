##
# Utility functions for interfacing Matlab and R
# to use D2CM2 PostgreSQL DB (compressed column in table)
#
# [v1.21]
# - desc: bug fixing translateWaferData() / variable name 'resultSet' -> 'waferDataRaw'
# - date: 05/03/2018
# - by: David Lim
#
# [v1.1]
# - desc: translateSummaryStatData() and translateWaferData()
# - date: 03/29/2018
# - by: David Lim
#
# [v1.0]
# - desc: toRaw(), translateData()
# - date: 01/03/2018
# - by: David Lim
#
library(data.table)
library(DBI)
library(RPostgreSQL)
library(plyr)
library(parallel)

##
# Converts 'text' into hex raw data.
# This function 'toRaw' is called in 'translateData'.
# It is from Sool Park.
#
toRaw <- function(text) {
	sst <- strsplit(text, "")[[1]]
	as.raw(as.hexmode(paste0(sst[c(TRUE, FALSE)], sst[c(FALSE, TRUE)])))
}

##
# Uncompresses data in 'data' column from wafer_data & summary_stat tables
# and deserializes it
# - return value :
#	i) [wafer_data] array of vector ['msec', 'data']
#	ii) [summary_stat] sensor_names (field names~StepInfo~Stat)
#
translateData <- function(x) {
  dd_unserial <- unserialize(memDecompress(toRaw(x), "bzip2"))

  return(dd_unserial)
}

##
# Uncompresses wafer_data records in 'data' column from wafer_data
# and deserializes it, using translateData(), using multicores
# - params : waferDataRaw [wdid, sn, encode(data, 'escape') as data, wid]
# - return value : [wafer_data] array of vector ['msec', 'data']
#
translateWaferData <- function(waferDataRaw, noCores) {

	waferDataFrame <- mclapply(split(waferDataRaw, seq(nrow(waferDataRaw))), function(x) {
		waferData <- translateData(paste(x[3], "")) # for columns [sensor_name : value] & msec
		waferData$wdid <- x[1] # for column of wdid
		waferData$sensor_name <- x[2] # for column of sn
		waferData$wid <- x[4] # for column of wid

		return(waferData)
	}, mc.cores = 1)

	waferDataFrame <- ldply(waferDataFrame, rbind)

	return(waferDataFrame)
}

##
# Uncompresses summary_stat records in 'data' column from summary_stat
# and deserializes it, using translateData(), using multicores
# - params : summaryStatRaw [wid, dcpid, process_id, encode(data, 'escape') as data]
# - return value : [summary_stat] sensor_names (field names~StepInfo~Stat)~wid~dcpid
#
translateSummaryStatData <- function(summaryStatRaw, noCores) {

	# dTotal <- NULL
	dSumStat <- mclapply(split(summaryStatRaw, seq(nrow(summaryStatRaw))), function(x) {
		dd_unserial <- translateData(x$data)
		Sys.sleep(0)
		dd_unserial$wid <- x$wid
		dd_unserial$dcpid <- x$dcpid
		dd_unserial$pmid <- x$pmid
		dd_unserial$recipe <- x$recipe
		dd_unserial$process_datetime <- x$process_datetime
		return(dd_unserial)
	}, mc.cores = noCores)

	gc()
	# dTotal <- append(dTotal, dSumStat)

	dSumStat <- rbindlist(dSumStat, use.names = TRUE, fill=TRUE)

	return(dSumStat)
}

getSummaryStatData <- function(dcpId, toolId = NULL, pmId = NULL, limit = NULL, startDate = NULL, endDate = NULL) {

	toolExpr <- NULL
	if (!is.null(toolId)) {
		print(toolId)
		toolExpr
	}

	if (!is.null(pmId)) {
		print(pmId)
	}

	limitExpr <- NULL
	if (!is.null(limit)) {
		print(limit)
		limitExpr <- paste('LIMIT', limit)
	}

	if (!is.null(startDate)) {
		print(startDate)
	}

	if (!is.null(endDate)) {
		print(endDate)
	}

	sumStatData <- uFunc_SQL("query", paste("SELECT A.wid, dcpid, pmid, encode(data, 'escape') as data, recipe, process_datetime
                                         FROM summary_stat as A
											JOIN (
											SELECT wid, pmid, recipe, process_datetime
											FROM wafer_summary
											WHERE 1=1
											)
											as B ON A.wid = B.wid
											WHERE dcpid = 1",
											limitExpr))

	return(sumStatData)
}

##
# SQL wrapping functions
# from : server.R @ D2CM2_M12
# inserted by : David Lim
# date : 03/29/2018
#
uFunc_SQL <- function(type, sql){
	drv <- dbDriver("PostgreSQL")
	# con <- dbConnect(drv, dbname="d2cm", host = "localhost", port=5432, user="postgres", password="passw0rd")
	con <- dbConnect(drv, dbname="d2cm", host = "167.191.112.104", port=5432, user="postgres", password="passw0rd")
	#con <- dbConnect(drv, dbname=cfg$database$dbname, host = cfg$database$host, port=5432, user=cfg$database$user, password=cfg$database$password)

	on.exit(dbDisconnect(con))

	if(type=="query") {
		rs <- dbGetQuery(con, sql)
	}
	else { # insert delete
		rs <- dbSendQuery(con, sql)
		dbClearResult(rs)
	}

	return(rs)
}

##
# SQL wrapping functions
# from : server.R @ D2CM2_M12
# inserted by : David Lim
# date : 03/29/2018
#
uFunc_dbCmd <- function(type, name=NULL, value=NULL){
	drv <- dbDriver("PostgreSQL")
	# con <- dbConnect(drv, dbname="d2cm", host = "localhost", port=5432, user="postgres", password="passw0rd")
	con <- dbConnect(drv, dbname="d2cm", host = "167.191.112.104", port=5432, user="postgres", password="passw0rd")
	#con <- dbConnect(drv, dbname=cfg$database$dbname, host = cfg$database$host, port=5432, user=cfg$database$user, password=cfg$database$password)

	on.exit(dbDisconnect(con))

	if (type=="writetable") {
		rs <- dbWriteTable(con, name, value, row.names=FALSE, append=TRUE)
	}
}