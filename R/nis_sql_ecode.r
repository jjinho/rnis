#' Query NIS SQLite Core and Severity Files by Year and ICD9 E Codes
#'
#' Query NIS by year and ICD9 E Codes and returns a Dataframe of the Core and Severity data
#' @param year NIS Year
#' @param ecodes 			List of ICD9 procedures given as strings
#' @param nis_path		Path to the NIS DB files. Defaults to "~/NIS"
#' @keywords nis
#' @export
#' @examples
#' nis_sql_ecode(year, ecodes)
nis_sql_ecode <- function(year, nis_path="~/NIS", ecodes) {
  # Preprocess the ICD9 list
  # Remove all periods
  ecodes <- gsub("\\.", "", ecodes)
  
	# Convert to a string with each ICD9 ECODE encased in single quotes
  ecodes <- paste("(", toString(paste("'", ecodes, "'", sep="")), ")", sep="")
  
  # Get path to DB by year
  db_path <- gsub("path", nis_path, "path/y_/nis_y_.db")
	db_path <- gsub("y_", toString(year), db_path)

  # Connection
  con <- RSQLite::dbConnect(RSQLite::SQLite(), db_path)
  
  nis_ecodes_query <- "SELECT * FROM core_y_ WHERE ECODE1 IN ecodes OR ECODE2 IN ecodes OR ECODE3 IN ecodes OR ECODE4 IN ecodes"; 
  
  # Put the correct year
  nis_ecode_query <- gsub("y_", toString(year), nis_ecode_query)
  nis_ecode_query <- gsub("ecode", ecodes, nis_ecode_query)
  
  # Perform the query
  q <- RSQLite::dbSendQuery(con, nis_ecode_query)
  
  # Get the data
  ecode_df <- RSQLite::dbFetch(q, n=-1)
  
	# Clear the query to prevent memory leaks
  RSQLite::dbClearResult(q)
	
	# Obtaining the Severity data
	if(year == 2012) {
		key_list <- paste("(", toString(paste("'", ecode_df$KEY_NIS, "'", sep="")), ")", sep="")
	} else {
		key_list <- paste("(", toString(paste("'", ecode_df$KEY, "'", sep="")), ")", sep="")
	}
	if(year == 2012) {
		nis_sev_query <- "SELECT * FROM severity_year_ WHERE KEY_NIS in list;"
	} else {
		nis_sev_query <- "SELECT * FROM severity_year_ WHERE KEY in list;"
	}
  nis_sev_query <- gsub("year_", toString(year), nis_sev_query)
  nis_sev_query <- gsub("list", key_list, nis_sev_query)
	
	# Perform the query
  q <- RSQLite::dbSendQuery(con, nis_sev_query)
  
  # Get the data
  sev_df <- RSQLite::dbFetch(q, n=-1)
	
  # Clear the query to prevent memory leaks
  RSQLite::dbClearResult(q)
  
	# Format
	format_nis_sev_ <- function(df) {
		for(n in names(df)) {
			df[[n]] <- as.factor(df[[n]])
		}
		return(df)
	}
	
	sev_df <- format_nis_sev_(sev_df)

	#----------------------------------------------------------------------------
	# Reformating
	format_2005 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Num","Num","Char","Char")
	format_2006 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Num","Num","Char","Char")
	format_2007 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char")
	format_2008 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char")
	format_2009 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char")
	format_2010 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char","Char")
	format_2011 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Char","Char","Num","Num","Char","Char","Char","Char")
	format_2012 <- c("Num","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Char","Char","Char","Num","Num","Num","Char","Char","Num","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Num","Char","Num","Char","Char","Char","Char")

	format_nis_core_	<- function(df, format) {
		counter = 1
		for(f in format) {
			# Remove unknowns
			df[,counter][grep("-[0-9]+", df[,counter])] <- NA
			
			if(f == "Num") {
				# Num
				df[,counter] <- as.numeric(df[,counter])
			} else {
				# Char
				df[,counter] <- as.factor(df[,counter])
			}
			counter <- counter + 1
		}
		return(df)
	}
	
	if(year == 2005) ecode_df <- format_nis_core_(ecode_df, format_2005)
	if(year == 2006) ecode_df <- format_nis_core_(ecode_df, format_2006)
	if(year == 2007) ecode_df <- format_nis_core_(ecode_df, format_2007)
	if(year == 2008) ecode_df <- format_nis_core_(ecode_df, format_2008)
	if(year == 2009) ecode_df <- format_nis_core_(ecode_df, format_2009)
	if(year == 2010) ecode_df <- format_nis_core_(ecode_df, format_2010)
	if(year == 2011) ecode_df <- format_nis_core_(ecode_df, format_2011)
	if(year == 2012) ecode_df <- format_nis_core_(ecode_df, format_2012)

	# Merge ecode_df and sev_df
	if(year == 2012) {
		ecode_df <- merge(ecode_df, sev_df, by="KEY_NIS")	
	} else {
		ecode_df <- merge(ecode_df, sev_df, by="KEY")
	}
	rm(sev_df)
	
		#----------------------------------------------------------------------------
	# Cleaning the data
	# Rename FEMALE to GENDER
	names(ecode_df)[names(ecode_df) == "FEMALE"] <- "GENDER"

	relevel_gender_ <- function(female) {
		levels(female)[levels(female) == "0"] <- "M"
		levels(female)[levels(female) == "1"] <- "F"
		return(female)
	}

	relevel_0_1_ <- function(f) {
		levels(f)[levels(f) == "0"] <- "N"
		levels(f)[levels(f) == "1"] <- "Y"
		return(f)
	}

	relevel_race_ <- function(f) {
		levels(f)[levels(f) == "1"] <- "White"
		levels(f)[levels(f) == "2"] <- "Black"
		levels(f)[levels(f) == "3"] <- "Hispanic"
		levels(f)[levels(f) == "4"] <- "Asian_Pacific"
		levels(f)[levels(f) == "5"] <- "Native_American"
		levels(f)[levels(f) == "6"] <- "Other"
		return(f)
	}

	relevel_pay_ <- function(f) {
		levels(f)[levels(f) == "1"] <- "Medicare"
		levels(f)[levels(f) == "2"] <- "Medicaid"
		levels(f)[levels(f) == "3"] <- "Private_Insurance"
		levels(f)[levels(f) == "4"] <- "Self_Pay"
		levels(f)[levels(f) == "5"] <- "No Charge"
		levels(f)[levels(f) == "6"] <- "Other"
		return(f)
	}

	relevel_zipinc_qrtl_ <- function(f) {
		levels(f)[levels(f) == "1"] <- "Quartile1"
		levels(f)[levels(f) == "2"] <- "Quartile2"
		levels(f)[levels(f) == "3"] <- "Quartile3"
		levels(f)[levels(f) == "4"] <- "Quartile4"
		return(f)
	}

	# Remove duplicates
	ecode_df <- unique(ecode_df)
	
	ecode_df$CM_ALCOHOL     <- relevel_0_1_(ecode_df$CM_ALCOHOL)
	ecode_df$CM_ANEMDEF     <- relevel_0_1_(ecode_df$CM_ANEMDEF)
	ecode_df$CM_AIDS        <- relevel_0_1_(ecode_df$CM_AIDS)
	ecode_df$CM_ARTH        <- relevel_0_1_(ecode_df$CM_ARTH)
	ecode_df$CM_BLDLOSS     <- relevel_0_1_(ecode_df$CM_BLDLOSS)
	ecode_df$CM_CHF         <- relevel_0_1_(ecode_df$CM_CHF)
	ecode_df$CM_CHRNLUNG    <- relevel_0_1_(ecode_df$CM_CHRNLUNG)
	ecode_df$CM_COAG        <- relevel_0_1_(ecode_df$CM_COAG)
	ecode_df$CM_DEPRESS     <- relevel_0_1_(ecode_df$CM_DEPRESS)
	ecode_df$CM_DM          <- relevel_0_1_(ecode_df$CM_DM)
	ecode_df$CM_DMCX  			<- relevel_0_1_(ecode_df$CM_DMCX)
	ecode_df$CM_DRUG        <- relevel_0_1_(ecode_df$CM_DRUG)
	ecode_df$CM_HTN_C       <- relevel_0_1_(ecode_df$CM_HTN_C)
	ecode_df$CM_HYPOTHY     <- relevel_0_1_(ecode_df$CM_HYPOTHY)
	ecode_df$CM_LIVER       <- relevel_0_1_(ecode_df$CM_LIVER)
	ecode_df$CM_LYMPH       <- relevel_0_1_(ecode_df$CM_LYMPH)
	ecode_df$CM_LYTES       <- relevel_0_1_(ecode_df$CM_LYTES)
	ecode_df$CM_METS        <- relevel_0_1_(ecode_df$CM_METS)
	ecode_df$CM_NEURO       <- relevel_0_1_(ecode_df$CM_NEURO)
	ecode_df$CM_OBESE       <- relevel_0_1_(ecode_df$CM_OBESE)
	ecode_df$CM_PARA        <- relevel_0_1_(ecode_df$CM_PARA)
	ecode_df$CM_PERIVASC    <- relevel_0_1_(ecode_df$CM_PERIVASC)
	ecode_df$CM_PSYCH       <- relevel_0_1_(ecode_df$CM_PSYCH)
	ecode_df$CM_PULMCIRC    <- relevel_0_1_(ecode_df$CM_PULMCIRC)
	ecode_df$CM_RENLFAIL    <- relevel_0_1_(ecode_df$CM_RENLFAIL)
	ecode_df$CM_TUMOR       <- relevel_0_1_(ecode_df$CM_TUMOR)
	ecode_df$CM_ULCER       <- relevel_0_1_(ecode_df$CM_ULCER)
	ecode_df$CM_VALVE       <- relevel_0_1_(ecode_df$CM_VALVE)
	ecode_df$CM_WGHTLOSS    <- relevel_0_1_(ecode_df$CM_WGHTLOSS)
	
	ecode_df$ELECTIVE    <- relevel_0_1_(ecode_df$ELECTIVE)
	ecode_df$GENDER      <- relevel_gender_(ecode_df$GENDER)
	ecode_df$DIED        <- relevel_0_1_(ecode_df$DIED)
	ecode_df$RACE        <- relevel_race_(ecode_df$RACE)
	ecode_df$PAY1        <- relevel_pay_(ecode_df$PAY1)
	
	# Van Walraven Score
	# Calculating the Elixhauser-van Walraven Comorbidity Composite Score
	ecode_df <- ecode_df dplyr::`%>%`
		dplyr::mutate(vanwalraven = ifelse(CM_AIDS			== "Y", 0,	0) +
																ifelse(CM_ALCOHOL		== "Y", 0,	0) +
																ifelse(CM_ANEMDEF		== "Y", -2,	0) +
																ifelse(CM_ARTH			== "Y", 0,	0) +
																ifelse(CM_BLDLOSS		== "Y", -2,	0) +
																ifelse(CM_CHF				== "Y", 7,	0) +
																ifelse(CM_CHRNLUNG	== "Y", 6,	0) +
																ifelse(CM_COAG			== "Y", 3,	0) +
																ifelse(CM_DEPRESS		== "Y", -3,	0) +
																ifelse(CM_DM				== "Y", 0,	0) +
																ifelse(CM_DMCX			== "Y", 0,	0) +
																ifelse(CM_DRUG			== "Y", -7,	0) +
																ifelse(CM_HTN_C			== "Y", 0,	0) +
																ifelse(CM_HYPOTHY		== "Y", 0,	0) +
																ifelse(CM_LIVER			== "Y", 11,	0) +
																ifelse(CM_LYMPH			== "Y", 9,	0) +
																ifelse(CM_LYTES			== "Y", 5,	0) +
																ifelse(CM_METS			== "Y", 12,	0) +
																ifelse(CM_NEURO			== "Y", 6,	0) +
																ifelse(CM_OBESE			== "Y", -4,	0) +
																ifelse(CM_PARA			== "Y", 7,	0) +
																ifelse(CM_PERIVASC	== "Y", 2,	0) +
																ifelse(CM_PSYCH			== "Y", 0,	0) +
																ifelse(CM_PULMCIRC	== "Y", 4,	0) +
																ifelse(CM_RENLFAIL	== "Y", 5,	0) +
																ifelse(CM_TUMOR			== "Y", 4,	0) +
																ifelse(CM_ULCER			== "Y", 0,	0) +
																ifelse(CM_VALVE			== "Y", -1,	0) +
																ifelse(CM_WGHTLOSS	== "Y", 6,	0))												
	
	if(year == 2005) {
		ecode_df$ZIPInc_Qrtl <- relevel_zipinc_qrtl_(	ecode_df$ZIPInc_Qrtl)
	} else {
		ecode_df$ZIPINC_QRTL <- relevel_zipinc_qrtl_(	ecode_df$ZIPINC_QRTL)
	}
	
	# Rename Variables
	# ZIPInc_Qrtl
	if(year == 2005) {
		ecode_df$ZIPINC_QRTL <- ecode_df$ZIPInc_Qrtl
		ecode_df$ZIPInc_Qrtl <- NULL
	}
	# KEY_NIS
	if(year == 2012) {
		ecode_df$KEY <- ecode_df$KEY_NIS
		ecode_df$KEY_NIS <- NULL
	}
	# HOSPID.x
	ecode_df$HOSPID		<- ecode_df$HOSPID.x	
	ecode_df$HOSPID.x	<- NULL
	ecode_df$HOSPID.y	<- NULL
	if(year == 2012) {
		ecode_df$HOSP_NIS		<- ecode_df$HOSP_NIS.x
		ecode_df$HOSP_NIS.x	<- NULL
		ecode_df$HOSP_NIS.y <- NULL
	}
	
	return(ecode_df)
}