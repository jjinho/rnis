#' Query NIS SQLite Core and Severity Files by Year and Diagnostic ICD9 Codes
#'
#' Query NIS by year and ICD9 codes and returns a Dataframe of the Core and Severity data
#' @param year NIS Year
#' @param dx_codes 		List of ICD9 diagnoses given as strings
#' @param nis_path		Path to the NIS DB files. Defaults to "~/NIS"
#' @keywords nis
#' @export
#' @examples
#' nis_sql_dx(year, dx_codes)
nis_sql_dx <- function(year, nis_path="~/NIS", dx_codes) {
  # Preprocess the ICD9 list
  # Remove all periods
  dx_codes <- gsub("\\.", "", dx_codes)
	
	# Convert to a string with each ICD9 diagnosis encased in single quotes
  dx_codes <- paste("(", toString(paste("'", dx_codes, "'", sep="")), ")", sep="")
  
  # Get path to DB by year
  db_path <- gsub("path", nis_path, "path/y_/nis_y_.db")
	db_path <- gsub("y_", toString(year), db_path)
	
  # Connection
  con <- RSQLite::dbConnect(RSQLite::SQLite(), db_path)
  
  if(year > 2008) {
    # Years that have 25 DXs
		nis_core_query <- "SELECT * FROM core_y_ WHERE DX1 IN dx_codes OR DX2 IN dx_codes OR DX3 IN dx_codes OR DX4 IN dx_codes OR DX5 IN dx_codes OR DX6 IN dx_codes OR DX7 IN dx_codes OR DX8 IN dx_codes OR DX9 IN dx_codes OR DX10 IN dx_codes OR DX11 IN dx_codes OR DX12 IN dx_codes OR DX13 IN dx_codes OR DX14 IN dx_codes OR DX15 IN dx_codes OR DX16 IN dx_codes OR DX17 IN dx_codes OR DX18 IN dx_codes OR DX19 IN dx_codes OR DX20 IN dx_codes OR DX21 IN dx_codes OR DX22 IN dx_codes OR DX23 IN dx_codes OR DX24 IN dx_codes OR DX25 IN dx_codes"; 
  } else {
    # Years that have 15 Dx
    nis_core_query <- "SELECT * FROM core_y_ WHERE (DX1 IN dx_codes OR DX2 IN dx_codes OR DX3 IN dx_codes OR DX4 IN dx_codes OR DX5 IN dx_codes OR DX6 IN dx_codes OR DX7 IN dx_codes OR DX8 IN dx_codes OR DX9 IN dx_codes OR DX10 IN dx_codes OR DX11 IN dx_codes OR DX12 IN dx_codes OR DX13 IN dx_codes OR DX14 IN dx_codes OR DX15 IN dx_codes)"; 
  }
  # Put the correct year
  nis_core_query <- gsub("y_", toString(year), nis_core_query)
  # Put the correct list of ICD9 diagnoses
  nis_core_query <- gsub("dx_codes", dx_codes, nis_core_query)
  
  # Perform the query
  q <- RSQLite::dbSendQuery(con, nis_core_query)
  
  # Get the data
  core_df <- RSQLite::dbFetch(q, n=-1)
  
	# Clear the query to prevent memory leaks
  RSQLite::dbClearResult(q)
	
	# Obtaining the Severity data
	if(year == 2012) {
		key_list <- paste("(", toString(paste("'", core_df$KEY_NIS, "'", sep="")), ")", sep="")
	} else {
		key_list <- paste("(", toString(paste("'", core_df$KEY, "'", sep="")), ")", sep="")
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
	
	if(year == 2005) core_df <- format_nis_core_(core_df, format_2005)
	if(year == 2006) core_df <- format_nis_core_(core_df, format_2006)
	if(year == 2007) core_df <- format_nis_core_(core_df, format_2007)
	if(year == 2008) core_df <- format_nis_core_(core_df, format_2008)
	if(year == 2009) core_df <- format_nis_core_(core_df, format_2009)
	if(year == 2010) core_df <- format_nis_core_(core_df, format_2010)
	if(year == 2011) core_df <- format_nis_core_(core_df, format_2011)
	if(year == 2012) core_df <- format_nis_core_(core_df, format_2012)

	# Merge core_df and sev_df
	if(year == 2012) {
		core_df <- dplyr::merge(core_df, sev_df, by="KEY_NIS")	
	} else {
		core_df <- dplyr::merge(core_df, sev_df, by="KEY")
	}
	rm(sev_df)
	
		#----------------------------------------------------------------------------
	# Cleaning the data
	# Rename FEMALE to GENDER
	names(core_df)[names(core_df) == "FEMALE"] <- "GENDER"

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
	core_df <- unique(core_df)
	
	core_df$CM_ALCOHOL     <- relevel_0_1_(core_df$CM_ALCOHOL)
	core_df$CM_ANEMDEF     <- relevel_0_1_(core_df$CM_ANEMDEF)
	core_df$CM_AIDS        <- relevel_0_1_(core_df$CM_AIDS)
	core_df$CM_ARTH        <- relevel_0_1_(core_df$CM_ARTH)
	core_df$CM_BLDLOSS     <- relevel_0_1_(core_df$CM_BLDLOSS)
	core_df$CM_CHF         <- relevel_0_1_(core_df$CM_CHF)
	core_df$CM_CHRNLUNG    <- relevel_0_1_(core_df$CM_CHRNLUNG)
	core_df$CM_COAG        <- relevel_0_1_(core_df$CM_COAG)
	core_df$CM_DEPRESS     <- relevel_0_1_(core_df$CM_DEPRESS)
	core_df$CM_DM          <- relevel_0_1_(core_df$CM_DM)
	core_df$CM_DMCX  			 <- relevel_0_1_(core_df$CM_DMCX)
	core_df$CM_DRUG        <- relevel_0_1_(core_df$CM_DRUG)
	core_df$CM_HTN_C       <- relevel_0_1_(core_df$CM_HTN_C)
	core_df$CM_HYPOTHY     <- relevel_0_1_(core_df$CM_HYPOTHY)
	core_df$CM_LIVER       <- relevel_0_1_(core_df$CM_LIVER)
	core_df$CM_LYMPH       <- relevel_0_1_(core_df$CM_LYMPH)
	core_df$CM_LYTES       <- relevel_0_1_(core_df$CM_LYTES)
	core_df$CM_METS        <- relevel_0_1_(core_df$CM_METS)
	core_df$CM_NEURO       <- relevel_0_1_(core_df$CM_NEURO)
	core_df$CM_OBESE       <- relevel_0_1_(core_df$CM_OBESE)
	core_df$CM_PARA        <- relevel_0_1_(core_df$CM_PARA)
	core_df$CM_PERIVASC    <- relevel_0_1_(core_df$CM_PERIVASC)
	core_df$CM_PSYCH       <- relevel_0_1_(core_df$CM_PSYCH)
	core_df$CM_PULMCIRC    <- relevel_0_1_(core_df$CM_PULMCIRC)
	core_df$CM_RENLFAIL    <- relevel_0_1_(core_df$CM_RENLFAIL)
	core_df$CM_TUMOR       <- relevel_0_1_(core_df$CM_TUMOR)
	core_df$CM_ULCER       <- relevel_0_1_(core_df$CM_ULCER)
	core_df$CM_VALVE       <- relevel_0_1_(core_df$CM_VALVE)
	core_df$CM_WGHTLOSS    <- relevel_0_1_(core_df$CM_WGHTLOSS)
	
	core_df$ELECTIVE    <- relevel_0_1_(core_df$ELECTIVE)
	core_df$GENDER      <- relevel_gender_(core_df$GENDER)
	core_df$DIED        <- relevel_0_1_(core_df$DIED)
	core_df$RACE        <- relevel_race_(core_df$RACE)
	core_df$PAY1        <- relevel_pay_(core_df$PAY1)
	
	# Van Walraven Score
	# Calculating the Elixhauser-van Walraven Comorbidity Composite Score
	core_df <- core_df %>%
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
		core_df$ZIPInc_Qrtl <- relevel_zipinc_qrtl_(	core_df$ZIPInc_Qrtl)
	} else {
		core_df$ZIPINC_QRTL <- relevel_zipinc_qrtl_(	core_df$ZIPINC_QRTL)
	}
	
	# Rename Variables
	# ZIPInc_Qrtl
	if(year == 2005) {
		core_df$ZIPINC_QRTL <- core_df$ZIPInc_Qrtl
		core_df$ZIPInc_Qrtl <- NULL
	}
	# KEY_NIS
	if(year == 2012) {
		core_df$KEY <- core_df$KEY_NIS
		core_df$KEY_NIS <- NULL
	}
	# HOSPID.x
	core_df$HOSPID		<- core_df$HOSPID.x	
	core_df$HOSPID.x	<- NULL
	core_df$HOSPID.y	<- NULL
	if(year == 2012) {
		core_df$HOSP_NIS		<- core_df$HOSP_NIS.x
		core_df$HOSP_NIS.x	<- NULL
		core_df$HOSP_NIS.y 	<- NULL
	}
		
	return(core_df)
}
