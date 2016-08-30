#' Query NIS SQLite Hospital File by Year
#'
#' Query NIS Hospital File by year and returns a Dataframe
#' @param year NIS Year
#' @keywords nis
#' @export
#' @examples
#' nis_sql_hosp(year)
nis_sql_hosp <- function(year) {  
  # Get path to DB by year
  RSQLite::db_path <- gsub("y_", toString(year), "~/NIS/y_/nis_y_.db")
	
  # Connection
  con <- RSQLite::dbConnect(SQLite(), db_path)
  
	nis_hospital_query <- "SELECT * FROM hospital_y_;"
	
	# Put the correct year
  nis_hospital_query <- gsub("y_", toString(year), nis_hospital_query)
  
  # Perform the query
  q <- RSQLite::dbSendQuery(con, nis_hospital_query)
  
  # Get the data
  hospital_df <- RSQLite::dbFetch(q, n=-1)
  
	# Clear the query to prevent memory leaks
  RSQLite::dbClearResult(q)

	format_2005 <- c("Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char")
	format_2006 <- c("Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char")
	format_2007 <- c("Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char")
	format_2008 <- c("Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char")
	format_2009 <- c("Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char")
	format_2010 <- c("Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char")
	format_2011 <- c("Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char")
	format_2012 <- c("Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char","Char")

	format_nis_hospital_	<- function(df, format) {
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
	
	if(year == 2005) hospital_df <- format_nis_hospital_(hospital_df, format_2005)
	if(year == 2006) hospital_df <- format_nis_hospital_(hospital_df, format_2006)
	if(year == 2007) hospital_df <- format_nis_hospital_(hospital_df, format_2007)
	if(year == 2008) hospital_df <- format_nis_hospital_(hospital_df, format_2008)
	if(year == 2009) hospital_df <- format_nis_hospital_(hospital_df, format_2009)
	if(year == 2010) hospital_df <- format_nis_hospital_(hospital_df, format_2010)
	if(year == 2011) hospital_df <- format_nis_hospital_(hospital_df, format_2011)
	if(year == 2012) hospital_df <- format_nis_hospital_(hospital_df, format_2012)
	
	return(hospital_df)
}