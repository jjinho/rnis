% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/nis_sql_hosp.r
\name{nis_sql_hosp}
\alias{nis_sql_hosp}
\title{Query NIS SQLite Hospital File by Year}
\usage{
nis_sql_hosp(year, nis_path = "~/NIS")
}
\arguments{
\item{year}{NIS Year}

\item{nis_path}{Path to the NIS DB files. Defaults to "~/NIS"}
}
\description{
Query NIS Hospital File by year and returns a Dataframe
}
\examples{
nis_sql_hosp(year)
}
\keyword{nis}

