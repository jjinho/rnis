#' NIS Include by Procedure Codes
#'
#' Returns rows from NIS Core that have the procedure codes
#' @param pr_codes List of ICD9 procedure codes given as strings
#' @keywords nis
#' @export
#' @examples
#' nis_inc_pr(df, pr_codes)
nis_inc_pr <- function(df, pr_codes) {
  dplyr::filter(df, PR1  %in% pr_codes |
										PR2  %in% pr_codes |
										PR3  %in% pr_codes |
										PR4  %in% pr_codes |
										PR5  %in% pr_codes |
										PR6  %in% pr_codes |
										PR7  %in% pr_codes |
										PR8  %in% pr_codes |
										PR9  %in% pr_codes |
										PR10 %in% pr_codes |
										PR11 %in% pr_codes |
										PR12 %in% pr_codes |
										PR13 %in% pr_codes |
										PR14 %in% pr_codes |
										PR15 %in% pr_codes)
}