#' NIS Exclude by Diagnostic Codes
#'
#' Returns rows from NIS Core that do not have the diagnostic codes
#' @param dx_codes List of ICD9 diagnostic codes given as strings
#' @keywords nis
#' @export
#' @examples
#' nis_exc_dx(df, dx_codes)
nis_exc_dx <- function(df, dx_codes) {
  if(as.numeric(df$YEAR) >= 2009) {
    dplyr::filter(df, !DX1  %in% dx_codes & 
											!DX2  %in% dx_codes & 
											!DX3  %in% dx_codes & 
											!DX4  %in% dx_codes & 
											!DX5  %in% dx_codes & 
											!DX6  %in% dx_codes & 
											!DX7  %in% dx_codes & 
											!DX8  %in% dx_codes & 
											!DX9  %in% dx_codes & 
											!DX10 %in% dx_codes & 
											!DX11 %in% dx_codes & 
											!DX12 %in% dx_codes & 
											!DX13 %in% dx_codes & 
											!DX14 %in% dx_codes & 
											!DX15 %in% dx_codes &
											!DX16 %in% dx_codes & 
											!DX17 %in% dx_codes & 
											!DX18 %in% dx_codes & 
											!DX19 %in% dx_codes & 
											!DX20 %in% dx_codes & 
											!DX21 %in% dx_codes & 
											!DX22 %in% dx_codes & 
											!DX23 %in% dx_codes & 
											!DX24 %in% dx_codes & 
											!DX25 %in% dx_codes)
  } else {
    dplyr::filter(df, !DX1  %in% dx_codes & 
											!DX2  %in% dx_codes & 
											!DX3  %in% dx_codes & 
											!DX4  %in% dx_codes & 
											!DX5  %in% dx_codes & 
											!DX6  %in% dx_codes & 
											!DX7  %in% dx_codes & 
											!DX8  %in% dx_codes & 
											!DX9  %in% dx_codes & 
											!DX10 %in% dx_codes & 
											!DX11 %in% dx_codes & 
											!DX12 %in% dx_codes & 
											!DX13 %in% dx_codes & 
											!DX14 %in% dx_codes & 
											!DX15 %in% dx_codes)
  }
}
