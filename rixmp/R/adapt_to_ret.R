#' @title Auxiliary function for enabling correct conversion from R dataframes to python and GAMS
#' @name adapt_to_ret
#' @description Auxiliary function for enabling correct conversion from R dataframes to python and GAMS
#'
#' @param set_par dataframe corresponding to a set or a parameter to be loaded in ixmp
#'
#' @examples 
#' a.df = data.frame( i = c('a','b'), value = c(350 , 600) , unit = 'cases')
#' scen$add_par("a", adapt_to_ret(a.df))
#'
#' @export

adapt_to_ret = function(set_par) {
  tmp_par = set_par


  if (is.data.frame(set_par)){
    if (length(set_par) == 1){
      tmp_par[1] = as.character(tmp_par[[1]])
    } else {
      tmp_par[,] <- sapply(tmp_par[,,drop=FALSE],as.character)
    }
  } else {
    tmp_par = as.character(tmp_par)
  }
  return(reticulate::r_to_py(tmp_par))
}
