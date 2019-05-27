#' @title Auxiliary function for enabling correct conversion from R objects to python and GAMS
#' @name adapt_to_ret
#' @description Auxiliary function for enabling correct conversion from R objects to python and GAMS
#'
#' @param set_par a set or a parameter to be loaded in ixmp
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
