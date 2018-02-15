#' @title Auxiliary functions for executing GAMS
#' @name run_gams
#' @description Auxiliary functions for executing GAMS
#'
#' @param model model name
#' @param ingdx input gdx file
#' @param outgdx output gdx file
#' @param model_pth the path to the gams file
#' @param args additional arguments for the CLI call to gams
#'
#' @export
run_gams = function(model_file, args, gams_args='LogOption=4') {
  cmd = paste("gams ", model_file," ", args, sep = '')
  cmd = paste(cmd, " Inputdir=", dirname(model_file), sep='')
  cmd = paste(cmd, gams_args, sep=' ')
  system(cmd)
}
