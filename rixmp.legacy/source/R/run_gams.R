#' @title Auxiliary functions for executing GAMS
#' @name run_gams
#' @description Auxiliary functions for executing GAMS
#'
#' @param model_file the path to the gams file
#' @param args arguments related to the GAMS code (input/output gdx paths, etc.)
#' @param gams_args additional arguments for the CLI call to gams
#' `LogOption=4` prints output to stdout (not console) and the log file
#'
#' @export
run_gams = function(model_file, args, gams_args='LogOption=4') {
  cmd = paste("gams ", model_file," ", args, sep = '')
  cmd = paste(cmd, " Inputdir=", gsub("/","\\\\" , dirname(model_file) ), sep='')
  cmd = paste(cmd, gams_args, sep=' ')
  system(cmd)
}
