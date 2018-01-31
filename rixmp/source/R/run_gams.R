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
run_gams = function(model, ingdx, outgdx, model_pth=NULL, args='LogOption=4') {
  cmd = paste("gams ", model, " --in=", ingdx, " --out=", outgdx, " ",
              args, sep = '')
  if (!is.null(model_pth))
    cmd = paste(cmd, " Inputdir=", model_pth, sep='')
  system(cmd)
}
