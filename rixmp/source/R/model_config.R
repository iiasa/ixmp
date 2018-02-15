#' @title Auxiliary functions configuring input and output for GAMS
#' @name model_config
#' @description Auxiliary functions configuring input and output for GAMS
#'
#' @param model model name
#' @param case case
#'
#' @export
model_config = function(model, case) {
  inp = file.path( ".", paste(model, '_in.gdx', sep = ''))
  outp = file.path(".", paste(model, '_out.gdx', sep = ''))

  ModelConfig2 <<- list(default = list(model_file = file.path( "." , paste(model, '.gms', sep = '')),
                                      inp = inp,
                                      outp = outp,
                                      args = paste('--in=',inp,' --out=',outp, sep = '')))
  return(ModelConfig2)
}
