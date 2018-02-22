#' ixmp core
#'
#' This page provides documentation for the ixmp-R implementation
#'
#' Main features:
#'
#'  - a platform core linked to a database instance,
#'
#'  - integration with the |MESSAGEix| Integrated Assessment model
#'    and other numerical models
#' @name rixmp
NULL

#' @import rJava
#' @importFrom methods setRefClass
#' @importFrom utils globalVariables
NULL

.onAttach = function(libname, pkgname){
  packageStartupMessage(
    sprintf("Loaded rixmp v%s. See ?rixmp for help, citation(\"rixmp\") for use in publication.\n",
            utils::packageDescription(pkgname)$Version) )
}

utils::globalVariables(c("ixmp_path"))

.onLoad <- function(libname, pkgname) {

  if (Sys.getenv("JAVA_HOME")!="")
    Sys.setenv(JAVA_HOME="")

  ## Set path where jar files are stored
  ixmp_r_path <<- Sys.getenv("IXMP_R_PATH")

  ## Set path ixmp folder
  ixmp_path <<- Sys.getenv("IXMP_PATH")

  if(ixmp_path == "")
    warning("Check IXMP installation")

  if(ixmp_r_path == "")
    stop("Check IXMP installation")

  # path to access the local db
  local_path <<- file.path(paste0(gsub("Documents.*","",path.expand("~")),".local/ixmp"))

  java_path_1 <- file.path(ixmp_r_path,"ixmp.jar")

  jars2 <- list.files(file.path(ixmp_r_path, "lib"),'.*')
  dir <- file.path(ixmp_r_path, "lib")
  java_path_2 <- paste(dir, jars2, sep='/')

  rJava::.jinit(java_path_1)
  rJava::.jaddClassPath(java_path_2)
  rJava::.jaddClassPath(ixmp_r_path)

  ModelConfig <<- list(default = list(model_file = gsub("/","\\\\" , file.path( "." , "{model}.gms") ),
                                      inp = gsub("/","\\\\" , file.path( ".", "{model}_in.gdx") ),
                                      outp = gsub("/","\\\\" , file.path(".", "{model}_out.gdx") ),
                                      args = gsub("/","\\\\" , paste('--in=',file.path( ".", "{model}_in.gdx"),' --out=',file.path(".", "{model}_out.gdx")))))
}

#' @description a function to convert a Java LinkedList into an R list
#' @noRd
.getRList = function(jList){
  if(!is.null(jList) & !jList$isEmpty()){
    unlist(lapply(1:jList$size(), function(i){ jList$get(as.integer(i-1)) } ))
  } else {
    vector()
  }
}

#' @description a function to convert an R list to a Java LinkedList
#' @noRd
.getJList = function(rList){
  jList = rJava::new(J("java.util.LinkedList"))
  if(!is.null(rList)){
    lapply(1:length(rList), function(i){ jList$add(rList[i]) } )
  }
  return(jList)
}


#' @description a function to convert an R list to a Java LinkedList
#' @noRd
.getCleanDims <- function(rList, rListDefault=NULL) {
  jList <- rJava::new(J("java.util.LinkedList"))
  if (!is.null(rList)) {
    for (i in 1:length(rList)) {
      jList$add(rList[i])
    }
  } else if (!is.null(rListDefault)) {
    for (i in 1:length(rListDefault)) {
      jList$add(rListDefault[i])
    }
  }
  return(jList)
}

#' @description a function to convert an R element as list into a Java LinkedList
#' @noRd
.getEleAsJList <- function(eleList) {
  jList <- rJava::new(J("java.util.LinkedList"))
  for (i in 1:length(eleList)) {
    jList$add(eleList[i])
  }
  return(jList)
  # todo: option to add an element as dataframe by index name
}

#' @description remove element from set or parameter
#' @noRd
.removeElement <- function(item, key) {
  if (item$getDim() > 0) {
    if (is.list(key) | is.data.frame(key)) {
      item$removeElement(.getEleAsJList(as.character(key)))
    } else {
      item$removeElement(as.character(key))
    }
  } else {
    if (is.list(key) | is.data.frame(key)) {
      item$removeElement(.getEleAsJList(as.character(key)))
    } else {
      item$removeElement(as.character(key))
    }
  }
}

