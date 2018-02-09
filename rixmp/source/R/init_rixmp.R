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

utils::globalVariables(c("message_ix_path"))

.onLoad <- function(libname, pkgname) {

  if (Sys.getenv("JAVA_HOME")!="")
    Sys.setenv(JAVA_HOME="")

  ixmp_r_path <<- Sys.getenv("IXMP_R_PATH")

  ## Set path ixmp folder in message_ix working copy
  message_ix_path <<- Sys.getenv("MESSAGE_IX_PATH")

  if(message_ix_path == "")
    warning("Check MESSAGE model installation XXXXX ")

  if(ixmp_r_path == "")
    stop("Check IXMP installation XXXXX")

  # path to access the local db
  local_path <<- file.path(paste0(gsub("Documents.*","",path.expand("~")),".local/ixmp"))

  java_path_1 <- file.path(ixmp_r_path,"ixmp.jar")

  jars2 <- list.files(file.path(ixmp_r_path, "lib"),'.*')
  dir <- file.path(ixmp_r_path, "lib")
  java_path_2 <- paste(dir, jars2, sep='/')

  rJava::.jinit(java_path_1)
  rJava::.jaddClassPath(java_path_2)
  rJava::.jaddClassPath(ixmp_r_path)

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


