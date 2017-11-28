# 
#' Classes and functions for the R interface
#' of the IX modeling platform (IXMP)
#

## import rJava library, assign 2g of RAM, start the VM and get the entrypoint
require(rJava)

## Set path to the message_ix repository clone location
message_ix_path = Sys.getenv("MESSAGE_IX_PATH")

## Set path to installed ixmp files
ixmp_r_path = Sys.getenv("IXMP_R_PATH")
java.pars <- paste0("-Djava.ext.dirs=", file.path(ixmp_r_path, "lib"))
options(java.parameters = java.pars)

## Launch the java virtual machine, add ixmp_r_path (in addition to jar file)
.jinit(file.path(ixmp_r_path, "ixToolbox.jar")) 
.jaddClassPath(ixmp_r_path)

# define auxiliary references to Java classes
java.Platform <- J("at.ac.iiasa.ixmp.Platform")
java.Integer  <- J("java.lang.Integer")
java.Double   <- J("java.lang.Double")
java.LinkedList <- J("java.util.LinkedList")
java.HashMap  <- J("java.util.HashMap")
java.LinkedHashMap <- J("java.util.LinkedHashMap")

#' The class 'Platform' is the central access point to
#' and accessing dataobjects (timeseries and reference data) 
#' and datastructures (structured model input data and results).
Platform <- setRefClass("Platform",
  fields = c(.jobj = "jobjRef"),

  methods = list(

    # launch the Platform 
    initialize = function(dbprops=NULL, dbtype=NULL) {
      
        if (is.null(dbtype)) { 
            .jobj <<- new(java.Platform, "R", dbprops) 
        } else {
            .jobj <<- new(java.Platform, "R", dbprops, dbtype)
        }
        
        return(.self)      
    },
    
    # initialize a new datastructure (structured model input data and solution)
    # or get an existing datastructure from the ix database
    DataStructure = function(model, scen, version=NULL,
                             scheme=NULL, annotation=NULL) {
        if (!is.null(version) && version=='new') {
            j_ds = .jobj$newDataStructure(model, scen, scheme, annotation)  
        } else if (!is.null(version)) {
            j_ds = .jobj$getDataStructure(model, scen, as.integer(version))
        } else {
            j_ds = .jobj$getDataStructure(model, scen)
        } 
      
        return(ixDataStructure(.jobj, model, scen, j_ds))     
    },     
    
    units = function() {
        return(.jobj$getUnitList())     
    }                   
  )
)

#' The class 'Datastructure' is a generic collection
#' of all data for a model instance (sets and parameters), as well as
#' the solution of a model run (levels/marginals of variables and equations).

#' The class includes functions to make changes to the data,
#' export all data to and import a solution from GAMS gdx,
#' and save the datastructure to the IXMP database.
#' All changes are logged to facilitate version control.
ixDataStructure <- setRefClass("ixDataStructure",
  fields = c(.jobj = "jobjRef",
             .platform = "jobjRef",                           
             # if the platform class were referenced directly,
             # importing this script would already launch the Platform
             model = "character",
             scenario = "character",
             scheme = "character" ),

  methods = list(

  ## initialize a new R-class datastructure object (via the Platform class) 
    initialize = function(platform, model, scen, javaobj) {
        model    <<- model                
        scenario <<- scen
        .platform <<- platform 
        .jobj    <<- javaobj

        return(.self)
    },

  ## functions for platform management

    # check out the dataobject/datastructure from the database for making changes
    check_out = function(timeseries_only = FALSE) {
        .jobj$checkOut(timeseries_only)
    },

    # commit all changes made to the dataobject/datastructure to the database
    commit = function(annotation) {
        .jobj$commit(annotation)
    },

    # discard all changes, reload all items of the datastructure from database
    discard_changes = function() {
        .jobj$discardChanges()
    },

    # set this instance of a model/scenario dataobject as default version
    set_as_default = function() {
        .jobj$setAsDefaultVersion()
    },

    # check whether this dataobject is set as default in the database
    is_default = function() {
        return(.jobj$isDefault())
    },

    # get the timestamp of the last update/edit of this dataobject  
    last_update = function() {
      return(.jobj$getLastUpdateTimestamp()$toString())
    },  
  
    # get the run id of this dataobject
    run_id = function() {
        return(.jobj$getRunId())
    },
  
    # get the version number of this dataobject
    version = function() {
        return(.jobj$getVersion())
    },

    # clone the datastructure (with new model and scenario name)
    #Parameters
    #----------
    #model : string
    #new model name
    #scen : string
    #new scenario name
    #annotation : string
    #explanatory comment (optional)
    #keep_sol : boolean
    #indicator whether to include an existing solution
    #shift_fyear
    #in the cloned datastructure (default: True)
    clone = function(new_model = model, new_scen = scenario, 
                     annotation, keep_sol = TRUE, shift_fyear=0) {
    
        return(ixDataStructure(.platform, new_model, new_scen,
                               .jobj$clone(new_model, new_scen, annotation, keep_sol, as.integer(shift_fyear))))
    },

    # write the datastructure to GAMS gdx
    #Parameters
    #----------
    #path : string
    #path to the folder
    #filename : string
    #name of the gdx file
    #include_var_equ : boolean
    #indicator whether to include variables/equations
    #in gdx (default: False)
    to_gdx = function(path, filename, include_var_equ=FALSE) {
        .jobj$toGDX(path, filename, include_var_equ)
    },
  
    # read solution from GAMS gdx
    read_sol_from_gdx = function(path, filename, comment=NULL,
                                 var_list=NULL, equ_list=NULL, check_sol=TRUE) {
	      .jobj$readSolutionFromGDX(path, filename, comment, 
                                  .getJList(var_list), .getJList(equ_list), 
                                  check_sol)
    },
 
    # remove solution from run
    remove_sol = function() {
        .jobj$removeSolution()
    }, 

    # solve the model (export to gdx, execute GAMS, import the solution)
    #Parameters
    #----------
    #model : string
    #model (e.g., MESSAGE) or GAMS code located in current working dir
    #case : string
    #identifier of the gdx (for MESSAGE model instances),
    #defaults to 'model_name_scen_name'
    #comment : string
    #additional comment added to changelog when importing the solution
    #check_sol : boolean
    #flag whether a non-optimal solution raises an exception
    #(default = True)
    solve = function(model='MESSAGE', case=NULL, comment=NULL, check_sol=TRUE) {
    
        msg = ((model == 'MESSAGE') | (model == 'MESSAGE-MACRO'))
    
        # define case name for MSG gdx export/import, replace spaces by '_'
        if (msg & is.null(case)) {
          case = gsub(' ', '_', paste(.self$model, .self$scenario, sep = "_")) # self.model and self.scenario
        }    
        # define variables for writing to gdx and reading the solution
        if (msg) {
          ingdx = paste("MsgData_", case, ".gdx", sep = '')
          outgdx = paste("MsgOutput_", case, ".gdx", sep = '')
          ipth = paste(message_ix_path, "/model/data", sep = '')
          opth = paste(message_ix_path, "/model/output", sep = '')
        } else {
          ingdx = paste(model, "_in.gdx", sep = '')  
          outgdx = paste(model, "_out.gdx", sep = '')
          ipth = "."
          opth = "."
        }
        inp = file.path(ipth, ingdx)
        out = file.path(opth, outgdx)
        
        # write to gdx
        .self$to_gdx(ipth, ingdx)

        # execute GAMS
        if (msg) {
          mpth = paste(message_ix_path, "/model", sep = '')
          model_run = paste(model, "_run.gms", sep = '')
          run_gams(model_run, inp, out, mpath)
        } else {
          run_gams(paste(model, '.gms', sep = ''), inp, out)
        } 

        # read solution from gdx
        .self$read_sol_from_gdx(opth, outgdx, comment=comment, check_sol=check_sol)
        
    },  
    
    # return a list of years in which a technology of certain vintage at a specific node can be active
    #Parameters
    #----------
    #node : string
    #node name
    #tec : string
    #name of the technology
    #yr_vtg : string
    #vintage year
    years_active = function(node, tec, yr_vtg) {
        return(.jobj$getTecActYrs(node, tec, as.character(yr_vtg)))
    },
    
  ## data processing functions
  
    # get an item from the datastructure
    item = function(ix_type, name) {
        switch(ix_type,
            "item" = return(.jobj$getItem(name)),
            "set"  = return(.jobj$getSet(name)),
            "par"  = return(.jobj$getPar(name)), 
            "var"  = return(.jobj$getVar(name)), 
            "equ"  = return(.jobj$getEqu(name))
        )
    }, 
    
    element = function(ix_type, name, filters) {
      item = .self$item(ix_type, name)
      
      # get list of elements, with filter HashMap if provided
      if (!is.null(filters)) {
          jFilter <- new(java.HashMap)
          for (idx_name in names(filters)) {
            jFilter$put(idx_name, .getEleAsList(as.character(filters[[idx_name]])))
          }
          eleList = item$getElements(jFilter)
      } else {
          eleList = item$getElements()
      }

      # get meta-info of the item        
      dim = item$getDim()
      col_names = .getRList(item$getIdxNames())
      col_sets = .getRList(item$getIdxSets())

      # case of multi-dimensional items
      if (dim>0) {
        dictionary <- 
          lapply(1:dim-1, 
            function(i){ 
              unlist( lapply(1:eleList$size()-1, 
                # TODO this function could be speeded up by using getKey()
                # and the assigning the vector to dimensions (DH, Feb 8 2017)
                function(j){ eleList$get(as.integer(j))$getKey(as.integer(i)) } 
              ) ) 
            } 
          )
          
        names(dictionary) <- col_names
        # TODO switch entries in "year" columns to values (instead of strings)
        #if( "year" %in% col_names ){ 
        #  dictionary[[ "year" ]] = as.numeric(dictionary[[ "year" ]]) 
        #}
         
        switch(ix_type,
          "par"  = {
              dictionary[["value"]] = 
                unlist( lapply(1:eleList$size()-1, 
                  function(j){ eleList$get(as.integer(j))$getValue() } ) )
              dictionary[["unit"]] = 
                unlist( lapply(1:eleList$size()-1, 
                  function(j){ eleList$get(as.integer(j))$getUnit() } ) )
              }, 
            "var"  = {
              dictionary[["level"]] = 
                unlist( lapply(1:eleList$size()-1, 
                  function(j){ eleList$get(as.integer(j))$getLevel() } ) )
              dictionary[["marginal"]] = 
                unlist( lapply(1:eleList$size()-1, 
                  function(j){ eleList$get(as.integer(j))$getMarginal() } ) )
              },
            # TODO merge var and equ below, to avoid code duplication
            "equ"  = {
              dictionary[["level"]] = 
                unlist( lapply(1:eleList$size()-1, 
                  function(j){ eleList$get(as.integer(j))$getLevel() } ) )
              dictionary[["marginal"]] = 
                unlist( lapply(1:eleList$size()-1, 
                  function(j){ eleList$get(as.integer(j))$getMarginal() } ) )
              }
        )
         
        dictionary = data.frame(dictionary)
      # case of one-dimensional items (in Java-speak 0-dimensional)
      } else {
        switch(ix_type, 
          "set"  = {
            dictionary = item$getIndexSetKeys()
            }, 
          "par"  = {
            dictionary = data.frame(value = item$getScalarValue(), 
                                    unit = item$getScalarUnit())
            },
          "var"  = {
            dictionary = data.frame(level = item$getScalarLevel(), 
                                    marginal = item$getScalarMarginal())
            },
          # TODO merge var and equ below, to avoid code duplication
          "equ"  = {
            dictionary = data.frame(level = item$getScalarLevel(), 
                                    marginal = item$getScalarMarginal())
           }
        )  
      }
    
      return(dictionary)  
    },

    # return the list of index sets for an item (set, par, var, equ)
    idx_sets = function(name) {
        return(.getRList(.self$item("item", name)$getIdxSets()))
    },

    # return the list of index names for an item (set, par, var, equ)
    idx_names = function(name) {
        return(.getRList(.self$item("item", name)$getIdxNames()))
    },

    # return a list of all categories for a set
    #Parameters
    #----------
    #name : string
    #name of the set
    cat_list = function(name) {
        return(.jobj$getTypeList(name))
    },    

    # add a set element key to the respective category mapping
    #Parameters
    #----------
    #name : string
    #name of the set
    #cat : string
    #name of the category
    #keys : list of strings
    #element keys to be added to the category mapping
    add_cat = function(name, cat, keys, is_unique=FALSE) {
        .jobj$addCatEle(name, cat, .getJList(keys), is_unique)
    },
    
    # return a list of all set elements mapped to a category
    #Parameters
    #----------
    #name : string
    #name of the set
    #cat : string
    #name of the category
    cat = function(name, cat) {
        return(.jobj$getCatEle(name, cat))
    },

    # get list of all sets in the datastructure
    set_list = function() {
      return(.getRList(.jobj$getSetList()))
    },
  
    # check whether the datastructure has a set with that name
    has_set = function(name) {
      return(.jobj$hasSet(name))
    },

    # initialize a new set in the datastructure
    #Parameters
    #----------
    #name : string
    #name of the item
    #idx_sets : list of strings
    #index set list
    #idx_names : list of strings
    #index name list (optional, default to 'idx_sets')
    init_set = function(name, idx_sets=NULL, idx_names=NULL) {
        invisible(.jobj$initializeSet(name, .getCleanDims(idx_sets), 
                                      .getCleanDims(idx_names, idx_sets)))
    },
  
    # get a dataframe with set elements (optional: filtered by index names)
    #Parameters
    #----------
    #name : string
    #name of the item
    #filters : dictionary
    #index names mapped list of index set elements
    set = function(name, filters=NULL) {
        return(element("set", name, filters))
    },
  
    # add set elements
    #Parameters
    #----------
    #name : string
    #name of the set
    #key : string, list/range of strings/values, dictionary, dataframe
    #element(s) to be added
    #comment : string, list/range of strings
    #comment (optional, only used if 'key' is a string or list/range)
    add_set = function(name, key, comment=NULL) {
        set = .self$item("set", name)             
        if (length(key)==1) {
          invisible(set$addElement(key, comment))
        } else {
          invisible(set$addElement(.getEleAsList(key), comment))
        }
    },
  
    # remove set or remove a specific element (or list of elements) from a set (if key is specified)
    #Parameters
    #----------
    #name : string
    #name of the set
    #key : dataframe or key list or concatenated string
    #elements to be removed
    remove_set = function(name, key=NULL) {
        if (is.null(key)) {
          .jobj$removeSet(name)  
        } else {
          .removeElement(.self$item("set", name), key)  
        }
    },

    # return list of all parameters initialized in the datastructure
    par_list = function() {
        return(.getRList(.jobj$getParList()))
    },

    # check whether the datastructure has a parameter with that name
    has_par = function(name) {
        return(.jobj$hasPar(name))
    },
  
    # initialize a new parameter or scalar
    #Parameters
    #----------
    #name : string
    #name of the item
    #idx_sets : list of strings
    #index set list
    #idx_names : list of strings
    #index name list (optional, default to 'idx_sets')
    init_par = function(name, idx_sets, idx_names=NULL) {
        invisible(.jobj$initializePar(name, .getCleanDims(idx_sets), 
                                    .getCleanDims(idx_names, idx_sets)))
    },
  
    # return a dataframe of (optionally filtered by index name) elements for a specific parameter
    #Parameters
    #----------
    #name : string
    #name of the parameter
    #filters : dictionary
    #index names mapped list of index set elements
    par = function(name, filters=NULL) {
        return(element("par", name, filters))
    },
  
    # add set elements
    #Parameters
    #----------
    #name : string
    #name of the parameter
    #key : string, list/range of strings/values, dictionary, dataframe
    #element(s) to be added
    #val : values, list/range of values
    #element values (only used if 'key' is a string or list/range)
    #unit : string, list/range of strings
    #element units (only used if 'key' is a string or list/range)
    #comment : string, list/range of strings
    #comment (optional, only used if 'key' is a string or list/range)
    add_par = function(name, key, val, unit, comment=NULL) {
        par = .self$item("par", name)             
        if (length(key)==1) {
          invisible(par$addElement(key, new(java.Double, val), unit, comment))
        } else {
          invisible(par$addElement(.getEleAsList(key), new(java.Double, val), unit, comment))
        }
    }, 
  
    # delete a parameter from the datastructure or remove an element from a parameter (if key is specified)
    #Parameters
    #----------
    #name : string
    #name of the parameter
    #key : dataframe or key list or concatenated string
    #elements to be removed
    remove_par = function(name, key=NULL) {
        if (is.null(key)) {
          .jobj$removePar(name)  
        } else {
          .removeElement(.self$item("par", name), key)  
        }
    },
  
    # initialize a new scalar and assign the value/unit
    #Parameters
    #----------
    #name : string
    #name of the scalar
    #val : number
    #value
    #unit : string
    #unit
    #comment : string
    #explanatory comment (optional)
    init_scalar = function(name, val, unit, comment=NULL) {
        par = .jobj$initializePar(name, .getCleanDims(NULL), .getCleanDims(NULL))
        invisible(par$addElement(new(java.Double, val), unit, comment))
    },
  
    #return a dictionary of the value and unit for a scalar
    #Parameters
    #----------
    #name : string
    #name of the scalar
    scalar = function(name) {
        return(element("par", name, filters=NULL))
    },

    # change the value or unit of a scalar
    #Parameters
    #----------
    #name : string
    #name of the scalar
    #val : number
    #value
    #unit : string
    #unit
    #comment : string
    #explanatory comment (optional)
    change_scalar = function(name, val=NULL, unit=NULL, comment=NULL) {
      scalar = .self$item("par", name)
      scalar$addElement(new(java.Double, val), unit, comment)
    },

    # return a list of variables initialized in the datastructure
    var_list = function() {
      return(.getRList(.jobj$getVarList()))
    },
    
    # check whether the datastructure has a variable with that name
    has_var = function(name) {
      return(.jobj$hasVar(name))
    },
  
    # initialize a new variable in the datastrucutre
    #Parameters
    #----------
    #name : string
    #name of the item
    #idx_sets : list of strings
    #index set list
    #idx_names : list of strings
    #index name list (optional, default to 'idx_sets')
    init_var = function(name, idx_sets=NULL, idx_names=NULL) {
        invisible(.jobj$initializeVar(name, .getCleanDims(idx_sets), 
                                    .getCleanDims(idx_names, idx_sets)))
    },
    
    # return a dataframe with variable elements (optional: filtered by index names)
    #Parameters
    #----------
    #name : string
    #name of the variable
    #filters : dictionary
    #index names mapped list of index set elements
    var = function(name, filters=NULL) {
        return(element("var", name, filters))
    },
  
    # check whether the datastructure has a equation with that name
    has_equ = function(name) {
        return(.jobj$hasEqu(name))
    },
  
    # return a list of equations initialized in the datastructure
    equ_list = function() {
      return(.getRList(.jobj$getEquList()))
    },
    
    # initialize a new equation in the datastructure
    #Parameters
    #----------
    #name : string
    #name of the item
    #idx_sets : list of strings
    #index set list
    #idx_names : list of strings
    #index name list (optional, default to 'idx_sets')
    init_equ = function(name, idx_sets=NULL, idx_names=NULL) {
        invisible(.jobj$initializeEqu(name, .getCleanDims(idx_sets), 
                                    .getCleanDims(idx_names, idx_sets)))
    },
  
    # return a dataframe with equation elements (optional: filtered by index names)
    #Parameters
    #----------
    #name : string
    #name of the equation
    #filters : dictionary
    #index names mapped list of index set elements
    equ = function(name, filters=NULL) {
        return(element("equ", name, filters))
    }
    
    )  
)


# a function to convert a Java LinkedList into an R list
.getRList = function(jList){ 
  if(!is.null(jList) & !jList$isEmpty()){ 
    unlist(lapply(1:jList$size(), function(i){ jList$get(as.integer(i-1)) } ))
  } else {
    vector() 
  }
}

# a function to convert an R list to a Java LinkedList
.getJList = function(rList){
	jList = new(J("java.util.LinkedList"))
 	if(!is.null(rList)){ 
    lapply(1:length(rList), function(i){ jList$add(rList[i]) } ) 
  }
	return(jList)
}


# a function to convert an R list to a Java LinkedList
.getCleanDims <- function(rList, rListDefault=NULL) {
  jList <- new(J("java.util.LinkedList"))
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

# a function to convert an R element as list into a Java LinkedList
.getEleAsList <- function(eleList) {
   jList <- new(J("java.util.LinkedList"))
   for (i in 1:length(eleList)) {
       jList$add(eleList[i])    
     }
   return(jList)
   # todo: option to add an element as dataframe by index name
}

# remove element from set or parameter
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

## auxiliary functions for executing GAMS

run_gams = function(model, ingdx, outgdx, model_pth=NULL, args='LogOption=4') {
  cmd = paste("gams ", model, " --in=", ingdx, " --out=", outgdx, " ", 
              args, sep = '')
  if (!is.null(model_pth))
     cmd = paste(cmd, " Inputdir=", model_pth, sep='')
  system(cmd)
}

