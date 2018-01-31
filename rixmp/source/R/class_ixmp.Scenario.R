#' @title Class ixmp.Scenario
#' @name ixmp.Scenario
#' @include init_rixmp.R
#'
#' @description The class 'ixmp.Scenario' is a generic collection
#' of all data for a model instance (sets and parameters), as well as
#' the solution of a model run (levels/marginals of variables and equations).
#'
#' The class includes functions to make changes to the data,
#' export all data to and import a solution from GAMS gdx,
#' and save the scenario to the IXMP database.
#' All changes are logged to facilitate version control.
#'
#' @field .jobj "jobjRef"
#' @field .platform "jobjRef"
#' @field model A character string.
#' @field scenario A character string.
#' @field scheme A character string.
#' @export ixmp.Scenario
ixmp.Scenario <- methods::setRefClass("ixmp.Scenario",
                                        fields = c(.jobj = "jobjRef",
                                                   .platform = "jobjRef",
                                                   # if the platform class were referenced directly,
                                                   # importing this script would already launch the ixmp.Platform
                                                   model = "character",
                                                   scenario = "character",
                                                   scheme = "character" ),

                                        # initialize a new R-class scenario object (via the ixmp.Platform class)
                                        methods = list(
                                          initialize = function(platform, model, scen, javaobj) {
                                            "initialize a new R-class Scenario object (via the ixmp.Platform class)"
                                            model    <<- model
                                            scenario <<- scen
                                            .platform <<- platform
                                            .jobj    <<- javaobj

                                            return(.self)
                                          },

                                          ## functions for platform management

                                          # check out the scenario from the database for making changes
                                          check_out = function(timeseries_only = FALSE) {
                                            "check out the scenario from the database for making changes"
                                            .jobj$checkOut(timeseries_only)
                                          },

                                          # commit all changes made to the scenario to the database
                                          commit = function(annotation) {
                                            "commit all changes made to the scenario to the database"
                                            .jobj$commit(annotation)
                                          },

                                          # discard all changes, reload all items of the scenario from database
                                          discard_changes = function() {
                                            "discard all changes, reload all items of the scenario from database"
                                            .jobj$discardChanges()
                                          },

                                          set_as_default = function() {
                                            "set this instance of a model scenario as default version"
                                            .jobj$setAsDefaultVersion()
                                          },

                                          # check whether this scenario is set as default in the database
                                          is_default = function() {
                                            "check whether this scenario is set as default in the database"
                                            return(.jobj$isDefault())
                                          },

                                          # get the timestamp of the last update/edit of this scenario
                                          last_update = function() {
                                            "get the timestamp of the last update/edit of this scenario"
                                            return(.jobj$getLastUpdateTimestamp()$toString())
                                          },

                                          # get the run id of this scenario
                                          run_id = function() {
                                            "get the run id of this scenario"
                                            return(.jobj$getRunId())
                                          },

                                          # get the version number of this scenario
                                          version = function() {
                                            "get the version number of this scenario"
                                            return(.jobj$getVersion())
                                          },

                                          # clone the scenario (with new model and scenario name)
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
                                          #in the cloned scenario (default: True)
                                          clone = function(new_model = model, new_scen = scenario,
                                                           annotation, keep_sol = TRUE, shift_fyear=0) {
                                            "clone the the given scenario and return the new scenario"
                                            return(ixmp.Scenario(.platform, new_model, new_scen,
                                                                   .jobj$clone(new_model, new_scen, annotation, keep_sol, as.integer(shift_fyear))))
                                          },

                                          # write the scenario to GAMS gdx
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
                                            "write the scenario to GAMS gdx"
                                            .jobj$toGDX(path, filename, include_var_equ)
                                          },

                                          # read solution from GAMS gdx
                                          read_sol_from_gdx = function(path, filename, comment=NULL,
                                                                       var_list=NULL, equ_list=NULL, check_sol=TRUE) {
                                            "read solution from GAMS gdx"
                                            .jobj$readSolutionFromGDX(path, filename, comment,
                                                                      .getJList(var_list), .getJList(equ_list),
                                                                      check_sol)
                                          },

                                          # remove solution from run
                                          remove_sol = function() {
                                            "remove solution from run"
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
                                            "solve the model (export to gdx, execute GAMS, import the solution)"
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
                                              mpath = paste(message_ix_path, "/model", sep = '')
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
                                            "return a list of years in which a technology of certain vintage at a specific node can be active"
                                            return(.jobj$getTecActYrs(node, tec, as.character(yr_vtg)))
                                          },

                                          ## data processing functions

                                          # get an item from the scenario
                                          item = function(ix_type, name) {
                                            "get an item from the scenario"
                                            switch(ix_type,
                                                   "item" = return(.jobj$getItem(name)),
                                                   "set"  = return(.jobj$getSet(name)),
                                                   "par"  = return(.jobj$getPar(name)),
                                                   "var"  = return(.jobj$getVar(name)),
                                                   "equ"  = return(.jobj$getEqu(name))
                                            )
                                          },

                                          element = function(ix_type, name, filters) {
                                            "internal function to retrieve a dataframe of item elements"
                                            item = .self$item(ix_type, name)

                                            # get list of elements, with filter HashMap if provided
                                            if (!is.null(filters)) {
                                              jFilter <- rJava::new(rJava::J("java.util.HashMap"))
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
                                            "return the list of index sets for an item (set, par, var, equ)"
                                            return(.getRList(.self$item("item", name)$getIdxSets()))
                                          },

                                          # return the list of index names for an item (set, par, var, equ)
                                          idx_names = function(name) {
                                            "return the list of index names for an item (set, par, var, equ)"
                                            return(.getRList(.self$item("item", name)$getIdxNames()))
                                          },

                                          # return a list of all categories for a set
                                          #Parameters
                                          #----------
                                          #name : string
                                          #name of the set
                                          cat_list = function(name) {
                                            "return a list of all categories for a set"
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
                                            "add a set element key to the respective category mapping"
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
                                            "return a list of all set elements mapped to a category"
                                            return(.jobj$getCatEle(name, cat))
                                          },

                                          # get list of all sets in the scenario
                                          set_list = function() {
                                            "get list of all sets in the scenario"
                                            return(.getRList(.jobj$getSetList()))
                                          },

                                          # check whether the scenario has a set with that name
                                          has_set = function(name) {
                                            "check whether the scenario has a set with that name"
                                            return(.jobj$hasSet(name))
                                          },

                                          # initialize a new set in the scenario
                                          #Parameters
                                          #----------
                                          #name : string
                                          #name of the item
                                          #idx_sets : list of strings
                                          #index set list
                                          #idx_names : list of strings
                                          #index name list (optional, default to 'idx_sets')
                                          init_set = function(name, idx_sets=NULL, idx_names=NULL) {
                                            "initialize a new set in the scenario"
                                            invisible(.jobj$initializeSet(name, .getCleanDims(idx_sets),
                                                                          .getCleanDims(idx_names, idx_sets)))
                                          },

                                          # return a dataframe of (filtered) elements for a specific set
                                          #Parameters
                                          #----------
                                          #name : string
                                          #name of the item
                                          #filters : dictionary
                                          #index names mapped list of index set elements
                                          set = function(name, filters=NULL) {
                                            "return a dataframe of (filtered) elements for a specific set"
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
                                            "add set elements"
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
                                            "remove set or remove a specific element (or list of elements) from a set (if key is specified)"
                                            if (is.null(key)) {
                                              .jobj$removeSet(name)
                                            } else {
                                              .removeElement(.self$item("set", name), key)
                                            }
                                          },

                                          # return list of all parameters initialized in the scenario
                                          par_list = function() {
                                            "return list of all parameters initialized in the scenario"
                                            return(.getRList(.jobj$getParList()))
                                          },

                                          # check whether the scenario has a parameter with that name
                                          has_par = function(name) {
                                            "check whether the scenario has a parameter with that name"
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
                                            "initialize a new parameter or scalar"
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
                                            "return a dataframe of (optionally filtered by index name) elements for a specific parameter"
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
                                            "add set elements"
                                            par = .self$item("par", name)
                                            if (length(key)==1) {
                                              invisible(par$addElement(key, rJava::new(rJava::J("java.lang.Double"), val), unit, comment))
                                            } else {
                                              invisible(par$addElement(.getEleAsList(key), rJava::new(rJava::J("java.lang.Double"), val), unit, comment))
                                            }
                                          },

                                          # delete a parameter from the scenario or remove an element from a parameter (if key is specified)
                                          #Parameters
                                          #----------
                                          #name : string
                                          #name of the parameter
                                          #key : dataframe or key list or concatenated string
                                          #elements to be removed
                                          remove_par = function(name, key=NULL) {
                                            "delete a parameter from the scenario or remove an element from a parameter (if key is specified)"
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
                                            "initialize a new scalar and assign the value/unit"
                                            par = .jobj$initializePar(name, .getCleanDims(NULL), .getCleanDims(NULL))
                                            invisible(par$addElement(rJava::new(rJava::J("java.lang.Double"), val), unit, comment))
                                          },

                                          #return a dictionary of the value and unit for a scalar
                                          #Parameters
                                          #----------
                                          #name : string
                                          #name of the scalar
                                          scalar = function(name) {
                                            "return a dictionary of the value and unit for a scalar"
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
                                            "change the value or unit of a scalar"
                                            scalar = .self$item("par", name)
                                            scalar$addElement(rJava::new(rJava::J("java.lang.Double"), val), unit, comment)
                                          },

                                          # return a list of variables initialized in the scenario
                                          var_list = function() {
                                            "return a list of variables initialized in the scenario"
                                            return(.getRList(.jobj$getVarList()))
                                          },

                                          # check whether the scenario has a variable with that name
                                          has_var = function(name) {
                                            "check whether the scenario has a variable with that name"
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
                                            "initialize a new variable in the datastrucutre"
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
                                            "return a dataframe with variable elements (optional: filtered by index names)"
                                            return(element("var", name, filters))
                                          },

                                          # check whether the scenario has a equation with that name
                                          has_equ = function(name) {
                                            "check whether the scenario has a equation with that name"
                                            return(.jobj$hasEqu(name))
                                          },

                                          # return a list of equations initialized in the scenario
                                          equ_list = function() {
                                            "return a list of equations initialized in the scenario"
                                            return(.getRList(.jobj$getEquList()))
                                          },

                                          # initialize a new equation in the scenario
                                          #Parameters
                                          #----------
                                          #name : string
                                          #name of the item
                                          #idx_sets : list of strings
                                          #index set list
                                          #idx_names : list of strings
                                          #index name list (optional, default to 'idx_sets')
                                          init_equ = function(name, idx_sets=NULL, idx_names=NULL) {
                                            "initialize a new equation in the scenario"
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
                                            "return a dataframe with equation elements (optional: filtered by index names)"
                                            return(element("equ", name, filters))
                                          }

                                        )
)
