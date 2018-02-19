#' @title Class ixmp.Platform
#' @name ixmp.Platform
#' @include init_rixmp.R
#'
#' @description The class 'ixmp.Platform' is the central access point to
#' and accessing dataobjects (timeseries and reference data)
#' and scenarios (structured model input data and results).
#'
#' @field .jobj "jobjRef"
#' @export ixmp.Platform
ixmp.Platform <- methods::setRefClass("ixmp.Platform",
    fields = c(.jobj = "jobjRef"),

    #launch the Platform
    methods = list(
      initialize = function(dbprops=NULL, dbtype=NULL) {
        "launch the Platform"

        if (is.null(dbtype)) {
          if (is.null(dbprops)) dbprops = 'default.properties'
          if (!file.exists(dbprops)) {
            dbprops = paste(ixmp_path, "/config/",dbprops, sep = '')
            if (!file.exists(dbprops)) {
              stop('no properties file ', dbprops , '!')
            }
          }
          print(paste0("launching ixmp.Platform using config file at ",dbprops))
          .jobj <<- rJava::new(rJava::J("at.ac.iiasa.ixmp.Platform"), "R", dbprops)
        } else {
          if (is.null(dbprops)) dbprops = paste(local_path, "/localdb/default", sep = '')
            print(paste0("launching ixmp.Platform with local ",dbtype," database at "
                      , dbprops))
          .jobj <<- rJava::new(rJava::J("at.ac.iiasa.ixmp.Platform"), "R", dbprops, dbtype)
        }

        return(.self)
      },

      #initialize a new Scenario (structured model input data and solution)
      # or get an existing Scenario from the IXMP database
      Scenario = function(model, scen, version=NULL,
                              scheme=NULL, annotation=NULL) {
        "initialize a new Scenario (structured model input data and solution)
        or get an existing Scenario from the IXMP database"
        if (!is.null(version) && version=='new') {
          jScen = .jobj$newScenario(model, scen, scheme, annotation)
        } else if (!is.null(version)) {
          jScen = .jobj$getScenario(model, scen, as.integer(version))
        } else {
          jScen = .jobj$getScenario(model, scen)
        }

        return(ixmp.Scenario(.jobj, model, scen, jScen))
      },

      units = function() {
        "units function"
        return(.jobj$getUnitList())
      },
      open_db = function(){
        "(re-)open the database connection of the platform instance,
        e.g., to continue working after using 'close_db()"
        .jobj$openDB()
      },

      close_db = function(){
        "close the database connection of the platform instance
        this is important when working with local database files ('HSQLDB') "
        .jobj$closeDB()
      },

      scenario_list = function(default=TRUE, model=NULL, scenario=NULL){
        "Get a list of all dataobjects (timeseries)
        and scenarios initialized in the IX Modeling Platform"
        #for more complete doc
        # Parameters
        # ----------
        # default : boolean, default True
        # include only default model/scenario version (true) or all versions
        # model : string
        # the model name (optional)
        # scen : string
        # the scenario name (optional)

        mod_scen_list = .jobj$getModelScenarioList(default, model, scenario)
        ##----- TO BE CONVERTED IN R
        #p mod_range = range(mod_scen_list.size())
        # mod_range = seq(1,length(mod_scen_list),1)
        #  cols = c('model_name', 'scen_name', 'scheme', 'is_default', 'is_locked',
        #          'cre_user', 'cre_date', 'upd_user', 'upd_date',
        #          'lock_user', 'lock_date', 'annotation')
        #
        #p data = {}
        #p for i in cols:
        #p   data[i] = [str(mod_scen_list.get(j).get(i)) for j in mod_range]
        #
        #p data['version'] = [int(str(mod_scen_list.get(j).get('version')))
        #                    for j in mod_range]
        #p cols.append("version")

        #p df = pd.DataFrame
        #    df = data.frame()
        #p    df = df$from_dict(data, orient='columns', dtype=NULL)
        #p    df = df[cols]
        return(mod_scen_list)
      }
    )
)
