# Tutorial 1 for R
# Solve Dantzig's Transport Problem using the ix modeling platform (ixmp)
#
# This problem finds a least cost shipping schedule that meets requirements 
# at markets and supplies at factories.
#
# Dantzig, G B, Chapter 3.3. In Linear Programming and Extensions. 
# Princeton University Press, Princeton, New Jersey, 1963.
#
# This formulation is described in detail in: 
# Rosenthal, R E, Chapter 2: A GAMS Tutorial. In GAMS: A User's Guide. 
# The Scientific Press, Redwood City, California, 1988.
#
# see http://www.gams.com/mccarl/trnsport.gms


### Set working directory to the folder of the tutorial
# This is required for exporting the GAMS gdx file to the correct location
# and calling GAMS to solve the transport example

setwd(<...>)


### Launching the platform and initializing a new datastructure

#
# This datastructure will be used to store all data required 
# to solve Dantzig's transport problem as well as the solution 
# after solving it in GAMS.

# load the ixmp 'package' source code
source(file.path(Sys.getenv("IXMP_R_PATH"), "ixmp.R"))

# launch the IX modeling platform using the local default database     
mp <- Platform("C:\\Users\\<user>\\.local\\ixmp\\localdb\\default", "HSQLDB")

# details for creating a new datastructure in the IX modeling platform         
model <- "canning problem" 
scen  <- "standard"
annot <- "Dantzig's transportation problem for illustration and testing" 

# initialize a new datastructure
# the parameter version='new' indicates that this is a new datastructure
ds <- mp$DataStructure(model, scen, version="new", annotation=annot)

### Defining the sets in the datastructure

# define the sets of locations of canning plants and markets  
ds$init_set("i")
ds$add_set("i", "seattle")
ds$add_set("i", "san-diego")

### markets set
ds$init_set("j")
ds$add_set("j", "new-york")
ds$add_set("j", "chicago")
ds$add_set("j", "topeka")

# display the set 'i'
ds$set('i')

### Defining parameters in the datastructure

# capacity of plant i in cases                                                                                                                                                           
ds$init_par("a", c("i"))
ds$add_par("a", "seattle",   350, "cases")
ds$add_par("a", "san-diego", 600, "cases")

# demand at market j in cases
ds$init_par("b", c("j"))
ds$add_par("b", "new-york",   325, "cases")
ds$add_par("b", "chicago",    300, "cases")
ds$add_par("b", "topeka",     275, "cases")

# distance in thousands of miles
ds$init_par("d", c("i","j"))
ds$add_par("d", "seattle.new-york",   2.5, "km")
ds$add_par("d", "seattle.chicago",    1.7, "km")
ds$add_par("d", "seattle.topeka",     1.8, "km")
ds$add_par("d", "san-diego.new-york", 2.5, "km")
ds$add_par("d", "san-diego.chicago",  1.8, "km")
ds$add_par("d", "san-diego.topeka",   1.4, "km")

# display the parameter 'b'
ds$par('b')

# cost per case per 1000 miles
# initialize scalar with a value and a unit (and optionally a comment)
ds$init_scalar("f", 90.0, "USD/km")

### Saving the datastructure to the IX Modeling Platform database

# commit new datastructure to the database
# no changes can then be made to the datastructure until a check-out is performed
comment = "importing Dantzig's transport problem for illustration of the R interface"
ds$commit(comment)

# set this new datastructure as the default version for the model/scenario name
ds$set_as_default()

### Defining variables and equations in the datastructure�

# The levels and marginals of these variables and equations 
# will be imported to the datastructure when reading the gdx solution file.

# perform a check_out to make further changes
ds$check_out()

# initialize the decision variables and equations
ds$init_var("z", NULL, NULL)
ds$init_var("x", idx_sets=c("i", "j"))
ds$init_equ("demand", idx_sets=c("j"))

# save changes to database
change_comment = "inialize the model variables and equations"
ds$commit(change_comment)

### Solve the model

# The solve() function exports the datastructure to a GAMS gdx file, 
# executes GAMS, and then imports the solution from an output GAMS gdx 
# file to the database.

# For the model equations and the GAMS workflow (reading the data from gdx, 
# solving the model, writing the results to gdx), see ``transport_ixmp.gms``.


ds$solve(model="transport_ixmp")

### Display and analyze the results

# display the objective value of the solution
ds$var("z")

# display the quantities transported from canning plants to demand locations
ds$var("x")

# display the quantities and marginals (=shadow prices) of the demand balance constraints
ds$equ("demand")
