setwd(file.path(Sys.getenv("MESSAGE_IX_PATH"), "tutorial"))

suppressPackageStartupMessages(library(optparse, quietly=TRUE))

# cli
option_list = list(
    make_option(c("--props"), type="character",
                help=paste("testdb props path"),
                metavar="character")
);
opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);


source(file.path(Sys.getenv("IXMP_R_PATH"),"ixmp.R"))

mp = ixPlatform(dbprops=opt$props)

# test not message
model <- "canning problem"
case <- "standard"
ixDS <- mp$datastructure(model, case)
ixDS$check_out()
ixDS$init_var('z', NULL, NULL)
ixDS$commit('foo')
ixDS$solve(model='transport_ixmp')

stopifnot(ixDS$var('z')['level'] == 153.675)

# test message
model <- 'canning problem (MESSAGE scheme)'
case <- 'standard'
ixDS <- mp$datastructure(model, case)

ixDS$solve(model='MESSAGE', case='3')

stopifnot(ixDS$var('OBJ')['level'] == 153.675)
