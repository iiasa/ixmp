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


source(file.path(Sys.getenv("IXMP_R_PATH"), "ixmp.R"))

mp = ixPlatform(dbprops=opt$props)

# test not message
model <- "canning problem"
scenario <- "standard"
scen <- mp$Scenario(model, scenario)
scen$check_out()
scen$init_var('z', NULL, NULL)
scen$commit('foo')
scen$solve(model='transport_ixmp')

stopifnot(scen$var('z')['level'] == 153.675)
