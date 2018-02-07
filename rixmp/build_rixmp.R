#Build a developed R package to binary and display help

# check_load_pkg <- function(x){
#   if (!require(x,character.only = TRUE) == TRUE) {
#     install.packages(x,dep=TRUE, repos='http://cran.r-project.org')
#     if (!require(x,character.only = TRUE) == TRUE) stop("Package not found")
#   }
#   library(x,character.only = TRUE)
# }
# lapply(c("devtools","rJava"),check_load_pkg)

require(devtools)

# set in Github/local folder
rixmp_path=paste0(getwd(),"/rixmp/")
setwd(paste0(rixmp_path,"/source/"))

# Build binary
devtools::build(pkg = ".", path=rixmp_path,binary=T)

# install the package from binary
setwd(rixmp_path)
install.packages("rixmp_0.0.0.9000.zip", repos=NULL)

pkg = "rixmp"
setwd(paste0(rixmp_path,"/source/"))

static_help = function(pkg, links = tools::findHTMLlinks()) {
  pkgRdDB = tools:::fetchRdDB(file.path(find.package(pkg), 'help', pkg))
  force(links); topics = names(pkgRdDB)
  for (p in topics) {
    tools::Rd2HTML(pkgRdDB[[p]], paste("./inst/docum/",p, '.html', sep = ''),
                   package = pkg, Links = links, no_links = is.null(links))
  }
}

static_help(pkg,links = tools::findHTMLlinks())
print("Documentation created!!")
