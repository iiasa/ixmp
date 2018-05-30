#Build a developed R package to binary and display help
require(devtools)

# set in Github/local folder
rixmp_path=paste0(getwd(),"/retixmp/")
setwd(paste0(rixmp_path,"/source/"))

# Build binary
devtools::build(pkg = ".", path=rixmp_path,binary=T)

# install the package from binary
setwd(rixmp_path)
install.packages("retixmp_0.0.1.zip", repos=NULL)

pkg = "retixmp"
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
