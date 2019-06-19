#Build a developed R package to binary and display help
require(devtools)

# set in Github/local folder
rixmp.legacy_path=paste0(getwd(),"/rixmp.legacy/")
setwd(paste0(rixmp.legacy_path,"/source/"))

# Build binary
devtools::build(pkg = ".", path=rixmp.legacy_path,binary=T)

# install the package from binary
setwd(rixmp.legacy_path)
install.packages("rixmp.legacy_0.0.0.9000.zip", repos=NULL)

pkg = "rixmp.legacy"
setwd(paste0(rixmp.legacy_path,"/source/"))

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
