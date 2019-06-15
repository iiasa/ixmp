step <- commandArgs(TRUE)[1]

options(repos=c('https://cloud.r-project.org'))

if ( step == '1' ) {
  install.packages(c('devtools', 'IRkernel'), quiet = TRUE)

  # mime 0.7 (2019-06-11) not yet available as binary; attempt to install from
  # source causes a compilation error on Appveyor
  url <- 'https://cran.r-project.org/bin/windows/contrib/3.6/mime_0.6.zip'
  install.packages(url, repos = NULL)

  IRkernel::installspec()

} else if ( step == '2' ) {

  # upgrade = FALSE is to keep mime (above) at 0.6
  devtools::install(file.path('.', 'rixmp'),
                    args = '--no-multiarch',
                    dependencies = TRUE,
                    upgrade = FALSE)

}
