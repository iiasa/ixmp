step <- commandArgs(TRUE)[1]

options(repos=c('https://cloud.r-project.org'))

if ( step == '1' ) {
  install.packages(c('devtools', 'IRkernel'), quiet = TRUE)

  IRkernel::installspec()

} else if ( step == '2' ) {

  # upgrade = FALSE is to keep mime (above) at 0.6
  devtools::install(file.path('.', 'rixmp'),
                    args = '--no-multiarch',
                    dependencies = TRUE,
                    upgrade = FALSE)

}
