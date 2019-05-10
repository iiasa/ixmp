step <- commandArgs(TRUE)

print(step)

step <- step[0]

if ( step == '1' ) {

  install.packages(c('devtools', 'IRkernel'), quiet = TRUE)

  IRkernel::installspec()

} else if ( step == '2') {

  devtools::install(file.path('.', 'retixmp', 'source'),
                    args = '--no-multiarch', dependencies = TRUE)

}
