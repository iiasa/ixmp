# Install R packages needed for testing
mkdir -p $R_LIBS_USER
echo 'options(repos=c("https://cloud.r-project.org"))' >$R_PROFILE
Rscript -e 'install.packages("devtools")'
Rscript -e 'devtools::install_dev_deps("retixmp/source")'
