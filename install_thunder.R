install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
  }
}

# 1. Asegurar remotes
install_if_missing("remotes")

# 2. Instalar thundeR solo si no existe
if (!("thundeR" %in% installed.packages()[, "Package"])) {
  remotes::install_github("bczernecki/thundeR@ML_MU_CAPE", force = TRUE)
}
