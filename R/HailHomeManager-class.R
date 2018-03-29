### =========================================================================
### Management of the Hail home directory global
### -------------------------------------------------------------------------
###

HailHomeManager_getHome <- function(.self) {
    home <- .self$home
    if (is.null(home)) {
        home <- .self$setHome(default_hail_home())
    }
    home
}

HailHomeManager_setHome <- function(.self, home) {
    if (!is.null(home)) {
        stopifnot(is.character(home),
                  length(home) == 1L && !is.na(home))
        if (!file.exists(hail_jar(home)))
            stop("No Hail installation found at '", home, "'")
    }
    .self$home <- home
    invisible(home)
}

HailHomeManager <- 
    setRefClass("HailHomeManager", fields=c(home="character_OR_NULL"),
                methods=list(getHome=HailHomeManager_getHome,
                             setHome=HailHomeManager_setHome))()

default_hail_home <- function() {
    select_hail(installed_hails())$home
}

use_hail_home <- function(home) {
    HailHomeManager$setHome(home)
}

hail_home <- function() {
    HailHomeManager$getHome()
}

ensure_hail_home <- function() {
    home <- hail_home()
    if (is.null(home)) {
        home <- use_hail_home(install_hail())
    }
    home
}

hail_jar <- function(home = ensure_hail_home()) {
    file.path(home, "hail", "jars", "hail-all-spark.jar")
}
