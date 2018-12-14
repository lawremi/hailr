### =========================================================================
### Management of the Hail configuration, inc. home directory global
### -------------------------------------------------------------------------
###

HailConfig_getHome <- function(.self) {
    home <- .self$home
    if (is.null(home)) {
        home <- .self$setHome(default_hail_home())
    }
    home
}

HailConfig_setHome <- function(.self, home) {
    if (!is.null(home)) {
        stopifnot(is.character(home),
                  length(home) == 1L && !is.na(home))
        if (!file.exists(hail_jar(home)))
            stop("No Hail installation found at '", home, "'")
    }
    .self$home <- home
    invisible(home)
}

HailConfig <- 
    setRefClass("HailConfig", fields=c(home="character_OR_NULL"),
                methods=list(getHome=HailConfig_getHome,
                             setHome=HailConfig_setHome))()

default_hail_home <- function() {
    select_hail(installed_hails())$home
}

use_hail_home <- function(home) {
    HailConfig$setHome(home)
}

hail_home <- function() {
    HailConfig$getHome()
}

ensure_hail_home <- function() {
    home <- hail_home()
    if (is.null(home)) {
        home <- use_hail_home(prompt_to_install_hail())
    }
    home
}

hail_jar <- function(home = ensure_hail_home()) {
    file.path(home, "hail", "hail-all-spark.jar")
}
