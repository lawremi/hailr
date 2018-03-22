### =========================================================================
### SparkDriverManager objects
### -------------------------------------------------------------------------
###
### Supports abstraction of Spark driver (sparklyr, SparkR, ...)
###

SparkDriverManager_setCurrentDriver <-
    function(.self, driver) {
        stopifnot(is.character(driver),
                  length(driver) == 1L && !is.na(driver))
        if (is.null(.self$getConnectionConstructor()))
            stop("No Spark driver found named '", driver, "'")
        .self$currentDriver <- driver
        .self
    }

SparkDriverManager_getCurrentDriver <- function(.self) .self$currentDriver

SparkDriverManager_getConnectionConstructor <-
    function(.self, driver = .self$getCurrentDriver())
        .self$constructors[[driver]]
    
SparkDriverManager_registerDriver <-
    function(.self, driver, connectionConstructor) {
        .self$constructors[[driver]] <- connectionConstructor
        .self
    }

SparkDriverManager <-
    setRefClass("SparkDriverManager",
                fields=c(currentDriver="character",
                         constructors="list"),
                methods=list(setCurrentDriver =
                                 SparkDriverManager_setCurrentDriver,
                             getCurrentDriver =
                                 SparkDriverManager_getCurrentDriver,
                             getConnectionConstructor =
                                 SparkDriverManager_getConnectionConstructor,
                             registerDriver =
                                 SparkDriverManager_registerDriver))()

register_spark_driver <- function(driver, connectionConstructor) {
    SparkDriverManager$registerDriver(driver, connectionConstructor)
}

use_spark_driver <- function(driver) {    
    SparkDriverManager$setCurrentDriver(driver)
    invisible()
}
