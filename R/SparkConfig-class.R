### =========================================================================
### Spark config management
### -------------------------------------------------------------------------
###

SparkConfig <- setRefClass("SparkConfig",
                            fields=c(master="character",
                                     home="character",
                                     version="character_OR_NULL"))()

use_spark <- function(master, home = Sys.getenv("SPARK_HOME")) {
    stopifnot(isSingleString(master), isSingleString(home))
    SparkConfig$master <- master
    SparkConfig$home <- home
    SparkConfig$version <- NULL
    invisible(NULL)
}

use_local_spark <- function(version = NULL) {
    stopifnot(is.null(version) || isSingleString(version))
    use_spark("local")
    SparkConfig$version <- version
    invisible(NULL)
}

spark_master <- function() SparkConfig$master

spark_home <- function() SparkConfig$home

spark_version <- function() SparkConfig$version

use_local_spark()
