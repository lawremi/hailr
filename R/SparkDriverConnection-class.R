### =========================================================================
### SparkDriverConnection objects
### -------------------------------------------------------------------------
###
### Wrapper of connection from driver implementation.
###

setClass("SparkDriverConnection", slots=c(impl="ANY"))

SparkDriverConnection <- function(...) {
    SparkDriverManager$getConnectionConstructor()(...)
}

setMethod("sparkContext", "SparkDriverConnection",
          function(x) fromDriver(sparkContext(impl(x))))

