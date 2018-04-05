### =========================================================================
### SparkConnection objects
### -------------------------------------------------------------------------
###
### Abstraction for Spark connections. Implementations can call static
### Java methods, and create Spark contexts.
###

setClass("SparkConnection", slots=c(impl="ANY"), contains="JavaMethodTarget")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SparkConnection <- function(.impl, ...) {
    if (missing(.impl)) {
        .impl <- SparkDriverConnection(...)
    }
    new("SparkConnection", impl=.impl)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setGeneric("sparkContext", function(x, ...) standardGeneric("sparkContext"))

setMethod("sparkContext", "SparkConnection",
          function(x) SparkObject(sparkContext(impl(x))))

setMethod("jvm", "SparkConnection", function(x) x)
