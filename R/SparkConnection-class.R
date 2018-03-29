### =========================================================================
### SparkConnection objects
### -------------------------------------------------------------------------
###
### Abstraction for Spark connections. Implementations can call static
### Java methods, and create Spark contexts.
###

setClass("SparkConnection", slots=c(impl="SparkDriverConnection"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SparkConnection <- function(.impl, ...) {
    if (missing(.impl)) {
        .impl <- SparkDriverConnection(...)
    } else {
        .impl <- fromDriver(.impl)
    }
    new("SparkConnection", impl=.impl)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setGeneric("sparkContext", function(x, ...) standardGeneric("sparkContext"))

setMethod("sparkContext", "SparkConnection",
          function(x) SparkObject(sparkContext(impl(x))))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Method invocation
###

setMethod("$", "SparkConnection", function(x, name) JavaPath(x, name))
