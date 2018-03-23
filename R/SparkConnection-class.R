### =========================================================================
### SparkConnection objects
### -------------------------------------------------------------------------
###
### Abstraction for Spark connections. Implementations can call static
### Java methods, and create Spark contexts.
###

setClass("SparkDriverConnection")

setClass("SparkConnection", slots=c(impl="SparkDriverConnection"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SparkConnection <- function(.impl, ...) {
    if (missing(.impl)) {
        .impl <- SparkDriverConnection(hail_jar())
    }
    stopifnot(is(.impl, "SparkDriverConnection"))
    new("SparkConnection", impl=.impl)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("impl", "SparkConnection", function(x) x@impl)

setGeneric("sparkContext", function(x, ...) standardGeneric("sparkContext"))

setMethod("sparkContext", "SparkConnection",
          function(x) SparkContext(sparkContext(impl(x))))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Method invocation
###

setMethod("$", "SparkConnection", function(x, name) JavaPath(x, name))
