### =========================================================================
### Sparklyr driver
### -------------------------------------------------------------------------
###
### Implementation of the driver API on top of sparklyr
###
### FIXME: This will need to move to its own package (hailr.sparklyr)
###        at some point, because we rely on classes defined by sparklyr,
###        and we do not want hailr to depend on sparklyr. But until
###        there is another viable backend, we are keeping it here.
###

setClass("SparklyrConnection", slots=c(impl="spark_connection"),
         contains="SparkDriverConnection")

setClass("SparklyrObject", slots=c(impl="spark_jobj"),
         contains="SparkDriverObject")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

SparklyrConnection <- function(x) {
    new("SparklyrConnection", impl=x)
}

SparklyrObject <- function(x) {
    new("SparklyrObject", impl=x)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Method invocation
###

setMethod("fromDriver", "spark_jobj", function(x) SparklyrObject(x))

setMethod("fromDriver", "spark_connection", function(x) SparklyrConnection(x))

setMethod("callMethod", "spark_connection",
          function(target, name, args) {
              do.call(sparklyr::invoke_static,
                      c(list(target), paste(head(name, -1L), collapse="."),
                        tail(name, 1L), args))
          })

setMethod("callMethod", "spark_jobj",
          function(target, name, args) {
              do.call(sparklyr::invoke, c(list(target, name), args))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### SparkContext factory
###

setMethod("sparkContext", "spark_connection",
          function(x) sparklyr::spark_context(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### sparkConnection() accessor
###

setMethod("sparkConnection", "spark_jobj",
          function(x) sparklyr::spark_connection(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Driver registration and selection
###

register_spark_driver("sparklyr", function(jars, config=spark_config(), ...) {
    if (!requireNamespace("sparklyr", quietly=TRUE))
        stop("The sparklyr package is required to use the sparklyr backend")
    config[["sparklyr.jars.default"]] <- jars
    con <- sparklyr::spark_connect("local", config=config, ...)
    SparklyrConnection(con)
})
use_spark_driver("sparklyr")
