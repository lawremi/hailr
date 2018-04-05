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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Method invocation
###

setMethod("callMethod", "spark_connection",
          function(target, path, args) {
              do.call(sparklyr::invoke_static,
                      c(list(target), pathToClassName(head(path, -1L)),
                        tail(path, 1L), args))
          })

setMethod("callMethod", "spark_jobj",
          function(target, path, args) {
              do.call(sparklyr::invoke, c(list(target, path), args))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### sparkContext() accessor
###

setMethod("sparkContext", "spark_connection",
          function(x) sparklyr::spark_context(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### sparkConnection() accessor
###

setMethod("sparkConnection", "spark_jobj",
          function(x) sparklyr::spark_connection(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### fromJava() coercion
###

setMethod("fromJava", "spark_jobj", function(x) SparkObject(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Driver registration and selection
###

register_spark_driver("sparklyr", function(jars, config=spark_config(), ...) {
    if (!requireNamespace("sparklyr", quietly=TRUE))
        stop("The sparklyr package is required to use the sparklyr backend")
    config[["sparklyr.jars.default"]] <- jars
    sparklyr::spark_connect("local", config=config, ...)
})
use_spark_driver("sparklyr")
