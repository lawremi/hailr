### =========================================================================
### Sparklyr driver
### -------------------------------------------------------------------------
###
### Implementation of the driver API on top of sparklyr
###

setOldClass("spark_connection")
setIs("spark_connection", "SparkDriverConnection")

setOldClass("spark_jobj")
setIs("spark_jobj", "SparkDriverObject")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Method invocation
###

setMethod("callMethod", "spark_connection",
          function(target, name, args) {
              do.call(sparklyr::invoke_static,
                      c(list(target, paste(head(name, -1L), collapse="/"),
                           tail(name, 1L)),
                        args))
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

register_spark_driver("sparklyr", function(hail_jar, ...) {
    if (!requireNamespace("sparklyr", quietly=TRUE))
        stop("The sparklyr package is required to use the sparklyr backend")
    args <- list(...)
    if (is.null(args$config))
        args$config <- sparklyr::spark_config()
    args$config[["sparklyr.jars.default"]] <- hail_jar
    do.call(sparklyr::spark_connect, args)
})
use_spark_driver("sparklyr")
