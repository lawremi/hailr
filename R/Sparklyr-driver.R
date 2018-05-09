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

setOldClass("spark_connection")
setOldClass("spark_jobj")

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
### Object construction
###

setMethod("constructObject", "spark_connection", function(target, path, args) {
    do.call(sparklyr::invoke_new, c(list(target, path), args))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### sparkContext() accessor
###

setMethod("sparkContext", "spark_connection",
          function(x) sparklyr::spark_context(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### jvm() accessor
###

setMethod("jvm", "spark_jobj",
          function(x) sparklyr::spark_connection(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Copying to/from Spark
###

setMethod("fromJava", "spark_jobj", function(x) JavaObject(x))

setMethod("transmit", c("ANY", "spark_connection"), function(x, dest) {
    tbl <- dplyr::copy_to(dest, x, uuid("table"))
    sparklyr::spark_dataframe(tbl)
})

setMethod("marshal", c("ANY", "spark_connection"), function(x, dest) {
    as(x, "data.frame")
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### dplyr integration
###

tbl.HailDataFrame <- function(src) {
    id <- uuid("table")
    src$createOrReplaceTempView(id)
    dplyr::tbl(jvm(src), id)
}

tbl.JVM <- function(src, from) {
    dplyr::tbl(impl(src), from)
}

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
