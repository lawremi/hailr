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
              stopifnot(isSingleString(path))
              do.call(sparklyr::invoke, c(list(target, path), args))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Object construction
###

setMethod("constructObject", "spark_connection", function(target, path, args) {
    do.call(sparklyr::invoke_new, c(list(target, pathToClassName(path)), args))
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
    data.frame(lapply(as(x, "data.frame"), marshalColumn, dest))
})

setGeneric("marshalColumn", function(x, dest) x)

setMethod("marshalColumn", c("list", "spark_connection"), function(x, dest) {
    delimit(x)
})

internal_separator <- "\030"
empty_element_indicator <- "\025"

delimit <- function(x) {
    cx <- CharacterList(x)
    ifelse(lengths(cx) == 0L, empty_element_indicator,
           unstrsplit(cx, internal_separator))
}

setMethod("unmarshal", c("StringPromise", "list"), function(x, skeleton) {
    ifelse2(x == empty_element_indicator, list(character(0L)),
            strsplit(x, internal_separator))
})

setMethod("unmarshal", c("StringPromise", "integer_list"),
          function(x, skeleton) {
              cast(callNextMethod(), hailType(skeleton))
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

add_jars_to_config <- function(config, jars) {
    config[["sparklyr.jars.default"]] <- jars
    classpath_vars <-
        c(spark.driver.extraClassPath=paste(jars, collapse=.Platform$path.sep),
          spark.executor.extraClassPath=paste(basename(jars),
                                              collapse=.Platform$path.sep))
    config[["sparklyr.shell.conf"]] <-
        paste0(names(classpath_vars), "='", classpath_vars, "'")
    config
}

register_spark_driver("sparklyr", function(jars, config=spark_config(), ...) {
    if (!requireNamespace("sparklyr", quietly=TRUE))
        stop("The sparklyr package is required to use the sparklyr backend")
    config <- add_jars_to_config(config, jars)
    sparklyr::spark_connect(spark_master(), spark_home(),
                            version = spark_version(),
                            config=config, ...)
})
use_spark_driver("sparklyr")
