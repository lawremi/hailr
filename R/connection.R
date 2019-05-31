### =========================================================================
### Connecting to Hail
### -------------------------------------------------------------------------
###

### FIXME: are we sure that these settings are still needed?
#' Title
#'
#' @return
#' @export
#'
#' @examples
hail_config <- function() {
    list(spark.serializer="org.apache.spark.serializer.KryoSerializer",
         spark.kryo.registrator="is.hail.kryo.HailKryoRegistrator")
}

HailConnection <- function(jars = character(), config = list(), ...) {
    jars <- c(hail_jar(), jars)
    conf <- hail_config()
    conf[names(config)] <- config
    JVM(jars=jars, config=conf, ...)
}
