### =========================================================================
### HailContext objects
### -------------------------------------------------------------------------
###
### R proxy for a HailContext
###

setClass("is.hail.HailContext", contains="SparkObject")

hail_config <- function() {
    list(spark.serializer="org.apache.spark.serializer.KryoSerializer",
         spark.kryo.registrator="is.hail.kryo.HailKryoRegistrator")
}

HailConnection <- function(jars = character(), config = list(), ...) {
    jars <- c(hail_jar(), jars)
    conf <- hail_config()
    conf[names(config)] <- config
    SparkConnection(jars=jars, config=conf, ...)
}

HailContext <- function(context = sparkContext(HailConnection()),
                        logFile = "hail.log", append = FALSE,
                        branchingFactor = 50L) {
    con <- sparkConnection(context)
    ## 'appName', 'master', 'local' and 'minBlockSize' taken from 'context'
    impl <- con$is$hail$HailContext$apply(context,
                                          appName = "Hail", # ignored
                                          master = NULL, # ignored
                                          local = "local[*]", # ignored
                                          logFile = logFile,
                                          quiet = TRUE,
                                          append = append,
                                          minBlockSize = 1L, # ignored
                                          branchingFactor = branchingFactor,
                                          tmpDir = tempdir())
    new("is.hail.HailContext", impl=impl)
}
