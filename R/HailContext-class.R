### =========================================================================
### HailContext objects
### -------------------------------------------------------------------------
###
### R proxy for a HailContext
###

setClass("is.hail.HailContext", contains="SparkObject")

setClass("HailContext", slots=c(impl="is.hail.HailContext"))

HailContext <- function(context = sparkContext(SparkConnection()),
                        logFile = "hail.log", append = FALSE,
                        branchingFactor = 50L) {
    con <- sparkConnection(context)
    ## 'appName', 'master', 'local' and 'minBlockSize' taken from 'context'
    impl <- con$is$hail$HailContext$apply(sc = context,
                                          appName = "Hail", # ignored
                                          master = NULL, # ignored
                                          local = "local[*]", # ignored
                                          logFile = logFile,
                                          quiet = TRUE,
                                          append = append,
                                          minBlockSize = 1L, # ignored
                                          branchingFactor = branchingFactor,
                                          tmpDir = tmpdir())    
    new("HailContext", impl=impl)
}
