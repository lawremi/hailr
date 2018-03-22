### =========================================================================
### HailContext objects
### -------------------------------------------------------------------------
###
### R proxy for a HailContext
###

setClass("is.hail.HailContext", contains="SparkObject")

setClass("HailContext", slots=c(impl="is.hail.HailContext"))

### FIXME: HailContext() makes static changes to the runtime instance
###        (SparkConnection). It seems like Hail is designed to
###        support only a single HailContext per Spark
###        connection. This is limited to the logFile and append
###        logging options, though.  The Python interface manages a
###        singleton HailContext. Related: we want to get a
###        HailContext directly from a Spark object, e.g., via the
###        SparkContext/Connection, or just access a singleton stored
###        globally, like Python. Since sparklyr allows multiple Spark
###        connections, it makes sense to have one HailContext per
###        connection. If HailContext were a singleton, it would not
###        make sense to pass anything (like a Spark connection) to
###        the HailContext constructor. Of course, we would have to
###        ensure that the Spark connection is identical between two
###        objects involved in the same operation.
###
###        We will often want the hail context to be implicit. For
###        example, we want to call readHailExperiment(file), without
###        necessarily specifying a HailContext. It could be an
###        argument, though. HailContext() to make a one new one (with
###        the caveats), and hail_context() to get the global one. Can
###        the user set the global one? If not, APIs will need to
###        support passing the context down the stack. If so, things
###        could go badly. The HailExperiment can keep its own
###        reference to the HailContext. Only "bootstrapping"
###        functions like readHailExperiment() would need it provided.

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
