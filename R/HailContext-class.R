### =========================================================================
### HailContext objects
### -------------------------------------------------------------------------
###
### R proxy for a HailContext
###

setClass("is.hail.HailContext", contains="JavaObject")

setClass("HailExpressionContext")
setIs("HailExpressionContext", "Context")

.HailContext <- setRefClass("HailContext",
                            fields = c(impl="is.hail.HailContext"),
                            contains="HailExpressionContext")

HailContext_impl <- function(context, logFile, append, branchingFactor)
{
    ## 'appName', 'master', 'local' and 'minBlockSize' taken from 'context'
    jvm(context)$is$hail$HailContext$apply(context,
                                           appName = "Hail", # ignored
                                           master = NULL, # ignored
                                           local = "local[*]", # ignored
                                           logFile = logFile,
                                           quiet = TRUE,
                                           append = append,
                                           minBlockSize = 1L, # ignored
                                           branchingFactor = branchingFactor,
                                           tmpDir = tempdir())
}

HailContext <- function(context = sparkContext(HailConnection()),
                        logFile = "hail.log", append = FALSE,
                        branchingFactor = 50L)
{
    impl <- HailContext_impl(context, logFile, append, branchingFactor)
    .HailContext(impl=impl)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("expressionClass", "HailExpressionContext",
          function(x) "HailExpression")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Evaluation
###

### Collecting a Hail promise is analogous to Solr: we derive a new
### table using the promise expression, collect that table and extract
### the column.

setMethod("eval", c("HailExpression", "HailExpressionContext"),
          function (expr, envir, enclos) {
              df <- deriveTable(envir, expr)$collect()
              df[[1L]]
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### File I/O
###

.HailContext$methods(
    importVCF = function(file, force=FALSE,
                         force.bgz=FALSE, header.file=NULL,
                         min.partitions=NULL,
                         drop.samples=FALSE,
                         call.fields=character(),
                         reference.genome="default",
                         contig.recoding=NULL)
    {
        stopifnot(isTRUEorFALSE(force),
                  isTRUEorFALSE(force.bgz),
                  is.null(header.file) || isSingleString(header.file),
                  isTRUEorFALSE(drop.samples),
                  is.character(call.fields), !anyNA(call.fields),
                  is.null(reference.genome) || isSingleString(reference.genome),
                  is.null(contig.recoding) ||
                      is.list(contig.recoding) &&
                      !is.null(names(contig.recoding)) &&
                      all(vapply(contig.recoding, is.character, logical(1L))))

        jvm <- jvm(.self$impl)
        genome <- jvm$is$hail$variant$ReferenceGenome$getReference(genome)
        ans <- .self$impl$importVCF(file, force, force.bgz,
                                    ScalaOption(header.file),
                                    ScalaOption(min.partitions),
                                    drop.samples,
                                    ScalaSet(call.fields),
                                    ScalaOption(genome),
                                    ScalaOption(contig.recoding))
        HailMatrixTable(ans)
    },
    readMatrixTable = function(file, drop.cols = FALSE, drop.rows = FALSE) {
        stopifnot(isSingleString(file),
                  isTRUEorFALSE(drop.cols),
                  isTRUEorFALSE(drop.rows))
        HailMatrixTable(.self$impl$read(file, drop.cols, drop.rows))
    },
    importTable = function(file,
                           keyNames = character(0L),
                           nPartitions = NULL,
                           types = list(),
                           comment = character(0L),
                           separator = "\t",
                           missing = "NA",
                           noHeader = FALSE,
                           impute = FALSE,
                           quote = NULL,
                           skipBlankLines = FALSE,
                           forceBGZ = FALSE)
    {
        stopifnot(isSingleString(file),
                  isTRUEorFALSE(noHeader),
                  isSingleString(separator),
                  is.null(quote) || isSingleString(quote),
                  nchar(quote) == 1L,
                  isSingleString(missing),
                  is.list(types),
                  all(vapply(types, is, logical(1L), "HailType")),
                  is.character(comment), !anyNA(comment),
                  is.character(keyNames), !anyNA(keyNames),
                  isTRUEorFALSE(skipBlankLines),
                  is.null(nPartitions) || isSingleNumber(nPartitions),
                  isTRUEorFALSE(impute),
                  isTRUEorFALSE(forceBGZ))

        if (!is.null(nPartitions))
            nPartitions <- as.integer(nPartitions)

        HailTable(.self$impl$importTable(JavaArrayList(file),
                                         JavaArrayList(keyNames),
                                         nPartitions, JavaHashMap(types),
                                         JavaArrayList(comment), separator,
                                         missing,
                                         noHeader, impute, quote,
                                         skipBlankLines,
                                         forceBGZ))
    },
    readTable = function(file) {
        stopifnot(isSingleString(file))
        HailTable(.self$impl$readTable(file))
    }
)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Copying data to Hail
###

setMethod("marshal", c("ANY", "HailContext"), function(x, dest) {
    marshal(x, dest$impl)
})

setMethod("transmit", c("ANY", "HailContext"), function(x, dest) {
    transmit(x, dest$impl)
})
