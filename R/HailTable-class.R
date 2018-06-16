### =========================================================================
### HailTable objects
### -------------------------------------------------------------------------
###
### Direct mapping of Hail Table API to R. HailDataFrame wraps this to
### provide the familiar data.frame API. HailPromises can be derived
### from a HailTable (and serve as columns in a DataFrame).
###

setClass("is.hail.table.Table", contains="JavaObject")

setClass("org.apache.spark.sql.Dataset", contains="JavaObject")

### This is a reference class, because:
### (1) It holds a reference to the Hail table, although that is immutable.
### (2) Practically it would be infeasible to directly map the Hail API to R
###     top-level functions due to name collisions.
### (3) This effectively reimplements the Python glue, so it seems natural
###     for it to be structured like Python, or at least the non-canonical
###     syntax indicates the presence of an external interface.
.HailTable <- setRefClass("HailTable",
                          fields=c(impl="is.hail.table.Table"))

setClass("HailContext", contains="Context")

.HailTableRowContext <- setClass("HailTableRowContext",
                                 slots=c(table="HailTable"),
                                 contains="HailContext")

.HailTableGlobalContext <- setClass("HailTableGlobalContext",
                                    slots=c(table="HailTable"),
                                    contains="HailContext")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction
###

HailTable <- function(impl) {
    .HailTable(impl=impl)
}

setMethod("transmit", c("org.apache.spark.sql.Dataset", "is.hail.HailContext"),
          function(x, dest) {
              jvm(dest)$is$hail$table$Table$fromDF(dest, x,
                                                   keys=JavaArrayList())
          })

setMethod("unmarshal", c("is.hail.table.Table", "ANY"),
          function(x, skeleton) unmarshal(HailTable(x), skeleton))

HailTableRowContext <- function(table) {
    .HailTableRowContext(table=table)
}

HailTableGlobalContext <- function(table) {
    .HailTableGlobalContext(table=table)
}

setMethod("expressionClass", "HailContext", function(x) "HailExpression")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("hailType", "HailTable",
          function(x) as(x$impl$tir()$typ(), "HailType"))
setMethod("hailType", "HailTableRowContext",
          function(x) rowType(hailType(x@table)))
setMethod("hailType", "HailTableGlobalContext",
          function(x) globalType(hailType(x@table)))

.HailTable$methods(
    rows = function() {
        promises(HailTableRowContext(.self))
    },
    globals = function() {
        promises(HailTableGlobalContext(.self))
    }
)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### I/O
###

readHailTable <- function(file) HailTable(hail_context()$readTable(file))

readHailTableFromText <- function(file,
                                  keyNames = character(0L),
                                  nPartitions = NULL,
                                  types = list(),
                                  comment = character(0L),
                                  separator = "\t",
                                  missing = "NA",
                                  noHeader = FALSE,
                                  impute = FALSE,
                                  quote = NULL,
                                  skipBlankLines = FALSE)
{
    stopifnot(isSingleString(file),
              isTRUEorFALSE(noHeader),
              isSingleString(separator),
              is.null(quote) || isSingleString(quote),
              nchar(quote) == 1L,
              isSingleString(missing),
              is.list(types), all(vapply(types, is, logical(1L), "HailType")),
              is.character(comment), !anyNA(comment),
              is.character(keyNames), !anyNA(keyNames),
              isTRUEorFALSE(skipBlankLines),
              is.null(nPartitions) || isSingleNumber(nPartitions),
              isTRUEorFALSE(impute))

    if (!is.null(nPartitions))
        nPartitions <- as.integer(nPartitions)

    ### FIXME: 'comment' will be an ArrayList<String> in next Hail
    hail_context()$importTable(JavaArrayList(file), JavaArrayList(keyNames),
                               nPartitions, JavaHashMap(types),
                               comment, separator, missing,
                               noHeader, impute, quote
                               # FIXME: next version: skipBlankLines
                               )
}
