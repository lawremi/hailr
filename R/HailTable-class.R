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

setClass("HailTableContext", slots=c(hailTable="HailTable"),
         contains="HailExpressionContext")

.HailTableRowContext <- setClass("HailTableRowContext",
                                 contains="HailTableContext")

.HailTableGlobalContext <- setClass("HailTableGlobalContext",
                                    contains="HailTableContext")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction
###

HailTable <- function(impl) {
    .HailTable(impl=impl)
}

RangeHailTable <- function(hc, n, n_partitions=NULL) {
    HailTable(jvm(hc)$is$hail$table$Table$range(hc, n,
                                                JavaOption(n_partitions)))
}

setMethod("transmit", c("org.apache.spark.sql.Dataset", "is.hail.HailContext"),
          function(x, dest) {
              jvm(dest)$is$hail$table$Table$fromDF(dest, x,
                                                   keys=JavaArrayList())
          })

setMethod("unmarshal", c("is.hail.table.Table", "ANY"),
          function(x, skeleton) unmarshal(HailTable(x), skeleton))

HailTableRowContext <- function(hailTable) {
    .HailTableRowContext(hailTable=hailTable)
}

HailTableGlobalContext <- function(hailTable) {
    .HailTableGlobalContext(hailTable=hailTable)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("hailType", "HailTable",
          function(x) as(x$impl$tir()$typ(), "HailType"))

.HailTable$methods(
    row = function() {
        Promise(rowType(hailType(.self)), "row",
                HailTableRowContext(.self))
    },
    globals = function() {
        Promise(globalType(hailType(.self)), "global",
                HailTableGlobalContext(.self))
    },
    select = function(...) {
        expr <- HailStructDeclaration(...)
        HailTable(impl(.self)$select(as.character(expr)))
    },
    selectGlobals = function(...) {
        expr <- HailStructDeclaration(...)
        HailTable(impl(.self)$selectGlobal(as.character(expr)))
    },
    joinGlobals = function(right) {
        left <- .self
        utils <- jvm(impl(left))$is$hail$utils
        HailTable(utils$joinGlobals(impl(left), impl(right), "x"))
    },
    globalTable = function() {
        .self$joinGlobals(RangeHailTable(.self$hailContext(), 1L, 1L))
    },
    head = function(n) {
        HailTable(impl(.self)$head(n))
    },
    collect = function() {
        fromJSON(impl(.self)$collectJSON())
    },
    hailContext = function() {
        impl(.self)$hc()
    }
)

## Could record nrow upon construction to avoid repeated Java calls
setMethod("nrow", "HailTable", function(x) impl(x)$count())

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Evaluation
###

setGeneric("toHailTable",
           function(context, expr) standardGeneric("toHailTable"),
           signature="context")

setMethod("toHailTable", "HailTableRowContext", function(context, expr) {
    context(envir)$select(x = expr)$selectGlobals()
})

setMethod("toHailTable", "HailTableGlobalContext", function(context, expr) {
    table <- context(envir)$selectGlobals(x = expr)$globalTable()
    table$select(x = global$x$x)
})

setMethod("eval", c("HailExpression", "HailTableContext"),
          function (expr, envir, enclos) {
              df <- toHailTable(envir, expr)$collect()
              df[[1L]]
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarization
###

setMethod("head", "HailTableRowContext", function(x, n) {
    initialize(x, context=context(x)$head(n))
})

setMethod("head", "HailTableGlobalContext", function(x, n) {
    initialize(x, context=context(x)$globalTable()$head(n))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### I/O
###

readHailTable <- function(file) hail_context()$readTable(file)

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
    hail_context()$importTable(file, keyNames, nPartitions, types, comment,
                               separator, missing, noHeader, impute, quote,
                               skipBlankLines)
}
