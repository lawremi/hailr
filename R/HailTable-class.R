### =========================================================================
### HailTable objects
### -------------------------------------------------------------------------
###
### Direct mapping of Hail Table API to R. HailDataFrame wraps this to
### provide the familiar data.frame API. HailPromises can be derived
### from a HailTable (and serve as columns in a DataFrame).
###

setClass("org.apache.spark.sql.Dataset", contains="JavaObject")

### This is a reference class, because:
### (1) Practically it would be infeasible to directly map the Hail API to R
###     top-level functions due to name collisions.
### (2) This effectively reimplements the Python glue, so it seems natural
###     for it to be structured like Python, or at least the non-canonical
###     syntax indicates the presence of an external interface.
.HailTable <- setRefClass("HailTable",
                          fields=c(expr="HailTableExpression",
                                   context="HailContext",
                                   .count="integer_OR_NULL"))
### The HailContext is a singleton in Scala, so we only store it to
### access the JVM.  We could just enforce a single JVM and store it
### globally, but currently we are more flexible: a single R session
### can communicate with multiple JVMs (Hail instances).

### A utility class that lets us dispatch on whether a promise is
### derived from the 'row' struct, which holds the columns in the
### context of a HailTableMapRows() call.
.HailTableMapRowsContext <- setClass("HailTableMapRowsContext",
                                     slots=c(hailTable="HailTable"),
                                     contains="HailExpressionContext")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction
###

HailTable <- function(expr, context) {
    .HailTable(expr=as(expr, "HailTableExpression", strict=FALSE),
               context=context)
}

RangeHailTable <- function(hc, n, n_partitions=NULL) {
    HailTable(jvm(hc$impl)$is$hail$table$Table$range(hc$impl, n,
                                                     JavaOption(n_partitions)))
}

setMethod("transmit", c("org.apache.spark.sql.Dataset", "is.hail.HailContext"),
          function(x, dest) {
              keys <- JavaArrayList()
              jvm(dest)$is$hail$table$Table$pyFromDF(x, keys)
          })

setMethod("unmarshal", c("is.hail.expr.ir.TableIR", "ANY"),
          function(x, skeleton) unmarshal(HailTable(x, context(x)), skeleton))

HailTableMapRowsContext <- function(hailTable) {
    .HailTableMapRowsContext(hailTable=hailTable)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("hailType", "HailTable", function(x) hailType(x$expr))

setMethod("hailType", "HailTableMapRowsContext",
          function(x) hailType(x@hailTable))

## We lazily (as features are needed) reimplement the Python API

.HailTable$methods(
    row = function() {
        Promise(HailRef(HailSymbol("row")), HailTableMapRowsContext(.self))
    },
    rowValue = function() {
        row <- .self$row()
        row[setdiff(names(row), .self$keys())]
    },
    keys = function() {
        keys(hailType(.self))
    },
    globals = function() {
        Promise(HailRef(HailSymbol("global")), HailMapGlobalsContext(.self))
    },
    select = function(...) {
        fields <- as.character(c(...))
        .self$mapRows(.self$row()[fields])
    },
    selectGlobals = function(...) {
        fields <- as.character(c(...))
        .self$mapGlobals(.self$globals()[fields])
    },
    mapRows = function(promise) {
        HailTable(HailTableMapRows(.self$expr, expr(promise)), .self$context)
    },
    mapGlobals = function(promise) {
        HailTable(HailTableMapGlobals(.self$expr, expr(promise)), .self$context)
    },
    annotate = function(...) {
        s <- DataFrame(...)
        r <- .self$row()
        r[names(s)] <- s
        .self$mapRows(r)
    },
    project = function(...) {
        s <- DataFrame(...)
        r <- .self$row()
        r[names(s)] <- s
        .self$mapRows(r[names(s)])
    },
    annotateGlobals = function(...) {
        s <- DataFrame(...)
        r <- .self$globals()
        r[names(s)] <- s
        .self$mapGlobals(r)
    },
    addIndex = function(name) {
        r <- .self$row()
        r[[name]] <- Promise(HailApplyScanOp(Accumulation("Count")),
                             context(r))
        .self$mapRows(r)
    },
    filter = function(promise) {
        HailTable(TableFilter(.self$expr, expr(promise)), .self$context)
    },
    keyBy = function(...) {
        ## In Python, this also accepts keyword arg select expressions,
        ## but we intentionally do not support that.
        keys <- as.character(c(...))
        eval(HailTableKeyBy(.self$expr(), keys), context(.self))
    },
    join = function(right, how = "inner") {
        left <- .self
        check_compatible_keys(left, right)
        HailTable(HailTableJoin(left, right, how, left$keys()), .self$context)
    },
    count = function() {
        if (is.null(.self$.count))
            .self$.count <- asLength(eval(HailTableCount(.self$expr),
                                          .self$context))
        .self$.count
    },
    head = function(n) {
        HailTable(HailTableHead(.self$expr, n), .self$context)
    },
    collect = function() { # as an array of row structs, not a local R object
        Promise(expr=HailTableCollect(.self$expr), context=.self$context)$rows
    }
)

check_compatible_keys <- function(left, right) {
    identical(keyType(hailType(left)), keyType(hailType(right)))
}

setMethod("nrow", "HailTable", function(x) x$count())

hailTable <- function(x) x@hailTable
`hailTable<-` <- function(x, value) {
    x@hailTable <- value
    x
}

setMethod("contextualLength", c("HailPromise", "HailTableMapRowsContext"),
          function(x, context) nrow(hailTable(context)))

src <- function(x) x@src

### TODO: this needs to drop NAs from 'i'
## setMethod("extractROWS", c("HailTable", "HailWhichPromise"), function(x, i) {
##     extractROWS(x, logicalPromise(i))
## })

setMethod("parent", "HailTableMapRowsContext",
          function(x) context(hailTable(x)))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Fulfillment
###

setMethod("contextualDeriveTable", "HailTableMapRowsContext",
          function(context, x) {
              hailTable(context)$project(x = x)$selectGlobals()
          })

setMethod("contextualDeriveTable", "HailMapGlobalsContext",
          function(context, x) {
              table <- globalTable(src(context)$projectGlobals(x = x))
              table$project(x = table$globals()$x)
          })

globalTable <- function(x) {
    singleRowTable <- RangeHailTable(context(x), 1L, 1L)
    joinGlobals(singleRowTable, x)
}

joinGlobals <- function(left, right) {
    utils <- jvm(left$impl)$is$hail$utils
    HailTable(utils$joinGlobals(left$impl, right$impl, "x"))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarization
###

setMethod("head", "HailTableMapRowsContext", function(x, n) {
    initialize(x, hailTable=hailTable(x)$head(n))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Merging
###

bindCols <- function(...) {
    args <- Filter(Negate(is.null), list(...))
    Reduce(joinByRowIndex, args)
}

joinByRowIndex <- function(left, right) {
    idx <- uuid("idx")
    left$addIndex(idx)$keyBy(idx)$join(right$addIndex(idx)$keyBy(idx))$drop(idx)
}

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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### show()
###

setMethod("show", "HailTable", function(object) {
    cat(object$expr, "\n")
})
