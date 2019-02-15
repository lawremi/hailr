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

.HailTableRowContext <- setClass("HailTableRowContext",
                                 slots=c(hailTable="HailTable"),
                                 contains="HailExpressionContext")

.HailGlobalContext <- setClass("HailGlobalContext",
                               slots=c(src="ANY"),
                               contains="HailExpressionContext")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction
###

HailTable <- function(impl) {
    .HailTable(impl=impl)
}

RangeHailTable <- function(hc, n, n_partitions=NULL) {
    HailTable(jvm(hc$impl)$is$hail$table$Table$range(hc$impl, n,
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

HailGlobalContext <- function(src) {
    .HailGlobalContext(src=src)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("hailType", "HailTable",
          function(x) as(x$impl$tir()$typ(), "HailType"))

## We lazily (as features are needed) reimplement the Python API

.HailTable$methods(
    row = function() {
        Promise(rowType(hailType(.self)), HailRef(HailSymbol("row")),
                HailTableRowContext(.self))
    },
    rowValue = function() {
        row <- .self$row()
        row[setdiff(names(row), .self$keys())]
    },
    keys = function() {
        as.character(.self$impl$key())
    },
    globals = function() {
        Promise(globalType(hailType(.self)), HailRef(HailSymbol("global")),
                HailGlobalContext(.self))
    },
    select = function(...) {
        .self$selectStruct(struct(...))
    },
    selectStruct = function(expr) {
        if (getOption("verbose")) {
            message("select: {", as.character(expr), "}")
        }
        HailTable(.self$impl$mapRows(as.character(expr)))
    },
    selectGlobals = function(...) {
        expr <- struct(...)
        HailTable(.self$impl$selectGlobal(as.character(expr)))
    },
    annotate = function(...) {
        s <- struct(...)
        r <- expr(.self$row())
        r[names(s)] <- s
        .self$selectStruct(r)
    },
    annotate1 = function(name, value) {
        .self$annotate(setNames(list(value), name))
    },
    filter = function(expr) {
        HailTable(.self$impl$filter(as.character(expr), FALSE))
    },
    keyBy = function(...) {
        ## In Python, this also accepts keyword arg select expressions,
        ## but we intentionally do not support that.
        keys <- as.character(c(...))
        HailTable(.self$impl$keyBy(as.array(keys)))
    },
    join = function(left, right, how = "inner") {
        check_compatible_keys(left, right)
        HailTable(.self$impl$join(right$impl, how))
    },
    count = function() {
        .self$impl$count()
    },
    head = function(n) {
        HailTable(.self$impl$head(n))
    },
    collect = function() {
        df <- fromJSON(.self$impl$collectJSON())
        df[] <- Map(cast, df, rowType(hailType(.self)))
        df
    }
)

check_compatible_keys <- function(left, right) {
    identical(keyType(hailType(left)), keyType(hailType(right)))
}

struct <- function(...) {
    args <- list(...)
    if (length(args) == 1L && is.list(args[[1L]]))
        args <- args[[1L]]
    if (is.null(names(args)))
        names(args)[] <- ""
    unnamed <- names(args) == ""
    names(args)[unnamed] <- vapply(args[unnamed],
                                   function(x) name(symbol(x)),
                                   character(1L))
    args[unnamed] <- lapply(args[unnamed], HailRef)
    HailMakeStruct(args)
}

## Could record nrow upon construction to avoid repeated Java calls
setMethod("nrow", "HailTable", function(x) x$count())

hailTable <- function(x) x@hailTable
`hailTable<-` <- function(x, value) {
    x@hailTable <- value
    x
}

setMethod("contextualLength", c("HailPromise", "HailTableRowContext"),
          function(x, context) nrow(hailTable(context)))

src <- function(x) x@src

### TODO: this needs to drop NAs from 'i'
## setMethod("extractROWS", c("HailTable", "HailWhichPromise"), function(x, i) {
##     extractROWS(x, logicalPromise(i))
## })

setMethod("context", "HailTable", function(x) {
    HailContext(x$impl$hc())
})

setMethod("parent", "HailTableRowContext", function(x) context(hailTable(x)))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Collection
###

setMethod("deriveTable", "HailTableRowContext", function(context, expr) {
    ### TODO: we always get the keys back, so if expr is simply a key
    ###       there is no reason to $select() here.
    hailTable(context)$select(x = expr)$selectGlobals()
})

setMethod("deriveTable", "HailGlobalContext", function(context, expr) {
    table <- globalTable(src(context)$selectGlobals(x = expr))
    table$select(x = expr(table$globals()$x))
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

setMethod("head", "HailTableRowContext", function(x, n) {
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
    left_idx <- left$addIndex(idx)$keyBy(idx)
    right_idx <- right$addIndex(idx)$keyBy(idx)
    left_idx$join(right_idx)$drop(idx)
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
