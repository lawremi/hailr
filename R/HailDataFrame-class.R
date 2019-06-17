### =========================================================================
### HailDataFrame objects
### -------------------------------------------------------------------------
###
### Implements a data.frame-style API on top of an is.hail.table.Table.
###

### Major differences between is.hail.table.Table and data.frame:
## - Tables have a set of 'keys', columns by which the table can be
##   split and joined with other tables. We should keep this as an
##   internal detail.

.HailDataFrame <- setClass("HailDataFrame",
                           slots=c(hailTable="HailTable"),
                           contains="DataFrame")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

HailDataFrame <- function(table) {
    ### FIXME: this will query for the length() of every column. We
    ### could just query the 'table' and set the length on the promise
    ### (requires adding a nullable length slot).
    length(as.list(table$row())[[1L]])
    .HailDataFrame(DataFrame(as.list(table$row())),
                   hailTable=table,
                   metadata=as.list(table$globals()))
}

setMethod("unmarshal", c("HailTable", "ANY"),
          function(x, skeleton) HailDataFrame(x))

setMethod("unmarshal", c("HailTable", "DataFrame"),
          function(x, skeleton) {
              df <- callNextMethod()
              ncl <- lapply(skeleton, ncolsAsDF)
              cnl <- split(colnames(df), PartitioningByWidth(ncl))
              cols <- mapply(function(cn, skel) {
                  if (length(cn) == 1L)
                      unmarshal(df[[cn]], skel)
                  else unmarshal(df[cn], skel)
              }, cnl, skeleton, SIMPLIFY=FALSE)
              project(df, cols)
          })

setMethod("unmarshal", c("HailTable", "data.frame"),
          function(x, skeleton) {
              df <- callNextMethod()
              cols <- mapply(unmarshal, df, skeleton, SIMPLIFY=FALSE)
              project(df, cols)
          })

setAs("ANY", "HailDataFrame", function(from) {
    push(as(from, "DataFrame"), hail())
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Synchronize underlying table with current columns
###

pushCols <- function(cols, src_context, target_context) {
    if (derivesFrom(src_context, target_context))
        hailTable(src_context)$project(cols)
    else {
        hailTable(send(DataFrame(cols), target_context))
    }
}

setGeneric("push", function(x, remote) standardGeneric("push"))

setMethod("push", c("HailDataFrame", "missing"),
          function(x, remote) {
              push(x, context(x))
          })

scalar <- function(x, remote) {
    if (!derivesFrom(context(x), remote) && length(ans <- unique(x)) == 1L)
        ans
}

setMethods("push",
           list(c("DataFrame", "HailContext"),
                c("data.frame", "HailContext")),
           function(x, remote) {
               table <- if (is(x, "HailDataFrame")) hailTable(x)
               if (ncol(x) == 0L) {
                   return(send(x, remote))
               }
               cols <- as.list(x)
               ctx <- context(x[[1L]])
               ans_table <- NULL

               ### Fastpath currently disabled while we make the slow path work
               ## if (!is.null(table)) {
               ##     scalars <- Filter(Negate(is.null), lapply(cols, scalar))
               ##     cols[names(scalars)] <- lapply(scalars, Promise,
               ##                                    type=, context=table)
               ## }
               
               repeat {
                   in_ctx <- vapply(cols,
                                    function(xi) identical(context(xi), ctx),
                                    logical(1L))
                   ctx_table <- pushCols(cols[in_ctx], ctx, remote)
                   ans_table <- bindCols(ans_table, ctx_table)
                   cols <- cols[!in_ctx]
                   if (length(cols) == 0L)
                       break
                   ctx <- context(cols[[1L]])
               }
               
               HailDataFrame(ans_table)[names(x)]
           })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessor methods.
###

setMethod("context", "HailDataFrame", function(x) context(hailTable(x)))

setMethod("extractCOLS", "HailDataFrame", function(x, i) {
    ans <- callNextMethod()
    if (!identical(names(ans), names(x)))
        select(ans, names(ans))
    else ans
})

push_replacement <- function(x, value) {
    if (is.null(value)) {
        select(x, names(x))
    } else {
        push(x)
    }
}

setMethod("replaceCOLS", "HailDataFrame", function(x, i, value) {
    push_replacement(callNextMethod(), value)   
})

setMethod("setListElement", "HailDataFrame", function(x, i, value) {
    push_replacement(callNextMethod(), value)
})

select <- function(x, names) {
    HailDataFrame(hailTable(x)$select(names))
}

project <- function(x, cols) {
    HailDataFrame(hailTable(x)$project(cols))
}

with_temps <- function(x, FUN, use.names = FALSE, temps = list(), ...) {
    temps <- c(temps, list(...))
    to_push <- vapply(temps,
                       function(temp) {
                           !identical(context(temp), x) ||
                               (use.names && !is(expr(temp), "HailRef"))
                       },
                       logical(1L))
    names(temps)[to_push] <- vapply(names(to_push)[to_push], uuid,
                                    character(1L))
    x[names(temps)[to_push]] <- temps[to_push]
    if (use.names) {
        nms <- names(temps)
        nms[!to_push] <- vapply(temps[!to_push],
                                function(temp) name(symbol(expr(temp))),
                                character(1L))
        ans <- FUN(x, nms)
    } else {
        temps[to_push] <- as.list(x[to_push])
        ans <- do.call(FUN, c(list(x), temps))
    }
    ans[to_push] <- NULL
    ans
}

setMethod("extractROWS", c("HailDataFrame", "HailOrderPromise"),
          function(x, i) {
              with_temps(x, function(x, nms) {
                  x$orderBy(nms, sort_types(i))
              }, use.names=TRUE, promises(i))
          })

setMethods("extractROWS",
           list(c("HailDataFrame", "logical"),
                c("HailDataFrame", "BooleanPromise")),
           function(x, i) {
               with_temps(x, function(x, i) {
                   x$filter(i)
               }, i = i)
           })

isSlice <- function(i) length(i) > 0L && identical(i, head(i, 1L):tail(i, 1L))

isHead <- function(i) identical(head(i, 1L), 1L) && isSlice(i)

setMethods("extractROWS",
           list(c("HailDataFrame", "numeric"),
                c("HailDataFrame", "NumericPromise")),
           function(x, i) {
               if (isHead(i)) {
                   ans <- x$head(length(i))
               } else {
                   ind <- seq_along_rows(x)
                   if (isSlice(i)) {
                       ans <- x$filter(ind >= head(i, 1L) & ind <= tail(i, 1L))
                   } else {
                       idf <- push(DataFrame(ind = i), context(x))
                       ans <- with_temps(x, function(x, key) {
                           idf$keyBy("ind")$join(x$keyBy(key))
                       }, use.names=TRUE, ind)
                   }
               }
               ans
           })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Summarizing
###

setMethod("head", "HailDataFrame", function(x, n) {
    HailDataFrame(impl(x)$head(n))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### I/O
###

readHailDataFrame <- function(file) {
    HailDataFrame(readHailTable(file))
}

colClassesToHailTypes <- function(colClasses) {
    if (length(colClasses) > 0L && is.null(names(colClasses)))
        stop("'colClasses' must be named and not contain NAs")
    lapply(colClasses, function(cc) hailType(new(cc)))
}

## Major differences:
##
## quote: only supports a single character
## na.strings: only supports a single string
## colClasses: must be named, or identical to "character";
##             if columns omitted, taken as character, not dropped
readHailDataFrameFromText <- function(file, header = FALSE, sep = "",
                                      quote = "\"",
                                      na.strings = "NA",
                                      colClasses = character(0L),
                                      comment.char = "#",
                                      blank.lines.skip = TRUE,
                                      key.names = character(0L),
                                      n.partitions = NULL)
{
    stopifnot(isSingleString(file),
              isTRUEorFALSE(header),
              isSingleString(sep),
              isSingleString(quote),
              nchar(quote) == 1L,
              isSingleString(na.strings),
              is.character(colClasses), !anyNA(colClasses),
              isSingleString(comment.char),
              isTRUEorFALSE(blank.lines.skip),
              is.character(key.names), !anyNA(key.names),
              is.null(n.partitions) || isSingleNumber(n.partitions))

    impute <- TRUE
    if (identical(colClasses, "character")) {
        impute <- FALSE
        types <- list()
    } else {
        types <- colClassesToHailTypes(colClasses)
    }

    if (comment.char == "") {
        comment.char <- character(0L)
    }
    if (quote == "") {
        quote <- NULL
    }
    if (sep == "") {
        sep <- "[ \\r\\n\\t]+"
    }
    
    HailDataFrame(readHailTableFromText(file, key.names,
                                        n.partitions,
                                        types,
                                        comment.char,
                                        sep, na.strings, !header, impute,
                                        quote, blank.lines.skip))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

setGeneric("escapeColumn", function(x) x)
setMethod("escapeColumn", "List", function(x) escapeColumn(as.list(x)))
setMethod("escapeColumn", "list", function(x) I(x))

setGeneric("ncolsAsDF", function(x) ncol(as.data.frame(escapeColumn(x))))

seq_along_rows <- function(x) {
    expr <- HailApplyScanOp(Accumulation("Count"))
    Promise(hailTable(x), expr)
}
