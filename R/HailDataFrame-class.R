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
              select(df, cols)
          })

setMethod("unmarshal", c("HailTable", "data.frame"),
          function(x, skeleton) {
              df <- callNextMethod()
              cols <- mapply(unmarshal, df, skeleton, SIMPLIFY=FALSE)
              select(df, cols)
          })

setAs("ANY", "HailDataFrame", function(from) {
    push(as(from, "DataFrame"), hail())
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Synchronize underlying table with current columns
###

tableForCols <- function(cols, table, context) {
    if (!is(table, "HailTable"))
        hailTable(send(DataFrame(cols), context))
    else table$select(lapply(cols, expr))
}

setGeneric("push", function(x, remote) standardGeneric("push"))

setMethod("push", c("HailDataFrame", "missing"),
          function(x, remote = hailTable(x)$hailContext()) {
              push(x, remote)
          })

setMethods("push",
           list(c("DataFrame", "is.hail.HailContext"),
                c("data.frame", "is.hail.HailContext")),
           function(x, remote) {
               if (ncol(x) == 0L) {
                   return(tableForCols(x,
                                       if (is(x, "HailDataFrame")) hailTable(x),
                                       remote))
               }
               cols <- as.list(x)
               ctx <- context(x[[1L]])
               ans_table <- NULL
               
               repeat {
                   in_ctx <- vapply(cols,
                                    function(xi) identical(context(xi), ctx),
                                    logical(1L))
                   table <- tableForCols(cols[in_ctx], ctx, remote)
                   ans_table <- bindCols(ans_table, table)
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

setMethod("extractROWS", c("HailDataFrame", "ANY"), function(x, i) {
    HailDataFrame(extractROWS(hailTable(x), i))
})

setMethod("[", "HailDataFrame", function(x, i, j, ..., drop = TRUE) {
    df <- callNextMethod()
    if (!identical(names(df), names(x)))
        hailTable(df) <- hailTable(df)$select(lapply(df, expr))
    df
})

setReplaceMethod("[", "HailDataFrame", function(x, i, j, ..., value) {
    df <- callNextMethod() # an invalid HailDataFrame
    push(df)
})

setMethod("setListElement", "HailDataFrame", function(x, i, value) {
    df <- callNextMethod()
    push(df)
})

## like dplyr's transmute()
select <- function(x, vars) {
    if (identical(as.list(x), vars))
        return(x)
    HailDataFrame(hailTable(x)$select(lapply(vars, expr)))
}

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

setGeneric("escapeColumn", function(x) standardGeneric("escapeColumn"))
setMethod("escapeColumn", "List", function(x) escapeColumn(as.list(x)))
setMethod("escapeColumn", "list", function(x) I(x))

setGeneric("ncolsAsDF", function(x) standardGeneric("ncolsAsDF"))
setMethod("ncolsAsDF", "ANY", function(x) ncol(as.data.frame(escapeColumn(x))))

