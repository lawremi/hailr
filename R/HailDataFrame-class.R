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
                           slots=c(table="HailTable"),
                           contains="DataFrame")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

HailDataFrame <- function(table) {
    .HailDataFrame(DataFrame(as.list(table$row())),
                   table=table,
                   metadata=as.list(table$globals()))
}

setMethod("unmarshal", c("HailTable", "ANY"),
          function(x, skeleton) HailDataFrame(x))

setMethod("unmarshal", c("HailTable", "DataFrame"),
          function(x, skeleton) {
              df <- callNextMethod()
              ncl <- lapply(skeleton, ncolsAsDF)
              cnl <- split(colnames(df), PartitioningByWidth(ncl))
              cols <- mapply(function(cn, target) {
                  as(df[cn], promiseClass(target), strict=FALSE)
              }, cnl, skeleton, SIMPLIFY=FALSE)
              do.call(transform, c(list(df), cols))[names(cols)]
          })

setAs("ANY", "HailDataFrame", function(from) {
    copy(as(from, "DataFrame"), hail())
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessor methods.
###

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

