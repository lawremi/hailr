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
    .HailDataFrame(DataFrame(table$rows()),
                   table=table,
                   metadata=table$globals())
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
                                      key.names = character(0L),
                                      n.partitions = NULL)
{
    stopifnot(is.logical(header), length(header) == 1L, !anyNA(header),
              is.character(sep), length(sep) == 1L, !anyNA(sep),
              is.character(quote), length(quote) == 1L, !anyNA(quote),
              nchar(quote) == 1L,
              is.character(na.strings), length(na.strings) == 1L,
              !anyNA(na.strings),
              is.character(colClasses), !anyNA(colClasses),
              is.character(comment.char), length(comment.char) == 1L,
              !anyNA(comment.char),
              is.character(key.names), !anyNA(key.names),
              !anyNA(n.partitions),
              is.null(n.partitions) ||
                  (is.numeric(n.partitions) && length(n.partitions) == 1L))

    impute <- TRUE
    if (identical(colClasses, "character")) {
        impute <- FALSE
        types <- list()
    } else {
        types <- colClassesToHailTypes(colClasses)
    }

    if (comment.char == "") {
        comment.char <- NULL
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
                                        quote))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

setGeneric("escapeColumn", function(x) standardGeneric("escapeColumn"))
setMethod("escapeColumn", "List", function(x) escapeColumn(as.list(x)))
setMethod("escapeColumn", "list", function(x) I(x))

setGeneric("ncolsAsDF", function(x) standardGeneric("ncolsAsDF"))
setMethod("ncolsAsDF", "ANY", function(x) ncol(as.data.frame(escapeColumn(x))))

