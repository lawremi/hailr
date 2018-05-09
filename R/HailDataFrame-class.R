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

HailDataFrame <- function(hailTable) {
    .HailDataFrame(DataFrame(promises(hailTable)), hailTable=hailTable)
}

setMethod("unmarshal", c("HailTable", "ANY"),
          function(x, skeleton) HailDataFrame(x))

setMethod("unmarshal", c("HailTable", "DataFrame"),
          function(x, skeleton) {
              df <- callNextMethod()
              ncl <- lapply(skeleton, ncolsAsDF)
              cnl <- split(colnames(df), PartitioningByWidth(ncl))
              cols <- mapply(function(cn, target) {
                  as(df[cn], class(target), strict=FALSE)
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

## Better way to get the Hail type for R objects:

## - Create a "literal" promise in the HailContext
##   - Requires R type => Hail promise type mapping
## - Ask the promise for its Hail type
##   - Typically this is just a query to e.g. the Hail table
##   - Since we have a literal, there needs to be a default mapping from
##     the promise type to the Hail type.

R_TYPE_TO_HAIL_TYPE <- c(logical="TBoolean", integer="TInt32",
                         numeric="TFloat64", character="TString")

normColClasses <- function(colClasses) {
    if (length(colClasses) > 0L && is.null(names(colClasses)))
        stop("'colClasses' must be named and not contain NAs")
    colClasses[] <- R_TYPE_TO_HAIL_TYPE[colClasses]
    if (anyNA(colClasses))
        stop("colClass ", paste(colClasses[is.na(colClasses)], collapse=", "),
             " not supported by Hail. Valid classes: ",
             paste0(R_TYPE_TO_HAIL_TYPE, collapse=", "), ".")
    pkg <- jvm(hail_context())$is$hail$expr$types
    lapply(colClasses, function(x) scala_object(pkg[[x]]))
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
        colClasses <- list()
    } else {
        colClasses <- normColClasses(colClasses)
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
                                        colClasses,
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

