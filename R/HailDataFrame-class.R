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

setClass("is.hail.table.Table", contains="SparkObject")

### FIXME: Should this inherit from DataFrame, with @listData
###        just prefilled with promises? Or just DataTable?
###
### DataTable has: NROW(), NCOL(), ROWNAMES(), dim(), dimnames(),
###                head(), tail() [smart interpretation of subscripts],
###                subset() [if evalqForSubset returned promise],
###                transform()?, show()?
###
### Basically, if something could be done with DataFrame but not
### DataTable, then we should just push it up to DataTable. But
### prepolulation would have performance benefits.

### The Python interface creates the equivalent of a list promise of
### the columns. The list promise eagerly populates itself with
### element promises. We will probably need a list (really a 'struct')
### promise anyway, so we could reuse that here. But it is simpler to
### create the promises on the fly, so the only motivation for eagerly
### creating the promises is performance: many queries of the table
### signature (needed to create a promise) would start to add up. But
### we would only need to store the signatures (of metadata and table
### data) to avoid interacting with Spark whenever creating a promise.

### In rsolr, SolrFrame was a tuple of the SolrQuery and
### SolrCore. Deferred evaluation was captured in the SolrQuery, and
### eventually it materialized the query using the SolrCore. In this
### case, the Hail table already defers, so there is no need for a
### query, and really the table corresponds to the Solr core. Unlike
### with Solr, however, it is easy to imagine operations across data
### structures (cores) within the same Spark instance (server). But
### anyway, a SolrCore had the reference to the core (URL) and a
### downloaded schema, what Hail calls the 'signature'.

setClass("HailDataFrame",
         slots=c(hailTable="is.hail.table.Table",
                 type="is.hail.expr.types.TableType"),
         contains="DataTable")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

HailTable <- function(table) {
    new("HailTable", table=table, type=table$tir$typ, "HailType")
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessor methods.
###

### TODO: colnames(), rownames(), dimnames(), nrow(), ncol()

hailTable <- function(x) x@hailTable

setMethod("nrow", "HailDataFrame", function(x) hailTable(x)$count())

setMethod("ncol", "HailDataFrame", function(x) length())

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### I/O
###

readHailDataFrame <- function(file) {
    HailDataFrame(hail_context()$readTable(file))
}

R_TYPE_TO_HAIL_TYPE <- c(logical="TBoolean", integer="TInt32",
                         numeric="TFloat32", character="TString")

normColClasses <- function(colClasses) {
    if (length(colClasses) > 0L && is.null(names(colClasses)))
        stop("'colClasses' must be named and not contain NAs")
    colClasses[] <- R_TYPE_TO_HAIL_TYPE[colClasses]
    if (anyNA(colClasses))
        stop("colClass ", paste(colClasses[is.na(colClasses)], collapse=", "),
             " not supported by Hail. Valid classes: ",
             paste0(R_TYPE_TO_HAIL_TYPE, collapse=", "), ".")
    sc <- sparkConnection(hail_context())
    pkg <- sc$is$hail$expr$types
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
                                      colClasses = character(),
                                      comment.char = "#",
                                      key.names = character(),
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
        colClasses <- character()
    }
    colClasses <- normColClasses(colClasses)

    if (comment.char == "") {
        comment.char <- NULL
    }
    if (quote == "") {
        quote <- NULL
    } else {
        quote <- jvm(hail_context())$java$lang$Character$new(quote)
    }
    if (sep == "") {
        sep <- "[ \\r\\n\\t]+"
    }
    
    HailDataFrame(hail_context()$importTable(file, key.names,
                                             as.integer(n.partitions),
                                             colClasses,
                                             ScalaOption(comment.char),
                                             sep, na.strings, !header, impute,
                                             quote))
}
