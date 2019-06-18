### =========================================================================
### HailMatrixTable objects
### -------------------------------------------------------------------------
###
### Direct mapping of Hail MatrixTable API to R. HailExperiment wraps
### this to provide the familiar SE API. Provides promises of its
### entries, i.e., assay matrices, and access to metadata tables.
###

.is.hail.variant.MatrixTable <- setClass("is.hail.variant.MatrixTable",
                                         contains="JavaObject")

.HailMatrixTable <- setRefClass("HailMatrixTable",
                                fields=c(impl="is.hail.variant.MatrixTable"))

setClass("HailMatrixTableContext", slots=c(matrixTable="HailMatrixTable"),
         contains="HailExpressionContext")

.HailMatrixTableEntryContext <- setClass("HailMatrixTableEntryContext",
                                         contains="HailMatrixTableContext")

.HailMatrixTableRowContext <- setClass("HailMatrixTableRowContext",
                                       contains="HailMatrixTableContext")

.HailMatrixTableColContext <- setClass("HailMatrixTableColContext",
                                       contains="HailMatrixTableContext")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction
###

HailMatrixTable <- function(.impl) {
    .is.hail.variant.MatrixTable(impl=.impl)
}

HailMatrixTableEntryContext <- function(matrixTable) {
    .HailMatrixTableEntryContext(matrixTable=matrixTable)
}

HailMatrixTableRowContext <- function(matrixTable) {
    .HailMatrixTableRowContext(matrixTable=matrixTable)
}

HailMatrixTableColContext <- function(matrixTable) {
    .HailMatrixTableColContext(matrixTable=matrixTable)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Copying
###

## Copying a SummarizedExperiment would involve:
## 1) Marshalling to a list of data.frames (SEFrames)
## 2) Transmitting a list just transmits the elements (data.frames),
##    but we need to downcast to SESparkFrames
## 3) So that transmission converts the Spark SQL DFs to Hail Tables.
## 4) Call equivalent of Python's from_row_table() to make a MatrixTable
##    and use annotate_* to add the entries, column data, and globals.

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("hailType", "HailMatrixTable",
          function(x) as(x$impl$ast()$typ(), "HailType"))

.HailMatrixTable$methods(
    entry = function() {
        Promise(entryType(hailType(.self)), "g",
                HailMatrixTableEntryContext(.self))
    },
    row = function() {
        Promise(rowType(hailType(.self)), "va",
                HailMatrixTableRowContext(.self))
    },
    col = function() {
        Promise(colType(hailType(.self)), "sa",
                HailMatrixTableColContext(.self))
    },
    globals = function() {
        Promise(globalType(hailType(.self)), "global",
                HailMapGlobalsContext(.self))
    },
    entries = function() {
        HailTable(.self$impl$entriesTable())
    },
    rows = function() {
        HailTable(.self$impl$rowsTable())
    },
    cols = function() {
        HailTable(.self$impl$colsTable())
    },
    selectEntries = function(...) {
        expr <- HailMakeStruct(...)
        HailMatrixTable(.self$impl$selectEntries(as.character(expr)))
    },
    selectRows = function(...) {
        expr <- HailMakeStruct(...)
        HailMatrixTable(.self$impl$selectRows(as.character(expr),
                                              JavaArrayList()))
    },
    selectCols = function(...) {
        expr <- HailMakeStruct(...)
        HailMatrixTable(.self$impl$selectCols(as.character(expr),
                                              JavaArrayList()))
    },
    selectGlobals = function(...) {
        expr <- HailMakeStruct(...)
        HailMatrixTable(.self$impl$selectGlobals(as.character(expr)))
    },
    count = function() {
        .self$impl$count()
    },
    countRows = function() {
        .self$impl$countRows()
    },
    countCols = function() {
        .self$impl$countCols()
    }
)

setMethod("contextualLength", c("HailPromise", "HailMatrixTableRowContext"),
          function(x, context) matrixTable(context)$countRows())

setMethod("contextualLength", c("HailPromise", "HailMatrixTableColContext"),
          function(x, context) matrixTable(context)$countCols())

setGeneric("contextualDim", function(x, context) NULL)

setMethod("contextualDim", c("HailPromise", "HailMatrixTableEntryContext"),
          function(x, context) matrixTable(context)$count())

matrixTable <- function(x) x@matrixTable

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Collection
###

setMethod("deriveTable", "HailMatrixTableEntryContext", function(context, expr)
{
    entries <- matrixTable(context)$selectEntries(x = expr)
    entries$selectRows()$selectCols()$entries()$selectGlobals()
})

### TODO: we always get the keys back, so if expr is simply a key
###       there is no reason to $selectRows/Cols() here.

setMethod("deriveTable", "HailMatrixTableRowContext", function(context, expr) {
    matrixTable(context)$selectRows(x = expr)$rows()$selectGlobals()
})

setMethod("deriveTable", "HailMatrixTableColContext", function(context, expr) {
    matrixTable(context)$selectCols(x = expr)$cols()$selectGlobals()
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### I/O
###

readMatrixTable <- function(file, drop.rows=FALSE, drop.cols=FALSE)
{
    hail_context()$readMatrixTable(file, drop.rows, drop.cols)
}

genomeFromVCFHeader <- function(file) {
    if (requireNamespace("VariantAnnotation")) {
        genome <- unique(genome(VariantAnnotation::scanVcfHeader(file)))
        if (length(genome) != 1L) {
            stop("Unable to determine 'genome' from VCF header, please specify")
        }
        genome
    } else {
        stop("If 'genome' is missing, the VariantAnnotation package is ",
             "required to extract the genome from the VCF header")
    }
}

readHailMatrixTableFromVCF <- function(file, genome=NA_character_)
{
    stopifnot(isSingleStringOrNA(genome))
    if (is.na(genome)) {
        genome <- genomeFromVCFHeader(file)
    }
    hail_context()$importVCF(file, genome)
}

## TODO: readMatrixTableFromPlink()
