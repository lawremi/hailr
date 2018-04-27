### =========================================================================
### HailExperiment objects
### -------------------------------------------------------------------------
###
### Represents a Hail VariantDataset as a SummarizedExperiment derivative.
###

setClass("HailExperiment", slots=c(matrixTable="is.hail.variant.MatrixTable"),
         contains="SummarizedExperiment")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

HailExperiment <- function(matrixTable) {
    new("HailExperiment", matrixTable=matrixTable)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessor methods.
###

setMethod("colData", "HailExperiment", function(x) {
    
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### I/O
###

readHailExperiment <- function(file, ...)
{
    HailExperiment(readMatrixTable(file, ...))
}

readHailExperimentFromVCF <- function(file, ...)
{
    HailExperiment(readMatrixTableFromVCF(file, ...))
}

