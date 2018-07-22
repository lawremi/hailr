### =========================================================================
### HailExperiment objects
### -------------------------------------------------------------------------
###
### Represents a Hail VariantDataset as a SummarizedExperiment derivative.
###

.HailExperiment <- setClass("HailExperiment",
                            slots=c(matrixTable="HailMatrixTable"),
                            contains="SummarizedExperiment")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

HailExperiment <- function(matrixTable) {
    .HailExperiment(matrixTable=matrixTable)
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

