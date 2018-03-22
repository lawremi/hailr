### =========================================================================
### HailExperiment objects
### -------------------------------------------------------------------------
###
### Represents a Hail VariantDataset as a SummarizedExperiment derivative.
###

setClass("HailExperiment", slots=c(matrixTable="MatrixTable"),
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### I/O
###

readHailExperiment <- function(file, ..., hail = hail_context())
{
    HailExperiment(readMatrixTable(file, ..., hail=hail))
}

