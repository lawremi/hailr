### =========================================================================
### HailMatrixTable objects
### -------------------------------------------------------------------------
###
### Direct mapping of Hail MatrixTable API to R. HailExperiment wraps
### this to provide the familiar SE API. Provides promises of its
### entries, i.e., assay matrices, and access to metadata tables.
###

setClass("is.hail.variant.MatrixTable", contains="JavaObject")

.HailMatrixTable <- setRefClass("HailMatrixTable",
                                fields=c(impl="is.hail.variant.MatrixTable"),
                                contains="Context")

