### =========================================================================
### HailGRanges objects
### -------------------------------------------------------------------------
###
### A GRanges backed by a HailDataFrame
###

setClass("HailGRanges", slots=c(df="HailDataFrame"), contains="GenomicRanges")


