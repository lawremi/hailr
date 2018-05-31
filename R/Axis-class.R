### =========================================================================
### Axis objects
### -------------------------------------------------------------------------
###
### Sort of an enumeration of available axes, used for dispatch
###

setClass("Axis")

ROW_AXIS <- setClass("RowAxis", contains="Axis")()

COL_AXIS <- setClass("ColumnAxis", contains="Axis")()

GLOBAL_AXIS <- setClass("GlobalAxis", contains="Axis")()
