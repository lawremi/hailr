### =========================================================================
### HailMetaDataFrame objects
### -------------------------------------------------------------------------
###
### Represents a DataFrame that is backed by the row or col metadata
### table in the Hail MatrixTable.

### Although the MatrixTable has accessors that construct and return
### Table objects for the metadata tables, they are independent
### contexts from the MatrixTable itself, i.e., the top-level
### references in the expressions are different. The promises
### themselves definitely need to backed by the MatrixTable, or have
### some knowledge that they are derived from it. However, the
### DataFrame derivative could just be backed by the Hail Table.
###
### There are two obvious solutions to this:

### 1) Introduce HailMetaDataFrame that is backed by the MatrixTable,
###    via the row/col context. These could share code with
###    HailDataFrame if we introduce a secondary layer of dispatch
###    based on the context/table.
###
### 2) Just reuse HailDataFrame, backing it by the corresponding
###    Table. The promises would probably still be backed by the
###    row/col context of the MatrixTable. This would require
###    translating top-level references when generating Table-level
###    expressions.

### For now we will try (1) and assess the extent of duplication. If
### it is a lot simpler to use the underlying Hail Table, we can
### switch to (2), but for now (1) has the advantage since it matches
### up better with the Python interface (where btw mt.rows() is
### incompatible with mt's row dimension).

.HailMetaDataFrame <- setClass("HailMetaDataFrame",
                               slots=c(src="HailMatrixTableContext"),
                               contains="DataFrame")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

HailMetaDataFrame <- function(struct) {
    .HailMetaDataFrame(DataFrame(as.list(struct)), src=context(struct))
}
