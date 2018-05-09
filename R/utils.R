uuid <- function(prefix) {
    path <- tempfile(prefix, "")
    substring(path, 2)
}
