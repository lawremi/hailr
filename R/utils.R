uuid <- function(prefix) {
    path <- tempfile(paste0("__", prefix, "_"), "")
    substring(path, 2)
}
