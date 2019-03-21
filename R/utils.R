uuid <- function(prefix) {
    path <- tempfile(paste0("__", prefix, "_"), "")
    substring(path, 2)
}

asLength <- function(x) {
    if (x <= .Machine$integer.max)
        as.integer(x)
    else x
}
