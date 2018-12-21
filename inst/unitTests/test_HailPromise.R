library(hailr)
test_HailPromise_operators <- function() {
    rows <- data.frame(a = c(4L, 0L, 4L),
                       b = c(1L, 5L, 2L),
                       c = c(3L, 13L, 20L),
                       d = c(5L, -1L, 3L),
                       e = c("hello", "cat", "dog"),
                       f = integer_list(1:3, integer(0L), 5:7),
                       stringsAsFactors = FALSE
                       )
    options(verbose=TRUE)
    kt <- send(rows, hail())

    do_transform <- function(x)
        transform(x,
                  x1 = a + 5L,
                  x2 = 5L + a,
                  x3 = a + b,
                  x4 = a - 5,
                  x5 = 5L - a,
                  x6 = a - b,
                  x7 = a * 5,
                  x8 = 5L * a,
                  x9 = a * b,
                  x10 = a / 5L,
                  x11 = 5L / a,
                  x12 = a / b,
                  x13 = -a,
                  x14 = +a,
                  x15 = a == b,
                  x16 = a == 5L,
                  x17 = 5L == a,
                  x18 = a != b,
                  x19 = a != 5L,
                  x20 = 5L != a,
                  x21 = a > b,
                  x22 = a > 5L,
                  x23 = 5L > a,
                  x24 = a >= b,
                  x25 = a >= 5L,
                  x26 = 5L >= a,
                  x27 = a < b,
                  x28 = a < 5L,
                  x29 = 5L < a,
                  x30 = a <= b,
                  x31 = a <= 5L,
                  x32 = 5L <= a,
                  x33 = (a == 0L) & (b == 5L),
                  x34 = (a == 0L) | (b == 5L),
                  x35 = FALSE,
                  x36 = TRUE)

    do_transform2 <- function(x)
        transform(x,
                  x35 = FALSE,
                  x36 = TRUE)

    result <- as.data.frame(head(do_transform2(kt), 1L))
    expected <- head(do_transform(rows), 1L)
    checkEquals(result, expected)
}
