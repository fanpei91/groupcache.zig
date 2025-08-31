const std = @import("std");

///  Returns the smallest index i in [0, n) at which predicate(i) is true.
///  Returns n if not found.
pub fn binarySearch(
    len: usize,
    ctx: anytype,
    comptime predicate: *const fn (ctx: @TypeOf(ctx), i: usize) bool,
) usize {
    var i: usize = 0;
    var j: usize = len;
    while (i < j) {
        const h: usize = i + (j - i) / 2;
        // i ≤ h < j
        if (!predicate(ctx, h)) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    return i;
}
