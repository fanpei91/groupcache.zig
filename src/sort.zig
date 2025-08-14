const std = @import("std");

///  Returns the smallest index i in [0, n) at which predicate(i) is true.
///  Returns n if not found.
pub fn binarySearch(
    n: usize,
    ctx: anytype,
    comptime predicate: fn (ctx: @TypeOf(ctx), i: usize) bool,
) usize {
    var i: usize = 0;
    var j: usize = n;
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

test binarySearch {
    const numbers = [_]u8{ 1, 5, 10, 15, 20, 25 };
    const f1 = struct {
        fn compare(_: void, i: usize) bool {
            return numbers[i] >= 3;
        }
    }.compare;
    var idx = binarySearch(numbers.len, {}, f1);
    try std.testing.expectEqual(1, idx);

    const f2 = struct {
        fn compare(_: void, i: usize) bool {
            return numbers[i] >= 30;
        }
    }.compare;
    idx = binarySearch(numbers.len, {}, f2);
    try std.testing.expectEqual(numbers.len, idx);
}
