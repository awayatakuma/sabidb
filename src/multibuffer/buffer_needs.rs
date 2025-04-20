use std::i32;

pub fn best_root(available: i32, size: i32) -> i32 {
    let avail = available - 2;
    if avail <= 1 {
        return 1;
    }
    let mut k = i32::MAX;
    let mut i = 1;
    while k > avail {
        i += 1;
        k = i32::pow(size, 1 / i);
    }

    return k;
}

pub fn best_factor(available: i32, size: i32) -> i32 {
    let avail = available - 2;
    if avail <= 1 {
        return 1;
    }

    let mut k = size;
    let mut i = 1;
    while k > avail {
        i += 1;
        k = i32::pow(size, 1 / i);
    }

    return k;
}
