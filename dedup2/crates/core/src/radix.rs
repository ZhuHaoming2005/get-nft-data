use rayon::prelude::*;
use std::mem::MaybeUninit;

const RADIX_BITS: u32 = 11;
const RADIX_SIZE: usize = 1 << RADIX_BITS;
const RADIX_MASK: u32 = (RADIX_SIZE as u32) - 1;
const PARALLEL_RADIX_MIN_LEN: usize = 32 * 1024;

#[derive(Clone, Copy)]
struct SharedOutput<T>(*mut MaybeUninit<T>);

// SharedOutput is used only with precomputed disjoint write ranges.
unsafe impl<T: Send> Send for SharedOutput<T> {}
// Concurrent access is write-only and every element has exactly one owner.
unsafe impl<T: Send> Sync for SharedOutput<T> {}

impl<T> SharedOutput<T> {
    unsafe fn write(&self, position: usize, value: T) {
        unsafe {
            self.0.add(position).write(MaybeUninit::new(value));
        }
    }
}

fn counting_pass<T: Copy>(
    source: &[T],
    destination: &mut [T],
    digit: impl Fn(T) -> usize,
    counts: &mut [usize],
) {
    counts.fill(0);
    for &value in source {
        counts[digit(value)] += 1;
    }
    let mut offset = 0;
    for count in counts.iter_mut() {
        let next = offset + *count;
        *count = offset;
        offset = next;
    }
    for &value in source {
        let bucket = digit(value);
        destination[counts[bucket]] = value;
        counts[bucket] += 1;
    }
}

fn parallel_counting_pass<T: Copy + Send + Sync>(
    source: &[T],
    digit: &(impl Fn(T) -> usize + Sync),
) -> Vec<T> {
    let worker_chunks = rayon::current_num_threads().saturating_mul(4).max(1);
    let chunk_size = source.len().div_ceil(worker_chunks).max(1);
    let local_counts: Vec<[usize; RADIX_SIZE]> = source
        .par_chunks(chunk_size)
        .map(|chunk| {
            let mut counts = [0usize; RADIX_SIZE];
            for &value in chunk {
                counts[digit(value)] += 1;
            }
            counts
        })
        .collect();

    let mut bucket_offsets = [0usize; RADIX_SIZE];
    let mut total = 0;
    for bucket in 0..RADIX_SIZE {
        bucket_offsets[bucket] = total;
        total += local_counts
            .iter()
            .map(|counts| counts[bucket])
            .sum::<usize>();
    }
    let mut next = bucket_offsets;
    let mut chunk_offsets = Vec::with_capacity(local_counts.len());
    for counts in &local_counts {
        chunk_offsets.push(next);
        for bucket in 0..RADIX_SIZE {
            next[bucket] += counts[bucket];
        }
    }

    let mut output: Vec<MaybeUninit<T>> = Vec::with_capacity(source.len());
    // Every output slot is assigned exactly once by the disjoint per-chunk offsets below.
    unsafe {
        output.set_len(source.len());
    }
    let output_pointer = SharedOutput(output.as_mut_ptr());
    source
        .par_chunks(chunk_size)
        .zip(chunk_offsets.par_iter())
        .for_each(|(chunk, offsets)| {
            let mut cursors = *offsets;
            for &value in chunk {
                let bucket = digit(value);
                let position = cursors[bucket];
                cursors[bucket] += 1;
                // Prefix sums reserve a unique destination range for every
                // (chunk, bucket), so concurrent writes cannot overlap.
                unsafe {
                    output_pointer.write(position, value);
                }
            }
        });

    let pointer = output.as_mut_ptr().cast::<T>();
    let len = output.len();
    let capacity = output.capacity();
    std::mem::forget(output);
    // All elements were initialized above, and MaybeUninit<T> has T's layout.
    unsafe { Vec::from_raw_parts(pointer, len, capacity) }
}

pub(crate) fn sort_by_digits<T: Copy + Send + Sync>(
    values: &mut Vec<T>,
    digits: impl Fn(usize, T) -> usize + Sync,
    passes: usize,
) {
    if values.len() < 2 {
        return;
    }
    let mut source = std::mem::take(values);
    for pass in 0..passes {
        let pass_digit = |value| digits(pass, value);
        if source.len() >= PARALLEL_RADIX_MIN_LEN && rayon::current_num_threads() > 1 {
            source = parallel_counting_pass(&source, &pass_digit);
        } else {
            let mut destination = vec![source[0]; source.len()];
            let mut counts = [0usize; RADIX_SIZE];
            counting_pass(&source, &mut destination, pass_digit, &mut counts);
            source = destination;
        }
    }
    *values = source;
}

pub(crate) fn u32_digit(value: u32, half: usize) -> usize {
    ((value >> (half as u32 * RADIX_BITS)) & RADIX_MASK) as usize
}

pub(crate) fn u64_digit(value: u64, quarter: usize) -> usize {
    ((value >> (quarter as u32 * RADIX_BITS)) & u64::from(RADIX_MASK)) as usize
}

pub(crate) fn sort_u64(values: &mut Vec<u64>) {
    sort_by_digits(values, |pass, value| u64_digit(value, pass), 6);
}

pub(crate) fn sort_u32_pairs(values: &mut Vec<(u32, u32)>) {
    sort_by_digits(
        values,
        |pass, value| {
            let (field, shift) = match pass {
                0..=2 => (value.1, pass as u32 * RADIX_BITS),
                _ => (value.0, (pass as u32 - 3) * RADIX_BITS),
            };
            u32_digit(field, (shift / RADIX_BITS) as usize)
        },
        6,
    );
}

pub(crate) fn sort_u32_triples(values: &mut Vec<(u32, u32, u32)>) {
    sort_by_digits(
        values,
        |pass, value| {
            let field = match pass / 3 {
                0 => value.2,
                1 => value.1,
                _ => value.0,
            };
            let shift = (pass % 3) as u32 * RADIX_BITS;
            u32_digit(field, (shift / RADIX_BITS) as usize)
        },
        9,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_width_radix_orders_supported_keys() {
        let mut singles = vec![u64::MAX, 1, 0, u32::MAX as u64 + 1];
        sort_u64(&mut singles);
        assert_eq!(singles, vec![0, 1, u32::MAX as u64 + 1, u64::MAX]);

        let mut pairs = vec![(2, 0), (1, 4), (1, 2), (0, u32::MAX)];
        sort_u32_pairs(&mut pairs);
        assert_eq!(pairs, vec![(0, u32::MAX), (1, 2), (1, 4), (2, 0)]);

        let mut triples = vec![(1, 2, 1), (0, 9, 9), (1, 1, 5), (1, 1, 4)];
        sort_u32_triples(&mut triples);
        assert_eq!(triples, vec![(0, 9, 9), (1, 1, 4), (1, 1, 5), (1, 2, 1)]);
    }

    #[test]
    fn parallel_radix_matches_comparison_sort() {
        let mut state = 0x9e3779b97f4a7c15_u64;
        let mut values = (0..100_000)
            .map(|index| {
                state ^= state << 7;
                state ^= state >> 9;
                state ^= state << 8;
                (state as u32, (state >> 32) as u32, index as u32)
            })
            .collect::<Vec<_>>();
        let mut expected = values.clone();
        expected.sort_unstable();
        sort_u32_triples(&mut values);
        assert_eq!(values, expected);
    }
}
