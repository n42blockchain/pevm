// SPDX-License-Identifier: MIT OR Apache-2.0

//! Reading RecSplit minimal perfect hash indexes.
//!
//! This is a port of the query half of erigon's `recsplit`, which N42-gov5
//! vendors and uses for `codes.hidx` and its state snapshots. Only lookup is
//! implemented - indexes are built by gov5.
//!
//! An MPHF maps the N keys it was built over onto slots `[0, N)` with no
//! collisions, and stores no keys at all. It therefore answers *something* for
//! a key outside that set, and every caller has to validate the answer itself -
//! for code that means checking `keccak(code) == code_hash`.
//!
//! Everything here has to match the Go implementation bit for bit, so the
//! layout constants and the hash mixing are kept in the same shape as the
//! original rather than rewritten.

use eyre::{Context, Result};
use std::{fs, path::Path};

// Elias-Fano quantum parameters, from eliasfano16.
const LOG2Q: u64 = 8;
const Q: u64 = 1 << LOG2Q;
const Q_MASK: u64 = Q - 1;
const SUPER_Q: u64 = 1 << 14;
const Q_PER_SUPER_Q: u64 = SUPER_Q / Q;
const SUPER_Q_SIZE: u64 = 1 + Q_PER_SUPER_Q / 4;

/// Golomb-Rice code lengths for bijections up to leaf size 24.
const BIJ_MEMO: [u32; 25] = [
    0, 0, 0, 1, 3, 4, 5, 7, 8, 10, 11, 12, 14, 15, 16, 18, 19, 21, 22, 23, 25, 26, 28, 29, 30,
];

/// Position of the `k`-th set bit, counting from zero.
///
/// The Go side uses a byte-wise table for speed; this walks the bits instead,
/// which gives the same answer and keeps the table out of the port.
fn select64(mut value: u64, k: u32) -> u32 {
    let mut remaining = k;
    loop {
        debug_assert!(value != 0, "select64 past the last set bit");
        let position = value.trailing_zeros();
        if remaining == 0 {
            return position
        }
        value &= value - 1;
        remaining -= 1;
    }
}

/// `murmur3_x64_128` with a seed, matching `spaolacci/murmur3`'s `Sum128`.
fn murmur3_x64_128(key: &[u8], seed: u32) -> (u64, u64) {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    let mut h1 = u64::from(seed);
    let mut h2 = u64::from(seed);

    let blocks = key.len() / 16;
    for block in 0..blocks {
        let offset = block * 16;
        let mut k1 = u64::from_le_bytes(key[offset..offset + 8].try_into().unwrap());
        let mut k2 = u64::from_le_bytes(key[offset + 8..offset + 16].try_into().unwrap());

        k1 = k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(27).wrapping_add(h2).wrapping_mul(5).wrapping_add(0x52dc_e729);

        k2 = k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
        h2 ^= k2;
        h2 = h2.rotate_left(31).wrapping_add(h1).wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    let tail = &key[blocks * 16..];
    let mut k1 = 0u64;
    let mut k2 = 0u64;
    for (index, byte) in tail.iter().enumerate() {
        let value = u64::from(*byte);
        if index < 8 {
            k1 |= value << (8 * index);
        } else {
            k2 |= value << (8 * (index - 8));
        }
    }
    if !tail.is_empty() {
        if tail.len() > 8 {
            k2 = k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
            h2 ^= k2;
        }
        k1 = k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= key.len() as u64;
    h2 ^= key.len() as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1, h2)
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^ (k >> 33)
}

fn remix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// High 64 bits of `x * n`, i.e. `x` scaled into `[0, n)`.
fn remap(x: u64, n: u64) -> u64 {
    ((u128::from(x) * u128::from(n)) >> 64) as u64
}

const MASK48: u64 = (1 << 48) - 1;

fn remap16(x: u64, n: u16) -> u16 {
    (((x & MASK48) * u64::from(n)) >> 48) as u16
}

/// Reads the Golomb-Rice coded tree that describes the splitting.
struct GolombRiceReader<'a> {
    data: &'a [u64],
    current_fixed_offset: usize,
    current_pointer_unary: usize,
    current_window_unary: u64,
    valid_lower_bits_unary: i32,
}

impl<'a> GolombRiceReader<'a> {
    const fn new(data: &'a [u64]) -> Self {
        Self {
            data,
            current_fixed_offset: 0,
            current_pointer_unary: 0,
            current_window_unary: 0,
            valid_lower_bits_unary: 0,
        }
    }

    fn read_reset(&mut self, bit_position: usize, unary_offset: usize) {
        self.current_fixed_offset = bit_position;
        let unary_position = bit_position + unary_offset;
        self.current_pointer_unary = unary_position / 64;
        self.current_window_unary = self.data[self.current_pointer_unary] >> (unary_position & 63);
        self.current_pointer_unary += 1;
        self.valid_lower_bits_unary = 64 - (unary_position & 63) as i32;
    }

    fn skip_subtree(&mut self, nodes: usize, fixed_length: usize) {
        let mut missing = nodes as i32;
        let mut count = self.current_window_unary.count_ones() as i32;
        while count < missing {
            self.current_window_unary = self.data[self.current_pointer_unary];
            self.current_pointer_unary += 1;
            missing -= count;
            self.valid_lower_bits_unary = 64;
            count = self.current_window_unary.count_ones() as i32;
        }
        let count = select64(self.current_window_unary, (missing - 1) as u32) as i32;
        self.current_window_unary >>= count;
        self.current_window_unary >>= 1;
        self.valid_lower_bits_unary -= count + 1;

        self.current_fixed_offset += fixed_length;
    }

    fn read_next(&mut self, log2golomb: usize) -> u64 {
        let mut result = 0u64;

        if self.current_window_unary == 0 {
            result += self.valid_lower_bits_unary as u64;
            self.current_window_unary = self.data[self.current_pointer_unary];
            self.current_pointer_unary += 1;
            self.valid_lower_bits_unary = 64;
            while self.current_window_unary == 0 {
                result += 64;
                self.current_window_unary = self.data[self.current_pointer_unary];
                self.current_pointer_unary += 1;
            }
        }

        let position = self.current_window_unary.trailing_zeros() as i32;
        self.current_window_unary >>= position;
        self.current_window_unary >>= 1;
        self.valid_lower_bits_unary -= position + 1;

        result += position as u64;
        result <<= log2golomb;

        let index64 = self.current_fixed_offset >> 6;
        let shift = self.current_fixed_offset & 63;
        let mut fixed = self.data[index64] >> shift;
        if shift + log2golomb > 64 {
            fixed |= self.data[index64 + 1] << (64 - shift);
        }
        result |= fixed & ((1u64 << log2golomb) - 1);
        self.current_fixed_offset += log2golomb;
        result
    }
}

/// The bucket directory: two monotone sequences (cumulative keys and bit
/// positions) interleaved in one Elias-Fano structure.
struct DoubleEliasFano {
    lower_bits: Vec<u64>,
    upper_bits_cum_keys: Vec<u64>,
    upper_bits_position: Vec<u64>,
    jump: Vec<u64>,
    num_buckets: u64,
    l_position: u64,
    l_cum_keys: u64,
    lower_bits_mask_cum_keys: u64,
    lower_bits_mask_position: u64,
    cum_keys_min_delta: u64,
    position_min_delta: u64,
}

impl DoubleEliasFano {
    /// Parses the structure and reports how many bytes it consumed.
    fn read(raw: &[u8]) -> Result<Self> {
        if raw.len() < 40 {
            eyre::bail!("elias-fano header is truncated")
        }
        let num_buckets = u64::from_be_bytes(raw[0..8].try_into().unwrap());
        let u_cum_keys = u64::from_be_bytes(raw[8..16].try_into().unwrap());
        let u_position = u64::from_be_bytes(raw[16..24].try_into().unwrap());
        let cum_keys_min_delta = u64::from_be_bytes(raw[24..32].try_into().unwrap());
        let position_min_delta = u64::from_be_bytes(raw[32..40].try_into().unwrap());

        let l_position = if u_position / (num_buckets + 1) == 0 {
            0
        } else {
            63 ^ u64::from((u_position / (num_buckets + 1)).leading_zeros())
        };
        let l_cum_keys = if u_cum_keys / (num_buckets + 1) == 0 {
            0
        } else {
            63 ^ u64::from((u_cum_keys / (num_buckets + 1)).leading_zeros())
        };
        if l_cum_keys * 2 + l_position > 56 {
            eyre::bail!("elias-fano lower bits do not fit: {l_cum_keys} * 2 + {l_position} > 56")
        }

        let words_lower_bits =
            ((num_buckets + 1) * (l_cum_keys + l_position) + 63) / 64 + 1;
        let words_cum_keys = (num_buckets + 1 + (u_cum_keys >> l_cum_keys) + 63) / 64;
        let words_position = (num_buckets + 1 + (u_position >> l_position) + 63) / 64;
        let jump_words = jump_size_words(num_buckets);
        let total_words = words_lower_bits + words_cum_keys + words_position + jump_words;

        let needed = 40 + 8 * total_words as usize;
        if raw.len() < needed {
            eyre::bail!(
                "elias-fano body is {} bytes, needs {}",
                raw.len() - 40,
                8 * total_words
            )
        }
        let words = read_u64_slice(&raw[40..needed]);

        let (lower, rest) = words.split_at(words_lower_bits as usize);
        let (cum_keys, rest) = rest.split_at(words_cum_keys as usize);
        let (position, jump) = rest.split_at(words_position as usize);

        Ok(Self {
            lower_bits: lower.to_vec(),
            upper_bits_cum_keys: cum_keys.to_vec(),
            upper_bits_position: position.to_vec(),
            jump: jump.to_vec(),
            num_buckets,
            l_position,
            l_cum_keys,
            lower_bits_mask_cum_keys: (1u64 << l_cum_keys) - 1,
            lower_bits_mask_position: (1u64 << l_position) - 1,
            cum_keys_min_delta,
            position_min_delta,
        })
    }

    #[allow(clippy::type_complexity)]
    fn get2(&self, i: u64) -> (u64, u64, u64, u32, u64, u64, u64) {
        let position_lower = i * (self.l_cum_keys + self.l_position);
        let mut index64 = (position_lower / 64) as usize;
        let mut shift = position_lower % 64;
        let mut lower = self.lower_bits[index64] >> shift;
        if shift > 0 {
            lower |= self.lower_bits[index64 + 1] << (64 - shift);
        }

        let jump_super_q = (i / SUPER_Q) * SUPER_Q_SIZE * 2;
        let jump_inside_super_q = (i % SUPER_Q) / Q;
        let mut index16 = 4 * (jump_super_q + 2) + 2 * jump_inside_super_q;
        index64 = (index16 / 4) as usize;
        shift = 16 * (index16 % 4);
        let mut mask = 0xffffu64 << shift;
        let jump_cum_keys =
            self.jump[jump_super_q as usize] + ((self.jump[index64] & mask) >> shift);
        index16 += 1;
        index64 = (index16 / 4) as usize;
        shift = 16 * (index16 % 4);
        mask = 0xffffu64 << shift;
        let jump_position =
            self.jump[(jump_super_q + 1) as usize] + ((self.jump[index64] & mask) >> shift);

        let mut current_word_cum_keys = jump_cum_keys / 64;
        let mut current_word_position = jump_position / 64;
        let mut window_cum_keys = self.upper_bits_cum_keys[current_word_cum_keys as usize]
            & (u64::MAX << (jump_cum_keys % 64));
        let mut window_position = self.upper_bits_position[current_word_position as usize]
            & (u64::MAX << (jump_position % 64));
        let mut delta_cum_keys = (i & Q_MASK) as i32;
        let mut delta_position = (i & Q_MASK) as i32;

        let mut bit_count = window_cum_keys.count_ones() as i32;
        while bit_count <= delta_cum_keys {
            current_word_cum_keys += 1;
            window_cum_keys = self.upper_bits_cum_keys[current_word_cum_keys as usize];
            delta_cum_keys -= bit_count;
            bit_count = window_cum_keys.count_ones() as i32;
        }
        let mut bit_count = window_position.count_ones() as i32;
        while bit_count <= delta_position {
            current_word_position += 1;
            window_position = self.upper_bits_position[current_word_position as usize];
            delta_position -= bit_count;
            bit_count = window_position.count_ones() as i32;
        }

        let select_cum_keys = select64(window_cum_keys, delta_cum_keys as u32);
        let cum_delta = i * self.cum_keys_min_delta;
        let cum_keys = (((current_word_cum_keys * 64 + u64::from(select_cum_keys) - i)
            << self.l_cum_keys)
            | (lower & self.lower_bits_mask_cum_keys))
            + cum_delta;

        lower >>= self.l_cum_keys;
        let select_position = select64(window_position, delta_position as u32);
        let bit_delta = i * self.position_min_delta;
        let position = (((current_word_position * 64 + u64::from(select_position) - i)
            << self.l_position)
            | (lower & self.lower_bits_mask_position))
            + bit_delta;

        (
            cum_keys,
            position,
            window_cum_keys,
            select_cum_keys,
            current_word_cum_keys,
            lower,
            cum_delta,
        )
    }

    /// Returns `(cum_keys, cum_keys_next, bit_position)` for bucket `i`.
    fn get3(&self, i: u64) -> (u64, u64, u64) {
        let (cum_keys, position, mut window_cum_keys, select_cum_keys, mut current_word, mut lower, cum_delta) =
            self.get2(i);

        window_cum_keys &= (u64::MAX << select_cum_keys) << 1;
        while window_cum_keys == 0 {
            current_word += 1;
            window_cum_keys = self.upper_bits_cum_keys[current_word as usize];
        }

        lower >>= self.l_position;
        let cum_keys_next = (((current_word * 64
            + u64::from(window_cum_keys.trailing_zeros())
            - i
            - 1)
            << self.l_cum_keys)
            | (lower & self.lower_bits_mask_cum_keys))
            + cum_delta
            + self.cum_keys_min_delta;

        let _ = position;
        (cum_keys, cum_keys_next, position)
    }
}

fn jump_size_words(num_buckets: u64) -> u64 {
    let mut size = ((num_buckets + 1) / SUPER_Q) * SUPER_Q_SIZE * 2;
    if (num_buckets + 1) % SUPER_Q != 0 {
        size += (1 + ((((num_buckets + 1) % SUPER_Q + Q - 1) / Q + 3) / 4)) * 2;
    }
    size
}

fn read_u64_slice(raw: &[u8]) -> Vec<u64> {
    raw.chunks_exact(8)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

/// A RecSplit index, opened for lookups.
pub(super) struct RecSplitIndex {
    key_count: u64,
    bucket_count: u64,
    leaf_size: u16,
    primary_aggr_bound: u16,
    secondary_aggr_bound: u16,
    salt: u32,
    start_seed: Vec<u64>,
    golomb_rice: Vec<u32>,
    golomb_rice_data: Vec<u64>,
    ef: DoubleEliasFano,
    /// Present only when the index was built with the `Enums` feature, in which
    /// case a slot maps through this to a stored offset. `codes.hidx` is a pure
    /// MPHF, where the slot is the answer.
    bytes_per_record: usize,
    records: Vec<u8>,
    record_mask: u64,
}

impl std::fmt::Debug for RecSplitIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RecSplitIndex")
            .field("key_count", &self.key_count)
            .field("bucket_count", &self.bucket_count)
            .field("leaf_size", &self.leaf_size)
            .finish()
    }
}

impl RecSplitIndex {
    pub(super) fn open(path: &Path) -> Result<Self> {
        let raw = fs::read(path).wrap_err_with(|| format!("failed to read {}", path.display()))?;
        Self::parse(&raw).wrap_err_with(|| format!("failed to parse {}", path.display()))
    }

    fn parse(raw: &[u8]) -> Result<Self> {
        if raw.len() < 17 {
            eyre::bail!("index header is truncated")
        }
        let key_count = u64::from_be_bytes(raw[8..16].try_into().unwrap());
        let bytes_per_record = raw[16] as usize;
        let record_mask = if bytes_per_record == 0 {
            0
        } else {
            (1u64 << (8 * bytes_per_record)) - 1
        };
        let mut offset = 16 + 1 + key_count as usize * bytes_per_record;
        let records = raw[17..offset.min(raw.len())].to_vec();

        let bucket_count = u64::from_be_bytes(raw[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let _bucket_size = u16::from_be_bytes(raw[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let leaf_size = u16::from_be_bytes(raw[offset..offset + 2].try_into().unwrap());
        offset += 2;

        let primary_aggr_bound =
            leaf_size * (2.0f64).max((0.35 * f64::from(leaf_size) + 0.5).ceil()) as u16;
        let secondary_aggr_bound = if leaf_size < 7 {
            primary_aggr_bound * 2
        } else {
            primary_aggr_bound * (0.21 * f64::from(leaf_size) + 0.9).ceil() as u16
        };

        let salt = u32::from_be_bytes(raw[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let start_seed_len = raw[offset] as usize;
        offset += 1;
        let mut start_seed = Vec::with_capacity(start_seed_len);
        for _ in 0..start_seed_len {
            start_seed.push(u64::from_be_bytes(raw[offset..offset + 8].try_into().unwrap()));
            offset += 8;
        }

        let features = raw[offset];
        offset += 1;
        let enums = features & 0b1 != 0;
        let less_false_positives = features & 0b10 != 0;
        if features & !0b11 != 0 {
            eyre::bail!("index uses unknown features: {features:#04x}")
        }

        if enums && key_count > 0 {
            // Skip the offset Elias-Fano: this reader only answers slots.
            let count = u64::from_be_bytes(raw[offset..offset + 8].try_into().unwrap());
            let u = u64::from_be_bytes(raw[offset + 8..offset + 16].try_into().unwrap());
            let size = single_elias_fano_size(count, u);
            offset += size;
            if less_false_positives {
                let array_size = u64::from_be_bytes(raw[offset..offset + 8].try_into().unwrap());
                offset += 8 + array_size as usize;
            }
        }

        let golomb_param_size = u16::from_be_bytes(raw[offset..offset + 2].try_into().unwrap());
        // The writer emits a 2-byte count but advances by 4.
        offset += 4;
        let golomb_rice = build_golomb_rice(
            golomb_param_size,
            leaf_size,
            primary_aggr_bound,
            secondary_aggr_bound,
        );

        let words = u64::from_be_bytes(raw[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        let golomb_rice_data = read_u64_slice(&raw[offset..offset + 8 * words]);
        offset += 8 * words;

        let ef = DoubleEliasFano::read(&raw[offset..])?;

        Ok(Self {
            key_count,
            bucket_count,
            leaf_size,
            primary_aggr_bound,
            secondary_aggr_bound,
            salt,
            start_seed,
            golomb_rice,
            golomb_rice_data,
            ef,
            bytes_per_record,
            records,
            record_mask,
        })
    }

    pub(super) const fn key_count(&self) -> u64 {
        self.key_count
    }

    fn golomb_param(&self, m: u16) -> usize {
        (self.golomb_rice[m as usize] >> 27) as usize
    }

    fn skip_bits(&self, m: u16) -> usize {
        (self.golomb_rice[m as usize] & 0xffff) as usize
    }

    fn skip_nodes(&self, m: u16) -> usize {
        ((self.golomb_rice[m as usize] >> 16) & 0x7ff) as usize
    }

    /// Maps `key` onto its slot.
    ///
    /// A key that was not in the build set still gets a slot; the caller has to
    /// verify the result. Returns `None` only for an empty index.
    pub(super) fn lookup(&self, key: &[u8]) -> Option<u64> {
        if self.key_count == 0 {
            return None
        }
        if self.key_count == 1 {
            return Some(0)
        }

        let (bucket_hash, fingerprint) = murmur3_x64_128(key, self.salt);

        let mut reader = GolombRiceReader::new(&self.golomb_rice_data);
        let bucket = remap(bucket_hash, self.bucket_count);
        let (mut cum_keys, cum_keys_next, bit_position) = self.ef.get3(bucket);
        let mut m = (cum_keys_next - cum_keys) as u16;
        reader.read_reset(bit_position as usize, self.skip_bits(m));

        let mut level = 0usize;
        while m > self.secondary_aggr_bound {
            let d = reader.read_next(self.golomb_param(m));
            let hmod = remap16(remix(fingerprint.wrapping_add(self.start_seed[level]).wrapping_add(d)), m);
            let split = ((m + 1) / 2).div_ceil(self.secondary_aggr_bound) * self.secondary_aggr_bound;
            if hmod < split {
                m = split;
            } else {
                reader.skip_subtree(self.skip_nodes(split), self.skip_bits(split));
                m -= split;
                cum_keys += u64::from(split);
            }
            level += 1;
        }

        if m > self.primary_aggr_bound {
            let d = reader.read_next(self.golomb_param(m));
            let hmod = remap16(remix(fingerprint.wrapping_add(self.start_seed[level]).wrapping_add(d)), m);
            let part = hmod / self.primary_aggr_bound;
            m = if self.primary_aggr_bound < m - part * self.primary_aggr_bound {
                self.primary_aggr_bound
            } else {
                m - part * self.primary_aggr_bound
            };
            cum_keys += u64::from(self.primary_aggr_bound * part);
            if part != 0 {
                reader.skip_subtree(
                    self.skip_nodes(self.primary_aggr_bound) * part as usize,
                    self.skip_bits(self.primary_aggr_bound) * part as usize,
                );
            }
            level += 1;
        }

        if m > self.leaf_size {
            let d = reader.read_next(self.golomb_param(m));
            let hmod = remap16(remix(fingerprint.wrapping_add(self.start_seed[level]).wrapping_add(d)), m);
            let part = hmod / self.leaf_size;
            m = if self.leaf_size < m - part * self.leaf_size {
                self.leaf_size
            } else {
                m - part * self.leaf_size
            };
            cum_keys += u64::from(self.leaf_size * part);
            if part != 0 {
                reader.skip_subtree(part as usize, self.skip_bits(self.leaf_size) * part as usize);
            }
            level += 1;
        }

        let b = reader.read_next(self.golomb_param(m));
        let record = cum_keys
            + u64::from(remap16(
                remix(fingerprint.wrapping_add(self.start_seed[level]).wrapping_add(b)),
                m,
            ));

        if self.bytes_per_record == 0 {
            // Pure MPHF: the slot is the answer.
            return Some(record)
        }
        let position = self.bytes_per_record * (record as usize + 1);
        let end = position + 8;
        if end > self.records.len() + 9 {
            return None
        }
        // The Go side reads eight big-endian bytes starting at `1 + 8 + ...`
        // relative to the file, which is `position - 9` into the record area.
        let start = position.saturating_sub(9);
        let mut buffer = [0u8; 8];
        let available = self.records.len().saturating_sub(start).min(8);
        buffer[..available].copy_from_slice(&self.records[start..start + available]);
        Some(u64::from_be_bytes(buffer) & self.record_mask)
    }
}

/// Size in bytes of a single Elias-Fano structure, so it can be skipped.
fn single_elias_fano_size(count: u64, u: u64) -> usize {
    let count = count + 1;
    let l = if u / count == 0 {
        0
    } else {
        63 ^ u64::from((u / count).leading_zeros())
    };
    let words_lower_bits = (count * l + 63) / 64 + 1;
    let words_upper_bits = (count + (u >> l) + 63) / 64;
    let jump_words = {
        let mut size = (count / SUPER_Q) * SUPER_Q_SIZE;
        if count % SUPER_Q != 0 {
            size += 1 + (((count % SUPER_Q + Q - 1) / Q + 3) / 4);
        }
        size
    };
    24 + 8 * (words_lower_bits + words_upper_bits + jump_words) as usize
}

/// Rebuilds the Golomb-Rice parameter table the writer used.
fn build_golomb_rice(
    size: u16,
    leaf_size: u16,
    primary_aggr_bound: u16,
    secondary_aggr_bound: u16,
) -> Vec<u32> {
    let golomb_base_log2 = -(((5.0f64).sqrt() + 1.0) / 2.0).ln();
    let mut table = vec![0u32; size as usize];
    for m in 0..size {
        if m == 0 {
            table[0] = (BIJ_MEMO[0] << 27) | BIJ_MEMO[0];
        } else if m <= leaf_size {
            let memo = BIJ_MEMO[m as usize];
            table[m as usize] = (memo << 27) | (1 << 16) | memo;
        } else {
            let (fanout, unit) =
                split_params(m, leaf_size, primary_aggr_bound, secondary_aggr_bound);
            let mut k = vec![0u16; fanout as usize];
            k[fanout as usize - 1] = m;
            for i in 0..fanout - 1 {
                k[i as usize] = unit;
                k[fanout as usize - 1] -= unit;
            }
            let mut sqrt_product = 1.0f64;
            for value in &k {
                sqrt_product *= f64::from(*value).sqrt();
            }
            let p = f64::from(m).sqrt()
                / ((2.0 * std::f64::consts::PI).powf((f64::from(fanout) - 1.0) / 2.0)
                    * sqrt_product);
            let mut length = (golomb_base_log2 / (-p).ln_1p()).log2().ceil() as u32;
            table[m as usize] = length << 27;
            for value in &k {
                length += table[*value as usize] & 0xffff;
            }
            table[m as usize] |= length;
            let mut nodes = 1u32;
            for value in &k {
                nodes += (table[*value as usize] >> 16) & 0x7ff;
            }
            table[m as usize] |= nodes << 16;
        }
    }
    table
}

fn split_params(
    m: u16,
    leaf_size: u16,
    primary_aggr_bound: u16,
    secondary_aggr_bound: u16,
) -> (u16, u16) {
    if m > secondary_aggr_bound {
        (
            2,
            secondary_aggr_bound * (((m + 1) / 2).div_ceil(secondary_aggr_bound)),
        )
    } else if m > primary_aggr_bound {
        (m.div_ceil(primary_aggr_bound), primary_aggr_bound)
    } else {
        (m.div_ceil(leaf_size), leaf_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cross-checks the port against gov5's own index using the freezer on this
    /// machine: for every entry, the slot this returns must be the slot whose
    /// `hoff` record actually holds that code.
    #[test]
    fn matches_gov5_index_over_the_whole_codes_freezer() {
        let directory = std::path::PathBuf::from(r"D:/n42-codes-25765565");
        if !directory.join("codes.hidx").exists() {
            return
        }
        let Ok(verified) = fs::read(directory.join("codes.rhidx")) else {
            return
        };

        let index = RecSplitIndex::open(&directory.join("codes.hidx")).unwrap();
        let hoff = fs::read(directory.join("codes.hoff")).unwrap();

        let entries = (verified.len() - 8) / 42;
        assert_eq!(index.key_count() as usize, entries, "key counts disagree");

        let mut checked = 0usize;
        for entry in 0..entries {
            let base = 8 + entry * 42;
            let hash = &verified[base..base + 32];
            let expected_file = u16::from_le_bytes(verified[base + 32..base + 34].try_into().unwrap());
            let expected_offset = u32::from_le_bytes(verified[base + 34..base + 38].try_into().unwrap());
            let expected_length = u32::from_le_bytes(verified[base + 38..base + 42].try_into().unwrap());

            let slot = index.lookup(hash).expect("index is not empty") as usize;
            let position = slot * 10;
            assert!(position + 10 <= hoff.len(), "slot {slot} is past codes.hoff");
            let file = u16::from_le_bytes(hoff[position..position + 2].try_into().unwrap());
            let offset = u32::from_le_bytes(hoff[position + 2..position + 6].try_into().unwrap());
            let length = u32::from_le_bytes(hoff[position + 6..position + 10].try_into().unwrap());

            assert_eq!(
                (file, offset, length),
                (expected_file, expected_offset, expected_length),
                "entry {entry} resolved to slot {slot}, which points somewhere else"
            );
            checked += 1;
        }
        assert!(checked > 1_000_000, "only checked {checked} entries");
    }

    #[test]
    fn murmur3_matches_known_vectors() {
        // Reference values for MurmurHash3_x64_128 with seed 0.
        let (h1, h2) = murmur3_x64_128(b"", 0);
        assert_eq!((h1, h2), (0, 0));

        // "hello" - checked against spaolacci/murmur3's Sum128.
        let (h1, h2) = murmur3_x64_128(b"hello", 0);
        assert_eq!(h1, 0xcbd8_a7b3_41bd_9b02);
        assert_eq!(h2, 0x5b1e_906a_48ae_1d19);
    }

    #[test]
    fn select64_finds_set_bits() {
        assert_eq!(select64(0b1011, 0), 0);
        assert_eq!(select64(0b1011, 1), 1);
        assert_eq!(select64(0b1011, 2), 3);
        assert_eq!(select64(1 << 63, 0), 63);
    }

    #[test]
    fn remap16_stays_in_range() {
        for value in [0u64, 1, u64::MAX, 0x1234_5678_9abc_def0] {
            assert!(remap16(value, 100) < 100);
        }
    }
}
