// SPDX-License-Identifier: MIT OR Apache-2.0

//! Compression utilities for log data

use std::io::{Read, Write};
use std::time::{Duration, Instant};
use tracing::debug;

/// Compression algorithm types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    None,
    Zstd,
    Brotli,
    Lzma,
    Lz4,
}

impl CompressionAlgorithm {
    pub fn as_u8(self) -> u8 {
        match self {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Zstd => 1,
            CompressionAlgorithm::Brotli => 2,
            CompressionAlgorithm::Lzma => 3,
            CompressionAlgorithm::Lz4 => 4,
        }
    }

    pub fn from_u8(v: u8) -> eyre::Result<Self> {
        match v {
            0 => Ok(CompressionAlgorithm::None),
            1 => Ok(CompressionAlgorithm::Zstd),
            2 => Ok(CompressionAlgorithm::Brotli),
            3 => Ok(CompressionAlgorithm::Lzma),
            4 => Ok(CompressionAlgorithm::Lz4),
            _ => Err(eyre::eyre!("Unknown compression algorithm: {}", v)),
        }
    }
}

/// Compress data using the specified algorithm
pub fn compress_data(data: &[u8], algorithm: CompressionAlgorithm) -> eyre::Result<Vec<u8>> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Zstd => {
            zstd::encode_all(data, 5).map_err(|e| eyre::eyre!("Zstd compression failed: {}", e))
        }
        CompressionAlgorithm::Brotli => {
            let mut compressed = Vec::new();
            {
                let mut writer = brotli::CompressorWriter::new(&mut compressed, 4096, 11, 22);
                writer.write_all(data).map_err(|e| eyre::eyre!("Brotli compression failed: {}", e))?;
            }
            Ok(compressed)
        }
        CompressionAlgorithm::Lzma => {
            let mut compressed = Vec::new();
            let mut encoder = xz2::write::XzEncoder::new(compressed, 6);
            encoder.write_all(data).map_err(|e| eyre::eyre!("LZMA compression failed: {}", e))?;
            compressed = encoder.finish().map_err(|e| eyre::eyre!("LZMA compression failed: {}", e))?;
            Ok(compressed)
        }
        CompressionAlgorithm::Lz4 => {
            // lz4_flex requires knowing decompressed size, so we prepend size
            let compressed = lz4_flex::compress(data);
            let mut result = Vec::new();
            result.write_all(&(data.len() as u32).to_le_bytes())
                .map_err(|e| eyre::eyre!("LZ4 compression failed: {}", e))?;
            result.write_all(&compressed)
                .map_err(|e| eyre::eyre!("LZ4 compression failed: {}", e))?;
            Ok(result)
        }
    }
}

/// Decompress data using the specified algorithm
pub fn decompress_data(data: &[u8], algorithm: CompressionAlgorithm) -> eyre::Result<Vec<u8>> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Zstd => {
            zstd::decode_all(data).map_err(|e| eyre::eyre!("Zstd decompression failed: {}", e))
        }
        CompressionAlgorithm::Brotli => {
            let mut decompressed = Vec::new();
            let mut reader = brotli::Decompressor::new(data, 4096);
            reader.read_to_end(&mut decompressed)
                .map_err(|e| eyre::eyre!("Brotli decompression failed: {}", e))?;
            Ok(decompressed)
        }
        CompressionAlgorithm::Lzma => {
            let mut decompressed = Vec::new();
            xz2::read::XzDecoder::new(data)
                .read_to_end(&mut decompressed)
                .map_err(|e| eyre::eyre!("LZMA decompression failed: {}", e))?;
            Ok(decompressed)
        }
        CompressionAlgorithm::Lz4 => {
            // Read size prefix
            if data.len() < 4 {
                return Err(eyre::eyre!("LZ4 decompression failed: data too short"));
            }
            let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            let compressed_data = &data[4..];
            // Pre-allocate buffer to avoid reallocation
            let mut decompressed = Vec::with_capacity(size);
            unsafe {
                decompressed.set_len(size);
            }
            // Use decompress_into to avoid extra memory allocation
            lz4_flex::decompress_into(compressed_data, &mut decompressed)
                .map_err(|e| eyre::eyre!("LZ4 decompression failed: {}", e))?;
            Ok(decompressed)
        }
    }
}

/// Compression statistics
#[derive(Debug)]
pub struct CompressionStats {
    pub algorithm: CompressionAlgorithm,
    pub compressed_size: usize,
    pub compression_time: Duration,
    pub decompression_time: Duration,
    pub ratio: f64,
}

/// Test all compression algorithms and record statistics
pub fn compare_all_compressions(data: &[u8]) -> Vec<CompressionStats> {
    let algorithms = [
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Brotli,
        CompressionAlgorithm::Lzma,
        CompressionAlgorithm::Lz4,
    ];

    let mut stats = Vec::new();

    for &alg in &algorithms {
        // Compression time
        let compress_start = Instant::now();
        if let Ok(compressed) = compress_data(data, alg) {
            let compress_time = compress_start.elapsed();

            // Decompression time
            let decompress_start = Instant::now();
            if decompress_data(&compressed, alg).is_ok() {
                let decompress_time = decompress_start.elapsed();

                let ratio = (compressed.len() as f64 / data.len() as f64) * 100.0;
                stats.push(CompressionStats {
                    algorithm: alg,
                    compressed_size: compressed.len(),
                    compression_time: compress_time,
                    decompression_time: decompress_time,
                    ratio,
                });
            }
        }
    }

    stats
}

/// Choose compression algorithm (based on command line argument)
/// If compressed length is greater than original data, automatically use None
pub fn choose_compression_algorithm(
    data: &[u8],
    algorithm_str: &str
) -> eyre::Result<(CompressionAlgorithm, Vec<u8>, Vec<CompressionStats>)> {
    // Select algorithm based on command line argument
    let algorithm = match algorithm_str.to_lowercase().as_str() {
        "none" => CompressionAlgorithm::None,
        "zstd" => CompressionAlgorithm::Zstd,
        "brotli" => CompressionAlgorithm::Brotli,
        "lzma" => CompressionAlgorithm::Lzma,
        "lz4" => CompressionAlgorithm::Lz4,
        "auto" => {
            // Auto mode: test all algorithms and choose the best
            let stats = compare_all_compressions(data);

            // Print comparison info (debug level to avoid flooding)
            debug!("=== Compression Comparison ===");
            debug!("Original size: {} bytes", data.len());
            for stat in &stats {
                debug!("  {:?}: {} bytes ({:.2}%), compress={:.2}ms, decompress={:.2}ms",
                    stat.algorithm, stat.compressed_size, stat.ratio,
                    stat.compression_time.as_secs_f64() * 1000.0,
                    stat.decompression_time.as_secs_f64() * 1000.0);
            }

            // Auto-select best compression ratio (but must be smaller than original)
            let best = stats.iter()
                .filter(|s| s.compressed_size < data.len()) // Only consider smaller results
                .min_by(|a, b| a.compressed_size.cmp(&b.compressed_size));

            if let Some(best_stat) = best {
                best_stat.algorithm
            } else {
                // If all algorithms result in larger data, use no compression
                CompressionAlgorithm::None
            }
        }
        _ => return Err(eyre::eyre!("Unknown compression algorithm: {}. Use: none, zstd, brotli, lzma, lz4, or auto", algorithm_str)),
    };

    // Compress data
    let compressed = if algorithm == CompressionAlgorithm::None {
        data.to_vec()
    } else {
        compress_data(data, algorithm)?
    };

    // Check compressed size: if compressed length >= original, use no compression
    let (final_algorithm, final_data) = if compressed.len() >= data.len() {
        (CompressionAlgorithm::None, data.to_vec())
    } else {
        (algorithm, compressed)
    };

    // Return empty stats (no longer comparing all algorithms)
    Ok((final_algorithm, final_data, Vec::new()))
}
