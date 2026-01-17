// SPDX-License-Identifier: MIT OR Apache-2.0

//! Log reading utilities for EVM state replay.
//!
//! This module contains utilities for reading and decoding state logs
//! that were recorded during block execution.

use std::io::{Read, BufReader, Seek, SeekFrom};
use std::sync::Arc;
use alloy_primitives::{Address, B256, U256};
use reth_codecs::Compact;
use reth_primitives::Account;
use reth_provider::StateProvider;
use reth_revm::database::StateProviderDatabase;

use crate::revm::Database as RevmDatabase;
use crate::revm::state::{AccountInfo, Bytecode};

use super::compression::{CompressionAlgorithm, decompress_data};
use super::types::{ReadLogEntry, IndexEntry};
use super::file_io::{open_log_files, read_index_file, find_index_entry};
use super::get_empty_code_hash;

/// 线程局部缓存：解压后的原始字节数据（线程本地，无需锁）
/// 性能优化：每个块只解压一次，后续所有访问都是内存访问，只移动指针
/// 使用单一位置指针，按顺序读取：第一字节是长度，然后是内容，指针+长度+1指向下一个长度
/// 性能优化：支持共享内存数据（Arc<Vec<u8>>），避免在 in_memory_mode 下复制数据
pub(crate) struct ThreadLocalLogCache {
    // 性能优化：使用枚举支持两种模式：
    // - Owned: 拥有数据（解压后的数据，需要复制）
    // - Shared: 共享数据（in_memory_mode 下，直接使用 Arc，避免复制）
    uncompressed_data: Arc<Vec<u8>>, // 统一使用 Arc，支持共享和拥有
    current_pos: usize, // 当前位置指针（指向下一个条目的长度字节）
}

impl ThreadLocalLogCache {
    /// 从压缩的缓冲区创建线程局部缓存
    /// 测试模式：如果 is_uncompressed 为 true，直接使用 buffer（无压缩）
    /// 一次性解压所有数据，避免后续的文件读取和解压
    /// 性能优化：不预先解析所有条目，只记录数据开始位置
    /// 从压缩的缓冲区创建线程局部缓存
    /// 性能优化：接受 Vec<u8> 所有权，避免不必要的内存复制
    #[inline]
    pub fn from_compressed_buffer_owned(buffer: Vec<u8>, is_uncompressed: bool) -> eyre::Result<Self> {
        let uncompressed_data = if is_uncompressed {
            // 测试模式：数据已经是未压缩的格式（count(8) + entries）
            // 性能优化：直接使用传入的 Vec，零复制
            Arc::new(buffer)
        } else {
            // 正常模式：需要解压
            if buffer.is_empty() {
                return Err(eyre::eyre!("Empty buffer"));
            }

            let algorithm = CompressionAlgorithm::from_u8(buffer[0])?;

            // 读取压缩后的数据（跳过算法标识字节）
            let compressed_data = &buffer[1..];

            // 解压数据（一次性解压，缓存到线程本地）
            // 注意：原始 buffer 会被 drop，解压后的数据是新分配的
            Arc::new(decompress_data(compressed_data, algorithm)?)
        };

        // 性能优化：快速检查：读取条目数量（8字节），当前位置指向第一个条目的长度字节
        if uncompressed_data.len() < 8 {
            return Err(eyre::eyre!("Invalid uncompressed data: too short ({} bytes)", uncompressed_data.len()));
        }

        Ok(Self {
            uncompressed_data,
            current_pos: 8, // 跳过8字节的count，指向第一个条目的长度字节
        })
    }

    /// 兼容性方法：从引用创建（会复制数据）
    #[inline]
    #[allow(dead_code)]
    pub fn from_compressed_buffer(buffer: &[u8], is_uncompressed: bool) -> eyre::Result<Self> {
        Self::from_compressed_buffer_owned(buffer.to_vec(), is_uncompressed)
    }

    /// 从共享内存数据创建线程局部缓存（性能优化：避免复制）
    /// 用于 in_memory_mode 下，直接使用 GlobalLogFileHandle 的 in_memory_data
    #[inline]
    #[allow(dead_code)]
    pub fn from_shared_data(shared_data: Arc<Vec<u8>>) -> eyre::Result<Self> {
        if shared_data.len() < 8 {
            return Err(eyre::eyre!("Invalid uncompressed data: too short ({} bytes)", shared_data.len()));
        }

        Ok(Self {
            uncompressed_data: shared_data,
            current_pos: 8, // 跳过8字节的count，指向第一个条目的长度字节
        })
    }

    /// 读取下一个条目的位置信息（按顺序读取）
    /// 返回：Some((数据起始位置, 数据长度)) 或 None（已到末尾）
    /// 性能优化：只移动指针，不复制数据，不判断类型
    #[inline]
    pub fn next_entry_pos(&mut self) -> Option<(usize, usize)> {
        // 性能优化：缓存数据长度和当前位置，避免重复访问
        let data_len = self.uncompressed_data.len();
        let current = self.current_pos;

        // 性能优化：合并边界检查和长度读取
        if current >= data_len {
            return None;
        }

        // 读取长度字节（1字节）
        let entry_len = self.uncompressed_data[current] as usize;
        let data_start = current + 1; // 数据开始位置（跳过长度字节）

        // 性能优化：提前计算下一个位置，减少重复计算
        let next_pos = data_start + entry_len;

        // 检查边界（使用提前计算的位置）
        if next_pos > data_len {
            return None; // 数据损坏
        }

        // 移动指针到下一个条目的长度字节
        self.current_pos = next_pos;

        Some((data_start, entry_len))
    }

    /// 获取指定位置的数据切片（零拷贝，直接返回引用）
    /// 性能优化：内联函数，减少函数调用开销
    #[inline]
    pub fn get_data_at(&self, offset: usize, length: usize) -> Option<&[u8]> {
        // 性能优化：使用 get 方法，避免边界检查的额外开销（在 release 模式下）
        self.uncompressed_data.get(offset..offset + length)
    }
}

/// 从日志文件读取数据的 Database（按顺序访问，使用线程局部缓存）
pub(crate) struct LoggedDatabase {
    cache: ThreadLocalLogCache, // 线程局部缓存（解压后的原始数据）
    state_provider: Arc<dyn StateProvider>, // 用于 code_by_hash（代码不在日志文件中），使用 Arc 以便共享
    // 性能关键修复：移除 code_cache，直接运行模式没有缓存，使用日志模式也不应该有
    // 避免额外的内存分配和 HashMap 查找开销
}

impl std::fmt::Debug for LoggedDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoggedDatabase")
            .finish_non_exhaustive()
    }
}

impl LoggedDatabase {
    /// 从压缩的缓冲区创建新的 LoggedDatabase（性能优化：零复制版本）
    /// 接受 Vec<u8> 所有权，避免不必要的内存复制
    pub fn new_from_compressed_buffer_owned(
        buffer: Vec<u8>,
        state_provider: Arc<dyn StateProvider>,
        is_uncompressed: bool, // 测试模式：是否未压缩
    ) -> eyre::Result<Self> {
        let cache = ThreadLocalLogCache::from_compressed_buffer_owned(buffer, is_uncompressed)?;
        Ok(Self {
            cache,
            state_provider,
        })
    }

    /// 兼容性方法：从引用创建（会复制数据）
    #[allow(dead_code)]
    pub fn new_from_compressed_buffer(
        buffer: &[u8],
        state_provider: Arc<dyn StateProvider>,
        is_uncompressed: bool, // 测试模式：是否未压缩
    ) -> eyre::Result<Self> {
        Self::new_from_compressed_buffer_owned(buffer.to_vec(), state_provider, is_uncompressed)
    }

    /// 从共享内存数据创建新的 LoggedDatabase（性能优化：避免复制）
    /// 用于 in_memory_mode 下，直接使用 GlobalLogFileHandle 的 in_memory_data
    #[allow(dead_code)]
    pub fn new_from_shared_data(
        shared_data: Arc<Vec<u8>>,
        state_provider: Arc<dyn StateProvider>,
    ) -> eyre::Result<Self> {
        let cache = ThreadLocalLogCache::from_shared_data(shared_data)?;
        Ok(Self {
            cache,
            state_provider,
        })
    }

    /// 获取下一个条目数据（按顺序，使用指针访问，零拷贝）
    /// 性能优化：只移动指针，不复制数据，直接返回引用
    /// 不判断类型，只是按顺序返回下一个条目
    /// 内联函数，减少调用开销
    #[inline]
    fn next_entry_data(&mut self) -> Option<&[u8]> {
        let (offset, length) = self.cache.next_entry_pos()?;
        self.cache.get_data_at(offset, length)
    }

    /// 获取下一个 Account 数据（按顺序，使用指针访问，零拷贝）
    /// 性能优化：只移动指针，不复制数据，直接返回引用
    /// 按顺序返回下一个条目，不判断类型
    /// 内联函数，直接调用 next_entry_data
    #[inline]
    fn next_account_data(&mut self) -> Option<&[u8]> {
        self.next_entry_data()
    }

    /// 获取下一个 Storage 数据（按顺序，使用指针访问，零拷贝）
    /// 性能优化：只移动指针，不复制数据，直接返回引用
    /// 按顺序返回下一个条目，不判断类型
    /// 内联函数，直接调用 next_entry_data
    #[inline]
    fn next_storage_data(&mut self) -> Option<&[u8]> {
        self.next_entry_data()
    }

    /// 从 Compact 编码数据解码账户信息（替代 RLP，性能优化）
    /// 性能优化：内联函数，减少函数调用开销
    #[inline]
    pub fn decode_account_compact(data: &[u8]) -> eyre::Result<AccountInfo> {
        // 性能优化：快速检查：空数据表示账户不存在
        // 合并检查条件，减少分支预测失败
        if data.is_empty() || (data.len() == 1 && data[0] == 0x00) {
            return Err(eyre::eyre!("Account does not exist (empty data)"));
        }

        // 使用 Compact 解码 Account
        // 性能优化：直接使用数据长度，避免重复计算
        let data_len = data.len();
        let (account, _) = Account::from_compact(data, data_len);

        // 性能优化：直接使用 unwrap_or_else，但使用内联闭包减少开销
        // 使用模块级常量，避免重复创建 B256
        let code_hash = account.bytecode_hash.unwrap_or_else(get_empty_code_hash);

        // 性能优化：直接使用 default()，编译器会优化
        Ok(AccountInfo::new(
            account.balance,
            account.nonce,
            code_hash,
            Bytecode::default(),
        ))
    }

    /// 从 RLP 数据解码账户信息，处理可选的 storage_root 和 code_hash（向后兼容）
    #[allow(dead_code)]
    pub fn decode_account_rlp(data: &[u8]) -> eyre::Result<AccountInfo> {
        use alloy_rlp::Decodable;

        // 空 code 的 hash
        let empty_code_hash = get_empty_code_hash();

        let mut data = data;
        let header = alloy_rlp::Header::decode(&mut data)?;
        if !header.list {
            return Err(eyre::eyre!("Account RLP must be a list"));
        }

        // 解码 nonce
        let nonce = u64::decode(&mut data)?;
        // 解码 balance
        let balance = U256::decode(&mut data)?;

        // 可选字段：storage_root 和 code_hash
        // 如果还有数据，尝试解码 storage_root（32字节）
        let _storage_root = if !data.is_empty() {
            B256::decode(&mut data)?
        } else {
            B256::ZERO
        };

        // 可选字段：code_hash
        let code_hash = if !data.is_empty() {
            B256::decode(&mut data)?
        } else {
            empty_code_hash
        };

        Ok(AccountInfo::new(
            balance,
            nonce,
            code_hash,
            Bytecode::default(),
        ))
    }
}

impl RevmDatabase for LoggedDatabase {
    type Error = <StateProviderDatabase<Box<dyn StateProvider>> as RevmDatabase>::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // 按顺序从日志文件读取数据（不读数据库，不对比）
        // 性能优化：直接使用引用，零拷贝
        if let Some(compact_data) = self.next_account_data() {
            // 性能优化：快速检查空账户标记（Compact 编码：0x00 或 RLP 旧格式：0xc0）
            // 合并条件检查，减少分支预测失败
            if compact_data.is_empty() ||
               (compact_data.len() == 1 && (compact_data[0] == 0x00 || compact_data[0] == 0xc0)) {
                // 日志文件说账户不存在
                return Ok(None);
            }

            // 性能优化：直接使用 Compact 解码（新格式），只在必要时回退到 RLP
            // 使用 if let 替代 match，减少分支开销
            let mut logged_account_info = if let Ok(info) = Self::decode_account_compact(compact_data) {
                info
            } else {
                // 如果 Compact 解码失败，尝试 RLP 解码（向后兼容）
                match Self::decode_account_rlp(compact_data) {
                    Ok(info) => info,
                    Err(e) => {
                        tracing::debug!("LoggedDatabase::basic({}) failed to decode account from log file: {}", address, e);
                        return Ok(None);
                    }
                }
            };

            // 如果 code_hash 不是 empty_code_hash，加载 bytecode
            // 性能优化：直接使用 code_hash() 方法，避免中间变量
            // 性能优化：使用模块级常量进行快速比较（直接比较字节），避免不必要的数据库查询
            let code_hash = logged_account_info.code_hash();
            if code_hash.as_slice() != super::EMPTY_CODE_HASH_BYTES {
                // 只在必要时查询数据库
                // 性能优化：使用 if let 替代 match，减少分支
                if let Ok(code) = self.code_by_hash(code_hash) {
                    // 性能优化：避免不必要的 clone，直接设置
                    logged_account_info.code = Some(code);
                }
                // 加载失败，继续使用空的 code（不打印警告，避免日志噪音）
            }

            // 返回日志文件的数据
            Ok(Some(logged_account_info))
        } else {
            // 性能关键：不打印日志，避免热路径上的 tracing 开销
            // 返回 None 表示日志文件中没有更多账户数据
            Ok(None)
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // 性能关键修复：不使用缓存，直接查询数据库
        // 原因：直接运行模式没有缓存，使用日志模式也不应该有缓存，避免额外的内存分配和 HashMap 查找开销
        // 从数据库读取代码（代码不在日志文件中）
        // 注意：StateProviderDatabase 需要 &mut，所以每次都需要创建
        let mut inner_db = StateProviderDatabase::new(self.state_provider.as_ref() as &dyn StateProvider);
        inner_db.code_by_hash(code_hash)
    }

    fn storage(&mut self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
        // 性能优化：按顺序从日志文件读取数据（不读数据库，不对比）
        // 直接使用引用，零拷贝
        if let Some(compact_data) = self.next_storage_data() {
            // 性能优化：使用 Compact 解码 U256（新格式）
            // Compact 解码 U256 返回 (value, remaining_data)
            // 直接使用数据长度，避免重复计算
            let data_len = compact_data.len();
            let (logged_value, _remaining) = U256::from_compact(compact_data, data_len);
            Ok(logged_value)
        } else {
            // 日志文件中没有更多存储数据
            Ok(U256::ZERO)
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        // 创建一个临时的 StateProviderDatabase 来获取 block_hash
        let mut temp_db = StateProviderDatabase::new(self.state_provider.as_ref());
        temp_db.block_hash(number)
    }
}

/// 步骤2和3: 从累加文件系统或单个文件读取日志（版本5：按顺序，无address和key，无type，支持压缩）
/// 如果提供了缓存的索引条目，使用缓存；否则读取索引文件
/// 注意：此函数保留用于向后兼容，建议使用 read_log_compressed_data + new_from_compressed_buffer 以获得更好性能
#[allow(dead_code)]
pub(crate) fn read_read_logs_binary(
    log_path_or_block: &str,
    log_dir: Option<&std::path::Path>,
    cached_index_entries: Option<&[IndexEntry]>,
) -> eyre::Result<Vec<ReadLogEntry>> {
    let file_contents = if let Some(log_dir) = log_dir {
        // 步骤2和3: 从累加文件系统读取（使用索引查找偏移量和长度）
        // 尝试解析块号
        let block_number: u64 = log_path_or_block.parse()
            .map_err(|_| eyre::eyre!("Invalid block number: {}", log_path_or_block))?;

        // 使用缓存的索引条目或读取索引文件
        let entry = if let Some(cached) = cached_index_entries {
            // 使用缓存的索引条目（性能优化：避免每次读取都打开索引文件）
            *find_index_entry(cached, block_number)
                .ok_or_else(|| eyre::eyre!("Block {} not found in log index", block_number))?
        } else {
            // 读取索引文件（向后兼容，但性能较差）
            let (mut idx_file, _) = open_log_files(log_dir)?;
            let index_entries = read_index_file(&mut idx_file)?;
            *find_index_entry(&index_entries, block_number)
                .ok_or_else(|| eyre::eyre!("Block {} not found in log index", block_number))?
        };

        // 直接从文件读取（使用 BufReader 优化）
        let bin_path = log_dir.join("blocks_log.bin");
        let file = std::fs::File::open(&bin_path)?;
        const BUFFER_SIZE: usize = 1024 * 1024;
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);

        // Seek 到指定位置
        reader.seek(SeekFrom::Start(entry.offset))?;

        // 读取指定长度的数据
        let mut buffer = vec![0u8; entry.length as usize];
        reader.read_exact(&mut buffer)?;
        buffer
    } else {
        // 从单个文件读取（向后兼容，使用BufReader优化）
        let file = std::fs::File::open(log_path_or_block)?;
        const BUFFER_SIZE: usize = 1024 * 1024;
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        buffer
    };

    let mut cursor = std::io::Cursor::new(&file_contents);

    // 读取压缩算法标识（去掉前缀）
    let mut algorithm_byte = [0u8; 1];
    cursor.read_exact(&mut algorithm_byte)?;
    let algorithm = CompressionAlgorithm::from_u8(algorithm_byte[0])?;

    // 读取压缩后的数据
    let compressed_data = file_contents[cursor.position() as usize..].to_vec();

    // 解压数据
    let uncompressed_data = decompress_data(&compressed_data, algorithm)?;
    let mut cursor = std::io::Cursor::new(&uncompressed_data);

    // 读取条目数量
    let mut count_bytes = [0u8; 8];
    cursor.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes);

    let mut entries = Vec::new();

    // 版本5格式：data_len(1) + data（按顺序读取，类型通过数据内容推断）
    for _entry_index in 0..count {
        // 读取数据长度（1字节）
        let mut data_len_bytes = [0u8; 1];
        cursor.read_exact(&mut data_len_bytes)?;
        let data_len = data_len_bytes[0] as usize;

        // 读取数据
        let mut data = vec![0u8; data_len];
        cursor.read_exact(&mut data)?;

        // 通过数据内容推断类型：
        // Account 的 RLP 编码通常包含多个字段（nonce, balance, storage_root?, code_hash?），长度较长
        // Storage 的 RLP 编码只有一个 U256 值，长度较短（通常 <= 33 字节）
        // 如果数据长度 <= 33 字节，可能是 Storage；否则是 Account
        // 但更准确的方法是尝试解码：如果能解码为 U256 且长度匹配，则是 Storage；否则是 Account
        use alloy_rlp::Decodable;
        let data_copy = data.clone();
        let is_storage = if let Ok(_) = U256::decode(&mut data_copy.as_slice()) {
            // 能解码为 U256，且解码后数据应该被消耗完（或接近）
            data_len <= 33
        } else {
            false
        };

        if is_storage {
            entries.push(ReadLogEntry::Storage {
                address: Address::ZERO, // 占位符，不使用
                key: B256::ZERO, // 占位符，不使用
                data
            });
        } else {
            entries.push(ReadLogEntry::Account {
                address: Address::ZERO, // 占位符，不使用
                data
            });
        }
    }

    Ok(entries)
}
