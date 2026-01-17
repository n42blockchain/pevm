// SPDX-License-Identifier: MIT OR Apache-2.0

//! 基础类型定义

use alloy_primitives::{Address, B256, U256};
use reth_codecs::Compact;
use reth_primitives::Account;

use super::EMPTY_CODE_HASH_BYTES;
use crate::revm::state::AccountInfo;

/// 任务：执行块范围
#[derive(Debug, Clone)]
pub(crate) struct Task {
    pub start: u64,
    pub end: u64,
}

/// 日志条目类型 - 按访问顺序记录
#[derive(Debug, Clone)]
pub(crate) enum ReadLogEntry {
    Account { address: Address, data: Vec<u8> },
    Storage { address: Address, key: B256, data: Vec<u8> },
}

/// 原始日志条目（未序列化）- 用于延迟序列化优化
/// 在执行期间存储原始数据，批次结束时再统一序列化
///
/// 性能优化：不存储 Option<AccountInfo>，只存储需要的字段
/// 避免克隆 Option<Bytecode>（可能很大）
#[derive(Debug, Clone)]
pub(crate) enum RawLogEntry {
    /// 账户存在：存储 nonce, balance, code_hash
    AccountExists { address: Address, nonce: u64, balance: U256, code_hash: B256 },
    /// 账户不存在
    AccountNotExists { address: Address },
    /// 存储槽
    Storage { address: Address, slot: U256, value: U256 },
}

impl RawLogEntry {
    /// 从 AccountInfo 创建（避免克隆整个 AccountInfo）
    #[inline]
    pub fn from_account(address: Address, info: Option<&AccountInfo>) -> Self {
        if let Some(account_info) = info {
            RawLogEntry::AccountExists {
                address,
                nonce: account_info.nonce,
                balance: account_info.balance,
                code_hash: account_info.code_hash(),
            }
        } else {
            RawLogEntry::AccountNotExists { address }
        }
    }

    /// 转换为序列化后的 ReadLogEntry
    pub fn into_serialized(self) -> ReadLogEntry {
        match self {
            RawLogEntry::AccountExists { address, nonce, balance, code_hash } => {
                let bytecode_hash = if code_hash.as_slice() == EMPTY_CODE_HASH_BYTES {
                    None
                } else {
                    Some(code_hash)
                };
                let account = Account {
                    nonce,
                    balance,
                    bytecode_hash,
                };
                let mut buf = Vec::with_capacity(100);
                account.to_compact(&mut buf);
                ReadLogEntry::Account { address, data: buf }
            }
            RawLogEntry::AccountNotExists { address } => {
                ReadLogEntry::Account { address, data: vec![0x00] }
            }
            RawLogEntry::Storage { address, slot, value } => {
                let key_bytes = slot.to_be_bytes::<32>();
                let key = B256::from_slice(&key_bytes);
                let mut buf = Vec::with_capacity(40);
                value.to_compact(&mut buf);
                ReadLogEntry::Storage { address, key, data: buf }
            }
        }
    }
}

/// 索引条目：块号 -> (偏移量, 长度)
#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexEntry {
    pub block_number: u64,
    pub offset: u64,
    pub length: u64,
}

impl IndexEntry {
    /// 索引条目大小（24字节）
    pub const SIZE: usize = 24;

    /// 从字节创建
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < Self::SIZE {
            return None;
        }
        Some(Self {
            block_number: u64::from_le_bytes(data[0..8].try_into().ok()?),
            offset: u64::from_le_bytes(data[8..16].try_into().ok()?),
            length: u64::from_le_bytes(data[16..24].try_into().ok()?),
        })
    }

    /// 转换为字节
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.block_number.to_le_bytes());
        buf[8..16].copy_from_slice(&self.offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.length.to_le_bytes());
        buf
    }
}
