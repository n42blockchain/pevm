//! Tests for log file reading and writing functionality

use super::{write_read_logs_binary, read_read_logs_binary, LoggedDatabase, ReadLogEntry};
use alloy_primitives::{Address, B256, U256};
use crate::revm::state::AccountInfo;
use std::fs;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read_binary_log() {
        // 创建测试数据
        let test_entries = vec![
            ReadLogEntry::Account {
                address: Address::ZERO, // 占位符，实际不使用
                data: vec![0xc0, 0x80, 0x80, 0x80, 0x80], // RLP: [0, 0, 0, 0]
            },
            ReadLogEntry::Storage {
                address: Address::ZERO, // 占位符，实际不使用
                key: B256::ZERO, // 占位符，实际不使用
                data: vec![0x80], // RLP: 0
            },
        ];
        
        // 写入日志（新格式）
        let block_number = 99999; // 使用一个不太可能冲突的块号
        write_read_logs_binary(block_number, &test_entries).unwrap();
        
        // 读取日志
        let log_file_path = format!("block_{}_reads.bin", block_number);
        let read_entries = read_read_logs_binary(&log_file_path).unwrap();
        
        assert_eq!(read_entries.len(), test_entries.len());
        
        // 验证账户数据（只验证数据，不验证 address）
        match (&read_entries[0], &test_entries[0]) {
            (ReadLogEntry::Account { data: d1, .. }, ReadLogEntry::Account { data: d2, .. }) => {
                assert_eq!(d1, d2);
            }
            _ => panic!("First entry should be Account"),
        }
        
        // 验证存储数据（只验证数据，不验证 address 和 key）
        match (&read_entries[1], &test_entries[1]) {
            (ReadLogEntry::Storage { data: d1, .. }, ReadLogEntry::Storage { data: d2, .. }) => {
                assert_eq!(d1, d2);
            }
            _ => panic!("Second entry should be Storage"),
        }
        
        // 清理
        fs::remove_file(&log_file_path).ok();
    }
    
    #[test]
    fn test_decode_account_rlp_with_optional_fields() {
        use alloy_rlp::Encodable;
        
        // 测试1: 只有 nonce 和 balance（省略 storage_root 和 code_hash）
        let mut buf1 = Vec::new();
        let payload_len1 = 0u64.length() + U256::ZERO.length();
        let header1 = alloy_rlp::Header {
            list: true,
            payload_length: payload_len1,
        };
        header1.encode(&mut buf1);
        0u64.encode(&mut buf1);
        U256::ZERO.encode(&mut buf1);
        
        let account1 = LoggedDatabase::decode_account_rlp(&buf1).unwrap();
        assert_eq!(account1.nonce, 0);
        assert_eq!(account1.balance, U256::ZERO);
        // code_hash 应该是空 code 的 hash
        let empty_code_hash = B256::from_slice(&[
            0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
            0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
            0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
            0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
        ]);
        assert_eq!(account1.code_hash(), empty_code_hash);
        
        // 测试2: 包含 code_hash
        let test_code_hash = B256::from_slice(&[1u8; 32]);
        let mut buf2 = Vec::new();
        let payload_len2 = 1u64.length() + U256::from(1000).length() + test_code_hash.length();
        let header2 = alloy_rlp::Header {
            list: true,
            payload_length: payload_len2,
        };
        header2.encode(&mut buf2);
        1u64.encode(&mut buf2);
        U256::from(1000).encode(&mut buf2);
        test_code_hash.encode(&mut buf2);
        
        let account2 = LoggedDatabase::decode_account_rlp(&buf2).unwrap();
        assert_eq!(account2.nonce, 1);
        assert_eq!(account2.balance, U256::from(1000));
        assert_eq!(account2.code_hash(), test_code_hash);
    }
}

