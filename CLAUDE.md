# PEVM Project Instructions

## Common Commands

### EVM Log Execution (Windows)
```bash
./target/release/pevm.exe evm -b 3000000 -e 3500000 --log-block on --datadir "d:/reth2k" --log-dir "d:/reth900mac/states" -s 3 --mmap-log
```

### Parameters Reference
- `-b 3000000`: Start block number
- `-e 3500000`: End block number
- `--log-block on`: Enable block logging
- `--datadir "d:/reth2k"`: Reth database directory
- `--log-dir "d:/reth900mac/states"`: State log output directory
- `-s 3`: Skip parameter (process every 3rd block)
- `--mmap-log`: Use memory-mapped file for log reading (better performance for large files)
