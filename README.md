# PEVM

This project implements an Ethereum Virtual Machine (EVM) command executor, leveraging Rust's powerful concurrency features. The executor processes blocks from a blockchain, performing transactions in parallel using multiple threads to maximize efficiency.

## Headline result

The whole Ethereum mainnet chain — 25,864,982 blocks, 3,706,813,771
transactions, 315,472 Ggas — replays statelessly from per-block witnesses
and verifies every block against its header in **19.7 minutes** on one
128-core EPYC 9B45: **266.4 Ggas/s, 3.13M transactions per second**, 2.08
seconds of chain per second of wall clock. See `docs/witness.md` for the
measurement history, the profiling that got there, and how to reproduce
it (`scripts/pgo-build.sh`, `pevm evm --use-witness on ...`).

## Features

- Executes EVM commands within a defined block range.
- Employs a multi-threaded approach to enhance execution speed.
- Supports dynamic task distribution and load balancing across threads.
- Offers real-time monitoring of execution progress.

## Dependencies

The project relies on several external crates:

- `clap` for command-line argument parsing.
- `eyre` for error handling.
- `tracing` for logging.
- `reth_*` crates for blockchain-related functionalities.
- `num_cpus` to determine the number of CPU cores.

## Installation

1. Ensure you have Rust installed. If not, install it from [rustup.rs](https://rustup.rs).
2. Clone the repository:
    ```sh
    git clone <repository-url>
    cd <repository-directory>
    ```
3. Build the project:
    ```sh
    cargo build --release
    ```

## Usage

The main functionality is provided by the `EvmCommand` struct, which is configured and executed through the command line.

### Command Line Arguments

- `--begin` or `-b`: Begin block number (required).
- `--end` or `-e`: End block number (required).
- `--step` or `-s`: Step size for loop, default is 100.

### Example

To execute the EVM command for blocks from 1000 to 2000 with a step size of 50, run:
```sh
cargo run --release -- -b 1000 -e 2000 -s 50
```

## Code Overview

The core functionality is implemented in the `EvmCommand` struct. The `execute` method performs the following steps:

- Sets up the environment and consensus mechanism.
- Configures the blockchain tree and provider.
- Validates the specified block range.
- Creates a task queue and splits the block range into individual tasks.
- Launches multiple threads to process these tasks concurrently.
- Monitors and logs execution progress in real time.
- Waits for threads to complete and manages any errors that arise during execution.

### Task Execution

Each thread fetches tasks from a shared task queue, processes blocks within the specified range, and updates shared counters for gas usage, block count, and transaction count. The execution results are logged for monitoring.

### Real-Time Monitoring

A separate thread periodically records runtime metrics, such as the number of blocks and transactions processed, along with the current gas throughput.

## Contributing

1. Fork the repository.
2. Create a new branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -am 'Add your feature'`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Create a new Pull Request.

## License

This project is dual-licensed under either:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

## Acknowledgements

- Thanks to the authors of the `reth` crates for providing essential blockchain components.
- We are deeply grateful to the Rust community for their invaluable support and resources.



