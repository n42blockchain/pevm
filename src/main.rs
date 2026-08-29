// SPDX-License-Identifier: MIT OR Apache-2.0

#![allow(missing_docs)]

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

use clap::Parser;
use pevm::cli::chainspec::EthereumChainSpecParser;
use pevm::cli::Cli;
use reth_cli_commands::node::NoArgs;
use reth_cli_commands::launcher::FnLauncher;
use reth_node_builder::NodeHandle;
use reth_node_ethereum::EthereumNode;
use tracing::info;

fn main() {
    // A JIT backend in out-of-process mode re-executes this binary as its
    // compile helper; that process must answer the job and exit here.
    #[cfg(feature = "jit")]
    {
        match reth_node_ethereum::node::maybe_run_jit_helper() {
            Ok(std::ops::ControlFlow::Break(())) => return,
            Ok(std::ops::ControlFlow::Continue(())) => {}
            Err(err) => {
                eprintln!("Error: {err:?}");
                std::process::exit(1);
            }
        }
    }

    reth_cli_util::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) = Cli::<EthereumChainSpecParser, NoArgs>::parse().run(FnLauncher::new::<
        EthereumChainSpecParser,
        NoArgs,
    >(
        async move |builder, _| {
            info!(target: "reth::cli", "Launching node");
            let NodeHandle {
                node: _node,
                node_exit_future,
            } = builder
                .node(EthereumNode::default())
                .launch_with_debug_capabilities()
                .await?;

            node_exit_future.await
        },
    )) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
