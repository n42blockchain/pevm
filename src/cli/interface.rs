// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI definition and entrypoint to executable

use crate::{
    chainspec::EthereumChainSpecParser,
    cli::evm,
};
use clap::{Parser, Subcommand};
use reth_chainspec::ChainSpec;
use reth_cli::chainspec::ChainSpecParser;
use reth_cli_commands::{
    config_cmd, db, download, dump_genesis, import, import_era, init_cmd, init_state,
    launcher::Launcher,
    node::{self, NoArgs},
    p2p, prune, stage,
};
use reth_cli_runner::CliRunner;
use reth_node_core::args::{JitArgs, LogArgs};
use reth_node_ethereum::{consensus::EthBeaconConsensus, EthEvmConfig, EthereumNode};
use reth_node_metrics::recorder::install_prometheus_recorder;
use reth_tracing::TracingGuards;
use std::{ffi::OsString, fmt, sync::Arc};
use tracing::info;

/// The main reth cli interface.
///
/// This is the entrypoint to the executable.
#[derive(Debug, Parser)]
#[command(author, about = "PEVM", long_about = None)]
pub struct Cli<C: ChainSpecParser = EthereumChainSpecParser, Ext: clap::Args + fmt::Debug = NoArgs>
{
    /// The command to run
    #[command(subcommand)]
    pub command: Commands<C, Ext>,

    /// The logging configuration for the CLI.
    #[command(flatten)]
    pub logs: LogArgs,
}

impl Cli {
    /// Parsers only the default CLI arguments
    pub fn parse_args() -> Self {
        Self::parse()
    }

    /// Parsers only the default CLI arguments from the given iterator
    pub fn try_parse_args_from<I, T>(itr: I) -> Result<Self, clap::error::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        Self::try_parse_from(itr)
    }
}

impl<C: ChainSpecParser<ChainSpec = ChainSpec>, Ext: clap::Args + fmt::Debug> Cli<C, Ext> {
    /// Execute the configured cli command.
    ///
    /// This accepts a closure that is used to launch the node via the
    /// [`NodeCommand`](node::NodeCommand).
    ///
    /// This command will be run on the default runtime created by [`CliRunner`].
    ///
    ///
    /// # Example
    ///
    /// ```ignore
    /// use reth_ethereum_cli::interface::Cli;
    /// use reth_node_ethereum::EthereumNode;
    ///
    /// Cli::parse_args()
    ///     .run(async move |builder, _| {
    ///         let handle = builder.launch_node(EthereumNode::default()).await?;
    ///
    ///         handle.wait_for_node_exit().await
    ///     })
    ///     .unwrap();
    /// ```
    ///
    /// # Example
    ///
    /// Parse additional CLI arguments for the node command and use it to configure the node.
    ///
    /// ```ignore
    /// use clap::Parser;
    /// use reth_ethereum_cli::{chainspec::EthereumChainSpecParser, interface::Cli};
    ///
    /// #[derive(Debug, Parser)]
    /// pub struct MyArgs {
    ///     pub enable: bool,
    /// }
    ///
    /// Cli::<EthereumChainSpecParser, MyArgs>::parse()
    ///     .run(async move |builder, my_args: MyArgs|
    ///         // launch the node
    ///         Ok(()))
    ///     .unwrap();
    /// ```
    pub fn run<L>(self, launcher: L) -> eyre::Result<()>
    where
        L: Launcher<C, Ext>,
    {
        self.with_runner(CliRunner::try_default_runtime()?, launcher)
    }

    /// Execute the configured cli command with the provided [`CliRunner`].
    ///
    ///
    /// # Example
    ///
    /// ```ignore
    /// use reth_cli_runner::CliRunner;
    /// use reth_ethereum_cli::interface::Cli;
    /// use reth_node_ethereum::EthereumNode;
    ///
    /// let runtime = tokio::runtime::Builder::new_multi_thread()
    ///     .worker_threads(4)
    ///     .max_blocking_threads(256)
    ///     .enable_all()
    ///     .build()
    ///     .unwrap();
    /// let runner = CliRunner::from_runtime(runtime);
    ///
    /// Cli::parse_args()
    ///     .with_runner(runner, |builder, _| async move {
    ///         let handle = builder.launch_node(EthereumNode::default()).await?;
    ///         handle.wait_for_node_exit().await
    ///     })
    ///     .unwrap();
    /// ```
    pub fn with_runner<L>(mut self, runner: CliRunner, launcher: L) -> eyre::Result<()>
    where
        L: Launcher<C, Ext>,
    {
        // Add network name if available to the logs dir
        if let Some(chain_spec) = self.command.chain_spec() {
            self.logs.log_file_directory = self
                .logs
                .log_file_directory
                .join(chain_spec.chain.to_string());
        }
        let _guard = self.init_tracing()?;
        info!(target: "reth::cli", "Initialized tracing, debug log directory: {}", self.logs.log_file_directory);

        // Install the prometheus recorder to be sure to record all metrics
        let _ = install_prometheus_recorder();

        // reth v2.5 made EthEvmConfig generic over the EVM factory for its JIT
        // support, and the node components expect the RethEvmFactory flavour.
        // With JIT off this builds the same plain configuration as before.
        let components = |spec: Arc<C::ChainSpec>| {
            let (evm_config, _) =
                reth_node_ethereum::node::build_evm_config(spec.clone(), &JitArgs::default(), None)
                    .expect("failed to build EVM config");
            (evm_config, Arc::new(EthBeaconConsensus::new(spec)))
        };
        let rt = runner.runtime();
        match self.command {
            Commands::Node(command) => {
                runner.run_command_until_exit(|ctx| command.execute(ctx, launcher))
            }
            Commands::Init(command) => {
                runner.run_blocking_until_ctrl_c(command.execute::<EthereumNode>(rt))
            }
            Commands::InitState(command) => {
                runner.run_blocking_until_ctrl_c(command.execute::<EthereumNode>(rt))
            }
            Commands::Import(command) => {
                runner.run_blocking_until_ctrl_c(command.execute::<EthereumNode, _>(components, rt))
            }
            Commands::ImportEra(command) => {
                runner.run_blocking_until_ctrl_c(command.execute::<EthereumNode>(rt))
            }
            Commands::DumpGenesis(command) => runner.run_blocking_until_ctrl_c(command.execute()),
            Commands::Db(command) => {
                runner.run_command_until_exit(|ctx| command.execute::<EthereumNode>(ctx))
            }
            Commands::Download(command) => {
                runner.run_blocking_until_ctrl_c(command.execute::<EthereumNode>())
            }
            Commands::Stage(command) => runner
                .run_command_until_exit(|ctx| command.execute::<EthereumNode, _>(ctx, components)),
            Commands::P2P(command) => runner.run_until_ctrl_c(command.execute::<EthereumNode>()),
            // Note: TestVectors is temporarily disabled due to reth 1.10.1 requiring 'arbitrary' feature
            // #[cfg(feature = "dev")]
            // Commands::TestVectors(command) => runner.run_until_ctrl_c(command.execute()),
            Commands::Config(command) => runner.run_until_ctrl_c(command.execute()),
            Commands::Evm(command) => {
                runner.run_command_until_exit(|ctx| command.execute::<EthereumNode>(ctx, rt))
            }
            Commands::Prune(command) => {
                runner.run_command_until_exit(|ctx| command.execute::<EthereumNode>(ctx))
            }
        }
    }

    /// Initializes tracing with the configured options.
    ///
    /// If file logging is enabled, this function returns a guard that must be kept alive to ensure
    /// that all logs are flushed to disk.
    pub fn init_tracing(&self) -> eyre::Result<TracingGuards> {
        let guards = self.logs.init_tracing()?;
        Ok(guards)
    }
}

/// Commands to be executed
#[derive(Debug, Subcommand)]
pub enum Commands<C: ChainSpecParser, Ext: clap::Args + fmt::Debug> {
    /// Start the node
    #[command(name = "node")]
    Node(Box<node::NodeCommand<C, Ext>>),
    /// Initialize the database from a genesis file.
    #[command(name = "init")]
    Init(init_cmd::InitCommand<C>),
    /// Initialize the database from a state dump file.
    #[command(name = "init-state")]
    InitState(init_state::InitStateCommand<C>),
    /// This syncs RLP encoded blocks from a file.
    #[command(name = "import")]
    Import(import::ImportCommand<C>),
    /// This syncs ERA encoded blocks from a directory.
    #[command(name = "import-era")]
    ImportEra(import_era::ImportEraCommand<C>),
    /// Dumps genesis block JSON configuration to stdout.
    DumpGenesis(dump_genesis::DumpGenesisCommand<C>),
    /// Database debugging utilities
    #[command(name = "db")]
    Db(db::Command<C>),
    /// Download public node snapshots
    #[command(name = "download")]
    Download(download::DownloadCommand<C>),
    /// Manipulate individual stages.
    #[command(name = "stage")]
    Stage(stage::Command<C>),
    /// P2P Debugging utilities
    #[command(name = "p2p")]
    P2P(p2p::Command<C>),
    /// Generate Test Vectors
    // Note: TestVectors is temporarily disabled due to reth 1.10.1 requiring 'arbitrary' feature
    // #[cfg(feature = "dev")]
    // #[command(name = "test-vectors")]
    // TestVectors(reth_cli_commands::test_vectors::Command),
    /// Write config to stdout
    #[command(name = "config")]
    Config(config_cmd::Command),
    /// Various debug routines
    #[command(name = "debug")]
    /// Execute EVM commands
    #[command(name = "evm")]
    Evm(Box<evm::EvmCommand<C>>),
    /// Prune according to the configuration without any limits
    #[command(name = "prune")]
    Prune(prune::PruneCommand<C>),
}

impl<C: ChainSpecParser, Ext: clap::Args + fmt::Debug> Commands<C, Ext> {
    /// Returns the underlying chain being used for commands
    pub fn chain_spec(&self) -> Option<&Arc<C::ChainSpec>> {
        match self {
            Self::Node(cmd) => cmd.chain_spec(),
            Self::Init(cmd) => cmd.chain_spec(),
            Self::InitState(cmd) => cmd.chain_spec(),
            Self::Import(cmd) => cmd.chain_spec(),
            Self::ImportEra(cmd) => cmd.chain_spec(),
            Self::DumpGenesis(cmd) => cmd.chain_spec(),
            Self::Db(cmd) => cmd.chain_spec(),
            Self::Download(cmd) => cmd.chain_spec(),
            Self::Stage(cmd) => cmd.chain_spec(),
            Self::P2P(cmd) => cmd.chain_spec(),
            // Note: TestVectors is temporarily disabled due to reth 1.10.1 requiring 'arbitrary' feature
            // #[cfg(feature = "dev")]
            // Self::TestVectors(_) => None,
            Self::Config(_) => None,
            Self::Evm(cmd) => cmd.chain_spec(),
            Self::Prune(cmd) => cmd.chain_spec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chainspec::SUPPORTED_CHAINS;
    use clap::CommandFactory;
    use reth_node_core::args::ColorMode;

    #[test]
    fn parse_color_mode() {
        let reth = Cli::try_parse_args_from(["reth", "node", "--color", "always"]).unwrap();
        assert_eq!(reth.logs.color, ColorMode::Always);
    }

    /// Tests that the help message is parsed correctly. This ensures that clap args are configured
    /// correctly and no conflicts are introduced via attributes that would result in a panic at
    /// runtime
    #[test]
    fn test_parse_help_all_subcommands() {
        let reth = Cli::<EthereumChainSpecParser, NoArgs>::command();
        for sub_command in reth.get_subcommands() {
            let err = Cli::try_parse_args_from(["reth", sub_command.get_name(), "--help"])
                .err()
                .unwrap_or_else(|| {
                    panic!("Failed to parse help message {}", sub_command.get_name())
                });

            // --help is treated as error, but
            // > Not a true "error" as it means --help or similar was used. The help message will be sent to stdout.
            assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
        }
    }

    /// Tests that the log directory is parsed correctly when using the node command. It's
    /// always tied to the specific chain's name.
    #[test]
    fn parse_logs_path_node() {
        let mut reth = Cli::try_parse_args_from(["reth", "node"]).unwrap();
        if let Some(chain_spec) = reth.command.chain_spec() {
            reth.logs.log_file_directory = reth
                .logs
                .log_file_directory
                .join(chain_spec.chain.to_string());
        }
        let log_dir = reth.logs.log_file_directory;
        let end = format!("reth/logs/{}", SUPPORTED_CHAINS[0]);
        assert!(log_dir.as_ref().ends_with(end), "{log_dir:?}");

        let mut iter = SUPPORTED_CHAINS.iter();
        iter.next();
        for chain in iter {
            let mut reth = Cli::try_parse_args_from(["reth", "node", "--chain", chain]).unwrap();
            let chain = reth
                .command
                .chain_spec()
                .map(|c| c.chain.to_string())
                .unwrap_or(String::new());
            reth.logs.log_file_directory = reth.logs.log_file_directory.join(chain.clone());
            let log_dir = reth.logs.log_file_directory;
            let end = format!("reth/logs/{chain}");
            assert!(log_dir.as_ref().ends_with(end), "{log_dir:?}");
        }
    }

    /// Tests that the log directory is parsed correctly when using the init command. It
    /// uses the underlying environment in command to get the chain.
    #[test]
    fn parse_logs_path_init() {
        let mut reth = Cli::try_parse_args_from(["reth", "init"]).unwrap();
        if let Some(chain_spec) = reth.command.chain_spec() {
            reth.logs.log_file_directory = reth
                .logs
                .log_file_directory
                .join(chain_spec.chain.to_string());
        }
        let log_dir = reth.logs.log_file_directory;
        let end = format!("reth/logs/{}", SUPPORTED_CHAINS[0]);
        println!("{log_dir:?}");
        assert!(log_dir.as_ref().ends_with(end), "{log_dir:?}");
    }

    /// Tests that the config command does not return any chain spec leading to empty chain id.
    #[test]
    fn parse_empty_logs_path() {
        let mut reth = Cli::try_parse_args_from(["reth", "config"]).unwrap();
        if let Some(chain_spec) = reth.command.chain_spec() {
            reth.logs.log_file_directory = reth
                .logs
                .log_file_directory
                .join(chain_spec.chain.to_string());
        }
        let log_dir = reth.logs.log_file_directory;
        let end = "reth/logs".to_string();
        println!("{log_dir:?}");
        assert!(log_dir.as_ref().ends_with(end), "{log_dir:?}");
    }

    #[test]
    fn parse_env_filter_directives() {
        use reth_cli_commands::launcher::FnLauncher;

        let temp_dir = tempfile::tempdir().unwrap();
        let log_dir = temp_dir.path().join("logs");

        unsafe { std::env::set_var("RUST_LOG", "info,evm=debug") };
        let reth = Cli::try_parse_args_from([
            "reth",
            "init",
            "--datadir",
            temp_dir.path().to_str().unwrap(),
            "--log.file.directory",
            log_dir.to_str().unwrap(),
            "--log.file.filter",
            "debug,net=trace",
        ])
        .unwrap();
        assert!(reth
            .run(FnLauncher::new::<EthereumChainSpecParser, NoArgs>(
                async move |_, _| Ok(())
            ))
            .is_ok());
    }
}
