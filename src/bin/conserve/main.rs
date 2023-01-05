// Conserve backup system.
// Copyright 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022 Martin Pool.

// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

//! Command-line entry point for Conserve backups.

use std::error::Error;
use std::path::PathBuf;
use std::process::Termination;
use std::str::FromStr;

use clap::{Parser, Subcommand};
use log::{LogGuard, LoggingOptions};
use monitor::{
    BackupProgressModel, DeleteProcessModel, NutmegMonitor, ReferencedBlocksProgressModel,
    RestoreProgressModel, SizeProgressModel,
};
use monitor::{FileListVerbosity, ValidateProgressModel};
use show::{show_diff, show_versions, ShowVersionsOptions};
use tracing::{error, info, trace, warn, Level};

use conserve::backup::BackupOptions;
use conserve::ReadTree;
use conserve::RestoreOptions;
use conserve::*;

mod log;
mod monitor;
mod show;

#[derive(Debug, Parser)]
#[command(author, about, version)]
struct Args {
    #[command(subcommand)]
    command: Command,

    /// No progress bars.
    #[arg(long, short = 'P', global = true)]
    no_progress: bool,

    /// Show debug trace to stdout.
    #[arg(long, short = 'D', global = true)]
    debug: bool,

    /// Don't show log timestamps and levels for the terminal output.
    #[arg(long, short = 'R', global = true)]
    log_raw: bool,

    /// Set the log level to trace
    #[arg(long, short = 'L', global = true)]
    log_level: Option<tracing::Level>,

    /// Path to the output log file
    #[arg(long, short = 'F', global = true)]
    log_file: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Copy source directory into an archive.
    Backup {
        /// Path of an existing archive.
        archive: String,
        /// Source directory to copy from.
        source: PathBuf,
        /// Print copied file names.
        #[arg(long, short)]
        verbose: bool,
        #[arg(long, short)]
        exclude: Vec<String>,
        #[arg(long, short = 'E')]
        exclude_from: Vec<String>,
        #[arg(long)]
        no_stats: bool,
        /// Don't use the backups index.
        /// Instead store all files (deduplication will still be active).
        #[arg(long)]
        no_index: bool,
        /// Show permissions, owner, and group in verbose output.
        #[arg(long, short = 'l')]
        long_listing: bool,
    },

    #[command(subcommand)]
    Debug(Debug),

    /// Delete backups from an archive.
    Delete {
        /// Archive to delete from.
        archive: String,
        /// Backup to delete, as an id like 'b1'. May be repeated with commas.
        #[arg(long, short, value_delimiter = ',', required(true))]
        backup: Vec<BandId>,
        /// Don't actually delete, just check what could be deleted.
        #[arg(long)]
        dry_run: bool,
        /// Break a lock left behind by a previous interrupted gc operation, and then gc.
        #[arg(long)]
        break_lock: bool,
        #[arg(long)]
        no_stats: bool,
    },

    /// Compare a stored tree to a source directory.
    Diff {
        archive: String,
        source: PathBuf,
        #[arg(long, short)]
        backup: Option<BandId>,
        #[arg(long, short)]
        exclude: Vec<String>,
        #[arg(long, short = 'E')]
        exclude_from: Vec<String>,
        #[arg(long)]
        include_unchanged: bool,
    },

    /// Create a new archive.
    Init {
        /// Path for new archive.
        archive: String,
    },

    /// Delete blocks unreferenced by any index.
    ///
    /// CAUTION: Do not gc while a backup is underway.
    Gc {
        /// Archive to delete from.
        archive: String,
        /// Don't actually delete, just check what could be deleted.
        #[arg(long)]
        dry_run: bool,
        /// Break a lock left behind by a previous interrupted gc operation, and then gc.
        #[arg(long)]
        break_lock: bool,
        #[arg(long)]
        no_stats: bool,
    },

    /// List files in a stored tree or source directory, with exclusions.
    Ls {
        #[command(flatten)]
        stos: StoredTreeOrSource,

        #[arg(long, short)]
        exclude: Vec<String>,
        #[arg(long, short = 'E')]
        exclude_from: Vec<String>,

        /// Show permissions, owner, and group.
        #[arg(short = 'l')]
        long_listing: bool,
    },

    /// Copy a stored tree to a restore directory.
    Restore {
        archive: String,
        destination: PathBuf,
        #[arg(long, short)]
        backup: Option<BandId>,
        #[arg(long, short)]
        force_overwrite: bool,
        #[arg(long, short)]
        verbose: bool,
        #[arg(long, short)]
        exclude: Vec<String>,
        #[arg(long, short = 'E')]
        exclude_from: Vec<String>,
        #[arg(long = "only", short = 'i')]
        only_subtree: Option<Apath>,
        #[arg(long)]
        no_stats: bool,
        /// Show permissions, owner, and group in verbose output.
        #[arg(long, short = 'l')]
        long_listing: bool,
    },

    /// Show the total size of files in a stored tree or source directory, with exclusions.
    Size {
        #[command(flatten)]
        stos: StoredTreeOrSource,

        /// Count in bytes, not megabytes.
        #[arg(long)]
        bytes: bool,

        #[arg(long, short)]
        exclude: Vec<String>,
        #[arg(long, short = 'E')]
        exclude_from: Vec<String>,
    },

    /// Check that an archive is internally consistent.
    Validate {
        /// Path of the archive to check.
        archive: String,

        /// Skip reading and checking the content of data blocks.
        #[arg(long, short = 'q')]
        quick: bool,
        #[arg(long)]
        no_stats: bool,
    },

    /// List backup versions in an archive.
    Versions {
        archive: String,
        /// Show only version names.
        #[arg(long, short = 'q')]
        short: bool,
        /// Sort bands to show most recent first.
        #[arg(long, short = 'n')]
        newest: bool,
        /// Show size of stored trees.
        #[arg(long, short = 'z', conflicts_with = "short")]
        sizes: bool,
        /// Show times in UTC.
        #[arg(long)]
        utc: bool,
    },
}

#[derive(Debug, Parser)]
struct StoredTreeOrSource {
    #[arg(required_unless_present = "source")]
    archive: Option<String>,

    /// List files in a source directory rather than an archive.
    #[arg(
        long,
        short,
        conflicts_with = "archive",
        required_unless_present = "archive"
    )]
    source: Option<PathBuf>,

    #[arg(long, short, conflicts_with = "source")]
    backup: Option<BandId>,
}

/// Show debugging information.
#[derive(Debug, Subcommand)]
enum Debug {
    /// Dump the index as json.
    Index {
        /// Path of the archive to read.
        archive: String,

        /// Backup version number.
        #[arg(long, short)]
        backup: Option<BandId>,
    },

    /// List all blocks.
    Blocks { archive: String },

    /// List all blocks referenced by any band.
    Referenced { archive: String },

    /// List garbage blocks referenced by no band.
    Unreferenced { archive: String },
}

#[repr(u8)]
enum ExitCode {
    Ok = 0,
    Failed = 1,
    PartialCorruption = 2,
}

impl Termination for ExitCode {
    fn report(self) -> std::process::ExitCode {
        std::process::ExitCode::from(self as u8)
    }
}

impl Command {
    fn run(&self, args: &Args) -> Result<ExitCode> {
        match self {
            Command::Backup {
                archive,
                source,
                verbose,
                exclude,
                exclude_from,
                no_stats,
                no_index,
                long_listing,
            } => {
                let exclude = ExcludeBuilder::from_args(exclude, exclude_from)?.build()?;
                let source = &LiveTree::open(source)?;
                let options = BackupOptions {
                    exclude,
                    no_index: *no_index,
                    ..Default::default()
                };

                let mut model = BackupProgressModel::default();
                if *long_listing {
                    model.file_list = FileListVerbosity::Full;
                } else if *verbose {
                    model.file_list = FileListVerbosity::NameOnly;
                }

                let monitor = NutmegMonitor::new(model, !args.no_progress);
                let stats = backup(
                    &Archive::open(open_transport(archive)?)?,
                    source,
                    &options,
                    Some(&monitor),
                )?;
                drop(monitor);

                if !no_stats {
                    info!("Backup complete.");
                    for line in format!("{}", stats).lines() {
                        info!("{}", line);
                    }
                }
            }
            Command::Debug(Debug::Blocks { archive }) => {
                for hash in Archive::open(open_transport(archive)?)?
                    .block_dir()
                    .block_names()?
                {
                    info!("{}", hash);
                }
            }
            Command::Debug(Debug::Index { archive, backup }) => {
                let st = stored_tree_from_opt(archive, backup)?;
                show::show_index_json(st.band())?;
            }
            Command::Debug(Debug::Referenced { archive }) => {
                let archive = Archive::open(open_transport(archive)?)?;
                let monitor =
                    NutmegMonitor::new(ReferencedBlocksProgressModel::default(), !args.no_progress);
                for hash in archive.referenced_blocks(&archive.list_band_ids()?, Some(&monitor))? {
                    info!("{}", hash);
                }
            }
            Command::Debug(Debug::Unreferenced { archive }) => {
                let monitor =
                    NutmegMonitor::new(ReferencedBlocksProgressModel::default(), !args.no_progress);
                for hash in
                    Archive::open(open_transport(archive)?)?.unreferenced_blocks(Some(&monitor))?
                {
                    info!("{}", hash);
                }
            }
            Command::Delete {
                archive,
                backup,
                dry_run,
                break_lock,
                no_stats,
            } => {
                let monitor = NutmegMonitor::new(DeleteProcessModel::default(), !args.no_progress);
                let stats = Archive::open(open_transport(archive)?)?.delete_bands(
                    backup,
                    &DeleteOptions {
                        dry_run: *dry_run,
                        break_lock: *break_lock,
                    },
                    Some(&monitor),
                )?;
                if !no_stats {
                    for line in format!("{}", stats).lines() {
                        info!("{}", line);
                    }
                }
            }
            Command::Diff {
                archive,
                source,
                backup,
                exclude,
                exclude_from,
                include_unchanged,
            } => {
                let exclude = ExcludeBuilder::from_args(exclude, exclude_from)?.build()?;
                let st = stored_tree_from_opt(archive, backup)?;
                let lt = LiveTree::open(source)?;
                let options = DiffOptions {
                    exclude,
                    include_unchanged: *include_unchanged,
                };

                show_diff(diff(&st, &lt, &options)?)?;
            }
            Command::Gc {
                archive,
                dry_run,
                break_lock,
                no_stats,
            } => {
                let monitor = NutmegMonitor::new(DeleteProcessModel::default(), !args.no_progress);

                let archive = Archive::open(open_transport(archive)?)?;
                let stats = archive.delete_bands(
                    &[],
                    &DeleteOptions {
                        dry_run: *dry_run,
                        break_lock: *break_lock,
                    },
                    Some(&monitor),
                )?;
                if !no_stats {
                    for line in format!("{}", stats).lines() {
                        info!("{}", line);
                    }
                }
            }
            Command::Init { archive } => {
                Archive::create(open_transport(archive)?)?;
                info!("Created new archive in {:?}", &archive);
            }
            Command::Ls {
                stos,
                exclude,
                exclude_from,
                long_listing,
            } => {
                let exclude = ExcludeBuilder::from_args(exclude, exclude_from)?.build()?;
                if let Some(archive) = &stos.archive {
                    // TODO: Option for subtree.
                    show::show_entry_names(
                        stored_tree_from_opt(archive, &stos.backup)?
                            .iter_entries(Apath::root(), exclude)?,
                        *long_listing,
                    )?;
                } else {
                    show::show_entry_names(
                        LiveTree::open(stos.source.clone().unwrap())?
                            .iter_entries(Apath::root(), exclude)?,
                        *long_listing,
                    )?;
                }
            }
            Command::Restore {
                archive,
                destination,
                backup,
                verbose,
                force_overwrite,
                exclude,
                exclude_from,
                only_subtree,
                no_stats,
                long_listing,
            } => {
                let band_selection = band_selection_policy_from_opt(backup);
                let archive = Archive::open(open_transport(archive)?)?;
                let exclude = ExcludeBuilder::from_args(exclude, exclude_from)?.build()?;
                let options = RestoreOptions {
                    exclude,
                    only_subtree: only_subtree.clone(),
                    band_selection,
                    overwrite: *force_overwrite,
                };

                let file_list = if *long_listing {
                    FileListVerbosity::Full
                } else if *verbose {
                    FileListVerbosity::NameOnly
                } else {
                    FileListVerbosity::None
                };

                let monitor =
                    NutmegMonitor::new(RestoreProgressModel::new(file_list), !args.no_progress);
                let stats = restore(&archive, destination, &options, Some(&monitor))?;
                if !no_stats {
                    info!("Restore complete.");
                    for line in format!("{}", stats).lines() {
                        info!("{}", line);
                    }
                }
            }
            Command::Size {
                stos,
                bytes,
                exclude,
                exclude_from,
            } => {
                let excludes = ExcludeBuilder::from_args(exclude, exclude_from)?.build()?;

                let monitor = NutmegMonitor::new(SizeProgressModel::default(), !args.no_progress);
                let size = if let Some(archive) = &stos.archive {
                    stored_tree_from_opt(archive, &stos.backup)?
                        .size(excludes, Some(&monitor as &dyn TreeSizeMonitor<_>))?
                        .file_bytes
                } else {
                    LiveTree::open(stos.source.as_ref().unwrap())?
                        .size(excludes, Some(&monitor as &dyn TreeSizeMonitor<_>))?
                        .file_bytes
                };
                drop(monitor);

                if *bytes {
                    info!("{}", size);
                } else {
                    info!("{}", &conserve::bytes_to_human_mb(size));
                }
            }
            Command::Validate {
                archive,
                quick,
                no_stats,
            } => {
                let options = ValidateOptions {
                    skip_block_hashes: *quick,
                };

                let monitor =
                    NutmegMonitor::new(ValidateProgressModel::default(), !args.no_progress);
                let stats = Archive::open(open_transport(archive)?)?
                    .validate(&options, Some(&monitor as &dyn ValidateMonitor))?;
                drop(monitor);

                if !no_stats {
                    for line in format!("{}", stats).lines() {
                        info!("{}", line);
                    }
                }
                if stats.has_problems() {
                    warn!("Archive has some problems.");
                    return Ok(ExitCode::PartialCorruption);
                } else {
                    info!("Archive is OK.");
                }
            }
            Command::Versions {
                archive,
                short,
                newest,
                sizes,
                utc,
            } => {
                let archive = Archive::open(open_transport(archive)?)?;
                let options = ShowVersionsOptions {
                    newest_first: *newest,
                    tree_size: *sizes,
                    utc: *utc,
                    start_time: !*short,
                    backup_duration: !*short,
                };
                show_versions(&archive, &options)?;
            }
        }
        Ok(ExitCode::Ok)
    }
}

fn stored_tree_from_opt(archive_location: &str, backup: &Option<BandId>) -> Result<StoredTree> {
    let archive = Archive::open(open_transport(archive_location)?)?;
    let policy = band_selection_policy_from_opt(backup);
    archive.open_stored_tree(policy)
}

fn band_selection_policy_from_opt(backup: &Option<BandId>) -> BandSelectionPolicy {
    if let Some(band_id) = backup {
        BandSelectionPolicy::Specified(band_id.clone())
    } else {
        BandSelectionPolicy::Latest
    }
}

fn initialize_log(args: &Args) -> std::result::Result<LogGuard, String> {
    let file = args
        .log_file
        .as_ref()
        .map(|file| PathBuf::from_str(file))
        .transpose()
        .map_err(|_| "Unparseable log file path".to_string())?;

    let level = args.log_level.unwrap_or({
        if args.debug {
            Level::TRACE
        } else {
            Level::INFO
        }
    });

    let guard = log::init(LoggingOptions {
        file,
        level,
        terminal_raw: args.log_raw,
    })?;

    trace!("tracing enabled");
    Ok(guard)
}

fn main() -> ExitCode {
    let args = Args::parse();
    let _log_guard = match initialize_log(&args) {
        Ok(guard) => guard,
        Err(message) => {
            eprintln!("Failed to initialize log system:");
            eprintln!("{}", message);
            return ExitCode::Failed;
        }
    };

    let result = args.command.run(&args);
    let exit_code = match result {
        Err(ref e) => {
            error!("{}", e.to_string());

            let mut cause: &dyn Error = e;
            while let Some(c) = cause.source() {
                error!("  caused by: {}", c);
                cause = c;
            }

            // NOTE(WolverinDEV): Reenable this as soon the feature backtrace lands in stable.
            //                    When doing so, logging trough tracing::trace would be recommanded.
            //                    (See https://github.com/sourcefrog/conserve/pull/173#discussion_r945195070)
            // if let Some(bt) = e.backtrace() {
            //     if std::env::var("RUST_BACKTRACE") == Ok("1".to_string()) {
            //         println!("{}", bt);
            //     }
            // }
            ExitCode::Failed
        }
        Ok(code) => code,
    };

    exit_code
}

#[test]
fn verify_clap() {
    use clap::CommandFactory;
    Args::command().debug_assert()
}
