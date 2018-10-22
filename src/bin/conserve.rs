// Conserve backup system.
// Copyright 2015, 2016, 2017, 2018 Martin Pool.

//! Command-line entry point for Conserve backups.

#![recursion_limit = "1024"] // Needed by error-chain

use std::path::Path;

#[macro_use]
extern crate clap;

extern crate chrono;
extern crate globset;

use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};

extern crate conserve;
use conserve::*;

fn main() {
    let matches = make_clap().get_matches();

    let (sub_name, subm) = matches.subcommand();
    let sub_fn = match sub_name {
        "backup" => backup,
        "debug" => debug,
        "init" => init,
        "list-source" => list_source,
        "ls" => ls,
        "restore" => restore,
        "validate" => validate,
        "versions" => versions,
        _ => panic!(),
    };
    let subm = subm.unwrap();

    let ui_name = matches
        .value_of("ui")
        .or_else(|| subm.value_of("ui"))
        .unwrap_or("auto");
    let no_progress = matches.is_present("no-progress");
    let ui = UI::by_name(ui_name, !no_progress).expect("Couldn't make UI");
    let mut report = Report::with_ui(ui);
    report.set_print_filenames(subm.is_present("v"));

    let result = sub_fn(subm, &report);
    report.finish();

    if matches.is_present("stats") {
        report.print(&format!("{}", report));
    }
    if let Err(e) = result {
        show_chained_errors(&report, &e);
        std::process::exit(1)
    }
}

fn make_clap<'a, 'b>() -> clap::App<'a, 'b> {
    fn archive_arg<'a, 'b>() -> Arg<'a, 'b> {
        Arg::with_name("archive")
            .help("Archive directory")
            .required(true)
    };

    fn backup_arg<'a, 'b>() -> Arg<'a, 'b> {
        Arg::with_name("backup")
            .help("Backup version number")
            .short("b")
            .long("backup")
            .takes_value(true)
            .value_name("VERSION")
    };

    fn exclude_arg<'a, 'b>() -> Arg<'a, 'b> {
        Arg::with_name("exclude")
            .long("exclude")
            .short("e")
            .takes_value(true)
            .multiple(true)
            .number_of_values(1)
            .value_name("GLOB")
            .help("Exclude files that match the provided glob pattern")
    };

    fn incomplete_arg<'a, 'b>() -> Arg<'a, 'b> {
        Arg::with_name("incomplete")
            .help("Read from incomplete (truncated) version")
            .long("incomplete")
    };

    fn verbose_arg<'a, 'b>() -> Arg<'a, 'b> {
        Arg::with_name("v").short("v").help("Print filenames")
    };

    // TODO: Allow the global options to occur even after the subcommand:
    // at the moment they have to be first.
    App::new("conserve")
        .about("A robust backup tool <http://conserve.fyi/>")
        .author(crate_authors!())
        .version(conserve::version())
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("ui")
                .long("ui")
                .short("u")
                .help("UI for progress and messages")
                .takes_value(true)
                .possible_values(&["auto", "plain", "color"]),
        )
        .arg(
            Arg::with_name("no-progress")
                .long("no-progress")
                .help("Hide progress bar"),
        )
        .arg(
            Arg::with_name("stats")
                .long("stats")
                .help("Show stats about IO, timing, and compression"),
        )
        .subcommand(
            SubCommand::with_name("debug")
                .about("Show developer-oriented information")
                .subcommand(
                    SubCommand::with_name("block")
                        .about("Debug blockdir")
                        .subcommand(
                            SubCommand::with_name("list")
                                .about("List hashes of all blocks in the blockdir")
                                .arg(Arg::with_name("archive").required(true)),
                        )
                        .subcommand(
                            SubCommand::with_name("referenced")
                                .about("List hashes of all blocks referenced by an index")
                                .arg(Arg::with_name("archive").required(true)),
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("validate")
                .about("Check whether an archive is internally consistent")
                .arg(archive_arg()),
        )
        .subcommand(
            SubCommand::with_name("init")
                .display_order(1)
                .about("Create a new archive")
                .arg(
                    Arg::with_name("archive")
                        .help(
                            "Path for new archive directory: \
                             should either not exist or be an empty directory",
                        )
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("backup")
                .display_order(2)
                .about("Copy source directory into an archive")
                .arg(archive_arg())
                .arg(
                    Arg::with_name("source")
                        .help("Backup from this directory")
                        .required(true),
                )
                .arg(exclude_arg())
                .arg(verbose_arg()),
        )
        .subcommand(
            SubCommand::with_name("restore")
                .display_order(3)
                .about("Copy a backup tree out of an archive")
                .arg(archive_arg())
                .arg(backup_arg())
                .arg(incomplete_arg())
                .after_help(
                    "\
                     Conserve will by default refuse to restore incomplete versions, \
                     to prevent you thinking you restored the whole tree when it may \
                     be truncated.  You can override this with --incomplete, or \
                     select an older version with --backup.",
                )
                .arg(
                    Arg::with_name("destination")
                        .help("Restore to this new directory")
                        .required(true),
                )
                .arg(
                    Arg::with_name("force-overwrite")
                        .long("force-overwrite")
                        .help("Overwrite existing destination directory"),
                )
                .arg(exclude_arg())
                .arg(verbose_arg()),
        )
        .subcommand(
            SubCommand::with_name("versions")
                .display_order(4)
                .about("List backup versions in an archive")
                .after_help(
                    "`conserve versions` shows one version per \
                     line.  For each version the output shows the version name, \
                     whether it is complete, when it started, and (if complete) \
                     how much time elapsed.",
                )
                .arg(
                    Arg::with_name("sizes")
                        .help("Show version disk sizes")
                        .long("sizes"),
                )
                .arg(archive_arg())
                .arg(
                    Arg::with_name("short")
                        .help("List just version name without details")
                        .long("short")
                        .short("s"),
                ),
        )
        .subcommand(
            SubCommand::with_name("ls")
                .display_order(5)
                .about("List files in a backup version")
                .arg(archive_arg())
                .arg(backup_arg())
                .arg(exclude_arg())
                .arg(incomplete_arg()),
        )
        .subcommand(
            SubCommand::with_name("list-source")
                .about("Recursive list files from source directory")
                .arg(
                    Arg::with_name("source")
                        .help("Source directory")
                        .required(true),
                )
                .arg(exclude_arg()),
        )
}

fn show_chained_errors(report: &Report, e: &Error) {
    report.problem(&format!("{}", e));
    for suberr in e.iter().skip(1) {
        // First was already printed
        report.problem(&format!("  {}", suberr));
    }
    if let Some(bt) = e.backtrace() {
        report.problem(&format!("{:?}", bt));
    }
}

fn init(subm: &ArgMatches, report: &Report) -> Result<()> {
    let archive_path = subm.value_of("archive").expect("'archive' arg not found");
    Archive::create(archive_path).and(Ok(()))?;
    report.print(&format!("Created new archive in {}", archive_path));
    Ok(())
}

fn backup(subm: &ArgMatches, report: &Report) -> Result<()> {
    let archive = Archive::open(subm.value_of("archive").unwrap(), &report)?;
    let lt = live_tree_from_options(subm, report)?;
    let mut bw = BackupWriter::begin(&archive)?;
    copy_tree(&lt, &mut bw)?;
    report.print("Backup complete.");
    report.print(&report.borrow_counts().summary_for_backup());
    Ok(())
}

fn validate(subm: &ArgMatches, report: &Report) -> Result<()> {
    let archive = Archive::open(subm.value_of("archive").unwrap(), &report)?;
    archive.validate()?;
    report.print("Archive is OK.");
    report.print(&report.borrow_counts().summary_for_validate());
    Ok(())
}

fn versions(subm: &ArgMatches, report: &Report) -> Result<()> {
    use conserve::output::ShowArchive;
    let archive = Archive::open(subm.value_of("archive").unwrap(), &report)?;
    if subm.is_present("short") {
        output::ShortVersionList::default().show_archive(&archive)
    } else {
        output::VerboseVersionList::default()
            .show_sizes(subm.is_present("sizes"))
            .show_archive(&archive)
    }
}

fn list_source(subm: &ArgMatches, report: &Report) -> Result<()> {
    let lt = live_tree_from_options(subm, report)?;
    list_tree_contents(&lt, report)?;
    Ok(())
}

fn ls(subm: &ArgMatches, report: &Report) -> Result<()> {
    let st = stored_tree_from_options(subm, report)?;
    list_tree_contents(&st, report)?;
    Ok(())
}

fn list_tree_contents<T: ReadTree>(tree: &T, report: &Report) -> Result<()> {
    // TODO: Maybe should be a specific concept in the UI.
    for entry in tree.iter_entries()? {
        report.print(&entry?.apath());
    }
    Ok(())
}

fn restore(subm: &ArgMatches, report: &Report) -> Result<()> {
    let dest = Path::new(subm.value_of("destination").unwrap());
    let st = stored_tree_from_options(subm, report)?;
    let mut rt = if subm.is_present("force-overwrite") {
        RestoreTree::create_overwrite(dest, report)
    } else {
        RestoreTree::create(dest, report)
    }?;
    copy_tree(&st, &mut rt)
}

fn debug(subm: &ArgMatches, report: &Report) -> Result<()> {
    match subm.subcommand() {
        ("block", Some(sm)) => match sm.subcommand() {
            ("list", Some(sm)) => debug_block_list(&sm, report),
            ("referenced", Some(sm)) => debug_block_referenced(&sm, report),
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn debug_block_list(subm: &ArgMatches, report: &Report) -> Result<()> {
    let archive = Archive::open(subm.value_of("archive").unwrap(), &report)?;
    for b in archive.block_dir().blocks(report)? {
        println!("{}", b);
    }
    Ok(())
}

fn debug_block_referenced(subm: &ArgMatches, report: &Report) -> Result<()> {
    let archive = Archive::open(subm.value_of("archive").unwrap(), report)?;
    for h in archive.referenced_blocks()? {
        report.print(&h);
    }
    Ok(())
}

fn stored_tree_from_options(subm: &ArgMatches, report: &Report) -> Result<StoredTree> {
    let archive = Archive::open(subm.value_of("archive").unwrap(), &report)?;
    let st = match band_id_from_option(subm)? {
        None => StoredTree::open_last(&archive),
        Some(ref b) => {
            if subm.is_present("incomplete") {
                StoredTree::open_incomplete_version(&archive, b)
            } else {
                StoredTree::open_version(&archive, b)
            }
        }
    }?;
    Ok(st.with_excludes(excludes_from_option(subm)?))
}

fn live_tree_from_options(subm: &ArgMatches, report: &Report) -> Result<LiveTree> {
    Ok(LiveTree::open(&subm.value_of("source").unwrap(), &report)?
        .with_excludes(excludes_from_option(subm)?))
}

fn band_id_from_option(subm: &ArgMatches) -> Result<Option<BandId>> {
    match subm.value_of("backup") {
        Some(b) => Ok(Some(BandId::from_string(b)?)),
        None => Ok(None),
    }
}

/// Make an exclusion globset from the `--exclude` option.
fn excludes_from_option(subm: &ArgMatches) -> Result<globset::GlobSet> {
    match subm.values_of("exclude") {
        Some(excludes) => excludes::from_strings(excludes),
        None => Ok(excludes::excludes_nothing()),
    }
}
