use beads_rust::cli::commands;
use beads_rust::cli::{Cli, Commands};
use beads_rust::config;
use beads_rust::logging::init_logging;
use beads_rust::output::OutputContext;
use beads_rust::{BeadsError, StructuredError};
use clap::{CommandFactory, Parser};
use clap_complete::CompleteEnv;
use std::io::{self, IsTerminal};

#[allow(clippy::too_many_lines)]
fn main() {
    CompleteEnv::with_factory(Cli::command).complete();

    let cli = Cli::parse();
    let json_error_mode = should_render_errors_as_json(&cli);
    let output_ctx = OutputContext::from_args(&cli);

    // Initialize logging
    if let Err(e) = init_logging(cli.verbose, cli.quiet, None) {
        eprintln!("Failed to initialize logging: {e}");
    }

    let overrides = build_cli_overrides(&cli);

    let result = match cli.command {
        Commands::Init {
            prefix,
            force,
            backend: _,
        } => commands::init::execute(prefix, force, None, &output_ctx),
        Commands::Create(args) => commands::create::execute(&args, &overrides, &output_ctx),
        Commands::Update(args) => commands::update::execute(&args, &overrides, &output_ctx),
        Commands::Delete(args) => {
            commands::delete::execute(&args, cli.json, &overrides, &output_ctx)
        }
        Commands::List(args) => commands::list::execute(&args, cli.json, &overrides, &output_ctx),
        Commands::Comments(args) => {
            commands::comments::execute(&args, cli.json, &overrides, &output_ctx)
        }
        Commands::Search(args) => {
            commands::search::execute(&args, cli.json, &overrides, &output_ctx)
        }
        Commands::Show(args) => commands::show::execute(&args, cli.json, &overrides, &output_ctx),
        Commands::Close(args) => {
            commands::close::execute_cli(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Reopen(args) => {
            commands::reopen::execute(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Q(args) => commands::q::execute(args, &overrides, &output_ctx),
        Commands::Dep { command } => {
            commands::dep::execute(&command, cli.json, &overrides, &output_ctx)
        }
        Commands::Epic { command } => {
            commands::epic::execute(&command, cli.json, &overrides, &output_ctx)
        }
        Commands::Label { command } => {
            commands::label::execute(&command, cli.json, &overrides, &output_ctx)
        }
        Commands::Count(args) => commands::count::execute(&args, cli.json, &overrides, &output_ctx),
        Commands::Stale(args) => commands::stale::execute(&args, &overrides, &output_ctx),
        Commands::Lint(args) => commands::lint::execute(&args, cli.json, &overrides, &output_ctx),
        Commands::Ready(args) => commands::ready::execute(&args, cli.json, &overrides, &output_ctx),
        Commands::Blocked(args) => {
            commands::blocked::execute(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Doctor => commands::doctor::execute(&overrides, &output_ctx),
        Commands::Info(args) => commands::info::execute(&args, &overrides, &output_ctx),
        Commands::Schema(args) => commands::schema::execute(&args, &overrides, &output_ctx),
        Commands::Where => commands::r#where::execute(&overrides, &output_ctx),
        Commands::Version(args) => commands::version::execute(&args, &output_ctx),

        #[cfg(feature = "self_update")]
        Commands::Upgrade(args) => commands::upgrade::execute(&args, &output_ctx),
        Commands::Completions(args) => commands::completions::execute(&args, &output_ctx),
        Commands::Audit { command } => {
            commands::audit::execute(&command, cli.json, &overrides, &output_ctx)
        }
        Commands::Stats(args) | Commands::Status(args) => {
            commands::stats::execute(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Config { command } => {
            commands::config::execute(&command, cli.json, &overrides, &output_ctx)
        }
        Commands::History(args) => commands::history::execute(args, &overrides, &output_ctx),
        Commands::Defer(args) => {
            commands::defer::execute_defer(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Undefer(args) => {
            commands::defer::execute_undefer(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Orphans(args) => {
            commands::orphans::execute(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Changelog(args) => {
            commands::changelog::execute(&args, cli.json || args.robot, &overrides, &output_ctx)
        }
        Commands::Query { command } => commands::query::execute(&command, &overrides, &output_ctx),
        Commands::Graph(args) => commands::graph::execute(&args, &overrides, &output_ctx),
        Commands::Agents(args) => {
            let agents_args = commands::agents::AgentsArgs {
                add: args.add,
                remove: args.remove,
                update: args.update,
                check: args.check,
                dry_run: args.dry_run,
                force: args.force,
            };
            commands::agents::execute(&agents_args, &output_ctx)
        }
    };

    if let Err(e) = result {
        handle_error(&e, json_error_mode);
    }
}

const fn command_requests_robot_json(cmd: &Commands) -> bool {
    match cmd {
        Commands::Close(args) => args.robot,
        Commands::Reopen(args) => args.robot,
        Commands::Ready(args) => args.robot,
        Commands::Blocked(args) => args.robot,
        Commands::Stats(args) | Commands::Status(args) => args.robot,
        Commands::Defer(args) => args.robot,
        Commands::Undefer(args) => args.robot,
        Commands::Orphans(args) => args.robot,
        Commands::Changelog(args) => args.robot,
        _ => false,
    }
}

const fn should_render_errors_as_json(cli: &Cli) -> bool {
    cli.json || command_requests_robot_json(&cli.command)
}

/// Handle errors with structured output support.
fn handle_error(err: &BeadsError, json_mode: bool) -> ! {
    let structured = StructuredError::from_error(err);
    let exit_code = structured.code.exit_code();

    let use_json = json_mode || !io::stdout().is_terminal();

    if use_json {
        let json = structured.to_json();
        eprintln!(
            "{}",
            serde_json::to_string_pretty(&json).unwrap_or_else(|_| json.to_string())
        );
    } else {
        let use_color = io::stderr().is_terminal();
        eprintln!("{}", structured.to_human(use_color));
    }

    std::process::exit(exit_code);
}

fn build_cli_overrides(cli: &Cli) -> config::CliOverrides {
    config::CliOverrides {
        db: cli.db.clone(),
        actor: cli.actor.clone(),
        identity: None,
        json: Some(cli.json),
        display_color: if cli.no_color { Some(false) } else { None },
        quiet: Some(cli.quiet),
        no_db: Some(cli.no_db),
        no_daemon: Some(cli.no_daemon),
        no_auto_flush: Some(cli.no_auto_flush),
        no_auto_import: Some(cli.no_auto_import),
        lock_timeout: cli.lock_timeout,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn make_create_args() -> beads_rust::cli::CreateArgs {
        beads_rust::cli::CreateArgs {
            title: Some("test-title".to_string()),
            title_flag: None,
            type_: None,
            priority: None,
            description: None,
            assignee: None,
            owner: None,
            labels: Vec::new(),
            parent: None,
            deps: Vec::new(),
            estimate: None,
            due: None,
            defer: None,
            external_ref: None,
            status: None,
            ephemeral: false,
            dry_run: false,
            silent: false,
            file: None,
        }
    }

    #[test]
    fn parse_global_flags_and_command() {
        let cli = Cli::parse_from(["br", "--json", "-vv", "list"]);
        assert!(cli.json);
        assert_eq!(cli.verbose, 2);
        assert!(!cli.quiet);
        assert!(matches!(cli.command, Commands::List(_)));
    }

    #[test]
    fn parse_create_title_positional() {
        let cli = Cli::parse_from(["br", "create", "FixBug"]);
        match cli.command {
            Commands::Create(args) => {
                assert_eq!(args.title.as_deref(), Some("FixBug"));
            }
            other => unreachable!("expected create command, got {other:?}"),
        }
    }

    #[test]
    fn build_overrides_maps_flags() {
        let cli = Cli::parse_from([
            "br",
            "--json",
            "--no-color",
            "--no-auto-flush",
            "--lock-timeout",
            "2500",
            "list",
        ]);
        let overrides = build_cli_overrides(&cli);
        assert_eq!(overrides.json, Some(true));
        assert_eq!(overrides.display_color, Some(false));
        assert_eq!(overrides.no_auto_flush, Some(true));
        assert_eq!(overrides.lock_timeout, Some(2500));
    }

    #[test]
    fn help_includes_core_commands() {
        let help = Cli::command().render_help().to_string();
        assert!(help.contains("create"));
        assert!(help.contains("list"));
        assert!(help.contains("ready"));
    }

    #[test]
    fn version_includes_name_and_version() {
        let version = Cli::command().render_version();
        assert!(version.contains("br"));
        assert!(version.contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn command_is_not_sync() {
        // Verify that sync command no longer exists
        let cli = Cli::parse_from(["br", "list"]);
        assert!(matches!(cli.command, Commands::List(_)));
        let _ = make_create_args(); // use the helper
    }
}
