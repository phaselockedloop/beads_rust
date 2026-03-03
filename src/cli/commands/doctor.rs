//! Doctor command implementation.

#![allow(clippy::option_if_let_else)]

use crate::config;
use crate::error::Result;
use crate::output::OutputContext;
use rich_rust::prelude::*;
use serde::Serialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// Check result status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum CheckStatus {
    Ok,
    Warn,
    Error,
}

#[derive(Debug, Clone, Serialize)]
struct CheckResult {
    name: String,
    status: CheckStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
struct DoctorReport {
    ok: bool,
    checks: Vec<CheckResult>,
}

fn push_check(
    checks: &mut Vec<CheckResult>,
    name: &str,
    status: CheckStatus,
    message: Option<String>,
    details: Option<serde_json::Value>,
) {
    checks.push(CheckResult {
        name: name.to_string(),
        status,
        message,
        details,
    });
}

fn has_error(checks: &[CheckResult]) -> bool {
    checks
        .iter()
        .any(|check| matches!(check.status, CheckStatus::Error))
}

#[allow(clippy::unnecessary_wraps)]
fn print_report(report: &DoctorReport, ctx: &OutputContext) -> Result<()> {
    if ctx.is_json() {
        ctx.json(report);
        return Ok(());
    }
    if ctx.is_quiet() {
        return Ok(());
    }
    if ctx.is_rich() {
        render_doctor_rich(report, ctx);
        return Ok(());
    }

    print_report_plain(report);
    Ok(())
}

fn print_report_plain(report: &DoctorReport) {
    println!("br doctor");
    for check in &report.checks {
        let label = match check.status {
            CheckStatus::Ok => "OK",
            CheckStatus::Warn => "WARN",
            CheckStatus::Error => "ERROR",
        };
        if let Some(message) = &check.message {
            println!("{label} {}: {}", check.name, message);
        } else {
            println!("{label} {}", check.name);
        }
    }
}

fn render_doctor_rich(report: &DoctorReport, ctx: &OutputContext) {
    let theme = ctx.theme();
    let mut content = Text::new("");

    let mut ok_count = 0usize;
    let mut warn_count = 0usize;
    let mut error_count = 0usize;
    for check in &report.checks {
        match check.status {
            CheckStatus::Ok => ok_count += 1,
            CheckStatus::Warn => warn_count += 1,
            CheckStatus::Error => error_count += 1,
        }
    }

    content.append_styled("Diagnostics Report\n", theme.emphasis.clone());
    content.append("\n");

    content.append_styled("Status: ", theme.dimmed.clone());
    if report.ok {
        content.append_styled("OK", theme.success.clone());
    } else {
        content.append_styled("Issues found", theme.error.clone());
    }
    content.append("\n");

    content.append_styled("Checks: ", theme.dimmed.clone());
    content.append_styled(
        &format!("{ok_count} ok, {warn_count} warn, {error_count} error"),
        theme.accent.clone(),
    );
    content.append("\n\n");

    for check in &report.checks {
        let (label, style) = match check.status {
            CheckStatus::Ok => ("[OK]", theme.success.clone()),
            CheckStatus::Warn => ("[WARN]", theme.warning.clone()),
            CheckStatus::Error => ("[ERROR]", theme.error.clone()),
        };

        content.append_styled(label, style);
        content.append(" ");
        content.append_styled(&check.name, theme.issue_title.clone());
        if let Some(message) = &check.message {
            content.append_styled(": ", theme.dimmed.clone());
            content.append(message);
        }
        content.append("\n");

        if !matches!(check.status, CheckStatus::Ok)
            && let Some(details) = &check.details
            && let Ok(details_text) = serde_json::to_string_pretty(details)
        {
            for line in details_text.lines() {
                content.append_styled("    ", theme.dimmed.clone());
                content.append_styled(line, theme.dimmed.clone());
                content.append("\n");
            }
        }
    }

    let panel = Panel::from_rich_text(&content, ctx.width())
        .title(Text::styled("Doctor", theme.panel_title.clone()))
        .box_style(theme.box_style)
        .border_style(theme.panel_border.clone());

    ctx.render(&panel);
}

fn check_merge_artifacts(beads_dir: &Path, checks: &mut Vec<CheckResult>) -> Result<()> {
    let mut artifacts = Vec::new();
    for entry in beads_dir.read_dir()? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.contains(".base.jsonl")
            || name.contains(".left.jsonl")
            || name.contains(".right.jsonl")
        {
            artifacts.push(name.to_string());
        }
    }

    if artifacts.is_empty() {
        push_check(checks, "jsonl.merge_artifacts", CheckStatus::Ok, None, None);
    } else {
        push_check(
            checks,
            "jsonl.merge_artifacts",
            CheckStatus::Warn,
            Some("Merge artifacts detected in .beads/".to_string()),
            Some(serde_json::json!({ "files": artifacts })),
        );
    }
    Ok(())
}

fn discover_jsonl(beads_dir: &Path) -> Option<PathBuf> {
    let issues = beads_dir.join("issues.jsonl");
    if issues.exists() {
        return Some(issues);
    }
    let legacy = beads_dir.join("beads.jsonl");
    if legacy.exists() {
        return Some(legacy);
    }
    None
}

fn check_jsonl(path: &Path, checks: &mut Vec<CheckResult>) -> Result<usize> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut total = 0usize;
    let mut invalid = Vec::new();
    let mut invalid_count = 0usize;

    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        total += 1;
        if serde_json::from_str::<serde_json::Value>(trimmed).is_err() {
            invalid_count += 1;
            if invalid.len() < 10 {
                invalid.push(idx + 1);
            }
        }
    }

    if invalid.is_empty() {
        push_check(
            checks,
            "jsonl.parse",
            CheckStatus::Ok,
            Some(format!("Parsed {total} records")),
            Some(serde_json::json!({
                "path": path.display().to_string(),
                "records": total
            })),
        );
    } else {
        push_check(
            checks,
            "jsonl.parse",
            CheckStatus::Error,
            Some(format!(
                "Malformed JSONL lines: {invalid_count} (first: {invalid:?})"
            )),
            Some(serde_json::json!({
                "path": path.display().to_string(),
                "records": total,
                "invalid_lines": invalid,
                "invalid_count": invalid_count
            })),
        );
    }

    Ok(total)
}

/// Execute the doctor command.
///
/// # Errors
///
/// Returns an error if report serialization fails or if IO operations fail.
pub fn execute(cli: &config::CliOverrides, ctx: &OutputContext) -> Result<()> {
    let mut checks = Vec::new();
    let Ok(beads_dir) = config::discover_beads_dir(None) else {
        push_check(
            &mut checks,
            "beads_dir",
            CheckStatus::Error,
            Some("Missing .beads directory (run `br init`)".to_string()),
            None,
        );
        let report = DoctorReport {
            ok: !has_error(&checks),
            checks,
        };
        print_report(&report, ctx)?;
        std::process::exit(1);
    };

    let paths = match config::resolve_paths(&beads_dir, cli.db.as_ref()) {
        Ok(paths) => paths,
        Err(err) => {
            push_check(
                &mut checks,
                "metadata",
                CheckStatus::Error,
                Some(format!("Failed to read metadata.json: {err}")),
                None,
            );
            let report = DoctorReport {
                ok: !has_error(&checks),
                checks,
            };
            print_report(&report, ctx)?;
            std::process::exit(1);
        }
    };

    push_check(
        &mut checks,
        "beads_dir",
        CheckStatus::Ok,
        Some(format!("{}", beads_dir.display())),
        None,
    );

    check_merge_artifacts(&beads_dir, &mut checks)?;

    let jsonl_path = if paths.jsonl_path.exists() {
        Some(paths.jsonl_path.clone())
    } else {
        discover_jsonl(&beads_dir)
    };

    if let Some(path) = jsonl_path.as_ref() {
        match check_jsonl(path, &mut checks) {
            Ok(_) => {}
            Err(err) => {
                push_check(
                    &mut checks,
                    "jsonl.parse",
                    CheckStatus::Error,
                    Some(format!("Failed to read JSONL: {err}")),
                    Some(serde_json::json!({ "path": path.display().to_string() })),
                );
            }
        }
    } else {
        push_check(
            &mut checks,
            "jsonl.parse",
            CheckStatus::Warn,
            Some("No JSONL file found (.beads/issues.jsonl or .beads/beads.jsonl)".to_string()),
            None,
        );
    }

    let report = DoctorReport {
        ok: !has_error(&checks),
        checks,
    };
    print_report(&report, ctx)?;

    if !report.ok {
        std::process::exit(1);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn find_check<'a>(checks: &'a [CheckResult], name: &str) -> Option<&'a CheckResult> {
        checks.iter().find(|check| check.name == name)
    }

    #[test]
    fn test_check_jsonl_detects_malformed() -> Result<()> {
        let mut file = NamedTempFile::new().unwrap();
        std::io::Write::write_all(file.as_file_mut(), b"{\"id\":\"ok\"}\n")?;
        std::io::Write::write_all(file.as_file_mut(), b"{bad json}\n")?;

        let mut checks = Vec::new();
        let count = check_jsonl(file.path(), &mut checks).unwrap();
        assert_eq!(count, 2);

        let check = find_check(&checks, "jsonl.parse").expect("check present");
        assert!(matches!(check.status, CheckStatus::Error));

        Ok(())
    }
}
