//! JSON-backed storage for `beads_rust`.
//!
//! Reads and writes `.beads/issues.jsonl` directly.
//! All relations (labels, dependencies, comments) are embedded in each `Issue`.
//! After every mutation the JSONL file is atomically rewritten.

use crate::error::{BeadsError, Result};
use crate::format::{IssueDetails, IssueWithDependencyMetadata};
use crate::model::{Comment, Dependency, DependencyType, Event, Issue, IssueType, Priority, Status};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};

// ─── Public types ─────────────────────────────────────────────────────────────

/// Filter options for listing issues.
#[derive(Debug, Clone, Default)]
#[allow(clippy::struct_excessive_bools)]
pub struct ListFilters {
    pub statuses: Option<Vec<Status>>,
    pub types: Option<Vec<IssueType>>,
    pub priorities: Option<Vec<Priority>>,
    pub assignee: Option<String>,
    pub unassigned: bool,
    pub include_closed: bool,
    pub include_deferred: bool,
    pub include_templates: bool,
    pub title_contains: Option<String>,
    pub limit: Option<usize>,
    /// Sort field (priority, `created_at`, `updated_at`, title)
    pub sort: Option<String>,
    /// Reverse sort order
    pub reverse: bool,
    /// Filter by labels (all must match)
    pub labels: Option<Vec<String>>,
    /// Filter by labels (OR logic)
    pub labels_or: Option<Vec<String>>,
    /// Filter by `updated_at` <= timestamp
    pub updated_before: Option<DateTime<Utc>>,
    /// Filter by `updated_at` >= timestamp
    pub updated_after: Option<DateTime<Utc>>,
}

/// Fields to update on an issue.
#[derive(Debug, Clone, Default)]
pub struct IssueUpdate {
    pub title: Option<String>,
    pub description: Option<Option<String>>,
    pub design: Option<Option<String>>,
    pub acceptance_criteria: Option<Option<String>>,
    pub notes: Option<Option<String>>,
    pub status: Option<Status>,
    pub priority: Option<Priority>,
    pub issue_type: Option<IssueType>,
    pub assignee: Option<Option<String>>,
    pub owner: Option<Option<String>>,
    pub estimated_minutes: Option<Option<i32>>,
    pub due_at: Option<Option<DateTime<Utc>>>,
    pub defer_until: Option<Option<DateTime<Utc>>>,
    pub external_ref: Option<Option<String>>,
    pub closed_at: Option<Option<DateTime<Utc>>>,
    pub close_reason: Option<Option<String>>,
    pub closed_by_session: Option<Option<String>>,
    pub deleted_at: Option<Option<DateTime<Utc>>>,
    pub deleted_by: Option<Option<String>>,
    pub delete_reason: Option<Option<String>>,
    /// Ignored; kept for API compatibility.
    pub skip_cache_rebuild: bool,
    /// If true, verify the issue is unassigned inside the write lock.
    pub expect_unassigned: bool,
    /// If true, reject re-claims even by the same actor.
    pub claim_exclusive: bool,
    /// The actor performing the claim.
    pub claim_actor: Option<String>,
}

impl IssueUpdate {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.title.is_none()
            && self.description.is_none()
            && self.design.is_none()
            && self.acceptance_criteria.is_none()
            && self.notes.is_none()
            && self.status.is_none()
            && self.priority.is_none()
            && self.issue_type.is_none()
            && self.assignee.is_none()
            && self.owner.is_none()
            && self.estimated_minutes.is_none()
            && self.due_at.is_none()
            && self.defer_until.is_none()
            && self.external_ref.is_none()
            && self.closed_at.is_none()
            && self.close_reason.is_none()
            && self.closed_by_session.is_none()
            && self.deleted_at.is_none()
            && self.deleted_by.is_none()
            && self.delete_reason.is_none()
            && !self.expect_unassigned
    }
}

/// Filter options for ready issues.
#[derive(Debug, Clone, Default)]
pub struct ReadyFilters {
    pub assignee: Option<String>,
    pub unassigned: bool,
    pub labels_and: Vec<String>,
    pub labels_or: Vec<String>,
    pub types: Option<Vec<IssueType>>,
    pub priorities: Option<Vec<Priority>>,
    pub include_deferred: bool,
    pub limit: Option<usize>,
    /// Filter to children of this parent issue ID.
    pub parent: Option<String>,
    /// Include all descendants (grandchildren, etc.) not just direct children.
    pub recursive: bool,
}

/// Sort policy for ready issues.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum ReadySortPolicy {
    /// P0/P1 first by `created_at` ASC, then others by `created_at` ASC
    #[default]
    Hybrid,
    /// Sort by priority ASC, then `created_at` ASC
    Priority,
    /// Sort by `created_at` ASC only
    Oldest,
}

// ─── Storage struct ───────────────────────────────────────────────────────────

/// JSON-backed storage backend.
///
/// Reads/writes `.beads/issues.jsonl` directly.
/// All relations (labels, dependencies, comments) are embedded in each `Issue`.
#[derive(Debug)]
pub struct JsonStorage {
    /// Canonical path to the JSONL file (None for in-memory/test mode).
    jsonl_path: Option<PathBuf>,
    /// In-memory issue store keyed by issue ID.
    issues: HashMap<String, Issue>,
    /// In-memory key-value config store.
    config: HashMap<String, String>,
    /// In-memory metadata store.
    metadata_kv: HashMap<String, String>,
}

impl JsonStorage {
    // ─── Constructors ─────────────────────────────────────────────────────────

    /// Open a JSONL file as the backing store.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_timeout(path, None)
    }

    /// Open with an optional lock timeout (ignored; kept for API compatibility).
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn open_with_timeout(path: &Path, _lock_timeout_ms: Option<u64>) -> Result<Self> {
        let issues = if path.is_file() {
            load_jsonl(path)?
        } else {
            HashMap::new()
        };

        Ok(Self {
            jsonl_path: Some(path.to_path_buf()),
            issues,
            config: HashMap::new(),
            metadata_kv: HashMap::new(),
        })
    }

    /// Create an empty in-memory storage (for tests and no-db mode).
    ///
    /// # Errors
    ///
    /// Always succeeds; error type kept for API compatibility.
    pub fn open_memory() -> Result<Self> {
        Ok(Self {
            jsonl_path: None,
            issues: HashMap::new(),
            config: HashMap::new(),
            metadata_kv: HashMap::new(),
        })
    }

    // ─── Persistence ──────────────────────────────────────────────────────────

    /// Atomically rewrite the JSONL file with all current issues.
    fn save(&self) -> Result<()> {
        let Some(ref path) = self.jsonl_path else {
            return Ok(()); // in-memory mode
        };

        // Sort by created_at for stable, human-friendly output
        let mut issues: Vec<&Issue> = self.issues.values().collect();
        issues.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));

        // Write to a temp file then rename for atomicity
        let tmp_path = path.with_extension("tmp");
        {
            let file = std::fs::File::create(&tmp_path)
                .map_err(|e| BeadsError::Io(e))?;
            let mut writer = BufWriter::new(file);
            for issue in &issues {
                let line = serde_json::to_string(issue)
                    .map_err(|e| BeadsError::Config(format!("Serialize error: {e}")))?;
                writeln!(writer, "{line}")
                    .map_err(|e| BeadsError::Io(e))?;
            }
        }
        std::fs::rename(&tmp_path, path).map_err(|e| BeadsError::Io(e))?;
        Ok(())
    }

    // ─── CRUD ─────────────────────────────────────────────────────────────────

    /// Create a new issue.
    ///
    /// # Errors
    ///
    /// Returns an error if an issue with the same ID already exists or if saving fails.
    pub fn create_issue(&mut self, issue: &Issue, _actor: &str) -> Result<()> {
        if self.issues.contains_key(&issue.id) {
            return Err(BeadsError::validation(
                "id",
                format!("Issue '{}' already exists", issue.id),
            ));
        }
        self.issues.insert(issue.id.clone(), issue.clone());
        self.save()
    }

    /// Update an issue's fields.
    ///
    /// # Errors
    ///
    /// Returns an error if the issue does not exist or a claim precondition fails.
    #[allow(clippy::too_many_lines)]
    pub fn update_issue(&mut self, id: &str, updates: &IssueUpdate, _actor: &str) -> Result<Issue> {
        let issue = self
            .issues
            .get_mut(id)
            .ok_or_else(|| BeadsError::IssueNotFound { id: id.to_string() })?;

        if updates.is_empty() {
            return Ok(issue.clone());
        }

        // Claim guard
        if updates.expect_unassigned {
            if let Some(ref current) = issue.assignee {
                let same_actor = updates
                    .claim_actor
                    .as_deref()
                    .map_or(false, |a| a == current.as_str());
                if updates.claim_exclusive || !same_actor {
                    return Err(BeadsError::validation(
                        "assignee",
                        format!("Issue '{id}' is already assigned to '{current}'"),
                    ));
                }
            }
        }

        if let Some(ref v) = updates.title { issue.title = v.clone(); }
        if let Some(ref v) = updates.description { issue.description = v.clone(); }
        if let Some(ref v) = updates.design { issue.design = v.clone(); }
        if let Some(ref v) = updates.acceptance_criteria { issue.acceptance_criteria = v.clone(); }
        if let Some(ref v) = updates.notes { issue.notes = v.clone(); }
        if let Some(ref v) = updates.status { issue.status = v.clone(); }
        if let Some(v) = updates.priority { issue.priority = v; }
        if let Some(ref v) = updates.issue_type { issue.issue_type = v.clone(); }
        if let Some(ref v) = updates.assignee { issue.assignee = v.clone(); }
        if let Some(ref v) = updates.owner { issue.owner = v.clone(); }
        if let Some(v) = updates.estimated_minutes { issue.estimated_minutes = v; }
        if let Some(v) = updates.due_at { issue.due_at = v; }
        if let Some(v) = updates.defer_until { issue.defer_until = v; }
        if let Some(ref v) = updates.external_ref { issue.external_ref = v.clone(); }
        if let Some(v) = updates.closed_at { issue.closed_at = v; }
        if let Some(ref v) = updates.close_reason { issue.close_reason = v.clone(); }
        if let Some(ref v) = updates.closed_by_session { issue.closed_by_session = v.clone(); }
        if let Some(v) = updates.deleted_at { issue.deleted_at = v; }
        if let Some(ref v) = updates.deleted_by { issue.deleted_by = v.clone(); }
        if let Some(ref v) = updates.delete_reason { issue.delete_reason = v.clone(); }

        issue.updated_at = Utc::now();
        issue.content_hash = Some(issue.compute_content_hash());

        let result = issue.clone();
        self.save()?;
        Ok(result)
    }

    /// Delete (tombstone) an issue.
    ///
    /// # Errors
    ///
    /// Returns an error if the issue does not exist or saving fails.
    pub fn delete_issue(
        &mut self,
        id: &str,
        actor: &str,
        reason: &str,
        deleted_at: Option<DateTime<Utc>>,
    ) -> Result<Issue> {
        {
            let issue = self
                .issues
                .get_mut(id)
                .ok_or_else(|| BeadsError::IssueNotFound { id: id.to_string() })?;

            let original_type = issue.issue_type.as_str().to_string();
            let timestamp = deleted_at.unwrap_or_else(Utc::now);
            let now = Utc::now();

            issue.status = Status::Tombstone;
            issue.deleted_at = Some(timestamp);
            issue.deleted_by = Some(actor.to_string());
            issue.delete_reason = Some(reason.to_string());
            issue.original_type = Some(original_type);
            issue.updated_at = now;
        }
        self.save()?;
        Ok(self.issues[id].clone())
    }

    /// Get an issue by ID (with embedded labels/deps/comments).
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn get_issue(&self, id: &str) -> Result<Option<Issue>> {
        Ok(self.issues.get(id).cloned())
    }

    /// Get multiple issues by their IDs.
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn get_issues_by_ids(&self, ids: &[String]) -> Result<Vec<Issue>> {
        Ok(ids.iter().filter_map(|id| self.issues.get(id).cloned()).collect())
    }

    // ─── Listing & filtering ──────────────────────────────────────────────────

    /// List issues with optional filters and sorting.
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn list_issues(&self, filters: &ListFilters) -> Result<Vec<Issue>> {
        let mut issues: Vec<Issue> = self
            .issues
            .values()
            .filter(|issue| self.matches_list_filters(issue, filters))
            .cloned()
            .collect();

        sort_issues(&mut issues, filters.sort.as_deref(), filters.reverse);

        if let Some(limit) = filters.limit
            && limit > 0
        {
            issues.truncate(limit);
        }

        Ok(issues)
    }

    fn matches_list_filters(&self, issue: &Issue, f: &ListFilters) -> bool {
        // Status filter
        if let Some(ref statuses) = f.statuses {
            if !statuses.iter().any(|s| *s == issue.status) {
                return false;
            }
        } else if !f.include_closed
            && matches!(issue.status, Status::Closed | Status::Tombstone)
        {
            return false;
        }

        if !f.include_closed && !f.include_deferred && issue.status == Status::Deferred {
            return false;
        }

        if !f.include_templates && issue.is_template {
            return false;
        }

        // Type filter
        if let Some(ref types) = f.types {
            if !types.iter().any(|t| *t == issue.issue_type) {
                return false;
            }
        }

        // Priority filter
        if let Some(ref priorities) = f.priorities {
            if !priorities.iter().any(|p| *p == issue.priority) {
                return false;
            }
        }

        // Assignee filter
        if let Some(ref assignee) = f.assignee {
            if issue.assignee.as_deref() != Some(assignee.as_str()) {
                return false;
            }
        }
        if f.unassigned && issue.assignee.is_some() {
            return false;
        }

        // Label filters (AND)
        if let Some(ref labels) = f.labels {
            for label in labels {
                if !issue.labels.contains(label) {
                    return false;
                }
            }
        }

        // Label filters (OR)
        if let Some(ref labels_or) = f.labels_or
            && !labels_or.is_empty()
            && !labels_or.iter().any(|l| issue.labels.contains(l))
        {
            return false;
        }

        // Title contains
        if let Some(ref tc) = f.title_contains {
            if !issue.title.to_ascii_lowercase().contains(tc.to_ascii_lowercase().as_str()) {
                return false;
            }
        }

        // Time range
        if let Some(ts) = f.updated_before
            && issue.updated_at > ts
        {
            return false;
        }
        if let Some(ts) = f.updated_after
            && issue.updated_at < ts
        {
            return false;
        }

        true
    }

    /// Search issues by text query with optional filters.
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn search_issues(&self, query: &str, filters: &ListFilters) -> Result<Vec<Issue>> {
        let trimmed = query.trim().to_ascii_lowercase();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }

        let mut issues: Vec<Issue> = self
            .issues
            .values()
            .filter(|issue| {
                let id_match = issue.id.to_ascii_lowercase().contains(&trimmed);
                let title_match = issue.title.to_ascii_lowercase().contains(&trimmed);
                let desc_match = issue
                    .description
                    .as_deref()
                    .map_or(false, |d| d.to_ascii_lowercase().contains(&trimmed));
                (id_match || title_match || desc_match)
                    && self.matches_list_filters(issue, filters)
            })
            .cloned()
            .collect();

        sort_issues(&mut issues, Some("priority"), false);

        if let Some(limit) = filters.limit
            && limit > 0
        {
            issues.truncate(limit);
        }

        Ok(issues)
    }

    // ─── Ready / Blocked ──────────────────────────────────────────────────────

    /// Compute the set of issue IDs that are blocked by open blockers.
    fn compute_blocked_ids(&self) -> HashSet<String> {
        let mut blocked = HashSet::new();
        for issue in self.issues.values() {
            if matches!(issue.status, Status::Closed | Status::Tombstone) {
                continue;
            }
            for dep in &issue.dependencies {
                if !dep.dep_type.is_blocking() {
                    continue;
                }
                // issue depends on dep.depends_on_id
                // so if dep.depends_on_id is open/not-closed, issue is blocked
                if let Some(blocker) = self.issues.get(&dep.depends_on_id) {
                    if !matches!(blocker.status, Status::Closed | Status::Tombstone) {
                        blocked.insert(issue.id.clone());
                    }
                }
            }
        }
        blocked
    }

    /// Get ready issues (unblocked, not deferred, not pinned, not ephemeral).
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn get_ready_issues(
        &self,
        filters: &ReadyFilters,
        sort: ReadySortPolicy,
    ) -> Result<Vec<Issue>> {
        let blocked = self.compute_blocked_ids();
        let now = Utc::now();

        // Optionally collect descendant IDs for parent filter
        let descendant_ids: Option<HashSet<String>> = if let Some(ref parent_id) = filters.parent
            && filters.recursive
        {
            Some(self.collect_descendant_ids_set(parent_id))
        } else {
            None
        };

        let mut issues: Vec<Issue> = self
            .issues
            .values()
            .filter(|issue| {
                // Status
                if filters.include_deferred {
                    if !matches!(issue.status, Status::Open | Status::InProgress | Status::Deferred) {
                        return false;
                    }
                } else if !matches!(issue.status, Status::Open | Status::InProgress) {
                    return false;
                }

                // Not blocked
                if blocked.contains(&issue.id) {
                    return false;
                }

                // Defer until
                if !filters.include_deferred {
                    if let Some(defer) = issue.defer_until {
                        if defer > now {
                            return false;
                        }
                    }
                }

                // Not pinned
                if issue.pinned {
                    return false;
                }

                // Not ephemeral
                if issue.ephemeral || issue.id.contains("-wisp-") {
                    return false;
                }

                // Not template
                if issue.is_template {
                    return false;
                }

                // Types
                if let Some(ref types) = filters.types {
                    if !types.iter().any(|t| *t == issue.issue_type) {
                        return false;
                    }
                }

                // Priorities
                if let Some(ref priorities) = filters.priorities {
                    if !priorities.iter().any(|p| *p == issue.priority) {
                        return false;
                    }
                }

                // Assignee
                if let Some(ref assignee) = filters.assignee {
                    if issue.assignee.as_deref() != Some(assignee.as_str()) {
                        return false;
                    }
                }
                if filters.unassigned && issue.assignee.is_some() {
                    return false;
                }

                // Labels AND
                for label in &filters.labels_and {
                    if !issue.labels.contains(label) {
                        return false;
                    }
                }

                // Labels OR
                if !filters.labels_or.is_empty()
                    && !filters.labels_or.iter().any(|l| issue.labels.contains(l))
                {
                    return false;
                }

                // Parent filter
                if let Some(ref parent_id) = filters.parent {
                    if filters.recursive {
                        if let Some(ref ids) = descendant_ids {
                            if !ids.contains(&issue.id) {
                                return false;
                            }
                        }
                    } else {
                        // Direct children only
                        let is_child = issue.dependencies.iter().any(|dep| {
                            dep.depends_on_id == *parent_id
                                && dep.dep_type == DependencyType::ParentChild
                        });
                        if !is_child {
                            return false;
                        }
                    }
                }

                true
            })
            .cloned()
            .collect();

        // Sort
        match sort {
            ReadySortPolicy::Hybrid => {
                issues.sort_by(|a, b| {
                    let a_high = a.priority.0 <= 1;
                    let b_high = b.priority.0 <= 1;
                    b_high
                        .cmp(&a_high)
                        .then(a.created_at.cmp(&b.created_at))
                });
            }
            ReadySortPolicy::Priority => {
                issues.sort_by(|a, b| a.priority.0.cmp(&b.priority.0).then(a.created_at.cmp(&b.created_at)));
            }
            ReadySortPolicy::Oldest => {
                issues.sort_by_key(|i| i.created_at);
            }
        }

        if let Some(limit) = filters.limit
            && limit > 0
        {
            issues.truncate(limit);
        }

        Ok(issues)
    }

    /// Get the set of blocked issue IDs (from the blocked_issues_cache).
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn get_blocked_ids(&self) -> Result<HashSet<String>> {
        Ok(self.compute_blocked_ids())
    }

    /// Get issue IDs blocked by `blocks` dependency type only.
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn get_blocked_by_blocks_deps_only(&self) -> Result<HashSet<String>> {
        let mut blocked = HashSet::new();
        for issue in self.issues.values() {
            if matches!(issue.status, Status::Closed | Status::Tombstone) {
                continue;
            }
            for dep in &issue.dependencies {
                if dep.dep_type != DependencyType::Blocks {
                    continue;
                }
                if let Some(blocker) = self.issues.get(&dep.depends_on_id) {
                    if !matches!(blocker.status, Status::Closed | Status::Tombstone) {
                        blocked.insert(issue.id.clone());
                    }
                }
            }
        }
        Ok(blocked)
    }

    /// Check if a specific issue is currently blocked.
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn is_blocked(&self, issue_id: &str) -> Result<bool> {
        let Some(issue) = self.issues.get(issue_id) else {
            return Ok(false);
        };
        for dep in &issue.dependencies {
            if !dep.dep_type.is_blocking() {
                continue;
            }
            if let Some(blocker) = self.issues.get(&dep.depends_on_id) {
                if !matches!(blocker.status, Status::Closed | Status::Tombstone) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Get IDs of issues that are blocking the given issue.
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn get_blockers(&self, issue_id: &str) -> Result<Vec<String>> {
        let Some(issue) = self.issues.get(issue_id) else {
            return Ok(Vec::new());
        };
        let mut blockers = Vec::new();
        for dep in &issue.dependencies {
            if !dep.dep_type.is_blocking() {
                continue;
            }
            if let Some(blocker) = self.issues.get(&dep.depends_on_id) {
                if !matches!(blocker.status, Status::Closed | Status::Tombstone) {
                    blockers.push(dep.depends_on_id.clone());
                }
            }
        }
        Ok(blockers)
    }

    /// Rebuild the blocked issues cache (no-op in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn rebuild_blocked_cache(&mut self, _force_rebuild: bool) -> Result<usize> {
        Ok(0) // Computed dynamically; no persistent cache needed
    }

    /// Get all currently blocked issues with their blockers.
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn get_blocked_issues(&self) -> Result<Vec<(Issue, Vec<String>)>> {
        let mut result = Vec::new();
        for issue in self.issues.values() {
            if matches!(issue.status, Status::Closed | Status::Tombstone) {
                continue;
            }
            let blockers = self.get_blockers(&issue.id)?;
            if !blockers.is_empty() {
                result.push((issue.clone(), blockers));
            }
        }
        // Sort by priority then id for stable output
        result.sort_by(|a, b| a.0.priority.0.cmp(&b.0.priority.0).then(a.0.id.cmp(&b.0.id)));
        Ok(result)
    }

    /// Resolve external dependency statuses (always empty in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn resolve_external_dependency_statuses(
        &self,
        _external_db_paths: &HashMap<String, PathBuf>,
        _blocking_only: bool,
    ) -> Result<HashMap<String, bool>> {
        Ok(HashMap::new())
    }

    /// Compute external blockers (always empty in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn external_blockers(
        &self,
        _external_statuses: &HashMap<String, bool>,
    ) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::new())
    }

    // ─── ID helpers ───────────────────────────────────────────────────────────

    /// Check if an issue ID exists.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn id_exists(&self, id: &str) -> Result<bool> {
        Ok(self.issues.contains_key(id))
    }

    /// Find issue IDs whose suffix (after the last `-`) starts with `hash_suffix`.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn find_ids_by_hash(&self, hash_suffix: &str) -> Result<Vec<String>> {
        let lower = hash_suffix.to_ascii_lowercase();
        let mut ids: Vec<String> = self
            .issues
            .keys()
            .filter(|id| {
                id.to_ascii_lowercase().contains(&lower)
                    || id
                        .rsplit_once('-')
                        .map_or(false, |(_, hash)| hash.starts_with(&lower))
            })
            .cloned()
            .collect();
        ids.sort();
        Ok(ids)
    }

    /// Count non-ephemeral, non-wisp issues.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn count_issues(&self) -> Result<usize> {
        Ok(self
            .issues
            .values()
            .filter(|i| !i.ephemeral && !i.id.contains("-wisp-"))
            .count())
    }

    /// Count all issues (including ephemeral/wisp).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn count_all_issues(&self) -> Result<usize> {
        Ok(self.issues.len())
    }

    /// Count non-ephemeral, non-wisp issues (alias used by export check).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn count_exportable_issues(&self) -> Result<usize> {
        self.count_issues()
    }

    /// Get all issue IDs sorted.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_all_ids(&self) -> Result<Vec<String>> {
        let mut ids: Vec<String> = self.issues.keys().cloned().collect();
        ids.sort();
        Ok(ids)
    }

    /// Compute epic (parent) issue counts: total children and closed children.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_epic_counts(&self) -> Result<HashMap<String, (usize, usize)>> {
        let mut counts: HashMap<String, (usize, usize)> = HashMap::new();
        for issue in self.issues.values() {
            for dep in &issue.dependencies {
                if dep.dep_type != DependencyType::ParentChild {
                    continue;
                }
                let parent_id = &dep.depends_on_id;
                let entry = counts.entry(parent_id.clone()).or_insert((0, 0));
                entry.0 += 1;
                if matches!(issue.status, Status::Closed | Status::Tombstone) {
                    entry.1 += 1;
                }
            }
        }
        Ok(counts)
    }

    /// Find the next available child number for a parent issue.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn next_child_number(&self, parent_id: &str) -> Result<u32> {
        let mut max_num: u32 = 0;
        for id in self.issues.keys() {
            if let Some(suffix) = id.strip_prefix(&format!("{parent_id}.")) {
                if let Ok(n) = suffix.parse::<u32>() {
                    if n > max_num {
                        max_num = n;
                    }
                }
            }
        }
        Ok(max_num + 1)
    }

    // ─── Dependencies ─────────────────────────────────────────────────────────

    /// Add a dependency to an issue.
    ///
    /// # Errors
    ///
    /// Returns an error if the issue doesn't exist, the dependency already exists,
    /// or if adding it would create a cycle.
    pub fn add_dependency(
        &mut self,
        issue_id: &str,
        depends_on_id: &str,
        dep_type: &str,
        actor: &str,
    ) -> Result<bool> {
        use std::str::FromStr as _;
        let dep_type = DependencyType::from_str(dep_type)
            .unwrap_or_else(|_| DependencyType::Custom(dep_type.to_string()));

        if !self.issues.contains_key(issue_id) {
            return Err(BeadsError::IssueNotFound { id: issue_id.to_string() });
        }

        // Cycle detection
        if dep_type.is_blocking()
            && self.would_create_cycle(issue_id, depends_on_id, true)?
        {
            return Err(BeadsError::DependencyCycle {
                path: format!("Adding {issue_id} -> {depends_on_id} would create a cycle"),
            });
        }

        // Duplicate check - idempotent
        if let Some(issue) = self.issues.get(issue_id) {
            if issue.dependencies.iter().any(|d| {
                d.depends_on_id == depends_on_id && d.dep_type == dep_type
            }) {
                return Ok(false);
            }
        }

        let dep = Dependency {
            issue_id: issue_id.to_string(),
            depends_on_id: depends_on_id.to_string(),
            dep_type,
            created_at: Utc::now(),
            created_by: Some(actor.to_string()),
            metadata: None,
            thread_id: None,
        };

        if let Some(issue) = self.issues.get_mut(issue_id) {
            issue.dependencies.push(dep);
            issue.updated_at = Utc::now();
        }
        self.save()?;
        Ok(true)
    }

    /// Remove a specific dependency from an issue.
    ///
    /// Returns `true` if the dependency was found and removed.
    ///
    /// # Errors
    ///
    /// Returns an error if the issue doesn't exist or saving fails.
    pub fn remove_dependency(
        &mut self,
        issue_id: &str,
        depends_on_id: &str,
        _actor: &str,
    ) -> Result<bool> {
        let Some(issue) = self.issues.get_mut(issue_id) else {
            return Err(BeadsError::IssueNotFound { id: issue_id.to_string() });
        };
        let before = issue.dependencies.len();
        issue
            .dependencies
            .retain(|d| d.depends_on_id != depends_on_id);
        let removed = issue.dependencies.len() < before;
        if removed {
            issue.updated_at = Utc::now();
            self.save()?;
        }
        Ok(removed)
    }

    /// Remove all dependencies from an issue.
    ///
    /// # Errors
    ///
    /// Returns an error if the issue doesn't exist or saving fails.
    pub fn remove_all_dependencies(&mut self, issue_id: &str, _actor: &str) -> Result<usize> {
        let Some(issue) = self.issues.get_mut(issue_id) else {
            return Ok(0);
        };
        let count = issue.dependencies.len();
        issue.dependencies.clear();
        if count > 0 {
            issue.updated_at = Utc::now();
            self.save()?;
        }
        Ok(count)
    }

    /// Remove the parent-child dependency from an issue.
    ///
    /// # Errors
    ///
    /// Returns an error if saving fails.
    pub fn remove_parent(&mut self, issue_id: &str, _actor: &str) -> Result<bool> {
        let Some(issue) = self.issues.get_mut(issue_id) else {
            return Ok(false);
        };
        let before = issue.dependencies.len();
        issue
            .dependencies
            .retain(|d| d.dep_type != DependencyType::ParentChild);
        let removed = issue.dependencies.len() < before;
        if removed {
            issue.updated_at = Utc::now();
            self.save()?;
        }
        Ok(removed)
    }

    /// Get dependency IDs for an issue.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_dependencies(&self, issue_id: &str) -> Result<Vec<String>> {
        Ok(self
            .issues
            .get(issue_id)
            .map(|i| i.dependencies.iter().map(|d| d.depends_on_id.clone()).collect())
            .unwrap_or_default())
    }

    /// Get IDs of issues that depend on the given issue.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_dependents(&self, issue_id: &str) -> Result<Vec<String>> {
        Ok(self
            .issues
            .values()
            .filter(|i| i.dependencies.iter().any(|d| d.depends_on_id == issue_id))
            .map(|i| i.id.clone())
            .collect())
    }

    /// Get the parent issue ID (via parent-child dependency).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_parent_id(&self, issue_id: &str) -> Result<Option<String>> {
        Ok(self
            .issues
            .get(issue_id)
            .and_then(|i| {
                i.dependencies
                    .iter()
                    .find(|d| d.dep_type == DependencyType::ParentChild)
                    .map(|d| d.depends_on_id.clone())
            }))
    }

    /// Get full dependency metadata for an issue.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_dependencies_with_metadata(
        &self,
        issue_id: &str,
    ) -> Result<Vec<IssueWithDependencyMetadata>> {
        let Some(issue) = self.issues.get(issue_id) else {
            return Ok(Vec::new());
        };
        Ok(issue
            .dependencies
            .iter()
            .map(|dep| {
                let target = self.issues.get(&dep.depends_on_id);
                IssueWithDependencyMetadata {
                    id: dep.depends_on_id.clone(),
                    title: target.map_or_else(|| dep.depends_on_id.clone(), |i| i.title.clone()),
                    status: target.map_or_else(
                        || crate::model::Status::default(),
                        |i| i.status.clone(),
                    ),
                    priority: target.map_or(crate::model::Priority::MEDIUM, |i| i.priority),
                    dep_type: dep.dep_type.as_str().to_string(),
                }
            })
            .collect())
    }

    /// Get full dependent metadata for an issue.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_dependents_with_metadata(
        &self,
        issue_id: &str,
    ) -> Result<Vec<IssueWithDependencyMetadata>> {
        Ok(self
            .issues
            .values()
            .flat_map(|i| {
                i.dependencies
                    .iter()
                    .filter(|d| d.depends_on_id == issue_id)
                    .map(|dep| IssueWithDependencyMetadata {
                        id: i.id.clone(),
                        title: i.title.clone(),
                        status: i.status.clone(),
                        priority: i.priority,
                        dep_type: dep.dep_type.as_str().to_string(),
                    })
            })
            .collect())
    }

    /// Count dependencies for an issue.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn count_dependencies(&self, issue_id: &str) -> Result<usize> {
        Ok(self
            .issues
            .get(issue_id)
            .map_or(0, |i| i.dependencies.len()))
    }

    /// Count dependents for an issue.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn count_dependents(&self, issue_id: &str) -> Result<usize> {
        Ok(self
            .issues
            .values()
            .filter(|i| i.dependencies.iter().any(|d| d.depends_on_id == issue_id))
            .count())
    }

    /// Count dependencies for multiple issues.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn count_dependencies_for_issues(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, usize>> {
        Ok(ids
            .iter()
            .map(|id| {
                let count = self.issues.get(id).map_or(0, |i| i.dependencies.len());
                (id.clone(), count)
            })
            .collect())
    }

    /// Count dependents for multiple issues.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn count_dependents_for_issues(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, usize>> {
        let id_set: HashSet<&String> = ids.iter().collect();
        let mut counts: HashMap<String, usize> = ids.iter().map(|id| (id.clone(), 0)).collect();
        for issue in self.issues.values() {
            for dep in &issue.dependencies {
                if id_set.contains(&dep.depends_on_id) {
                    *counts.entry(dep.depends_on_id.clone()).or_insert(0) += 1;
                }
            }
        }
        Ok(counts)
    }

    /// Check if a dependency exists between two issues.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn dependency_exists_between(&self, issue_id: &str, depends_on_id: &str) -> Result<bool> {
        Ok(self
            .issues
            .get(issue_id)
            .map_or(false, |i| {
                i.dependencies
                    .iter()
                    .any(|d| d.depends_on_id == depends_on_id)
            }))
    }

    /// Check if adding a dependency would create a cycle.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn would_create_cycle(
        &self,
        issue_id: &str,
        depends_on_id: &str,
        blocking_only: bool,
    ) -> Result<bool> {
        // BFS/DFS from depends_on_id following its dependencies.
        // If we reach issue_id, it's a cycle.
        let mut visited = HashSet::new();
        let mut queue = vec![depends_on_id.to_string()];

        while let Some(current) = queue.pop() {
            if current == issue_id {
                return Ok(true);
            }
            if !visited.insert(current.clone()) {
                continue;
            }
            if let Some(issue) = self.issues.get(&current) {
                for dep in &issue.dependencies {
                    if !blocking_only || dep.dep_type.is_blocking() {
                        queue.push(dep.depends_on_id.clone());
                    }
                }
            }
        }
        Ok(false)
    }

    /// Detect all dependency cycles in the graph.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn detect_all_cycles(&self) -> Result<Vec<Vec<String>>> {
        // Simple DFS-based cycle detection
        let mut cycles = Vec::new();
        let mut visited: HashSet<String> = HashSet::new();

        for start_id in self.issues.keys() {
            if visited.contains(start_id) {
                continue;
            }
            let mut path = Vec::new();
            let mut in_stack: HashSet<String> = HashSet::new();
            self.dfs_detect_cycles(start_id, &mut path, &mut in_stack, &mut visited, &mut cycles);
        }
        Ok(cycles)
    }

    fn dfs_detect_cycles(
        &self,
        node: &str,
        path: &mut Vec<String>,
        in_stack: &mut HashSet<String>,
        visited: &mut HashSet<String>,
        cycles: &mut Vec<Vec<String>>,
    ) {
        if in_stack.contains(node) {
            // Found a cycle - extract it
            if let Some(pos) = path.iter().position(|id| id == node) {
                cycles.push(path[pos..].to_vec());
            }
            return;
        }
        if visited.contains(node) {
            return;
        }

        visited.insert(node.to_string());
        in_stack.insert(node.to_string());
        path.push(node.to_string());

        if let Some(issue) = self.issues.get(node) {
            let deps: Vec<String> = issue
                .dependencies
                .iter()
                .map(|d| d.depends_on_id.clone())
                .collect();
            for dep in deps {
                self.dfs_detect_cycles(&dep, path, in_stack, visited, cycles);
            }
        }

        path.pop();
        in_stack.remove(node);
    }

    fn collect_descendant_ids_set(&self, parent_id: &str) -> HashSet<String> {
        let mut result = HashSet::new();
        let mut queue = vec![parent_id.to_string()];
        let mut seen = HashSet::new();

        while let Some(current) = queue.pop() {
            if !seen.insert(current.clone()) {
                continue;
            }
            for issue in self.issues.values() {
                for dep in &issue.dependencies {
                    if dep.depends_on_id == current
                        && dep.dep_type == DependencyType::ParentChild
                        && !seen.contains(&issue.id)
                    {
                        result.insert(issue.id.clone());
                        queue.push(issue.id.clone());
                    }
                }
            }
        }
        result
    }

    /// Get all dependency records across all issues.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_all_dependency_records(&self) -> Result<HashMap<String, Vec<Dependency>>> {
        let mut map: HashMap<String, Vec<Dependency>> = HashMap::new();
        for issue in self.issues.values() {
            map.entry(issue.id.clone())
                .or_default()
                .extend(issue.dependencies.iter().cloned());
        }
        Ok(map)
    }

    // ─── Labels ───────────────────────────────────────────────────────────────

    /// Add a label to an issue (idempotent).
    ///
    /// Returns `true` if the label was newly added.
    ///
    /// # Errors
    ///
    /// Returns an error if the issue doesn't exist or saving fails.
    pub fn add_label(&mut self, issue_id: &str, label: &str, _actor: &str) -> Result<bool> {
        let Some(issue) = self.issues.get_mut(issue_id) else {
            return Err(BeadsError::IssueNotFound { id: issue_id.to_string() });
        };
        if issue.labels.contains(&label.to_string()) {
            return Ok(false);
        }
        issue.labels.push(label.to_string());
        issue.updated_at = Utc::now();
        self.save()?;
        Ok(true)
    }

    /// Remove a label from an issue.
    ///
    /// Returns `true` if the label was found and removed.
    ///
    /// # Errors
    ///
    /// Returns an error if the issue doesn't exist or saving fails.
    pub fn remove_label(&mut self, issue_id: &str, label: &str, _actor: &str) -> Result<bool> {
        let Some(issue) = self.issues.get_mut(issue_id) else {
            return Err(BeadsError::IssueNotFound { id: issue_id.to_string() });
        };
        let before = issue.labels.len();
        issue.labels.retain(|l| l != label);
        let removed = issue.labels.len() < before;
        if removed {
            issue.updated_at = Utc::now();
            self.save()?;
        }
        Ok(removed)
    }

    /// Remove all labels from an issue.
    ///
    /// # Errors
    ///
    /// Returns an error if saving fails.
    pub fn remove_all_labels(&mut self, issue_id: &str, _actor: &str) -> Result<usize> {
        let Some(issue) = self.issues.get_mut(issue_id) else {
            return Ok(0);
        };
        let count = issue.labels.len();
        issue.labels.clear();
        if count > 0 {
            issue.updated_at = Utc::now();
            self.save()?;
        }
        Ok(count)
    }

    /// Replace all labels on an issue.
    ///
    /// # Errors
    ///
    /// Returns an error if the issue doesn't exist or saving fails.
    pub fn set_labels(&mut self, issue_id: &str, labels: &[String], _actor: &str) -> Result<()> {
        let Some(issue) = self.issues.get_mut(issue_id) else {
            return Err(BeadsError::IssueNotFound { id: issue_id.to_string() });
        };
        issue.labels = labels.to_vec();
        issue.updated_at = Utc::now();
        self.save()
    }

    /// Get labels for an issue.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_labels(&self, issue_id: &str) -> Result<Vec<String>> {
        Ok(self
            .issues
            .get(issue_id)
            .map(|i| i.labels.clone())
            .unwrap_or_default())
    }

    /// Get labels for multiple issues.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_labels_for_issues(&self, ids: &[String]) -> Result<HashMap<String, Vec<String>>> {
        Ok(ids
            .iter()
            .map(|id| {
                let labels = self
                    .issues
                    .get(id)
                    .map(|i| i.labels.clone())
                    .unwrap_or_default();
                (id.clone(), labels)
            })
            .collect())
    }

    /// Get all labels across all issues, keyed by issue ID.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_all_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(self
            .issues
            .values()
            .filter(|i| !i.labels.is_empty())
            .map(|i| (i.id.clone(), i.labels.clone()))
            .collect())
    }

    /// Get unique labels with their counts.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_unique_labels_with_counts(&self) -> Result<Vec<(String, i64)>> {
        let mut counts: HashMap<String, i64> = HashMap::new();
        for issue in self.issues.values() {
            for label in &issue.labels {
                *counts.entry(label.clone()).or_insert(0) += 1;
            }
        }
        let mut result: Vec<(String, i64)> = counts.into_iter().collect();
        result.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        Ok(result)
    }

    /// Rename a label across all issues.
    ///
    /// Returns the number of issues affected.
    ///
    /// # Errors
    ///
    /// Returns an error if saving fails.
    pub fn rename_label(&mut self, old_name: &str, new_name: &str, _actor: &str) -> Result<usize> {
        let mut count = 0;
        for issue in self.issues.values_mut() {
            let mut changed = false;
            for label in &mut issue.labels {
                if label == old_name {
                    *label = new_name.to_string();
                    changed = true;
                }
            }
            if changed {
                issue.updated_at = Utc::now();
                count += 1;
            }
        }
        if count > 0 {
            self.save()?;
        }
        Ok(count)
    }

    // ─── Comments ─────────────────────────────────────────────────────────────

    /// Get comments for an issue.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_comments(&self, issue_id: &str) -> Result<Vec<Comment>> {
        Ok(self
            .issues
            .get(issue_id)
            .map(|i| i.comments.clone())
            .unwrap_or_default())
    }

    /// Add a comment to an issue.
    ///
    /// # Errors
    ///
    /// Returns an error if the issue doesn't exist or saving fails.
    pub fn add_comment(&mut self, issue_id: &str, author: &str, text: &str) -> Result<Comment> {
        let Some(issue) = self.issues.get_mut(issue_id) else {
            return Err(BeadsError::IssueNotFound { id: issue_id.to_string() });
        };
        let next_id = issue
            .comments
            .iter()
            .map(|c| c.id)
            .max()
            .unwrap_or(0)
            + 1;
        let comment = Comment {
            id: next_id,
            issue_id: issue_id.to_string(),
            author: author.to_string(),
            body: text.to_string(),
            created_at: Utc::now(),
        };
        issue.comments.push(comment.clone());
        issue.updated_at = Utc::now();
        self.save()?;
        Ok(comment)
    }

    /// Get all comments across all issues.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_all_comments(&self) -> Result<HashMap<String, Vec<Comment>>> {
        Ok(self
            .issues
            .values()
            .filter(|i| !i.comments.is_empty())
            .map(|i| (i.id.clone(), i.comments.clone()))
            .collect())
    }

    // ─── Config ───────────────────────────────────────────────────────────────

    /// Get a config value by key.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_config(&self, key: &str) -> Result<Option<String>> {
        Ok(self.config.get(key).cloned())
    }

    /// Get all config key-value pairs.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_all_config(&self) -> Result<HashMap<String, String>> {
        Ok(self.config.clone())
    }

    /// Set a config value.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn set_config(&mut self, key: &str, value: &str) -> Result<()> {
        self.config.insert(key.to_string(), value.to_string());
        Ok(())
    }

    /// Delete a config value. Returns `true` if it existed.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn delete_config(&mut self, key: &str) -> Result<bool> {
        Ok(self.config.remove(key).is_some())
    }

    // ─── Metadata ─────────────────────────────────────────────────────────────

    /// Get a metadata value by key.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_metadata(&self, key: &str) -> Result<Option<String>> {
        Ok(self.metadata_kv.get(key).cloned())
    }

    /// Set a metadata value.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn set_metadata(&mut self, key: &str, value: &str) -> Result<()> {
        self.metadata_kv.insert(key.to_string(), value.to_string());
        Ok(())
    }

    /// Delete a metadata value. Returns `true` if it existed.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn delete_metadata(&mut self, key: &str) -> Result<bool> {
        Ok(self.metadata_kv.remove(key).is_some())
    }

    // ─── Issue details ────────────────────────────────────────────────────────

    /// Get full issue details with relations, comments, and events.
    ///
    /// # Errors
    ///
    /// Always succeeds in the JSON backend.
    pub fn get_issue_details(
        &self,
        id: &str,
        include_comments: bool,
        _include_events: bool,
        _event_limit: usize,
    ) -> Result<Option<IssueDetails>> {
        let Some(issue) = self.issues.get(id) else {
            return Ok(None);
        };

        let labels = issue.labels.clone();
        let dependencies = self.get_dependencies_with_metadata(id)?;
        let dependents = self.get_dependents_with_metadata(id)?;
        let comments = if include_comments {
            issue.comments.clone()
        } else {
            vec![]
        };
        let parent = self.get_parent_id(id)?;

        Ok(Some(IssueDetails {
            issue: issue.clone(),
            labels,
            dependencies,
            dependents,
            comments,
            events: vec![], // No event history in JSON backend
            parent,
        }))
    }

    /// Get an issue with all relations populated (for export/display).
    ///
    /// In the JSON backend this is the same as `get_issue` since relations are embedded.
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_issue_for_export(&self, id: &str) -> Result<Option<Issue>> {
        self.get_issue(id)
    }

    /// Get all issues with relations populated (for export).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_all_issues_for_export(&self) -> Result<Vec<Issue>> {
        let mut issues: Vec<Issue> = self.issues.values().cloned().collect();
        issues.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
        Ok(issues)
    }

    // ─── Events (stub) ────────────────────────────────────────────────────────

    /// Get audit events for an issue (always empty in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_events(&self, _issue_id: &str, _limit: usize) -> Result<Vec<Event>> {
        Ok(Vec::new())
    }

    /// Get all audit events (always empty in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_all_events(&self, _limit: usize) -> Result<Vec<Event>> {
        Ok(Vec::new())
    }

    // ─── Dirty tracking (no-ops) ──────────────────────────────────────────────

    /// Get count of dirty issues (always 0 in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_dirty_issue_count(&self) -> Result<usize> {
        Ok(0)
    }

    /// Get dirty issue IDs (always empty in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_dirty_issue_ids(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    /// Clear dirty flags for given IDs (no-op in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn clear_dirty_issues(&mut self, _ids: &[String]) -> Result<usize> {
        Ok(0)
    }

    /// Clear all dirty flags (no-op in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn clear_all_dirty_issues(&mut self) -> Result<usize> {
        Ok(0)
    }

    /// Clear dirty flags (alias used by sync code).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn clear_dirty_flags(&mut self, _ids: &[String]) -> Result<usize> {
        Ok(0)
    }

    /// Clear all dirty flags (alias).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn clear_all_dirty_flags(&mut self) -> Result<usize> {
        Ok(0)
    }

    // ─── Export hashes (no-ops) ───────────────────────────────────────────────

    /// Get export hash for an issue (always None in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_export_hash(&self, _issue_id: &str) -> Result<Option<(String, String)>> {
        Ok(None)
    }

    /// Set export hash (no-op in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn set_export_hash(&mut self, _issue_id: &str, _content_hash: &str) -> Result<()> {
        Ok(())
    }

    /// Set multiple export hashes (no-op in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn set_export_hashes(&mut self, _exports: &[(String, String)]) -> Result<usize> {
        Ok(0)
    }

    /// Clear all export hashes (no-op in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn clear_all_export_hashes(&mut self) -> Result<usize> {
        Ok(0)
    }

    /// Get issues needing export (always empty in JSON backend).
    ///
    /// # Errors
    ///
    /// Always succeeds.
    pub fn get_issues_needing_export(&self, _dirty_ids: &[String]) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Load all issues from a JSONL file into a HashMap.
fn load_jsonl(path: &Path) -> Result<HashMap<String, Issue>> {
    let file = std::fs::File::open(path).map_err(|e| BeadsError::Io(e))?;
    let reader = std::io::BufReader::new(file);
    let mut issues = HashMap::new();

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = line_result.map_err(|e| BeadsError::Io(e))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Issue>(trimmed) {
            Ok(issue) => {
                issues.insert(issue.id.clone(), issue);
            }
            Err(e) => {
                tracing::warn!(
                    line = line_num + 1,
                    path = %path.display(),
                    error = %e,
                    "Skipping malformed issue line"
                );
            }
        }
    }

    Ok(issues)
}

/// Sort a list of issues by field and direction.
fn sort_issues(issues: &mut Vec<Issue>, sort_field: Option<&str>, reverse: bool) {
    match sort_field {
        Some("priority") => {
            if reverse {
                issues.sort_by(|a, b| {
                    b.priority.0
                        .cmp(&a.priority.0)
                        .then(a.created_at.cmp(&b.created_at))
                });
            } else {
                issues.sort_by(|a, b| {
                    a.priority.0
                        .cmp(&b.priority.0)
                        .then(b.created_at.cmp(&a.created_at))
                });
            }
        }
        Some("created_at") | Some("created") => {
            if reverse {
                issues.sort_by(|a, b| a.created_at.cmp(&b.created_at));
            } else {
                issues.sort_by(|a, b| b.created_at.cmp(&a.created_at));
            }
        }
        Some("updated_at") | Some("updated") => {
            if reverse {
                issues.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));
            } else {
                issues.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
            }
        }
        Some("title") => {
            if reverse {
                issues.sort_by(|a, b| {
                    b.title
                        .to_ascii_lowercase()
                        .cmp(&a.title.to_ascii_lowercase())
                });
            } else {
                issues.sort_by(|a, b| {
                    a.title
                        .to_ascii_lowercase()
                        .cmp(&b.title.to_ascii_lowercase())
                });
            }
        }
        _ => {
            // Default: priority ASC, created_at DESC
            if reverse {
                issues.sort_by(|a, b| {
                    b.priority.0
                        .cmp(&a.priority.0)
                        .then(a.created_at.cmp(&b.created_at))
                });
            } else {
                issues.sort_by(|a, b| {
                    a.priority.0
                        .cmp(&b.priority.0)
                        .then(b.created_at.cmp(&a.created_at))
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{IssueType, Priority, Status};

    fn make_issue(id: &str, title: &str) -> Issue {
        let now = Utc::now();
        Issue {
            id: id.to_string(),
            title: title.to_string(),
            status: Status::Open,
            priority: Priority(2),
            issue_type: IssueType::Task,
            created_at: now,
            updated_at: now,
            content_hash: None,
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
            assignee: None,
            owner: None,
            estimated_minutes: None,
            due_at: None,
            defer_until: None,
            external_ref: None,
            ephemeral: false,
            created_by: None,
            closed_at: None,
            close_reason: None,
            closed_by_session: None,
            source_system: None,
            source_repo: None,
            deleted_at: None,
            deleted_by: None,
            delete_reason: None,
            original_type: None,
            compaction_level: None,
            compacted_at: None,
            compacted_at_commit: None,
            original_size: None,
            sender: None,
            pinned: false,
            is_template: false,
            labels: vec![],
            dependencies: vec![],
            comments: vec![],
        }
    }

    #[test]
    fn test_open_memory_empty() {
        let storage = JsonStorage::open_memory().unwrap();
        assert_eq!(storage.count_issues().unwrap(), 0);
    }

    #[test]
    fn test_create_and_get_issue() {
        let mut storage = JsonStorage::open_memory().unwrap();
        let issue = make_issue("bd-001", "Test issue");
        storage.create_issue(&issue, "alice").unwrap();

        let loaded = storage.get_issue("bd-001").unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().title, "Test issue");
    }

    #[test]
    fn test_create_duplicate_fails() {
        let mut storage = JsonStorage::open_memory().unwrap();
        let issue = make_issue("bd-001", "Test issue");
        storage.create_issue(&issue, "alice").unwrap();
        let result = storage.create_issue(&issue, "alice");
        assert!(result.is_err());
    }

    #[test]
    fn test_update_issue() {
        let mut storage = JsonStorage::open_memory().unwrap();
        let issue = make_issue("bd-001", "Original title");
        storage.create_issue(&issue, "alice").unwrap();

        let updates = IssueUpdate {
            title: Some("Updated title".to_string()),
            ..Default::default()
        };
        let updated = storage.update_issue("bd-001", &updates, "alice").unwrap();
        assert_eq!(updated.title, "Updated title");
    }

    #[test]
    fn test_delete_issue() {
        let mut storage = JsonStorage::open_memory().unwrap();
        let issue = make_issue("bd-001", "Test issue");
        storage.create_issue(&issue, "alice").unwrap();

        let deleted = storage.delete_issue("bd-001", "alice", "done", None).unwrap();
        assert_eq!(deleted.status, Status::Tombstone);
    }

    #[test]
    fn test_labels() {
        let mut storage = JsonStorage::open_memory().unwrap();
        let issue = make_issue("bd-001", "Test issue");
        storage.create_issue(&issue, "alice").unwrap();

        storage.add_label("bd-001", "backend", "alice").unwrap();
        storage.add_label("bd-001", "urgent", "alice").unwrap();

        let labels = storage.get_labels("bd-001").unwrap();
        assert!(labels.contains(&"backend".to_string()));
        assert!(labels.contains(&"urgent".to_string()));

        storage.remove_label("bd-001", "backend", "alice").unwrap();
        let labels = storage.get_labels("bd-001").unwrap();
        assert!(!labels.contains(&"backend".to_string()));
    }

    #[test]
    fn test_comments() {
        let mut storage = JsonStorage::open_memory().unwrap();
        let issue = make_issue("bd-001", "Test issue");
        storage.create_issue(&issue, "alice").unwrap();

        storage
            .add_comment("bd-001", "alice", "This is a comment")
            .unwrap();

        let comments = storage.get_comments("bd-001").unwrap();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].body, "This is a comment");
    }

    #[test]
    fn test_blocked_detection() {
        let mut storage = JsonStorage::open_memory().unwrap();

        let blocker = make_issue("bd-001", "Blocker");
        let blocked = make_issue("bd-002", "Blocked issue");
        storage.create_issue(&blocker, "alice").unwrap();
        storage.create_issue(&blocked, "alice").unwrap();

        storage
            .add_dependency("bd-002", "bd-001", "blocks", "alice")
            .unwrap();

        assert!(storage.is_blocked("bd-002").unwrap());
        assert!(!storage.is_blocked("bd-001").unwrap());
    }

    #[test]
    fn test_config_kv() {
        let mut storage = JsonStorage::open_memory().unwrap();
        storage.set_config("issue_prefix", "bd").unwrap();
        assert_eq!(
            storage.get_config("issue_prefix").unwrap(),
            Some("bd".to_string())
        );
    }

    #[test]
    fn test_next_child_number() {
        let mut storage = JsonStorage::open_memory().unwrap();
        let parent = make_issue("bd-abc", "Parent");
        storage.create_issue(&parent, "alice").unwrap();

        assert_eq!(storage.next_child_number("bd-abc").unwrap(), 1);

        let mut child1 = make_issue("bd-abc.1", "Child 1");
        child1.dependencies.push(Dependency {
            issue_id: "bd-abc.1".to_string(),
            depends_on_id: "bd-abc".to_string(),
            dep_type: DependencyType::ParentChild,
            created_at: Utc::now(),
            created_by: None,
            metadata: None,
            thread_id: None,
        });
        storage.create_issue(&child1, "alice").unwrap();

        assert_eq!(storage.next_child_number("bd-abc").unwrap(), 2);
    }
}
