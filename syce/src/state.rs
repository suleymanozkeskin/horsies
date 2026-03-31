use std::collections::{HashMap, HashSet};

use crate::action::{DataSource, ListenerState, SearchMatch, Tab, TaskStatusFilter, TimeWindow, WorkflowStatusFilter};
use crate::models::{
    ActiveWorkerRow, AggregatedBreakdownRow, ClusterCapacitySummary, ClusterUtilizationPoint,
    DeadWorkerRow, FilterValue, OverloadedWorkerAlert, SnapshotAgeBucket, StaleClaimsAlert,
    TaskDetail, TaskListRow, TaskStatusRow, WorkerLoadPoint, WorkerQueuesRow, WorkerUptimeRow,
    WorkflowRow, WorkflowSummary, WorkflowTaskRow,
};
use ratatui::prelude::Rect;
use ratatui::text::Line;

/// Which section of the sidebar is currently focused
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SidebarSection {
    #[default]
    None,
    TaskNames,
    Queues,
    Errors,
}

/// Search state for the reusable search modal
#[derive(Debug, Clone, Default)]
pub struct SearchState {
    /// Whether search modal is currently active
    pub active: bool,
    /// Current search query
    pub query: String,
    /// Search results matching the query
    pub matches: Vec<SearchMatch>,
    /// Currently selected match index
    pub selected_index: usize,
    /// Top-most visible result row in the search results viewport
    pub scroll_offset: usize,
    /// Visible results rows in current viewport (captured from render)
    pub results_view_height: usize,
    /// Search results area for mouse hit-testing
    pub results_area: Option<Rect>,
    /// Search scrollbar area for mouse hit-testing
    pub scrollbar_area: Option<Rect>,
    /// Cursor position in the query string
    pub cursor_position: usize,
}

impl SearchState {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Toast notification for brief feedback messages
#[derive(Debug, Clone)]
pub struct Toast {
    pub message: String,
    pub icon: ToastIcon,
    pub ticks_remaining: u8,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum ToastIcon {
    Success,
    Info,
    Warning,
    Error,
}

impl Toast {
    /// Create a success toast (e.g., for copy operations)
    pub fn success(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            icon: ToastIcon::Success,
            ticks_remaining: 8, // ~2 seconds at 4 ticks/sec
        }
    }

    /// Create an info toast
    pub fn info(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            icon: ToastIcon::Info,
            ticks_remaining: 8,
        }
    }

    /// Decrement tick counter, returns true if toast should be dismissed
    pub fn tick(&mut self) -> bool {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining == 0
    }
}

/// Search highlight - shows pointer emoji next to found item
#[derive(Debug, Clone)]
pub struct SearchHighlight {
    /// ID of the highlighted item (worker_id, workflow_id, or task_id)
    pub target_id: String,
    /// Pre-parsed line number for modal highlights (avoids allocations in render loop)
    pub target_line: Option<usize>,
    /// Ticks remaining before auto-dismiss
    pub ticks_remaining: u8,
}

impl SearchHighlight {
    pub fn new(target_id: String) -> Self {
        let target_line = target_id
            .strip_prefix("line:")
            .and_then(|n| n.parse().ok());
        Self {
            target_id,
            target_line,
            ticks_remaining: 10, // ~2.5 seconds
        }
    }

    /// Tick and return true if should dismiss
    pub fn tick(&mut self) -> bool {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining == 0
    }

    /// Get the pointer emoji - pulses between two states (points left to indicate content)
    pub fn pointer(&self) -> &'static str {
        if self.ticks_remaining % 2 == 0 {
            "👈"
        } else {
            " ◂"
        }
    }

    /// Check if this ID matches
    pub fn matches(&self, id: &str) -> bool {
        self.target_id == id
    }

    /// Check if this line number matches (for modal highlights)
    pub fn matches_line(&self, line_idx: usize) -> bool {
        self.target_line == Some(line_idx)
    }
}

impl SearchState {
    /// Open search and reset state
    pub fn open(&mut self) {
        self.active = true;
        self.query.clear();
        self.matches.clear();
        self.selected_index = 0;
        self.scroll_offset = 0;
        self.results_view_height = 0;
        self.results_area = None;
        self.scrollbar_area = None;
        self.cursor_position = 0;
    }

    /// Close search and clear state
    pub fn close(&mut self) {
        self.active = false;
        self.query.clear();
        self.matches.clear();
        self.selected_index = 0;
        self.scroll_offset = 0;
        self.results_view_height = 0;
        self.results_area = None;
        self.scrollbar_area = None;
        self.cursor_position = 0;
    }

    /// Insert a character at cursor position
    pub fn insert_char(&mut self, c: char) {
        self.query.insert(self.cursor_position, c);
        self.cursor_position += 1;
        self.selected_index = 0; // Reset selection on query change
    }

    /// Delete character before cursor
    pub fn backspace(&mut self) {
        if self.cursor_position > 0 {
            self.cursor_position -= 1;
            self.query.remove(self.cursor_position);
            self.selected_index = 0; // Reset selection on query change
        }
    }

    /// Clear the search query
    pub fn clear_query(&mut self) {
        self.query.clear();
        self.cursor_position = 0;
        self.matches.clear();
        self.selected_index = 0;
        self.scroll_offset = 0;
        self.results_area = None;
        self.scrollbar_area = None;
    }

    /// Move selection up
    pub fn select_up(&mut self) {
        if !self.matches.is_empty() && self.selected_index > 0 {
            self.selected_index -= 1;
            self.ensure_selected_visible();
        }
    }

    /// Move selection down
    pub fn select_down(&mut self) {
        if !self.matches.is_empty() && self.selected_index < self.matches.len() - 1 {
            self.selected_index += 1;
            self.ensure_selected_visible();
        }
    }

    pub fn select_page_up(&mut self) {
        if self.matches.is_empty() {
            return;
        }
        let step = self.results_view_height.max(1);
        self.selected_index = self.selected_index.saturating_sub(step);
        self.ensure_selected_visible();
    }

    pub fn select_page_down(&mut self) {
        if self.matches.is_empty() {
            return;
        }
        let step = self.results_view_height.max(1);
        self.selected_index = (self.selected_index + step).min(self.matches.len().saturating_sub(1));
        self.ensure_selected_visible();
    }

    pub fn select_home(&mut self) {
        if self.matches.is_empty() {
            return;
        }
        self.selected_index = 0;
        self.ensure_selected_visible();
    }

    pub fn select_end(&mut self) {
        if self.matches.is_empty() {
            return;
        }
        self.selected_index = self.matches.len().saturating_sub(1);
        self.ensure_selected_visible();
    }

    pub fn select_index(&mut self, idx: usize) {
        if self.matches.is_empty() {
            self.selected_index = 0;
            self.scroll_offset = 0;
            return;
        }
        self.selected_index = idx.min(self.matches.len().saturating_sub(1));
        self.ensure_selected_visible();
    }

    pub fn set_scroll_offset(&mut self, offset: usize) {
        if self.matches.is_empty() {
            self.selected_index = 0;
            self.scroll_offset = 0;
            return;
        }

        let visible = self.results_view_height.max(1);
        let max_scroll = self.matches.len().saturating_sub(visible);
        self.scroll_offset = offset.min(max_scroll);

        // Keep current selection visible after scrollbar jumps.
        if self.selected_index < self.scroll_offset
            || self.selected_index >= self.scroll_offset + visible
        {
            self.selected_index = self.scroll_offset;
        }
    }

    fn ensure_selected_visible(&mut self) {
        let visible = self.results_view_height.max(1);
        let max_scroll = self.matches.len().saturating_sub(visible);
        if self.scroll_offset > max_scroll {
            self.scroll_offset = max_scroll;
        }
        if self.selected_index < self.scroll_offset {
            self.scroll_offset = self.selected_index;
        } else if self.selected_index >= self.scroll_offset + visible {
            self.scroll_offset = self.selected_index + 1 - visible;
        }
    }

    /// Get the currently selected match
    pub fn get_selected_match(&self) -> Option<&SearchMatch> {
        self.matches.get(self.selected_index)
    }

    /// Update matches with new results
    pub fn set_matches(&mut self, matches: Vec<SearchMatch>) {
        self.matches = matches;
        // Ensure selected index is valid
        if self.selected_index >= self.matches.len() {
            self.selected_index = self.matches.len().saturating_sub(1);
        }
        self.ensure_selected_visible();
    }

    /// Check if query is non-empty
    pub fn has_query(&self) -> bool {
        !self.query.trim().is_empty()
    }
}

/// Application state holding all tab-specific data and UI state
pub struct AppState {
    // Current navigation
    pub current_tab: Tab,
    pub selected_time_window: TimeWindow,

    // Modal state
    pub show_help: bool,
    pub show_task_detail: bool,
    pub task_detail: Option<TaskDetail>,
    pub task_detail_cached_lines: Vec<Line<'static>>,
    pub task_detail_scroll: u16,
    pub task_detail_scrollbar_area: Option<Rect>,
    pub task_detail_content_height: u16,
    pub task_detail_visible_height: u16,
    pub show_error_modal: bool,

    // Search state
    pub search: SearchState,

    // Toast notification state
    pub toast: Option<Toast>,

    // Search highlight - shows pointer next to found item
    pub search_highlight: Option<SearchHighlight>,

    // Dashboard data
    pub cluster_summary: Option<ClusterCapacitySummary>,
    pub task_status_dist: Vec<TaskStatusRow>,
    pub utilization_trend: Vec<ClusterUtilizationPoint>,
    pub overloaded_alerts: Vec<OverloadedWorkerAlert>,
    pub stale_claims_alerts: Vec<StaleClaimsAlert>,

    // Workers tab data
    pub worker_list: Vec<ActiveWorkerRow>,
    pub dead_workers: Vec<DeadWorkerRow>,
    pub selected_worker_id: Option<String>,
    pub selected_worker_index: Option<usize>,
    pub worker_uptime: HashMap<String, WorkerUptimeRow>,
    pub worker_queues: HashMap<String, WorkerQueuesRow>,
    pub worker_load_history: HashMap<String, Vec<WorkerLoadPoint>>,

    // Tasks tab data
    pub task_aggregation: Vec<AggregatedBreakdownRow>,
    pub selected_task_index: Option<usize>,
    pub expanded_worker_index: Option<usize>,    // Which worker row is expanded
    pub selected_task_id_index: Option<usize>,   // Which task ID is selected within expanded row
    pub task_status_filter: TaskStatusFilter,    // Multi-select status filter
    pub retried_only_filter: bool,              // Show only tasks with retry_count > 0

    // Task list drill-down (Layer 2)
    pub task_list_active: bool,
    pub task_list_worker_id: Option<String>,
    pub task_list_rows: Vec<TaskListRow>,
    pub task_list_selected: Option<usize>,

    // Distinct filter values (populated from DB)
    pub distinct_task_names: Vec<FilterValue>,
    pub distinct_queues: Vec<FilterValue>,
    pub distinct_errors: Vec<FilterValue>,

    // Selected filter values (empty = no filter / show all)
    pub selected_task_names: HashSet<String>,
    pub selected_queues: HashSet<String>,
    pub selected_errors: HashSet<String>,

    // Which sidebar section is focused and cursor position within it
    pub sidebar_section: SidebarSection,
    pub sidebar_cursor: usize,

    // Maintenance tab data
    pub snapshot_age_dist: Vec<SnapshotAgeBucket>,

    // Workflows tab data
    pub workflow_summary: Option<WorkflowSummary>,
    pub workflow_list: Vec<WorkflowRow>,
    pub selected_workflow_index: Option<usize>,
    pub show_workflow_detail: bool,
    pub workflow_detail: Option<WorkflowRow>,
    pub workflow_tasks: Vec<WorkflowTaskRow>,
    pub workflow_detail_cached_lines: Vec<Line<'static>>,
    pub workflow_detail_scroll: u16,
    pub workflow_detail_scrollbar_area: Option<Rect>,
    pub workflow_detail_content_height: u16,
    pub workflow_detail_visible_height: u16,
    pub workflow_status_filter: WorkflowStatusFilter,

    // Loading & error state per dataset
    pub loading: HashMap<DataSource, bool>,
    pub errors: HashMap<DataSource, String>,

    // NOTIFY listener state
    pub listener_state: ListenerState,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            current_tab: Tab::Dashboard,
            selected_time_window: TimeWindow::ThirtyMinutes,
            show_help: false,
            show_task_detail: false,
            task_detail: None,
            task_detail_cached_lines: vec![],
            task_detail_scroll: 0,
            task_detail_scrollbar_area: None,
            task_detail_content_height: 0,
            task_detail_visible_height: 0,
            show_error_modal: false,
            search: SearchState::new(),
            toast: None,
            search_highlight: None,
            cluster_summary: None,
            task_status_dist: vec![],
            utilization_trend: vec![],
            overloaded_alerts: vec![],
            stale_claims_alerts: vec![],
            worker_list: vec![],
            dead_workers: vec![],
            selected_worker_id: None,
            selected_worker_index: None,
            worker_uptime: HashMap::new(),
            worker_queues: HashMap::new(),
            worker_load_history: HashMap::new(),
            task_aggregation: vec![],
            selected_task_index: None,
            expanded_worker_index: None,
            selected_task_id_index: None,
            task_status_filter: TaskStatusFilter::default(),
            retried_only_filter: false,
            task_list_active: false,
            task_list_worker_id: None,
            task_list_rows: vec![],
            task_list_selected: None,
            distinct_task_names: vec![],
            distinct_queues: vec![],
            distinct_errors: vec![],
            selected_task_names: HashSet::new(),
            selected_queues: HashSet::new(),
            selected_errors: HashSet::new(),
            sidebar_section: SidebarSection::None,
            sidebar_cursor: 0,
            snapshot_age_dist: vec![],
            workflow_summary: None,
            workflow_list: vec![],
            selected_workflow_index: None,
            show_workflow_detail: false,
            workflow_detail: None,
            workflow_tasks: vec![],
            workflow_detail_cached_lines: vec![],
            workflow_detail_scroll: 0,
            workflow_detail_scrollbar_area: None,
            workflow_detail_content_height: 0,
            workflow_detail_visible_height: 0,
            workflow_status_filter: WorkflowStatusFilter::default(),
            loading: HashMap::new(),
            errors: HashMap::new(),
            listener_state: ListenerState::default(),
        }
    }

    /// Mark a dataset as loading
    pub fn set_loading(&mut self, source: DataSource, loading: bool) {
        if loading {
            self.loading.insert(source, true);
        } else {
            self.loading.remove(&source);
        }
    }

    /// Set error for a dataset
    pub fn set_error(&mut self, source: DataSource, error: String) {
        self.errors.insert(source, error);
        self.set_loading(source, false);
    }

    /// Navigate worker selection up
    pub fn select_worker_up(&mut self) {
        if self.worker_list.is_empty() {
            return;
        }

        self.selected_worker_index = match self.selected_worker_index {
            Some(idx) if idx > 0 => Some(idx - 1),
            Some(_) => Some(0),
            None => Some(0),
        };

        self.update_selected_worker_id();
    }

    /// Navigate worker selection down
    pub fn select_worker_down(&mut self) {
        if self.worker_list.is_empty() {
            return;
        }

        let max_idx = self.worker_list.len().saturating_sub(1);
        self.selected_worker_index = match self.selected_worker_index {
            Some(idx) if idx < max_idx => Some(idx + 1),
            Some(idx) => Some(idx),
            None => Some(0),
        };

        self.update_selected_worker_id();
    }

    /// Update selected_worker_id based on selected_worker_index
    fn update_selected_worker_id(&mut self) {
        self.selected_worker_id = self
            .selected_worker_index
            .and_then(|idx| self.worker_list.get(idx))
            .map(|worker| worker.worker_id.clone());
    }

    /// Move worker selection up by page_size rows
    pub fn select_worker_page_up(&mut self, page_size: usize) {
        if self.worker_list.is_empty() {
            return;
        }
        self.selected_worker_index = match self.selected_worker_index {
            Some(idx) => Some(idx.saturating_sub(page_size)),
            None => Some(0),
        };
        self.update_selected_worker_id();
    }

    /// Move worker selection down by page_size rows
    pub fn select_worker_page_down(&mut self, page_size: usize) {
        if self.worker_list.is_empty() {
            return;
        }
        let max_idx = self.worker_list.len().saturating_sub(1);
        self.selected_worker_index = match self.selected_worker_index {
            Some(idx) => Some((idx + page_size).min(max_idx)),
            None => Some(0),
        };
        self.update_selected_worker_id();
    }

    /// Jump worker selection to first row
    pub fn select_worker_home(&mut self) {
        if self.worker_list.is_empty() {
            return;
        }
        self.selected_worker_index = Some(0);
        self.update_selected_worker_id();
    }

    /// Jump worker selection to last row
    pub fn select_worker_end(&mut self) {
        if self.worker_list.is_empty() {
            return;
        }
        self.selected_worker_index = Some(self.worker_list.len().saturating_sub(1));
        self.update_selected_worker_id();
    }

    /// Update task aggregation data and keep selection in sync
    pub fn set_task_aggregation(&mut self, rows: Vec<AggregatedBreakdownRow>) {
        self.task_aggregation = rows;
        self.ensure_task_selection();
    }

    /// Ensure selected task index points to a non-total row, or pick the first non-total row
    pub fn ensure_task_selection(&mut self) {
        // Check if current selection is valid (non-TOTAL)
        if let Some(idx) = self.selected_task_index {
            if idx < self.task_aggregation.len()
                && self.task_aggregation[idx].worker_id != "TOTAL"
            {
                return;
            }
        }

        // Find first non-TOTAL row
        self.selected_task_index = self.first_non_total_index();
    }

    /// Navigate task selection up (skipping TOTAL)
    pub fn select_task_up(&mut self) {
        let current = match self.selected_task_index {
            Some(idx) => idx,
            None => {
                // No selection, pick first non-TOTAL
                self.selected_task_index = self.first_non_total_index();
                return;
            }
        };

        // Find previous non-TOTAL index
        let new_idx = (0..current)
            .rev()
            .find(|&i| self.task_aggregation.get(i).is_some_and(|r| r.worker_id != "TOTAL"))
            .unwrap_or(current); // Stay at current if no previous found

        self.selected_task_index = Some(new_idx);
    }

    /// Navigate task selection down (skipping TOTAL)
    pub fn select_task_down(&mut self) {
        let current = match self.selected_task_index {
            Some(idx) => idx,
            None => {
                // No selection, pick first non-TOTAL
                self.selected_task_index = self.first_non_total_index();
                return;
            }
        };

        // Find next non-TOTAL index
        let new_idx = ((current + 1)..self.task_aggregation.len())
            .find(|&i| self.task_aggregation.get(i).is_some_and(|r| r.worker_id != "TOTAL"))
            .unwrap_or(current); // Stay at current if no next found

        self.selected_task_index = Some(new_idx);
    }

    /// Find the first non-TOTAL index (no allocation)
    fn first_non_total_index(&self) -> Option<usize> {
        self.task_aggregation
            .iter()
            .position(|row| row.worker_id != "TOTAL")
    }

    /// Find the last non-TOTAL index (no allocation)
    fn last_non_total_index(&self) -> Option<usize> {
        self.task_aggregation
            .iter()
            .rposition(|row| row.worker_id != "TOTAL")
    }

    /// Jump selection to first non-TOTAL row
    pub fn select_task_home(&mut self) {
        self.selected_task_index = self.first_non_total_index();
    }

    /// Jump selection to last non-TOTAL row
    pub fn select_task_end(&mut self) {
        self.selected_task_index = self.last_non_total_index();
    }

    /// Move selection up by page_size rows (skipping TOTAL), clamping to first non-TOTAL
    pub fn select_task_page_up(&mut self, page_size: usize) {
        let current = match self.selected_task_index {
            Some(idx) => idx,
            None => {
                self.selected_task_index = self.first_non_total_index();
                return;
            }
        };

        // Walk backwards counting non-TOTAL rows
        let mut remaining = page_size;
        let mut target = current;
        for i in (0..current).rev() {
            if self.task_aggregation.get(i).is_some_and(|r| r.worker_id != "TOTAL") {
                target = i;
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }
        self.selected_task_index = Some(target);
    }

    /// Move selection down by page_size rows (skipping TOTAL), clamping to last non-TOTAL
    pub fn select_task_page_down(&mut self, page_size: usize) {
        let current = match self.selected_task_index {
            Some(idx) => idx,
            None => {
                self.selected_task_index = self.first_non_total_index();
                return;
            }
        };

        // Walk forwards counting non-TOTAL rows
        let mut remaining = page_size;
        let mut target = current;
        for i in (current + 1)..self.task_aggregation.len() {
            if self.task_aggregation.get(i).is_some_and(|r| r.worker_id != "TOTAL") {
                target = i;
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }
        self.selected_task_index = Some(target);
    }

    /// Move up within expanded task IDs by page_size
    pub fn select_task_id_page_up(&mut self, page_size: usize) {
        if self.expanded_worker_index.is_none() {
            return;
        }
        if let Some(idx) = self.selected_task_id_index {
            self.selected_task_id_index = Some(idx.saturating_sub(page_size));
        }
    }

    /// Move down within expanded task IDs by page_size
    pub fn select_task_id_page_down(&mut self, page_size: usize) {
        if self.expanded_worker_index.is_none() {
            return;
        }
        let count = self.expanded_task_count();
        let max_idx = count.saturating_sub(1);
        if let Some(idx) = self.selected_task_id_index {
            self.selected_task_id_index = Some((idx + page_size).min(max_idx));
        }
    }

    /// Check if a worker row is currently expanded
    pub fn is_row_expanded(&self, idx: usize) -> bool {
        self.expanded_worker_index == Some(idx)
    }

    /// Toggle expansion of the selected worker row
    pub fn toggle_expand_selected(&mut self) {
        let Some(idx) = self.selected_task_index else {
            return;
        };

        // Don't expand TOTAL row
        if let Some(row) = self.task_aggregation.get(idx) {
            if row.worker_id == "TOTAL" {
                return;
            }
        }

        if self.expanded_worker_index == Some(idx) {
            // Collapse
            self.expanded_worker_index = None;
            self.selected_task_id_index = None;
        } else {
            // Expand and select first task ID
            self.expanded_worker_index = Some(idx);
            self.selected_task_id_index = Some(0);
        }
    }

    /// Collapse the expanded row
    pub fn collapse_expanded(&mut self) {
        self.expanded_worker_index = None;
        self.selected_task_id_index = None;
    }

    /// Get the count of expanded task IDs without allocating
    fn expanded_task_count(&self) -> usize {
        let Some(idx) = self.expanded_worker_index else {
            return 0;
        };
        let Some(row) = self.task_aggregation.get(idx) else {
            return 0;
        };

        let claimed_count = row.claimed_task_ids.as_ref().map_or(0, |v| v.len());
        let running_count = row.running_task_ids.as_ref().map_or(0, |v| v.len());
        claimed_count + running_count
    }

    /// Get a task ID at a specific index without allocating a full Vec
    fn get_expanded_task_id_at(&self, target_idx: usize) -> Option<&str> {
        let row_idx = self.expanded_worker_index?;
        let row = self.task_aggregation.get(row_idx)?;

        let claimed_len = row.claimed_task_ids.as_ref().map_or(0, |v| v.len());

        if target_idx < claimed_len {
            row.claimed_task_ids.as_ref()?.get(target_idx).map(|s| s.as_str())
        } else {
            let running_idx = target_idx - claimed_len;
            row.running_task_ids.as_ref()?.get(running_idx).map(|s| s.as_str())
        }
    }

    /// Get the retry count for an expanded task ID at a specific index
    pub fn get_expanded_retry_count(&self, target_idx: usize) -> i32 {
        let Some(row_idx) = self.expanded_worker_index else {
            return 0;
        };
        let Some(row) = self.task_aggregation.get(row_idx) else {
            return 0;
        };

        let claimed_len = row.claimed_retry_counts.as_ref().map_or(0, |v| v.len());

        if target_idx < claimed_len {
            row.claimed_retry_counts
                .as_ref()
                .and_then(|v| v.get(target_idx).copied())
                .unwrap_or(0)
        } else {
            let running_idx = target_idx - claimed_len;
            row.running_retry_counts
                .as_ref()
                .and_then(|v| v.get(running_idx).copied())
                .unwrap_or(0)
        }
    }

    /// Get all task IDs for the expanded row (claimed + running)
    /// Used by render loop which needs owned strings
    pub fn get_expanded_task_ids(&self) -> Vec<String> {
        let Some(idx) = self.expanded_worker_index else {
            return vec![];
        };
        let Some(row) = self.task_aggregation.get(idx) else {
            return vec![];
        };

        let claimed_len = row.claimed_task_ids.as_ref().map_or(0, |v| v.len());
        let running_len = row.running_task_ids.as_ref().map_or(0, |v| v.len());
        let mut ids = Vec::with_capacity(claimed_len + running_len);

        if let Some(claimed) = &row.claimed_task_ids {
            ids.extend(claimed.iter().cloned());
        }
        if let Some(running) = &row.running_task_ids {
            ids.extend(running.iter().cloned());
        }
        ids
    }

    /// Get the currently selected task ID (when expanded)
    pub fn get_selected_task_id(&self) -> Option<String> {
        let idx = self.selected_task_id_index?;
        self.get_expanded_task_id_at(idx).map(|s| s.to_string())
    }

    /// Navigate up within expanded task IDs, or collapse if at top
    pub fn select_task_id_up(&mut self) -> bool {
        if self.expanded_worker_index.is_none() {
            return false;
        }

        match self.selected_task_id_index {
            Some(idx) if idx > 0 => {
                self.selected_task_id_index = Some(idx - 1);
                true
            }
            Some(0) => {
                // At top of task list, collapse and stay on same row
                self.collapse_expanded();
                false
            }
            _ => false,
        }
    }

    /// Navigate down within expanded task IDs
    pub fn select_task_id_down(&mut self) -> bool {
        if self.expanded_worker_index.is_none() {
            return false;
        }

        let count = self.expanded_task_count();
        let max_idx = count.saturating_sub(1);

        match self.selected_task_id_index {
            Some(idx) if idx < max_idx => {
                self.selected_task_id_index = Some(idx + 1);
                true
            }
            _ => false,
        }
    }

    // =========== Task list (Layer 2) methods ===========

    /// Enter drill-down view for a specific worker (or all workers if None)
    pub fn enter_task_list(&mut self, worker_id: Option<String>) {
        self.task_list_active = true;
        self.task_list_worker_id = worker_id;
        self.task_list_rows.clear();
        self.task_list_selected = None;
        self.distinct_task_names.clear();
        self.distinct_queues.clear();
        self.distinct_errors.clear();
        self.selected_task_names.clear();
        self.selected_queues.clear();
        self.selected_errors.clear();
        self.sidebar_section = SidebarSection::None;
        self.sidebar_cursor = 0;
        self.collapse_expanded();
    }

    /// Exit drill-down view back to aggregation
    pub fn exit_task_list(&mut self) {
        self.task_list_active = false;
        self.task_list_worker_id = None;
        self.task_list_rows.clear();
        self.task_list_selected = None;
        self.distinct_task_names.clear();
        self.distinct_queues.clear();
        self.distinct_errors.clear();
        self.selected_task_names.clear();
        self.selected_queues.clear();
        self.selected_errors.clear();
        self.sidebar_section = SidebarSection::None;
    }

    /// Update task list rows and keep selection valid
    pub fn set_task_list_rows(&mut self, rows: Vec<TaskListRow>) {
        self.task_list_rows = rows;
        self.ensure_task_list_selection();
    }

    /// Ensure task list selection is valid
    fn ensure_task_list_selection(&mut self) {
        if self.task_list_rows.is_empty() {
            self.task_list_selected = None;
            return;
        }
        match self.task_list_selected {
            Some(idx) if idx >= self.task_list_rows.len() => {
                self.task_list_selected = Some(self.task_list_rows.len() - 1);
            }
            None => {
                self.task_list_selected = Some(0);
            }
            _ => {}
        }
    }

    pub fn select_task_list_up(&mut self) {
        if self.task_list_rows.is_empty() {
            return;
        }
        self.task_list_selected = match self.task_list_selected {
            Some(idx) if idx > 0 => Some(idx - 1),
            Some(_) => Some(0),
            None => Some(0),
        };
    }

    pub fn select_task_list_down(&mut self) {
        if self.task_list_rows.is_empty() {
            return;
        }
        let max_idx = self.task_list_rows.len().saturating_sub(1);
        self.task_list_selected = match self.task_list_selected {
            Some(idx) if idx < max_idx => Some(idx + 1),
            Some(idx) => Some(idx),
            None => Some(0),
        };
    }

    pub fn select_task_list_page_up(&mut self, page_size: usize) {
        if self.task_list_rows.is_empty() {
            return;
        }
        self.task_list_selected = match self.task_list_selected {
            Some(idx) => Some(idx.saturating_sub(page_size)),
            None => Some(0),
        };
    }

    pub fn select_task_list_page_down(&mut self, page_size: usize) {
        if self.task_list_rows.is_empty() {
            return;
        }
        let max_idx = self.task_list_rows.len().saturating_sub(1);
        self.task_list_selected = match self.task_list_selected {
            Some(idx) => Some((idx + page_size).min(max_idx)),
            None => Some(0),
        };
    }

    pub fn select_task_list_home(&mut self) {
        if !self.task_list_rows.is_empty() {
            self.task_list_selected = Some(0);
        }
    }

    pub fn select_task_list_end(&mut self) {
        if !self.task_list_rows.is_empty() {
            self.task_list_selected = Some(self.task_list_rows.len().saturating_sub(1));
        }
    }

    /// Get selected task ID in the task list view
    pub fn get_selected_task_list_id(&self) -> Option<String> {
        self.task_list_selected
            .and_then(|idx| self.task_list_rows.get(idx))
            .map(|row| row.id.clone())
    }

    /// Toggle a value in the named selection set. Returns true if a change occurred.
    pub fn toggle_sidebar_filter(&mut self) -> bool {
        let (values, selected) = match self.sidebar_section {
            SidebarSection::TaskNames => (&self.distinct_task_names, &mut self.selected_task_names),
            SidebarSection::Queues => (&self.distinct_queues, &mut self.selected_queues),
            SidebarSection::Errors => (&self.distinct_errors, &mut self.selected_errors),
            SidebarSection::None => return false,
        };
        let Some(fv) = values.get(self.sidebar_cursor) else {
            return false;
        };
        if selected.contains(&fv.value) {
            selected.remove(&fv.value);
        } else {
            selected.insert(fv.value.clone());
        }
        true
    }

    /// Get the length of the current sidebar section's value list
    pub fn sidebar_section_len(&self) -> usize {
        match self.sidebar_section {
            SidebarSection::TaskNames => self.distinct_task_names.len(),
            SidebarSection::Queues => self.distinct_queues.len(),
            SidebarSection::Errors => self.distinct_errors.len(),
            SidebarSection::None => 0,
        }
    }

    /// Move sidebar cursor up within current section
    pub fn sidebar_cursor_up(&mut self) {
        if self.sidebar_cursor > 0 {
            self.sidebar_cursor -= 1;
        }
    }

    /// Move sidebar cursor down within current section
    pub fn sidebar_cursor_down(&mut self) {
        let max = self.sidebar_section_len().saturating_sub(1);
        if self.sidebar_cursor < max {
            self.sidebar_cursor += 1;
        }
    }

    /// Enter a sidebar section
    pub fn enter_sidebar_section(&mut self, section: SidebarSection) {
        self.sidebar_section = section;
        self.sidebar_cursor = 0;
    }

    /// Exit the current sidebar section
    pub fn exit_sidebar_section(&mut self) {
        self.sidebar_section = SidebarSection::None;
        self.sidebar_cursor = 0;
    }

    /// Check if any distinct-value filter is active
    pub fn has_task_list_filters(&self) -> bool {
        !self.selected_task_names.is_empty()
            || !self.selected_queues.is_empty()
            || !self.selected_errors.is_empty()
    }

    /// Get selected task names as Vec for SQL bind
    pub fn selected_task_names_sql(&self) -> Vec<String> {
        self.selected_task_names.iter().cloned().collect()
    }

    /// Get selected queues as Vec for SQL bind
    pub fn selected_queues_sql(&self) -> Vec<String> {
        self.selected_queues.iter().cloned().collect()
    }

    /// Get selected errors as Vec for SQL bind
    pub fn selected_errors_sql(&self) -> Vec<String> {
        self.selected_errors.iter().cloned().collect()
    }

    /// Update distinct filter values from DB
    pub fn set_distinct_values(
        &mut self,
        task_names: Vec<FilterValue>,
        queues: Vec<FilterValue>,
        errors: Vec<FilterValue>,
    ) {
        self.distinct_task_names = task_names;
        self.distinct_queues = queues;
        self.distinct_errors = errors;
    }

    // =========== Workflow navigation methods ===========

    /// Navigate workflow selection up
    pub fn select_workflow_up(&mut self) {
        if self.workflow_list.is_empty() {
            return;
        }

        self.selected_workflow_index = match self.selected_workflow_index {
            Some(idx) if idx > 0 => Some(idx - 1),
            Some(_) => Some(0),
            None => Some(0),
        };
    }

    /// Navigate workflow selection down
    pub fn select_workflow_down(&mut self) {
        if self.workflow_list.is_empty() {
            return;
        }

        let max_idx = self.workflow_list.len().saturating_sub(1);
        self.selected_workflow_index = match self.selected_workflow_index {
            Some(idx) if idx < max_idx => Some(idx + 1),
            Some(idx) => Some(idx),
            None => Some(0),
        };
    }

    /// Move workflow selection up by page_size rows
    pub fn select_workflow_page_up(&mut self, page_size: usize) {
        if self.workflow_list.is_empty() {
            return;
        }
        self.selected_workflow_index = match self.selected_workflow_index {
            Some(idx) => Some(idx.saturating_sub(page_size)),
            None => Some(0),
        };
    }

    /// Move workflow selection down by page_size rows
    pub fn select_workflow_page_down(&mut self, page_size: usize) {
        if self.workflow_list.is_empty() {
            return;
        }
        let max_idx = self.workflow_list.len().saturating_sub(1);
        self.selected_workflow_index = match self.selected_workflow_index {
            Some(idx) => Some((idx + page_size).min(max_idx)),
            None => Some(0),
        };
    }

    /// Jump workflow selection to first row
    pub fn select_workflow_home(&mut self) {
        if self.workflow_list.is_empty() {
            return;
        }
        self.selected_workflow_index = Some(0);
    }

    /// Jump workflow selection to last row
    pub fn select_workflow_end(&mut self) {
        if self.workflow_list.is_empty() {
            return;
        }
        self.selected_workflow_index = Some(self.workflow_list.len().saturating_sub(1));
    }

    /// Get the currently selected workflow
    pub fn get_selected_workflow(&self) -> Option<&WorkflowRow> {
        self.selected_workflow_index
            .and_then(|idx| self.workflow_list.get(idx))
    }

    /// Get the currently selected workflow ID
    pub fn get_selected_workflow_id(&self) -> Option<String> {
        self.get_selected_workflow().map(|w| w.id.clone())
    }

    /// Update workflow list and ensure selection stays valid
    pub fn set_workflow_list(&mut self, rows: Vec<WorkflowRow>) {
        self.workflow_list = rows;
        self.ensure_workflow_selection();
    }

    /// Ensure selected workflow index is valid
    pub fn ensure_workflow_selection(&mut self) {
        if self.workflow_list.is_empty() {
            self.selected_workflow_index = None;
            return;
        }

        match self.selected_workflow_index {
            Some(idx) if idx >= self.workflow_list.len() => {
                self.selected_workflow_index = Some(self.workflow_list.len() - 1);
            }
            None => {
                self.selected_workflow_index = Some(0);
            }
            _ => {}
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
