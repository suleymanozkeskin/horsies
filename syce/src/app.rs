use std::cell::Cell;

use crate::errors::{Result, SyceError};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::prelude::Rect;
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::{
    action::{Action, DataSource, DataUpdate, ListenerState, SearchMatch, Tab, TaskStatus, WorkflowStatus},
    listener::NotifyListenerHandle,
    state::{SearchHighlight, Toast, ToastIcon},
    tui::NotifyBatch,
    components::{
        dashboard::Dashboard, error_modal::ErrorModal, help::HelpOverlay, maintenance::Maintenance,
        search::SearchModal, status_bar::StatusBar, task_detail::TaskDetailPanel, tasks::Tasks,
        workers::Workers, workflow_detail::WorkflowDetailPanel, workflows::Workflows, Component,
    },
    db::queries,
    state::AppState,
    theme::{Theme, ThemeFlavor},
    tui::{Event, Tui},
};
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::style::Style;
use ratatui::widgets::{Block, BorderType, Borders, Clear, Paragraph};

/// Helper to create a centered popup rect
fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

/// Render a toast notification in the bottom-right corner
fn render_toast(frame: &mut ratatui::Frame, toast: &Toast, theme: &Theme) {
    let area = frame.area();

    // Calculate toast dimensions
    let msg_len = toast.message.chars().count() as u16;
    let toast_width = (msg_len + 6).min(40).max(20); // icon + padding + message
    let toast_height = 3;

    // Position in bottom-right corner with padding
    let toast_x = area.width.saturating_sub(toast_width + 2);
    let toast_y = area.height.saturating_sub(toast_height + 2);

    let toast_area = Rect::new(toast_x, toast_y, toast_width, toast_height);

    // Clear background
    frame.render_widget(Clear, toast_area);

    // Choose icon and color based on toast type
    let (icon, border_color) = match toast.icon {
        ToastIcon::Success => ("✓", theme.success),
        ToastIcon::Info => ("ℹ", theme.accent),
        ToastIcon::Warning => ("⚠", theme.warning),
        ToastIcon::Error => ("✗", theme.error),
    };

    // Fade effect based on remaining ticks
    let opacity = if toast.ticks_remaining <= 2 {
        theme.muted
    } else {
        theme.text
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(theme.surface));

    let content = format!(" {} {} ", icon, toast.message);
    let paragraph = Paragraph::new(content)
        .style(Style::default().fg(opacity).bg(theme.surface))
        .alignment(Alignment::Center)
        .block(block);

    frame.render_widget(paragraph, toast_area);
}

pub struct App {
    tick_rate: f64,
    frame_rate: f64,
    theme: Theme,
    should_quit: bool,
    should_suspend: bool,
    action_tx: mpsc::UnboundedSender<Action>,
    action_rx: mpsc::UnboundedReceiver<Action>,
    state: AppState,
    pool: Option<PgPool>,
    tick_counter: u64,
    listener_handle: Option<NotifyListenerHandle>,
}

impl App {
    pub async fn new(tick_rate: f64, frame_rate: f64, database_url: Option<String>) -> Result<Self> {
        let (action_tx, action_rx) = mpsc::unbounded_channel();

        // Initialize database pool if URL provided
        let pool = if let Some(url) = database_url {
            match PgPool::connect(&url).await {
                Ok(pool) => {
                    eprintln!("✓ Connected to database");
                    Some(pool)
                }
                Err(e) => {
                    eprintln!("✗ Failed to connect to database: {}", e);
                    eprintln!("  Continuing without database (demo mode)");
                    None
                }
            }
        } else {
            eprintln!("No database URL provided. Running in demo mode.");
            None
        };

        Ok(Self {
            tick_rate,
            frame_rate,
            theme: Theme::new(ThemeFlavor::Mocha),
            should_quit: false,
            should_suspend: false,
            action_tx,
            action_rx,
            state: AppState::new(),
            pool,
            tick_counter: 0,
            listener_handle: None,
        })
    }

    fn send_action(tx: &mpsc::UnboundedSender<Action>, action: Action) -> Result<()> {
        tx.send(action)
            .map_err(|err| SyceError::Terminal(format!("failed to send action: {err}")))
    }

    /// Create a lightweight clone for async data fetching
    fn clone_for_fetch(&self) -> FetchContext {
        FetchContext {
            pool: self.pool.clone(),
            action_tx: self.action_tx.clone(),
        }
    }

    fn start_listener(&mut self, event_tx: mpsc::Sender<Event>) {
        if self.listener_handle.is_none() {
            self.listener_handle = NotifyListenerHandle::spawn(self.pool.clone(), event_tx);
        }
    }

    fn stop_listener(&mut self) {
        if let Some(handle) = self.listener_handle.take() {
            handle.stop();
        }
        self.state.listener_state = ListenerState::Disconnected;
    }

    /// Handle NOTIFY-triggered refresh based on which channels fired.
    fn handle_notify_refresh(&self, batch: NotifyBatch) {
        if self.pool.is_none() {
            return;
        }

        match self.state.current_tab {
            Tab::Dashboard => {
                if batch.task_status || batch.workflow_status || batch.worker_state {
                    let ctx = self.clone_for_fetch();
                    tokio::spawn(async move {
                        ctx.fetch_dashboard_data(false).await;
                    });
                }
            }
            Tab::Workers => {
                if batch.worker_state {
                    let ctx = self.clone_for_fetch();
                    let worker_id = self.state.selected_worker_id.clone();
                    let time_interval = self.state.selected_time_window.interval().to_string();
                    tokio::spawn(async move {
                        ctx.fetch_workers_data(worker_id, &time_interval).await;
                    });
                }
            }
            Tab::Tasks => {
                if batch.task_status {
                    let ctx = self.clone_for_fetch();
                    let filter = self.state.task_status_filter.to_sql_values();
                    tokio::spawn(async move {
                        ctx.fetch_tasks_data(filter).await;
                    });
                }
            }
            Tab::Workflows => {
                if batch.workflow_status || batch.task_status {
                    let ctx = self.clone_for_fetch();
                    let filter = self.state.workflow_status_filter.to_sql_values();
                    tokio::spawn(async move {
                        ctx.fetch_workflows_data(filter).await;
                    });
                }
            }
            Tab::Maintenance => {
                // Maintenance tab doesn't need real-time updates
            }
        }
    }

    /// Get the first task ID from the selected row's claimed/running tasks
    fn get_first_selected_task_id(&self) -> Option<String> {
        let idx = self.state.selected_task_index?;
        let row = self.state.task_aggregation.get(idx)?;

        // Skip TOTAL row
        if row.worker_id == "TOTAL" {
            return None;
        }

        // Try claimed tasks first, then running
        if let Some(claimed_ids) = &row.claimed_task_ids {
            if let Some(first) = claimed_ids.first() {
                return Some(first.clone());
            }
        }
        if let Some(running_ids) = &row.running_task_ids {
            if let Some(first) = running_ids.first() {
                return Some(first.clone());
            }
        }
        None
    }

    /// Perform search based on current context (tab or modal)
    fn perform_search(&mut self) {
        let query = self.state.search.query.to_lowercase();

        // No query = no results
        if query.trim().is_empty() {
            self.state.search.set_matches(vec![]);
            return;
        }

        // Determine context and search accordingly
        let matches = if self.state.show_task_detail {
            // Search within task detail modal content
            self.search_task_detail_content(&query)
        } else if self.state.show_workflow_detail {
            // Search within workflow detail modal content
            self.search_workflow_detail_content(&query)
        } else {
            // Search within current tab data
            match self.state.current_tab {
                Tab::Workers => self.search_workers(&query),
                Tab::Tasks => self.search_tasks(&query),
                Tab::Workflows => self.search_workflows(&query),
                Tab::Dashboard => self.search_dashboard(&query),
                Tab::Maintenance => vec![], // No searchable content
            }
        };

        self.state.search.set_matches(matches);
    }

    /// Search workers by worker_id or hostname
    fn search_workers(&self, query: &str) -> Vec<SearchMatch> {
        self.state
            .worker_list
            .iter()
            .filter(|w| {
                w.worker_id.to_lowercase().contains(query)
                    || w.hostname.to_lowercase().contains(query)
            })
            .map(|w| {
                // Derive status from tasks_running
                let status = if w.tasks_running > 0 {
                    "Running".to_string()
                } else {
                    "Idle".to_string()
                };
                SearchMatch::Worker {
                    worker_id: w.worker_id.clone(),
                    hostname: w.hostname.clone(),
                    status,
                }
            })
            .collect()
    }

    /// Search tasks by task_id or worker_id
    fn search_tasks(&self, query: &str) -> Vec<SearchMatch> {
        let mut matches = Vec::new();

        for row in &self.state.task_aggregation {
            if row.worker_id == "TOTAL" {
                continue;
            }

            // Check if worker_id matches
            let worker_matches = row.worker_id.to_lowercase().contains(query);

            // Check claimed task IDs
            if let Some(claimed_ids) = &row.claimed_task_ids {
                for task_id in claimed_ids {
                    if task_id.to_lowercase().contains(query) || worker_matches {
                        matches.push(SearchMatch::Task {
                            task_id: task_id.clone(),
                            worker_id: row.worker_id.clone(),
                            status: "CLAIMED".to_string(),
                        });
                    }
                }
            }

            // Check running task IDs
            if let Some(running_ids) = &row.running_task_ids {
                for task_id in running_ids {
                    if task_id.to_lowercase().contains(query) || worker_matches {
                        matches.push(SearchMatch::Task {
                            task_id: task_id.clone(),
                            worker_id: row.worker_id.clone(),
                            status: "RUNNING".to_string(),
                        });
                    }
                }
            }
        }

        matches
    }

    /// Search workflows by workflow_id or name
    fn search_workflows(&self, query: &str) -> Vec<SearchMatch> {
        self.state
            .workflow_list
            .iter()
            .filter(|w| {
                w.id.to_lowercase().contains(query)
                    || w.name.to_lowercase().contains(query)
            })
            .map(|w| SearchMatch::Workflow {
                workflow_id: w.id.clone(),
                name: w.name.clone(),
                status: w.status.clone(),
            })
            .collect()
    }

    /// Search dashboard (workers from alerts)
    fn search_dashboard(&self, query: &str) -> Vec<SearchMatch> {
        let mut matches = Vec::new();

        // Search overloaded workers
        for alert in &self.state.overloaded_alerts {
            if alert.worker_id.to_lowercase().contains(query)
                || alert.hostname.to_lowercase().contains(query)
            {
                matches.push(SearchMatch::Worker {
                    worker_id: alert.worker_id.clone(),
                    hostname: alert.hostname.clone(),
                    status: "OVERLOADED".to_string(),
                });
            }
        }

        matches
    }

    /// Search within task detail modal content
    /// Returns line numbers that match the actual rendered content structure
    fn search_task_detail_content(&self, query: &str) -> Vec<SearchMatch> {
        let Some(task) = &self.state.task_detail else {
            return vec![];
        };

        let panel = TaskDetailPanel::new(task, 0, None);
        panel
            .build_search_lines(&self.theme)
            .into_iter()
            .enumerate()
            .filter_map(|(line_number, line)| {
                if line.search_text.to_lowercase().contains(query) {
                    Some(SearchMatch::ModalLine {
                        line_number,
                        content: line.display,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Search within workflow detail modal content
    /// Returns line numbers that match the actual rendered content structure
    fn search_workflow_detail_content(&self, query: &str) -> Vec<SearchMatch> {
        let Some(workflow) = &self.state.workflow_detail else {
            return vec![];
        };

        let panel = WorkflowDetailPanel::new(workflow, &self.state.workflow_tasks, 0, None);
        panel
            .build_search_lines(&self.theme)
            .into_iter()
            .enumerate()
            .filter_map(|(line_number, line)| {
                if line.search_text.to_lowercase().contains(query) {
                    Some(SearchMatch::ModalLine {
                        line_number,
                        content: line.display,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Handle search confirmation (Enter key)
    fn handle_search_confirm(&mut self) -> Result<()> {
        let Some(selected_match) = self.state.search.get_selected_match().cloned() else {
            return Ok(());
        };

        // Close search first
        self.state.search.close();

        match selected_match {
            SearchMatch::Worker { worker_id, .. } => {
                // Set highlight for this worker
                self.state.search_highlight = Some(SearchHighlight::new(worker_id.clone()));

                // Navigate to worker in Workers tab or select in current tab
                if self.state.current_tab == Tab::Workers {
                    // Find and select the worker
                    if let Some(idx) = self
                        .state
                        .worker_list
                        .iter()
                        .position(|w| w.worker_id == worker_id)
                    {
                        self.state.selected_worker_index = Some(idx);
                        self.state.selected_worker_id = Some(worker_id.clone());

                        // Fetch worker details
                        if self.pool.is_some() {
                            let fetch_ctx = self.clone_for_fetch();
                            let time_interval =
                                self.state.selected_time_window.interval().to_string();
                            tokio::spawn(async move {
                                fetch_ctx
                                    .fetch_workers_data(Some(worker_id), &time_interval)
                                    .await;
                            });
                        }
                    }
                } else if self.state.current_tab == Tab::Dashboard {
                    // Switch to Workers tab and select worker
                    Self::send_action(&self.action_tx, Action::SwitchTab(Tab::Workers))?;
                    self.state.selected_worker_id = Some(worker_id);
                }
            }
            SearchMatch::Task {
                task_id,
                worker_id,
                ..
            } => {
                // Set highlight for this task
                self.state.search_highlight = Some(SearchHighlight::new(task_id.clone()));

                // Find the worker row and expand it, select the task
                if let Some(row_idx) = self
                    .state
                    .task_aggregation
                    .iter()
                    .position(|r| r.worker_id == worker_id)
                {
                    self.state.selected_task_index = Some(row_idx);
                    self.state.expanded_worker_index = Some(row_idx);

                    // Find task ID index within expanded row
                    let task_ids = self.state.get_expanded_task_ids();
                    if let Some(task_idx) = task_ids.iter().position(|id| id == &task_id) {
                        self.state.selected_task_id_index = Some(task_idx);

                        // Open task detail
                        Self::send_action(&self.action_tx, Action::OpenTaskDetail(task_id))?;
                    }
                }
            }
            SearchMatch::Workflow { workflow_id, .. } => {
                // Set highlight for this workflow
                self.state.search_highlight = Some(SearchHighlight::new(workflow_id.clone()));

                // Find and select the workflow
                if let Some(idx) = self
                    .state
                    .workflow_list
                    .iter()
                    .position(|w| w.id == workflow_id)
                {
                    self.state.selected_workflow_index = Some(idx);

                    // Open workflow detail
                    Self::send_action(&self.action_tx, Action::OpenWorkflowDetail(workflow_id))?;
                }
            }
            SearchMatch::ModalLine { line_number, .. } => {
                // Set highlight for this line
                self.state.search_highlight = Some(SearchHighlight::new(format!("line:{}", line_number)));

                // Scroll to the matched line in the modal
                // line_number is already the correct scroll offset
                if self.state.show_task_detail {
                    self.state.task_detail_scroll = line_number as u16;
                } else if self.state.show_workflow_detail {
                    self.state.workflow_detail_scroll = line_number as u16;
                }
            }
        }

        Ok(())
    }
}

/// Minimal context needed for async data fetching
struct FetchContext {
    pool: Option<PgPool>,
    action_tx: mpsc::UnboundedSender<Action>,
}

impl FetchContext {
    /// Fetch all dashboard data from the database
    async fn fetch_dashboard_data(&self, show_loading: bool) {
        let Some(pool) = &self.pool else {
            return;
        };

        let action_tx = &self.action_tx;

        // Fetch cluster summary
        if show_loading {
            App::send_action(action_tx, Action::StartLoading(DataSource::ClusterSummary)).ok();
        }
        match queries::fetch_cluster_capacity_summary(pool).await {
            Ok(summary) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::ClusterSummary(summary)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::ClusterSummary),
                )
                .ok();
            }
        }

        // Fetch workflow summary for dashboard
        if show_loading {
            App::send_action(action_tx, Action::StartLoading(DataSource::WorkflowSummary)).ok();
        }
        match queries::fetch_workflow_summary(pool).await {
            Ok(summary) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::WorkflowSummary(summary)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::WorkflowSummary),
                )
                .ok();
            }
        }

        // Fetch task status view
        if show_loading {
            App::send_action(action_tx, Action::StartLoading(DataSource::TaskStatus)).ok();
        }
        match queries::fetch_task_status_view(pool).await {
            Ok(rows) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::TaskStatusView(rows)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::TaskStatus),
                )
                .ok();
            }
        }

        // Fetch utilization trend
        if show_loading {
            App::send_action(action_tx, Action::StartLoading(DataSource::UtilizationTrend)).ok();
        }
        match queries::fetch_cluster_utilization_trend(pool).await {
            Ok(points) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::UtilizationTrend(points)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::UtilizationTrend),
                )
                .ok();
            }
        }

        // Fetch alerts (overloaded + stale claims)
        if show_loading {
            App::send_action(action_tx, Action::StartLoading(DataSource::Alerts)).ok();
        }
        match tokio::try_join!(
            queries::fetch_overloaded_workers(pool),
            queries::fetch_stale_claims(pool)
        ) {
            Ok((overloaded, stale)) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::Alerts(overloaded, stale)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::Alerts),
                )
                .ok();
            }
        }
    }

    /// Fetch all workers tab data from the database
    async fn fetch_workers_data(&self, selected_worker_id: Option<String>, time_window_interval: &str) {
        let Some(pool) = &self.pool else {
            return;
        };

        let action_tx = &self.action_tx;

        // Fetch active workers list
        App::send_action(action_tx, Action::StartLoading(DataSource::WorkerList)).ok();
        match queries::fetch_active_workers_list(pool).await {
            Ok(workers) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::WorkerList(workers)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::WorkerList),
                )
                .ok();
            }
        }

        // Fetch dead workers
        App::send_action(action_tx, Action::StartLoading(DataSource::DeadWorkers)).ok();
        match queries::fetch_dead_workers(pool).await {
            Ok(workers) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::DeadWorkers(workers)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::DeadWorkers),
                )
                .ok();
            }
        }

        // If a worker is selected, fetch its details
        if let Some(worker_id) = selected_worker_id {
            App::send_action(action_tx, Action::StartLoading(DataSource::WorkerDetails)).ok();

            // Fetch worker uptime, queues, and load history in parallel
            match tokio::try_join!(
                queries::fetch_worker_uptime(pool),
                queries::fetch_worker_queues(pool),
                queries::fetch_worker_load(pool, time_window_interval)
            ) {
                Ok((uptime_rows, queues_rows, load_points)) => {
                    // Find the data for the selected worker
                    if let Some(uptime) = uptime_rows.iter().find(|u| u.worker_id == worker_id) {
                        if let Some(queues) = queues_rows.iter().find(|q| q.worker_id == worker_id) {
                            App::send_action(
                                action_tx,
                                Action::DataLoaded(DataUpdate::WorkerDetails(
                                    worker_id.clone(),
                                    uptime.clone(),
                                    queues.clone(),
                                )),
                            )
                            .ok();
                        }
                    }

                    // Send load history for the selected worker
                    let worker_load: Vec<_> = load_points
                        .into_iter()
                        .filter(|p| p.worker_id == worker_id)
                        .collect();

                    if !worker_load.is_empty() {
                        App::send_action(
                            action_tx,
                            Action::DataLoaded(DataUpdate::WorkerLoad(worker_load)),
                        )
                        .ok();
                    }
                }
                Err(e) => {
                    App::send_action(
                        action_tx,
                        Action::DataLoadError(format!("{}", e), DataSource::WorkerDetails),
                    )
                    .ok();
                }
            }
        }
    }

    /// Fetch tasks tab data (aggregated breakdown) with optional status filter
    async fn fetch_tasks_data(&self, status_filter: Vec<&'static str>) {
        let Some(pool) = &self.pool else {
            return;
        };

        let action_tx = &self.action_tx;

        // Fetch task aggregation with filter
        App::send_action(action_tx, Action::StartLoading(DataSource::TaskAggregation)).ok();

        let result = if status_filter.is_empty() {
            // No filter = fetch all
            queries::fetch_task_aggregation(pool).await
        } else {
            queries::fetch_filtered_task_aggregation(pool, &status_filter).await
        };

        match result {
            Ok(rows) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::TaskAggregation(rows)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::TaskAggregation),
                )
                .ok();
            }
        }
    }

    /// Fetch maintenance tab data (snapshot age distribution)
    async fn fetch_maintenance_data(&self) {
        let Some(pool) = &self.pool else {
            return;
        };

        let action_tx = &self.action_tx;

        // Fetch snapshot age distribution
        App::send_action(action_tx, Action::StartLoading(DataSource::SnapshotAge)).ok();
        match queries::fetch_snapshot_age_distribution(pool).await {
            Ok(buckets) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::SnapshotAge(buckets)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::SnapshotAge),
                )
                .ok();
            }
        }
    }

    /// Fetch a single task by ID for the detail view
    async fn fetch_task_detail(&self, task_id: String) {
        let Some(pool) = &self.pool else {
            return;
        };

        let action_tx = &self.action_tx;

        App::send_action(action_tx, Action::StartLoading(DataSource::TaskDetailData)).ok();
        match queries::fetch_task_by_id(pool, &task_id).await {
            Ok(Some(task)) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::TaskDetailLoaded(task)),
                )
                .ok();
            }
            Ok(None) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(
                        format!("Task {} not found", task_id),
                        DataSource::TaskDetailData,
                    ),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::TaskDetailData),
                )
                .ok();
            }
        }
    }

    /// Fetch workflows tab data with optional status filter
    async fn fetch_workflows_data(&self, status_filter: Vec<&'static str>) {
        let Some(pool) = &self.pool else {
            return;
        };

        let action_tx = &self.action_tx;

        // Fetch workflow list with filter
        App::send_action(action_tx, Action::StartLoading(DataSource::WorkflowList)).ok();

        let result = if status_filter.is_empty() {
            queries::fetch_workflow_list(pool).await
        } else {
            queries::fetch_filtered_workflow_list(pool, &status_filter).await
        };

        match result {
            Ok(rows) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::WorkflowList(rows)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::WorkflowList),
                )
                .ok();
            }
        }
    }

    /// Fetch workflow summary for dashboard
    async fn fetch_workflow_summary(&self) {
        let Some(pool) = &self.pool else {
            return;
        };

        let action_tx = &self.action_tx;

        App::send_action(action_tx, Action::StartLoading(DataSource::WorkflowSummary)).ok();
        match queries::fetch_workflow_summary(pool).await {
            Ok(summary) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::WorkflowSummary(summary)),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::WorkflowSummary),
                )
                .ok();
            }
        }
    }

    /// Fetch workflow detail (workflow + tasks)
    async fn fetch_workflow_detail(&self, workflow_id: String) {
        let Some(pool) = &self.pool else {
            return;
        };

        let action_tx = &self.action_tx;

        App::send_action(action_tx, Action::StartLoading(DataSource::WorkflowDetail)).ok();

        // Fetch workflow and tasks in parallel
        match tokio::try_join!(
            queries::fetch_workflow_by_id(pool, &workflow_id),
            queries::fetch_workflow_tasks(pool, &workflow_id)
        ) {
            Ok((Some(workflow), tasks)) => {
                App::send_action(
                    action_tx,
                    Action::DataLoaded(DataUpdate::WorkflowDetailLoaded(workflow, tasks)),
                )
                .ok();
            }
            Ok((None, _)) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(
                        format!("Workflow {} not found", workflow_id),
                        DataSource::WorkflowDetail,
                    ),
                )
                .ok();
            }
            Err(e) => {
                App::send_action(
                    action_tx,
                    Action::DataLoadError(format!("{}", e), DataSource::WorkflowDetail),
                )
                .ok();
            }
        }
    }
}

impl App {

    pub async fn run(&mut self) -> Result<()> {
        let mut tui = Tui::new()?
            .mouse(true) // uncomment this line to enable mouse support
            .tick_rate(self.tick_rate)
            .frame_rate(self.frame_rate);
        tui.enter()?;

        // Spawn NOTIFY listener for real-time updates
        self.start_listener(tui.event_tx.clone());

        // Trigger initial dashboard data fetch
        if self.pool.is_some() {
            let fetch_ctx = self.clone_for_fetch();
            tokio::spawn(async move {
                fetch_ctx.fetch_dashboard_data(true).await; // Show loading on initial fetch
            });
        }

        let action_tx = self.action_tx.clone();
        loop {
            self.handle_events(&mut tui).await?;
            self.handle_actions(&mut tui)?;
            if self.should_suspend {
                self.stop_listener();
                tui.suspend()?;
                Self::send_action(&action_tx, Action::Resume)?;
                Self::send_action(&action_tx, Action::ClearScreen)?;
                // tui.mouse(true);
                tui.enter()?;
            } else if self.should_quit {
                tui.stop()?;
                break;
            }
        }
        self.stop_listener();
        tui.exit()?;
        Ok(())
    }

    async fn handle_events(&mut self, tui: &mut Tui) -> Result<()> {
        let Some(event) = tui.next_event().await else {
            return Ok(());
        };
        let action_tx = self.action_tx.clone();
        match event {
            Event::Quit => Self::send_action(&action_tx, Action::Quit)?,
            Event::Tick => Self::send_action(&action_tx, Action::Tick)?,
            Event::Render => Self::send_action(&action_tx, Action::Render)?,
            Event::Resize(x, y) => Self::send_action(&action_tx, Action::Resize(x, y))?,
            Event::Key(key) => self.handle_key_event(key)?,
            Event::DbNotify(batch) => Self::send_action(&action_tx, Action::NotifyRefresh(batch))?,
            Event::ListenerStateChanged(state) => Self::send_action(&action_tx, Action::ListenerStateChanged(state))?,
            _ => {}
        }
        Ok(())
    }

    fn handle_key_event(&mut self, key: KeyEvent) -> Result<()> {
        let action_tx = self.action_tx.clone();

        // If search is active, handle search-specific keys first
        if self.state.search.active {
            let action = match key.code {
                KeyCode::Esc => Some(Action::CloseSearch),
                KeyCode::Enter => Some(Action::SearchConfirm),
                KeyCode::Up => Some(Action::SearchSelectUp),
                KeyCode::Down => Some(Action::SearchSelectDown),
                KeyCode::Backspace => Some(Action::SearchBackspace),
                KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                    Some(Action::SearchInput(c))
                }
                // Ctrl+U to clear input
                KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    Some(Action::SearchClear)
                }
                _ => None,
            };
            if let Some(action) = action {
                Self::send_action(&action_tx, action)?;
            }
            return Ok(());
        }

        let action = match (key.code, key.modifiers) {
            // Global keybindings
            (KeyCode::Char('/'), _) => Some(Action::OpenSearch),
            (KeyCode::Char('?'), _) => Some(Action::ToggleHelp),
            (KeyCode::Char('q'), _) => Some(Action::Quit),
            (KeyCode::Esc, _) => {
                // Esc closes modals/expanded rows first, then quits
                if self.state.show_error_modal {
                    Some(Action::CloseErrorModal)
                } else if self.state.show_workflow_detail {
                    Some(Action::CloseWorkflowDetail)
                } else if self.state.show_task_detail {
                    Some(Action::CloseTaskDetail)
                } else if self.state.show_help {
                    Some(Action::ToggleHelp)
                } else if self.state.expanded_worker_index.is_some() {
                    self.state.collapse_expanded();
                    None // Just collapse, no action needed
                } else {
                    Some(Action::Quit)
                }
            }
            (KeyCode::Char('c'), modifiers) if modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::Quit)
            }
            (KeyCode::Char('z'), modifiers) if modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::Suspend)
            }
            (KeyCode::Char('t'), _) => Some(Action::NextTheme),

            // Tab switching (1-5 keys)
            (KeyCode::Char('1'), _) => Some(Action::SwitchTab(Tab::Dashboard)),
            (KeyCode::Char('2'), _) => Some(Action::SwitchTab(Tab::Workers)),
            (KeyCode::Char('3'), _) => Some(Action::SwitchTab(Tab::Tasks)),
            (KeyCode::Char('4'), _) => Some(Action::SwitchTab(Tab::Workflows)),
            (KeyCode::Char('5'), _) => Some(Action::SwitchTab(Tab::Maintenance)),

            // Manual refresh (but not on Workflows or Tasks tabs where 'r' toggles Running filter)
            (KeyCode::Char('r'), _) if self.state.current_tab != Tab::Workflows && self.state.current_tab != Tab::Tasks => Some(Action::RefreshCurrentTab),

            // Test error display (for development)
            #[cfg(debug_assertions)]
            (KeyCode::Char('e'), modifiers) if modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::DataLoadError(
                "Test error message from cluster summary".to_string(),
                DataSource::ClusterSummary,
            ))
            }

            // Test loading state (for development)
            #[cfg(debug_assertions)]
            (KeyCode::Char('l'), _) => Some(Action::StartLoading(DataSource::WorkerList)),

            // Error modal - 'e' to open when errors exist
            (KeyCode::Char('e'), _) if !self.state.errors.is_empty() && !self.state.show_error_modal => {
                Some(Action::OpenErrorModal)
            }
            // Error modal controls when open
            (KeyCode::Char('y'), _) if self.state.show_error_modal => {
                Some(Action::CopyErrorToClipboard)
            }
            (KeyCode::Char('c'), _) if self.state.show_error_modal => {
                Some(Action::ClearAllErrors)
            }

            // Worker navigation (only in Workers tab)
            (KeyCode::Up, _) if self.state.current_tab == Tab::Workers => {
                Some(Action::NavigateWorkerUp)
            }
            (KeyCode::Down, _) if self.state.current_tab == Tab::Workers => {
                Some(Action::NavigateWorkerDown)
            }

            // Time window navigation (only in Workers tab)
            (KeyCode::Char('['), _) if self.state.current_tab == Tab::Workers => {
                Some(Action::CycleTimeWindowBackward)
            }
            (KeyCode::Char(']'), _) if self.state.current_tab == Tab::Workers => {
                Some(Action::CycleTimeWindowForward)
            }

            // Task status filter keys (only in Tasks tab, not in modals)
            (KeyCode::Char('p'), _) if self.state.current_tab == Tab::Tasks && !self.state.show_task_detail => {
                Some(Action::ToggleTaskStatusFilter(TaskStatus::Pending))
            }
            (KeyCode::Char('c'), _) if self.state.current_tab == Tab::Tasks && !self.state.show_task_detail => {
                Some(Action::ToggleTaskStatusFilter(TaskStatus::Claimed))
            }
            (KeyCode::Char('r'), _) if self.state.current_tab == Tab::Tasks && !self.state.show_task_detail => {
                Some(Action::ToggleTaskStatusFilter(TaskStatus::Running))
            }
            (KeyCode::Char('o'), _) if self.state.current_tab == Tab::Tasks && !self.state.show_task_detail => {
                Some(Action::ToggleTaskStatusFilter(TaskStatus::Completed))
            }
            (KeyCode::Char('f'), _) if self.state.current_tab == Tab::Tasks && !self.state.show_task_detail => {
                Some(Action::ToggleTaskStatusFilter(TaskStatus::Failed))
            }
            (KeyCode::Char('a'), _) if self.state.current_tab == Tab::Tasks && !self.state.show_task_detail => {
                Some(Action::SelectAllTaskStatuses)
            }
            (KeyCode::Char('n'), _) if self.state.current_tab == Tab::Tasks && !self.state.show_task_detail => {
                Some(Action::ClearTaskStatuses)
            }

            // Task navigation (Tasks tab) - handle expanded state
            (KeyCode::Up, _) if self.state.current_tab == Tab::Tasks && !self.state.show_task_detail => {
                if self.state.expanded_worker_index.is_some() {
                    Some(Action::NavigateTaskIdUp)
                } else {
                    Some(Action::NavigateTaskUp)
                }
            }
            (KeyCode::Down, _) if self.state.current_tab == Tab::Tasks && !self.state.show_task_detail => {
                if self.state.expanded_worker_index.is_some() {
                    Some(Action::NavigateTaskIdDown)
                } else {
                    Some(Action::NavigateTaskDown)
                }
            }

            // Enter in Tasks tab - expand row or open task detail
            (KeyCode::Enter, _) if self.state.current_tab == Tab::Tasks && !self.state.show_task_detail => {
                if self.state.expanded_worker_index.is_some() {
                    // Already expanded - open task detail for selected task ID
                    self.state.get_selected_task_id().map(Action::OpenTaskDetail)
                } else {
                    // Not expanded - expand the row
                    Some(Action::ToggleTaskRow)
                }
            }

            // Task detail scroll (when task detail modal is open)
            (KeyCode::Up, _) if self.state.show_task_detail => {
                Some(Action::ScrollTaskDetailUp)
            }
            (KeyCode::Down, _) if self.state.show_task_detail => {
                Some(Action::ScrollTaskDetailDown)
            }
            // Copy task to clipboard (y = yank)
            (KeyCode::Char('y'), _) if self.state.show_task_detail => {
                Some(Action::CopyTaskToClipboard)
            }
            // Navigate to next/prev task while modal is open
            (KeyCode::Char(']'), _) if self.state.show_task_detail => {
                Some(Action::NavigateTaskDetailNext)
            }
            (KeyCode::Char('['), _) if self.state.show_task_detail => {
                Some(Action::NavigateTaskDetailPrev)
            }

            // Workflow detail scroll (when workflow detail modal is open)
            (KeyCode::Up, _) if self.state.show_workflow_detail => {
                Some(Action::ScrollWorkflowDetailUp)
            }
            (KeyCode::Down, _) if self.state.show_workflow_detail => {
                Some(Action::ScrollWorkflowDetailDown)
            }
            // Copy workflow to clipboard (y = yank)
            (KeyCode::Char('y'), _) if self.state.show_workflow_detail => {
                Some(Action::CopyWorkflowToClipboard)
            }
            // Navigate to next/prev workflow while modal is open
            (KeyCode::Char(']'), _) if self.state.show_workflow_detail => {
                Some(Action::NavigateWorkflowDetailNext)
            }
            (KeyCode::Char('['), _) if self.state.show_workflow_detail => {
                Some(Action::NavigateWorkflowDetailPrev)
            }

            // Workflow status filter keys (only in Workflows tab, not in modals)
            (KeyCode::Char('p'), _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                Some(Action::ToggleWorkflowStatusFilter(WorkflowStatus::Pending))
            }
            (KeyCode::Char('r'), _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                Some(Action::ToggleWorkflowStatusFilter(WorkflowStatus::Running))
            }
            (KeyCode::Char('o'), _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                Some(Action::ToggleWorkflowStatusFilter(WorkflowStatus::Completed))
            }
            (KeyCode::Char('f'), _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                Some(Action::ToggleWorkflowStatusFilter(WorkflowStatus::Failed))
            }
            (KeyCode::Char('u'), _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                Some(Action::ToggleWorkflowStatusFilter(WorkflowStatus::Paused))
            }
            (KeyCode::Char('x'), _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                Some(Action::ToggleWorkflowStatusFilter(WorkflowStatus::Cancelled))
            }
            (KeyCode::Char('a'), _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                Some(Action::SelectAllWorkflowStatuses)
            }
            (KeyCode::Char('n'), _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                Some(Action::ClearWorkflowStatuses)
            }

            // Workflow navigation (Workflows tab)
            (KeyCode::Up, _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                Some(Action::NavigateWorkflowUp)
            }
            (KeyCode::Down, _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                Some(Action::NavigateWorkflowDown)
            }

            // Enter in Workflows tab - open workflow detail
            (KeyCode::Enter, _) if self.state.current_tab == Tab::Workflows && !self.state.show_workflow_detail => {
                self.state.get_selected_workflow_id().map(Action::OpenWorkflowDetail)
            }

            _ => None,
        };

        if let Some(action) = action {
            Self::send_action(&action_tx, action)?;
        }

        Ok(())
    }

    fn handle_actions(&mut self, tui: &mut Tui) -> Result<()> {
        while let Ok(action) = self.action_rx.try_recv() {
            match action {
                Action::Tick => {
                    self.tick_counter += 1;

                    // Handle toast timeout
                    if let Some(toast) = &mut self.state.toast {
                        if toast.tick() {
                            self.state.toast = None;
                        }
                    }

                    // Handle search highlight timeout
                    if let Some(highlight) = &mut self.state.search_highlight {
                        if highlight.tick() {
                            self.state.search_highlight = None;
                        }
                    }

                    // Polling intervals: slower when listener connected (real-time updates),
                    // faster when disconnected (fallback polling).
                    // At 4 ticks/sec: 8 ticks = 2s, 32 ticks = 8s, 60 ticks = 15s, 240 ticks = 60s
                    let listener_connected = self.state.listener_state == ListenerState::Connected;
                    let dashboard_interval = if listener_connected { 60 } else { 8 };
                    let workers_interval = if listener_connected { 60 } else { 12 };
                    let tasks_interval = if listener_connected { 60 } else { 20 };
                    let workflows_interval = if listener_connected { 60 } else { 20 };
                    let maintenance_interval: u64 = 120; // Maintenance always slow

                    // Auto-refresh dashboard
                    if self.state.current_tab == Tab::Dashboard
                        && self.pool.is_some()
                        && self.tick_counter % dashboard_interval == 0
                    {
                        let app_clone = self.clone_for_fetch();
                        tokio::spawn(async move {
                            app_clone.fetch_dashboard_data(false).await;
                        });
                    }

                    // Auto-refresh workers
                    if self.state.current_tab == Tab::Workers
                        && self.pool.is_some()
                        && self.tick_counter % workers_interval == 0
                    {
                        let app_clone = self.clone_for_fetch();
                        let selected_worker = self.state.selected_worker_id.clone();
                        let time_interval = self.state.selected_time_window.interval().to_string();
                        tokio::spawn(async move {
                            app_clone.fetch_workers_data(selected_worker, &time_interval).await;
                        });
                    }

                    // Auto-refresh tasks
                    if self.state.current_tab == Tab::Tasks
                        && self.pool.is_some()
                        && self.tick_counter % tasks_interval == 0
                    {
                        let app_clone = self.clone_for_fetch();
                        let filter = self.state.task_status_filter.to_sql_values();
                        tokio::spawn(async move {
                            app_clone.fetch_tasks_data(filter).await;
                        });
                    }

                    // Auto-refresh maintenance (always slow, data doesn't change often)
                    if self.state.current_tab == Tab::Maintenance
                        && self.pool.is_some()
                        && self.tick_counter % maintenance_interval == 0
                    {
                        let app_clone = self.clone_for_fetch();
                        tokio::spawn(async move {
                            app_clone.fetch_maintenance_data().await;
                        });
                    }

                    // Auto-refresh workflows
                    if self.state.current_tab == Tab::Workflows
                        && self.pool.is_some()
                        && self.tick_counter % workflows_interval == 0
                    {
                        let app_clone = self.clone_for_fetch();
                        let filter = self.state.workflow_status_filter.to_sql_values();
                        tokio::spawn(async move {
                            app_clone.fetch_workflows_data(filter).await;
                        });
                    }
                }
                Action::Quit => {
                    self.stop_listener();
                    self.should_quit = true;
                }
                Action::Suspend => {
                    self.stop_listener();
                    self.should_suspend = true;
                }
                Action::Resume => {
                    self.should_suspend = false;
                    self.start_listener(tui.event_tx.clone());
                }
                Action::ClearScreen => tui.terminal.clear()?,
                Action::Resize(w, h) => self.handle_resize(tui, w, h)?,
                Action::Render => self.render(tui)?,
                Action::NextTheme => self.theme = self.theme.next(),
                Action::ToggleHelp => {
                    self.state.show_help = !self.state.show_help;
                }
                Action::Error(msg) => {
                    // Store generic errors under a special key
                    // For now, just log them or display in UI as general errors
                    // Components can use DataLoadError for specific data sources
                    eprintln!("Error: {}", msg);
                }

                // Tab switching
                Action::SwitchTab(tab) => {
                    self.state.current_tab = tab;

                    // Trigger initial data fetch when switching tabs
                    if self.pool.is_some() {
                        match self.state.current_tab {
                            Tab::Dashboard => {
                                let fetch_ctx = self.clone_for_fetch();
                                tokio::spawn(async move {
                                    fetch_ctx.fetch_dashboard_data(true).await; // Show loading on manual tab switch
                                });
                            }
                            Tab::Workers => {
                                let fetch_ctx = self.clone_for_fetch();
                                let selected_worker = self.state.selected_worker_id.clone();
                                let time_interval = self.state.selected_time_window.interval().to_string();
                                tokio::spawn(async move {
                                    fetch_ctx.fetch_workers_data(selected_worker, &time_interval).await;
                                });
                            }
                            Tab::Tasks => {
                                self.state.ensure_task_selection();
                                let fetch_ctx = self.clone_for_fetch();
                                let filter = self.state.task_status_filter.to_sql_values();
                                tokio::spawn(async move {
                                    fetch_ctx.fetch_tasks_data(filter).await;
                                });
                            }
                            Tab::Maintenance => {
                                let fetch_ctx = self.clone_for_fetch();
                                tokio::spawn(async move {
                                    fetch_ctx.fetch_maintenance_data().await;
                                });
                            }
                            Tab::Workflows => {
                                self.state.ensure_workflow_selection();
                                let fetch_ctx = self.clone_for_fetch();
                                let filter = self.state.workflow_status_filter.to_sql_values();
                                tokio::spawn(async move {
                                    fetch_ctx.fetch_workflows_data(filter).await;
                                });
                            }
                        }
                    }
                }

                // Worker navigation
                Action::NavigateWorkerUp => {
                    self.state.select_worker_up();

                    // Refetch worker details for the newly selected worker
                    if self.pool.is_some() {
                        if let Some(worker_id) = self.state.selected_worker_id.clone() {
                            let fetch_ctx = self.clone_for_fetch();
                            let time_interval = self.state.selected_time_window.interval().to_string();
                            tokio::spawn(async move {
                                fetch_ctx.fetch_workers_data(Some(worker_id), &time_interval).await;
                            });
                        }
                    }
                }
                Action::NavigateWorkerDown => {
                    self.state.select_worker_down();

                    // Refetch worker details for the newly selected worker
                    if self.pool.is_some() {
                        if let Some(worker_id) = self.state.selected_worker_id.clone() {
                            let fetch_ctx = self.clone_for_fetch();
                            let time_interval = self.state.selected_time_window.interval().to_string();
                            tokio::spawn(async move {
                                fetch_ctx.fetch_workers_data(Some(worker_id), &time_interval).await;
                            });
                        }
                    }
                }
                Action::CycleTimeWindowForward => {
                    self.state.selected_time_window = self.state.selected_time_window.next();

                    // Refetch worker load history with new time window
                    if self.pool.is_some() {
                        if let Some(worker_id) = self.state.selected_worker_id.clone() {
                            let fetch_ctx = self.clone_for_fetch();
                            let time_interval = self.state.selected_time_window.interval().to_string();
                            tokio::spawn(async move {
                                fetch_ctx.fetch_workers_data(Some(worker_id), &time_interval).await;
                            });
                        }
                    }
                }
                Action::CycleTimeWindowBackward => {
                    self.state.selected_time_window = self.state.selected_time_window.prev();

                    // Refetch worker load history with new time window
                    if self.pool.is_some() {
                        if let Some(worker_id) = self.state.selected_worker_id.clone() {
                            let fetch_ctx = self.clone_for_fetch();
                            let time_interval = self.state.selected_time_window.interval().to_string();
                            tokio::spawn(async move {
                                fetch_ctx.fetch_workers_data(Some(worker_id), &time_interval).await;
                            });
                        }
                    }
                }
                Action::NavigateTaskUp => {
                    self.state.select_task_up();
                }
                Action::NavigateTaskDown => {
                    self.state.select_task_down();
                }

                // Task detail modal actions
                Action::OpenTaskDetail(task_id) => {
                    // Reset scroll and show loading state
                    self.state.task_detail_scroll = 0;
                    self.state.task_detail = None;
                    self.state.show_task_detail = true;

                    // Fetch task detail
                    if self.pool.is_some() {
                        let fetch_ctx = self.clone_for_fetch();
                        tokio::spawn(async move {
                            fetch_ctx.fetch_task_detail(task_id).await;
                        });
                    }
                }
                Action::CloseTaskDetail => {
                    self.state.show_task_detail = false;
                    self.state.task_detail = None;
                    self.state.task_detail_scroll = 0;
                }
                Action::ScrollTaskDetailUp => {
                    self.state.task_detail_scroll = self.state.task_detail_scroll.saturating_sub(1);
                }
                Action::ScrollTaskDetailDown => {
                    self.state.task_detail_scroll = self.state.task_detail_scroll.saturating_add(1);
                }
                Action::CopyTaskToClipboard => {
                    if let Some(task) = &self.state.task_detail {
                        let json = task.to_clipboard_json();
                        match arboard::Clipboard::new() {
                            Ok(mut clipboard) => {
                                if let Err(e) = clipboard.set_text(&json) {
                                    Self::send_action(
                                        &self.action_tx,
                                        Action::Error(format!("Failed to copy: {}", e)),
                                    )?;
                                } else {
                                    self.state.toast = Some(Toast::success("Copied to clipboard"));
                                }
                            }
                            Err(e) => {
                                Self::send_action(
                                    &self.action_tx,
                                    Action::Error(format!("Clipboard unavailable: {}", e)),
                                )?;
                            }
                        }
                    }
                }
                Action::NavigateTaskDetailNext => {
                    // Navigate to next task while modal is open
                    if self.state.show_task_detail && self.state.expanded_worker_index.is_some() {
                        if self.state.select_task_id_down() {
                            // Load the new task detail
                            if let Some(task_id) = self.state.get_selected_task_id() {
                                self.state.task_detail_scroll = 0;
                                let fetch_ctx = self.clone_for_fetch();
                                tokio::spawn(async move {
                                    fetch_ctx.fetch_task_detail(task_id).await;
                                });
                            }
                        }
                    }
                }
                Action::NavigateTaskDetailPrev => {
                    // Navigate to previous task while modal is open
                    if self.state.show_task_detail && self.state.expanded_worker_index.is_some() {
                        // select_task_id_up returns false if at top (and collapses), so we need custom logic
                        if let Some(idx) = self.state.selected_task_id_index {
                            if idx > 0 {
                                self.state.selected_task_id_index = Some(idx - 1);
                                if let Some(task_id) = self.state.get_selected_task_id() {
                                    self.state.task_detail_scroll = 0;
                                    let fetch_ctx = self.clone_for_fetch();
                                    tokio::spawn(async move {
                                        fetch_ctx.fetch_task_detail(task_id).await;
                                    });
                                }
                            }
                        }
                    }
                }
                Action::OpenErrorModal => {
                    if !self.state.errors.is_empty() {
                        self.state.show_error_modal = true;
                    }
                }
                Action::CloseErrorModal => {
                    self.state.show_error_modal = false;
                }
                Action::CopyErrorToClipboard => {
                    // Copy all errors to clipboard
                    if !self.state.errors.is_empty() {
                        let error_text: String = self.state.errors.iter()
                            .map(|(source, msg)| format!("{}: {}", source, msg))
                            .collect::<Vec<_>>()
                            .join("\n\n");
                        match arboard::Clipboard::new() {
                            Ok(mut clipboard) => {
                                if let Err(e) = clipboard.set_text(&error_text) {
                                    Self::send_action(
                                        &self.action_tx,
                                        Action::Error(format!("Failed to copy: {}", e)),
                                    )?;
                                } else {
                                    self.state.toast = Some(Toast::success("Errors copied"));
                                }
                            }
                            Err(e) => {
                                Self::send_action(
                                    &self.action_tx,
                                    Action::Error(format!("Clipboard unavailable: {}", e)),
                                )?;
                            }
                        }
                    }
                }
                Action::ClearAllErrors => {
                    self.state.errors.clear();
                    self.state.show_error_modal = false;
                }

                // Task status filter actions
                Action::ToggleTaskStatusFilter(status) => {
                    self.state.task_status_filter.toggle(status);
                    // Collapse any expanded row when filter changes
                    self.state.collapse_expanded();
                    // Refetch with new filter
                    if self.pool.is_some() {
                        let fetch_ctx = self.clone_for_fetch();
                        let filter = self.state.task_status_filter.to_sql_values();
                        tokio::spawn(async move {
                            fetch_ctx.fetch_tasks_data(filter).await;
                        });
                    }
                }
                Action::SelectAllTaskStatuses => {
                    // Toggle: if all selected, clear; otherwise select all
                    if self.state.task_status_filter.selected.len() == TaskStatus::all().len() {
                        self.state.task_status_filter.clear();
                        self.state.collapse_expanded();
                        self.state.task_aggregation.clear();
                    } else {
                        self.state.task_status_filter.select_all();
                        self.state.collapse_expanded();
                        if self.pool.is_some() {
                            let fetch_ctx = self.clone_for_fetch();
                            let filter = self.state.task_status_filter.to_sql_values();
                            tokio::spawn(async move {
                                fetch_ctx.fetch_tasks_data(filter).await;
                            });
                        }
                    }
                }
                Action::ClearTaskStatuses => {
                    // Toggle: if none selected, select all; otherwise clear
                    if self.state.task_status_filter.selected.is_empty() {
                        self.state.task_status_filter.select_all();
                        self.state.collapse_expanded();
                        if self.pool.is_some() {
                            let fetch_ctx = self.clone_for_fetch();
                            let filter = self.state.task_status_filter.to_sql_values();
                            tokio::spawn(async move {
                                fetch_ctx.fetch_tasks_data(filter).await;
                            });
                        }
                    } else {
                        self.state.task_status_filter.clear();
                        self.state.collapse_expanded();
                        self.state.task_aggregation.clear();
                    }
                }
                Action::ToggleTaskRow => {
                    self.state.toggle_expand_selected();
                }
                Action::NavigateTaskIdUp => {
                    self.state.select_task_id_up();
                }
                Action::NavigateTaskIdDown => {
                    self.state.select_task_id_down();
                }

                Action::SelectWorker(worker_id) => {
                    self.state.set_selected_worker(worker_id.clone());

                    // Refetch worker details for the selected worker
                    if self.pool.is_some() && worker_id.is_some() {
                        let fetch_ctx = self.clone_for_fetch();
                        let time_interval = self.state.selected_time_window.interval().to_string();
                        tokio::spawn(async move {
                            fetch_ctx.fetch_workers_data(worker_id, &time_interval).await;
                        });
                    }
                }

                // Manual refresh actions
                Action::RefreshCurrentTab => {
                    let refresh_action = match self.state.current_tab {
                        Tab::Dashboard => Action::RefreshDashboard,
                        Tab::Workers => Action::RefreshWorkers,
                        Tab::Tasks => Action::RefreshTasks,
                        Tab::Maintenance => Action::RefreshMaintenance,
                        Tab::Workflows => Action::RefreshWorkflows,
                    };
                    Self::send_action(&self.action_tx, refresh_action)?;
                }
                Action::RefreshDashboard => {
                    if self.pool.is_some() {
                        let fetch_ctx = self.clone_for_fetch();
                        tokio::spawn(async move {
                            fetch_ctx.fetch_dashboard_data(true).await; // Show loading on manual refresh
                        });
                    }
                }
                Action::RefreshWorkers => {
                    if self.pool.is_some() {
                        let fetch_ctx = self.clone_for_fetch();
                        let selected_worker = self.state.selected_worker_id.clone();
                        let time_interval = self.state.selected_time_window.interval().to_string();
                        tokio::spawn(async move {
                            fetch_ctx.fetch_workers_data(selected_worker, &time_interval).await;
                        });
                    }
                }
                Action::RefreshTasks => {
                    if self.pool.is_some() {
                        let fetch_ctx = self.clone_for_fetch();
                        let filter = self.state.task_status_filter.to_sql_values();
                        tokio::spawn(async move {
                            fetch_ctx.fetch_tasks_data(filter).await;
                        });
                    }
                }
                Action::RefreshMaintenance => {
                    if self.pool.is_some() {
                        let fetch_ctx = self.clone_for_fetch();
                        tokio::spawn(async move {
                            fetch_ctx.fetch_maintenance_data().await;
                        });
                    }
                }
                Action::RefreshWorkflows => {
                    if self.pool.is_some() {
                        let fetch_ctx = self.clone_for_fetch();
                        let filter = self.state.workflow_status_filter.to_sql_values();
                        tokio::spawn(async move {
                            fetch_ctx.fetch_workflows_data(filter).await;
                        });
                    }
                }

                // Workflow navigation
                Action::NavigateWorkflowUp => {
                    self.state.select_workflow_up();
                }
                Action::NavigateWorkflowDown => {
                    self.state.select_workflow_down();
                }

                // Workflow detail modal actions
                Action::OpenWorkflowDetail(workflow_id) => {
                    self.state.workflow_detail_scroll = 0;
                    self.state.workflow_detail = None;
                    self.state.workflow_tasks.clear();
                    self.state.show_workflow_detail = true;

                    if self.pool.is_some() {
                        let fetch_ctx = self.clone_for_fetch();
                        tokio::spawn(async move {
                            fetch_ctx.fetch_workflow_detail(workflow_id).await;
                        });
                    }
                }
                Action::CloseWorkflowDetail => {
                    self.state.show_workflow_detail = false;
                    self.state.workflow_detail = None;
                    self.state.workflow_tasks.clear();
                    self.state.workflow_detail_scroll = 0;
                }
                Action::ScrollWorkflowDetailUp => {
                    self.state.workflow_detail_scroll = self.state.workflow_detail_scroll.saturating_sub(1);
                }
                Action::ScrollWorkflowDetailDown => {
                    self.state.workflow_detail_scroll = self.state.workflow_detail_scroll.saturating_add(1);
                }
                Action::NavigateWorkflowDetailNext => {
                    // Navigate to next workflow while modal is open
                    if self.state.show_workflow_detail {
                        self.state.select_workflow_down();
                        if let Some(wf_id) = self.state.get_selected_workflow_id() {
                            self.state.workflow_detail_scroll = 0;
                            let fetch_ctx = self.clone_for_fetch();
                            tokio::spawn(async move {
                                fetch_ctx.fetch_workflow_detail(wf_id).await;
                            });
                        }
                    }
                }
                Action::NavigateWorkflowDetailPrev => {
                    // Navigate to previous workflow while modal is open
                    if self.state.show_workflow_detail {
                        self.state.select_workflow_up();
                        if let Some(wf_id) = self.state.get_selected_workflow_id() {
                            self.state.workflow_detail_scroll = 0;
                            let fetch_ctx = self.clone_for_fetch();
                            tokio::spawn(async move {
                                fetch_ctx.fetch_workflow_detail(wf_id).await;
                            });
                        }
                    }
                }
                Action::CopyWorkflowToClipboard => {
                    if let Some(workflow) = &self.state.workflow_detail {
                        let json = workflow.to_clipboard_json(&self.state.workflow_tasks);
                        match arboard::Clipboard::new() {
                            Ok(mut clipboard) => {
                                if let Err(e) = clipboard.set_text(&json) {
                                    Self::send_action(
                                        &self.action_tx,
                                        Action::Error(format!("Failed to copy: {}", e)),
                                    )?;
                                } else {
                                    self.state.toast = Some(Toast::success("Copied to clipboard"));
                                }
                            }
                            Err(e) => {
                                Self::send_action(
                                    &self.action_tx,
                                    Action::Error(format!("Clipboard unavailable: {}", e)),
                                )?;
                            }
                        }
                    }
                }

                // Workflow status filter actions
                Action::ToggleWorkflowStatusFilter(status) => {
                    self.state.workflow_status_filter.toggle(status);
                    if self.pool.is_some() {
                        let fetch_ctx = self.clone_for_fetch();
                        let filter = self.state.workflow_status_filter.to_sql_values();
                        tokio::spawn(async move {
                            fetch_ctx.fetch_workflows_data(filter).await;
                        });
                    }
                }
                Action::SelectAllWorkflowStatuses => {
                    // Toggle: if all selected, clear; otherwise select all
                    if self.state.workflow_status_filter.selected.len() == WorkflowStatus::all().len() {
                        self.state.workflow_status_filter.clear();
                        self.state.workflow_list.clear();
                    } else {
                        self.state.workflow_status_filter.select_all();
                        if self.pool.is_some() {
                            let fetch_ctx = self.clone_for_fetch();
                            let filter = self.state.workflow_status_filter.to_sql_values();
                            tokio::spawn(async move {
                                fetch_ctx.fetch_workflows_data(filter).await;
                            });
                        }
                    }
                }
                Action::ClearWorkflowStatuses => {
                    // Toggle: if none selected, select all; otherwise clear
                    if self.state.workflow_status_filter.selected.is_empty() {
                        self.state.workflow_status_filter.select_all();
                        if self.pool.is_some() {
                            let fetch_ctx = self.clone_for_fetch();
                            let filter = self.state.workflow_status_filter.to_sql_values();
                            tokio::spawn(async move {
                                fetch_ctx.fetch_workflows_data(filter).await;
                            });
                        }
                    } else {
                        self.state.workflow_status_filter.clear();
                        self.state.workflow_list.clear();
                    }
                }

                // Data loading actions
                Action::StartLoading(source) => {
                    self.state.set_loading(source, true);
                }
                Action::DataLoaded(update) => {
                    self.handle_data_update(update);
                }
                Action::DataLoadError(error, source) => {
                    self.state.set_error(source, error);
                }

                // Search actions
                Action::OpenSearch => {
                    self.state.search.open();
                    self.perform_search();
                }
                Action::CloseSearch => {
                    self.state.search.close();
                }
                Action::SearchInput(c) => {
                    self.state.search.insert_char(c);
                    self.perform_search();
                }
                Action::SearchBackspace => {
                    self.state.search.backspace();
                    self.perform_search();
                }
                Action::SearchClear => {
                    self.state.search.clear_query();
                }
                Action::SearchSelectUp => {
                    self.state.search.select_up();
                }
                Action::SearchSelectDown => {
                    self.state.search.select_down();
                }
                Action::SearchConfirm => {
                    self.handle_search_confirm()?;
                }

                // Toast actions
                Action::ShowToast(message) => {
                    self.state.toast = Some(Toast::success(message));
                }
                Action::DismissToast => {
                    self.state.toast = None;
                }

                // NOTIFY/LISTEN actions
                Action::NotifyRefresh(batch) => {
                    self.handle_notify_refresh(batch);
                }
                Action::ListenerStateChanged(state) => {
                    let was_connected = self.state.listener_state == ListenerState::Connected;
                    self.state.listener_state = state;

                    // Show toast on connection state changes
                    match state {
                        ListenerState::Connected if !was_connected => {
                            self.state.toast = Some(Toast::info("Real-time updates active"));
                        }
                        ListenerState::Reconnecting => {
                            self.state.toast = Some(Toast {
                                message: "Reconnecting to database...".into(),
                                icon: ToastIcon::Warning,
                                ticks_remaining: 12,
                            });
                        }
                        _ => {}
                    }
                }

                _ => {}
            }
        }
        Ok(())
    }

    fn handle_data_update(&mut self, update: DataUpdate) {
        match update {
            DataUpdate::ClusterSummary(summary) => {
                self.state.cluster_summary = Some(summary);
                self.state.set_loading(DataSource::ClusterSummary, false);
            }
            DataUpdate::TaskStatusView(rows) => {
                self.state.task_status_dist = rows;
                self.state.set_loading(DataSource::TaskStatus, false);
            }
            DataUpdate::UtilizationTrend(points) => {
                self.state.utilization_trend = points;
                self.state.set_loading(DataSource::UtilizationTrend, false);
            }
            DataUpdate::Alerts(overloaded, stale) => {
                self.state.overloaded_alerts = overloaded;
                self.state.stale_claims_alerts = stale;
                self.state.set_loading(DataSource::Alerts, false);
            }
            DataUpdate::WorkerList(workers) => {
                self.state.worker_list = workers;
                self.state.set_loading(DataSource::WorkerList, false);
            }
            DataUpdate::WorkerDetails(worker_id, uptime, queues) => {
                self.state.worker_uptime.insert(worker_id.clone(), uptime);
                self.state.worker_queues.insert(worker_id, queues);
                self.state.set_loading(DataSource::WorkerDetails, false);
            }
            DataUpdate::WorkerLoad(load_points) => {
                if let Some(worker_id) = load_points.first().map(|p| p.worker_id.clone()) {
                    self.state.worker_load_history.insert(worker_id, load_points);
                }
                self.state.set_loading(DataSource::WorkerDetails, false);
            }
            DataUpdate::TaskAggregation(rows) => {
                self.state.set_task_aggregation(rows);
                self.state.set_loading(DataSource::TaskAggregation, false);
            }
            DataUpdate::SnapshotAge(buckets) => {
                self.state.snapshot_age_dist = buckets;
                self.state.set_loading(DataSource::SnapshotAge, false);
            }
            DataUpdate::DeadWorkers(workers) => {
                self.state.dead_workers = workers;
                self.state.set_loading(DataSource::DeadWorkers, false);
            }
            DataUpdate::TaskDetailLoaded(task) => {
                self.state.task_detail = Some(task);
                self.state.set_loading(DataSource::TaskDetailData, false);
            }
            DataUpdate::WorkflowSummary(summary) => {
                self.state.workflow_summary = Some(summary);
                self.state.set_loading(DataSource::WorkflowSummary, false);
            }
            DataUpdate::WorkflowList(rows) => {
                self.state.set_workflow_list(rows);
                self.state.set_loading(DataSource::WorkflowList, false);
            }
            DataUpdate::WorkflowDetailLoaded(workflow, tasks) => {
                self.state.workflow_detail = Some(workflow);
                self.state.workflow_tasks = tasks;
                self.state.set_loading(DataSource::WorkflowDetail, false);
            }
        }
    }

    fn handle_resize(&mut self, tui: &mut Tui, w: u16, h: u16) -> Result<()> {
        tui.resize(Rect::new(0, 0, w, h))?;
        self.render(tui)?;
        Ok(())
    }

    fn render(&mut self, tui: &mut Tui) -> Result<()> {
        // Cells to capture clamped scroll values from inside the draw closure.
        // After tui.draw() returns, we write them back so the stored offsets
        // never drift beyond the renderable maximum.
        let task_eff_scroll = Cell::new(self.state.task_detail_scroll);
        let wf_eff_scroll = Cell::new(self.state.workflow_detail_scroll);

        let state = &self.state;
        let theme = &self.theme;
        let action_tx = self.action_tx.clone();
        let current_tab = state.current_tab.clone();

        tui.draw(|frame| {
            // Split screen: main content area + status bar at bottom
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(0),      // Main content area
                    Constraint::Length(1),   // Status bar (1 line)
                ])
                .split(frame.area());

            // Render tab-specific content based on current tab
            let render_result = match current_tab {
                Tab::Dashboard => {
                    let mut dashboard = Dashboard::new(state);
                    dashboard.draw(frame, chunks[0], theme)
                }
                Tab::Workers => {
                    let mut workers = Workers::new(state);
                    workers.draw(frame, chunks[0], theme)
                }
                Tab::Tasks => {
                    let mut tasks = Tasks::new(state);
                    tasks.draw(frame, chunks[0], theme)
                }
                Tab::Maintenance => {
                    let mut maintenance = Maintenance::new(state);
                    maintenance.draw(frame, chunks[0], theme)
                }
                Tab::Workflows => {
                    let mut workflows = Workflows::new(state);
                    workflows.draw(frame, chunks[0], theme)
                }
            };

            if let Err(err) = render_result {
                let _ = action_tx.send(Action::Error(format!("Failed to draw tab: {:?}", err)));
            }

            // Render status bar at the bottom
            let mut status_bar = StatusBar::new(state);
            if let Err(err) = status_bar.draw(frame, chunks[1], theme) {
                let _ = action_tx.send(Action::Error(format!("Failed to draw status bar: {:?}", err)));
            }

            // Render task detail panel if visible
            if state.show_task_detail {
                if let Some(task) = &state.task_detail {
                    let mut panel = TaskDetailPanel::new(task, state.task_detail_scroll, state.search_highlight.as_ref());
                    if let Err(err) = panel.draw(frame, frame.area(), theme) {
                        let _ = action_tx.send(Action::Error(format!("Failed to draw task detail: {:?}", err)));
                    }
                    task_eff_scroll.set(panel.effective_scroll());
                } else {
                    // Show loading state
                    use ratatui::widgets::{Block, Borders, Clear, Paragraph};
                    let popup_area = centered_rect(40, 10, frame.area());
                    frame.render_widget(Clear, popup_area);
                    let block = Block::default()
                        .title(" Loading Task... ")
                        .borders(Borders::ALL)
                        .border_style(ratatui::style::Style::default().fg(theme.accent))
                        .style(ratatui::style::Style::default().bg(theme.background));
                    let loading = Paragraph::new("Fetching task details...")
                        .alignment(ratatui::layout::Alignment::Center)
                        .block(block);
                    frame.render_widget(loading, popup_area);
                }
            }

            // Render workflow detail panel if visible
            if state.show_workflow_detail {
                if let Some(workflow) = &state.workflow_detail {
                    let mut panel = WorkflowDetailPanel::new(workflow, &state.workflow_tasks, state.workflow_detail_scroll, state.search_highlight.as_ref());
                    if let Err(err) = panel.draw(frame, frame.area(), theme) {
                        let _ = action_tx.send(Action::Error(format!("Failed to draw workflow detail: {:?}", err)));
                    }
                    wf_eff_scroll.set(panel.effective_scroll());
                } else {
                    // Show loading state
                    use ratatui::widgets::{Block, Borders, Clear, Paragraph};
                    let popup_area = centered_rect(40, 10, frame.area());
                    frame.render_widget(Clear, popup_area);
                    let block = Block::default()
                        .title(" Loading Workflow... ")
                        .borders(Borders::ALL)
                        .border_style(ratatui::style::Style::default().fg(theme.accent))
                        .style(ratatui::style::Style::default().bg(theme.background));
                    let loading = Paragraph::new("Fetching workflow details...")
                        .alignment(ratatui::layout::Alignment::Center)
                        .block(block);
                    frame.render_widget(loading, popup_area);
                }
            }

            // Render error modal if visible
            if state.show_error_modal && !state.errors.is_empty() {
                let mut error_modal = ErrorModal::new(&state.errors);
                if let Err(err) = error_modal.draw(frame, frame.area(), theme) {
                    let _ = action_tx.send(Action::Error(format!("Failed to draw error modal: {:?}", err)));
                }
            }

            // Render search modal if active
            if state.search.active {
                let context_label = if state.show_task_detail {
                    "Task Detail"
                } else if state.show_workflow_detail {
                    "Workflow Detail"
                } else {
                    match state.current_tab {
                        Tab::Dashboard => "Dashboard",
                        Tab::Workers => "Workers",
                        Tab::Tasks => "Tasks",
                        Tab::Workflows => "Workflows",
                        Tab::Maintenance => "Maintenance",
                    }
                };

                let search_modal = SearchModal::new(&state.search, context_label);
                if let Err(err) = search_modal.draw(frame, frame.area(), theme) {
                    let _ = action_tx.send(Action::Error(format!("Failed to draw search modal: {:?}", err)));
                }
            }

            // Render toast notification if present
            if let Some(toast) = &state.toast {
                render_toast(frame, toast, theme);
            }

            // Render help overlay on top if visible (highest priority)
            if state.show_help {
                let mut help = HelpOverlay::new();
                if let Err(err) = help.draw(frame, frame.area(), theme) {
                    let _ = action_tx.send(Action::Error(format!("Failed to draw help: {:?}", err)));
                }
            }
        })?;

        // Write back clamped scroll so subsequent Up/Down operate from valid offsets
        self.state.task_detail_scroll = task_eff_scroll.get();
        self.state.workflow_detail_scroll = wf_eff_scroll.get();

        Ok(())
    }
}
