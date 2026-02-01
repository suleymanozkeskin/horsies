use crate::{
    action::Action,
    components::Component,
    errors::Result,
    state::AppState,
    theme::Theme,
};
use ratatui::{
    prelude::*,
    widgets::{Axis, Block, Borders, Cell, Chart, Dataset, GraphType, Paragraph, Row, Table},
};

pub struct Workers<'a> {
    state: &'a AppState,
}

impl<'a> Workers<'a> {
    pub fn new(state: &'a AppState) -> Self {
        Self { state }
    }

    /// Render the worker list table (left pane)
    fn render_worker_list(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Active Workers")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        if self.state.worker_list.is_empty() {
            let placeholder = Paragraph::new("No active workers")
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        }

        // Build table rows
        let rows: Vec<Row> = self
            .state
            .worker_list
            .iter()
            .enumerate()
            .map(|(idx, worker)| {
                let is_selected = self.state.selected_worker_index == Some(idx);
                let style = if is_selected {
                    Style::default()
                        .fg(theme.background)
                        .bg(theme.accent)
                        .bold()
                } else {
                    Style::default().fg(theme.text)
                };

                // Check if this worker should show the search pointer (appended to last cell)
                let pointer: &str = self.state.search_highlight.as_ref()
                    .filter(|h| h.matches(&worker.worker_id))
                    .map_or("", |h| h.pointer());

                // Build last cell with optional pointer
                let mem_cell = if pointer.is_empty() {
                    format!("{:.1}%", worker.memory_percent.unwrap_or(0.0))
                } else {
                    format!("{:.1}% {}", worker.memory_percent.unwrap_or(0.0), pointer)
                };

                Row::new(vec![
                    Cell::from(worker.worker_id.clone()),
                    Cell::from(worker.hostname.clone()),
                    Cell::from(format!("{}", worker.pid)),
                    Cell::from(format!("{}/{}", worker.tasks_running, worker.processes)),
                    Cell::from(format!("{:.1}%", worker.cpu_percent.unwrap_or(0.0))),
                    Cell::from(mem_cell),
                ])
                .style(style)
            })
            .collect();

        let widths = [
            Constraint::Fill(2),   // Worker ID (takes more space)
            Constraint::Fill(2),   // Hostname (takes more space)
            Constraint::Min(8),    // PID (min 8 cols)
            Constraint::Min(8),    // Tasks (min 8 cols)
            Constraint::Min(8),    // CPU% (min 8 cols)
            Constraint::Min(10),   // Mem% (min 10 cols to accommodate pointer)
        ];

        let table = Table::new(rows, widths)
            .header(
                Row::new(vec!["Worker ID", "Hostname", "PID", "Tasks", "CPU%", "Mem%"])
                    .style(Style::default().fg(theme.accent).bold())
                    .bottom_margin(1),
            )
            .block(block)
            .row_highlight_style(Style::default().bg(theme.accent))
            .style(Style::default().bg(theme.background).fg(theme.text));

        frame.render_widget(table, area);
    }

    /// Render worker details header (identity & uptime)
    fn render_worker_header(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Worker Details")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        let Some(worker_id) = self.state.selected_worker_id.as_ref() else {
            let placeholder = Paragraph::new("← Select a worker from the list")
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        };

        let inner = block.inner(area);
        frame.render_widget(block, area);

        // Get worker uptime and queues data
        let uptime = self.state.worker_uptime.get(worker_id);
        let queues = self.state.worker_queues.get(worker_id);

        let mut lines = Vec::new();

        // Worker identity
        lines.push(Line::from(vec![
            Span::styled("Worker: ", theme.muted_style()),
            Span::styled(worker_id.clone(), Style::default().fg(theme.accent).bold()),
        ]));

        if let Some(uptime_row) = uptime {
            lines.push(Line::from(vec![
                Span::styled("Hostname: ", theme.muted_style()),
                Span::styled(
                    uptime_row.hostname.clone(),
                    Style::default().fg(theme.text),
                ),
            ]));

            // Format uptime
            lines.push(Line::from(vec![
                Span::styled("Uptime: ", theme.muted_style()),
                Span::styled(
                    format!("{:?}", uptime_row.uptime),
                    Style::default().fg(theme.text),
                ),
            ]));

            lines.push(Line::from(vec![
                Span::styled("Started: ", theme.muted_style()),
                Span::styled(
                    uptime_row.worker_started_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                    Style::default().fg(theme.text),
                ),
            ]));
        }

        if let Some(queues_row) = queues {
            lines.push(Line::from(""));
            lines.push(Line::from(vec![
                Span::styled("Queues: ", theme.muted_style()),
                Span::styled(
                    queues_row.queues.join(", "),
                    Style::default().fg(theme.text),
                ),
            ]));

            lines.push(Line::from(vec![
                Span::styled("Tasks: ", theme.muted_style()),
                Span::styled(
                    format!("Running: {}, Claimed: {}", queues_row.tasks_running, queues_row.tasks_claimed),
                    Style::default().fg(theme.text),
                ),
            ]));
        }

        let paragraph = Paragraph::new(lines)
            .style(Style::default().bg(theme.background));
        frame.render_widget(paragraph, inner);
    }

    /// Render worker load charts with configurable time window
    fn render_worker_charts(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let time_window_label = self.state.selected_time_window.label();
        let block = Block::default()
            .title(format!("Load History ({}) [use [ ] to change]", time_window_label))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        let Some(worker_id) = self.state.selected_worker_id.as_ref() else {
            let placeholder = Paragraph::new("No worker selected")
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        };

        let Some(load_history) = self.state.worker_load_history.get(worker_id) else {
            let placeholder = Paragraph::new("No load history available")
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        };

        if load_history.is_empty() {
            let placeholder = Paragraph::new("No load history data yet")
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        }

        let inner = block.inner(area);
        frame.render_widget(block, area);

        // Split into two rows for different charts
        let chart_areas = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(inner);

        // Prepare data using actual timestamps (safe after empty check above)
        let Some(oldest) = load_history.first() else {
            return;
        };
        let now = chrono::Utc::now();
        let oldest_time = oldest.snapshot_at;
        let time_span = (now - oldest_time).num_seconds() as f64;

        // Convert to (x, y) coordinates using seconds from oldest
        let tasks_running_data: Vec<(f64, f64)> = load_history
            .iter()
            .map(|p| {
                let x = (p.snapshot_at - oldest_time).num_seconds() as f64;
                let y = p.tasks_running as f64;
                (x, y)
            })
            .collect();

        let tasks_claimed_data: Vec<(f64, f64)> = load_history
            .iter()
            .map(|p| {
                let x = (p.snapshot_at - oldest_time).num_seconds() as f64;
                let y = p.tasks_claimed as f64;
                (x, y)
            })
            .collect();

        let cpu_data: Vec<(f64, f64)> = load_history
            .iter()
            .map(|p| {
                let x = (p.snapshot_at - oldest_time).num_seconds() as f64;
                let y = p.cpu_percent.unwrap_or(0.0);
                (x, y)
            })
            .collect();

        let memory_data: Vec<(f64, f64)> = load_history
            .iter()
            .map(|p| {
                let x = (p.snapshot_at - oldest_time).num_seconds() as f64;
                let y = p.memory_percent.unwrap_or(0.0);
                (x, y)
            })
            .collect();

        // Calculate Y-axis bounds in a single pass
        let (max_tasks, max_cpu, max_mem) = load_history.iter().fold(
            (10i32, 0.0f64, 0.0f64),
            |(max_t, max_c, max_m), p| (
                max_t.max(p.tasks_running.max(p.tasks_claimed)),
                max_c.max(p.cpu_percent.unwrap_or(0.0)),
                max_m.max(p.memory_percent.unwrap_or(0.0)),
            ),
        );
        let tasks_y_max = (max_tasks as f64 * 1.2).max(5.0);
        let resources_y_max = (max_cpu.max(max_mem) * 1.2).max(1.0);

        // Get current (latest) values (safe after empty check above)
        let Some(latest) = load_history.last() else {
            return;
        };
        let current_running = latest.tasks_running;
        let current_claimed = latest.tasks_claimed;
        let current_cpu = latest.cpu_percent.unwrap_or(0.0);
        let current_memory = latest.memory_percent.unwrap_or(0.0);

        // Task Load Chart
        let tasks_datasets = vec![
            Dataset::default()
                .name(format!("Running: {}", current_running))
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(theme.accent))
                .data(&tasks_running_data),
            Dataset::default()
                .name(format!("Claimed: {}", current_claimed))
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(theme.success))
                .data(&tasks_claimed_data),
        ];

        // Create time labels for X-axis
        let time_labels = vec![
            Span::raw(format!("{}s ago", time_span as i64)),
            Span::raw(format!("{}s", (time_span / 2.0) as i64)),
            Span::raw("now"),
        ];

        let tasks_x_axis = Axis::default()
            .title("Time")
            .style(Style::default().fg(theme.text))
            .bounds([0.0, time_span])
            .labels(time_labels.clone());

        let tasks_y_axis = Axis::default()
            .title("Tasks")
            .style(Style::default().fg(theme.text))
            .bounds([0.0, tasks_y_max])
            .labels(vec![
                Span::raw("0"),
                Span::raw(format!("{:.0}", tasks_y_max / 2.0)),
                Span::raw(format!("{:.0}", tasks_y_max)),
            ]);

        let tasks_chart = Chart::new(tasks_datasets)
            .block(Block::default().title("Task Load").borders(Borders::NONE))
            .x_axis(tasks_x_axis)
            .y_axis(tasks_y_axis);

        frame.render_widget(tasks_chart, chart_areas[0]);

        // Resource Usage Chart
        let resources_datasets = vec![
            Dataset::default()
                .name(format!("CPU: {:.2}%", current_cpu))
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(theme.warning))
                .data(&cpu_data),
            Dataset::default()
                .name(format!("Mem: {:.2}%", current_memory))
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Cyan))
                .data(&memory_data),
        ];

        let resources_x_axis = Axis::default()
            .title("Time")
            .style(Style::default().fg(theme.text))
            .bounds([0.0, time_span])
            .labels(time_labels);

        let resources_y_axis = Axis::default()
            .title("%")
            .style(Style::default().fg(theme.text))
            .bounds([0.0, resources_y_max])
            .labels(vec![
                Span::raw("0"),
                Span::raw(format!("{:.2}", resources_y_max / 2.0)),
                Span::raw(format!("{:.2}", resources_y_max)),
            ]);

        let resources_chart = Chart::new(resources_datasets)
            .block(
                Block::default()
                    .title("Resource Usage (dynamic scale)")
                    .borders(Borders::NONE),
            )
            .x_axis(resources_x_axis)
            .y_axis(resources_y_axis);

        frame.render_widget(resources_chart, chart_areas[1]);
    }

    /// Render dead workers section (bottom)
    fn render_dead_workers(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Dead Workers")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        if self.state.dead_workers.is_empty() {
            let placeholder = Paragraph::new("No dead workers")
                .style(Style::default().fg(theme.success))
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        }

        // Build table rows
        let rows: Vec<Row> = self
            .state
            .dead_workers
            .iter()
            .map(|worker| {
                Row::new(vec![
                    Cell::from(worker.worker_id.clone()),
                    Cell::from(worker.hostname.clone()),
                    Cell::from(format!("{}", worker.pid)),
                    Cell::from(worker.last_seen.format("%H:%M:%S").to_string()),
                    Cell::from(format!("{}", worker.tasks_at_death)),
                ])
                .style(Style::default().fg(theme.error))
            })
            .collect();

        let widths = [
            Constraint::Fill(2),   // Worker ID
            Constraint::Fill(2),   // Hostname
            Constraint::Min(8),    // PID
            Constraint::Fill(1),   // Last Seen
            Constraint::Min(10),   // Tasks at Death
        ];

        let table = Table::new(rows, widths)
            .header(
                Row::new(vec!["Worker ID", "Hostname", "PID", "Last Seen", "Tasks"])
                    .style(Style::default().fg(theme.accent).bold())
                    .bottom_margin(1),
            )
            .block(block)
            .style(Style::default().bg(theme.background).fg(theme.text));

        frame.render_widget(table, area);
    }
}

impl<'a> Component for Workers<'a> {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        // Main horizontal split: left (worker list) and right (details)
        // Use Min/Fill for responsive layout on wide screens
        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Min(60),  // Left: Worker list (min 60 cols, won't expand unnecessarily)
                Constraint::Fill(1),  // Right: Details (takes remaining space)
            ])
            .split(area);

        // Render worker list on the left
        self.render_worker_list(frame, main_chunks[0], theme);

        // Split right side vertically: header, charts, dead workers
        let right_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(10), // Header: Identity & uptime
                Constraint::Min(8),     // Charts: Load history
                Constraint::Length(8),  // Dead workers
            ])
            .split(main_chunks[1]);

        self.render_worker_header(frame, right_chunks[0], theme);
        self.render_worker_charts(frame, right_chunks[1], theme);
        self.render_dead_workers(frame, right_chunks[2], theme);

        Ok(())
    }
}
