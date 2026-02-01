use crate::{
    action::Action,
    components::Component,
    errors::Result,
    state::AppState,
    theme::Theme,
};
use ratatui::{prelude::*, widgets::*};

pub struct Dashboard<'a> {
    state: &'a AppState,
}

impl<'a> Dashboard<'a> {
    pub fn new(state: &'a AppState) -> Self {
        Self { state }
    }

    /// Render the cluster capacity summary section (top)
    fn render_cluster_summary(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Cluster Capacity")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        if let Some(summary) = &self.state.cluster_summary {
            // Split into 4 columns for metrics
            // Use Fill for equal distribution that adapts to screen width
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Fill(1),
                    Constraint::Fill(1),
                    Constraint::Fill(1),
                    Constraint::Fill(1),
                ])
                .split(block.inner(area));

            // Active Workers
            let workers_text = vec![
                Line::from(vec![
                    Span::styled("Active Workers", theme.muted_style()),
                ]),
                Line::from(vec![
                    Span::styled(
                        format!("{}", summary.active_workers),
                        Style::default().fg(theme.accent).bold(),
                    ),
                ]),
            ];
            let workers_para = Paragraph::new(workers_text)
                .alignment(Alignment::Center);
            frame.render_widget(workers_para, chunks[0]);

            // Total Capacity
            let capacity = summary.total_capacity.unwrap_or(0);
            let capacity_text = vec![
                Line::from(vec![
                    Span::styled("Total Capacity", theme.muted_style()),
                ]),
                Line::from(vec![
                    Span::styled(
                        format!("{}", capacity),
                        Style::default().fg(theme.accent).bold(),
                    ),
                ]),
            ];
            let capacity_para = Paragraph::new(capacity_text)
                .alignment(Alignment::Center);
            frame.render_widget(capacity_para, chunks[1]);

            // Utilization
            let utilization = summary.cluster_utilization_pct.unwrap_or(0.0);
            let util_color = if utilization > 90.0 {
                theme.error
            } else if utilization > 70.0 {
                theme.warning
            } else {
                theme.success
            };
            let util_text = vec![
                Line::from(vec![
                    Span::styled("Utilization", theme.muted_style()),
                ]),
                Line::from(vec![
                    Span::styled(
                        format!("{:.1}%", utilization),
                        Style::default().fg(util_color).bold(),
                    ),
                ]),
            ];
            let util_para = Paragraph::new(util_text)
                .alignment(Alignment::Center);
            frame.render_widget(util_para, chunks[2]);

            // Running Tasks
            let running = summary.total_running.unwrap_or(0);
            let running_text = vec![
                Line::from(vec![
                    Span::styled("Running Tasks", theme.muted_style()),
                ]),
                Line::from(vec![
                    Span::styled(
                        format!("{}", running),
                        Style::default().fg(theme.accent).bold(),
                    ),
                ]),
            ];
            let running_para = Paragraph::new(running_text)
                .alignment(Alignment::Center);
            frame.render_widget(running_para, chunks[3]);
        } else {
            // No data placeholder
            let placeholder = Paragraph::new("No cluster data available")
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block.clone());
            frame.render_widget(placeholder, area);
            return;
        }

        frame.render_widget(block, area);
    }

    /// Render the task status distribution section (middle-left)
    fn render_task_status(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Task Status Distribution")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        if self.state.task_status_dist.is_empty() {
            let placeholder = Paragraph::new("No task data available")
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        }

        // Helper function to get color for each status
        let status_color = |status: &str| -> Color {
            match status.to_lowercase().as_str() {
                "total" => theme.accent,
                "pending" => Color::Yellow,
                "claimed" => Color::Cyan,
                "running" => Color::Blue,
                "completed" => theme.success,
                "failed" => theme.error,
                "cancelled" => theme.muted,
                "requeued" => Color::Magenta,
                _ => theme.text,
            }
        };

        // Build table rows with color coding
        let rows: Vec<Row> = self
            .state
            .task_status_dist
            .iter()
            .map(|row| {
                let color = status_color(&row.status);
                let is_total = row.status.to_uppercase() == "TOTAL";

                // Format status in uppercase
                let status_text = row.status.to_uppercase();

                let style = if is_total {
                    Style::default().fg(color).bold()
                } else {
                    Style::default().fg(color)
                };

                Row::new(vec![
                    Cell::from(status_text),
                    Cell::from(format!("{}", row.count)),
                ])
                .style(style)
            })
            .collect();

        let table = Table::new(
            rows,
            [Constraint::Percentage(50), Constraint::Percentage(50)],
        )
        .header(
            Row::new(vec!["Status", "Count"])
                .style(Style::default().fg(theme.accent).bold())
                .bottom_margin(1),
        )
        .block(block)
        .style(Style::default().bg(theme.background).fg(theme.text));

        frame.render_widget(table, area);
    }

    /// Render the utilization trend chart (middle-right)
    fn render_utilization_trend(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Cluster Utilization Trend (1h)")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        if self.state.utilization_trend.is_empty() {
            let placeholder = Paragraph::new("No trend data available")
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        }

        let inner = block.inner(area);
        frame.render_widget(block, area);

        // Get first and last points (safe after empty check above)
        let Some(oldest) = self.state.utilization_trend.first() else {
            return;
        };
        let Some(latest) = self.state.utilization_trend.last() else {
            return;
        };

        // Get current time for reference
        let now = chrono::Utc::now();
        let oldest_time = oldest.minute;
        let time_span = (now - oldest_time).num_seconds() as f64;

        // Convert to (x, y) coordinates using seconds from oldest
        let utilization_data: Vec<(f64, f64)> = self
            .state
            .utilization_trend
            .iter()
            .map(|p| {
                let x = (p.minute - oldest_time).num_seconds() as f64;
                let y = p.avg_utilization_pct.unwrap_or(0.0);
                (x, y)
            })
            .collect();

        // Get latest value for title
        let current_util = latest.avg_utilization_pct.unwrap_or(0.0);

        // Create dataset
        let dataset = Dataset::default()
            .name(format!("Utilization: {:.1}%", current_util))
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(theme.accent))
            .data(&utilization_data);

        // Create time labels for X-axis
        let oldest_label = oldest_time.format("%H:%M").to_string();
        let mid_time = oldest_time + chrono::Duration::seconds((time_span / 2.0) as i64);
        let mid_label = mid_time.format("%H:%M").to_string();
        let now_label = "now".to_string();

        let x_axis = Axis::default()
            .title("Time")
            .style(Style::default().fg(theme.text))
            .bounds([0.0, time_span])
            .labels(vec![
                Span::raw(oldest_label),
                Span::raw(mid_label),
                Span::raw(now_label),
            ]);

        let y_axis = Axis::default()
            .title("%")
            .style(Style::default().fg(theme.text))
            .bounds([0.0, 100.0])
            .labels(vec![
                Span::raw("0"),
                Span::raw("50"),
                Span::raw("100"),
            ]);

        let chart = Chart::new(vec![dataset])
            .x_axis(x_axis)
            .y_axis(y_axis);

        frame.render_widget(chart, inner);
    }

    /// Render workflow summary section
    fn render_workflow_summary(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Workflow Summary")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        if let Some(summary) = &self.state.workflow_summary {
            let inner = block.inner(area);
            frame.render_widget(block, area);

            // Build workflow status rows
            let rows = vec![
                Row::new(vec![
                    Cell::from("RUNNING"),
                    Cell::from(format!("{}", summary.running.unwrap_or(0))),
                ])
                .style(Style::default().fg(Color::Blue)),
                Row::new(vec![
                    Cell::from("PENDING"),
                    Cell::from(format!("{}", summary.pending.unwrap_or(0))),
                ])
                .style(Style::default().fg(Color::Yellow)),
                Row::new(vec![
                    Cell::from("PAUSED"),
                    Cell::from(format!("{}", summary.paused.unwrap_or(0))),
                ])
                .style(Style::default().fg(Color::Magenta)),
                Row::new(vec![
                    Cell::from("COMPLETED"),
                    Cell::from(format!("{}", summary.completed.unwrap_or(0))),
                ])
                .style(Style::default().fg(theme.success)),
                Row::new(vec![
                    Cell::from("FAILED"),
                    Cell::from(format!("{}", summary.failed.unwrap_or(0))),
                ])
                .style(Style::default().fg(theme.error)),
                Row::new(vec![
                    Cell::from("TOTAL"),
                    Cell::from(format!("{}", summary.total())),
                ])
                .style(Style::default().fg(theme.accent).bold()),
            ];

            let table = Table::new(
                rows,
                [Constraint::Percentage(60), Constraint::Percentage(40)],
            )
            .header(
                Row::new(vec!["Status", "Count"])
                    .style(Style::default().fg(theme.accent).bold())
                    .bottom_margin(1),
            )
            .style(Style::default().bg(theme.background).fg(theme.text));

            frame.render_widget(table, inner);
        } else {
            let placeholder = Paragraph::new("No workflow data available")
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
        }
    }

    /// Render the alerts section (bottom)
    fn render_alerts(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Active Alerts")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        let inner = block.inner(area);
        frame.render_widget(block, area);

        let total_alerts = self.state.overloaded_alerts.len() + self.state.stale_claims_alerts.len();

        if total_alerts == 0 {
            let placeholder = Paragraph::new("No active alerts")
                .style(Style::default().fg(theme.success))
                .alignment(Alignment::Center);
            frame.render_widget(placeholder, inner);
            return;
        }

        // Build alert lines
        let mut lines = Vec::new();

        // Overloaded worker alerts
        for alert in &self.state.overloaded_alerts {
            let alert_text = format!(
                "⚠ Overloaded: {} ({}%) - CPU: {:.1}%, Mem: {:.1}%",
                alert.worker_id,
                alert.hostname,
                alert.cpu_percent.unwrap_or(0.0),
                alert.memory_percent.unwrap_or(0.0),
            );
            lines.push(Line::from(vec![
                Span::styled("⚠ ", Style::default().fg(theme.warning)),
                Span::styled(alert_text, Style::default().fg(theme.text)),
            ]));
        }

        // Stale claims alerts
        for alert in &self.state.stale_claims_alerts {
            let alert_text = format!(
                "⚠ Stale Claims: {} ({}) - Running: {}, Claimed: {} (ratio: {:.2})",
                alert.worker_id,
                alert.hostname,
                alert.tasks_running,
                alert.tasks_claimed,
                alert.claim_ratio.unwrap_or(0.0),
            );
            lines.push(Line::from(vec![
                Span::styled("⚠ ", Style::default().fg(theme.warning)),
                Span::styled(alert_text, Style::default().fg(theme.text)),
            ]));
        }

        let alerts_para = Paragraph::new(lines)
            .style(Style::default().bg(theme.background))
            .wrap(Wrap { trim: true });
        frame.render_widget(alerts_para, inner);
    }
}

impl<'a> Component for Dashboard<'a> {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        // Main layout: top, middle, bottom
        let main_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5),   // Top: Cluster summary (2 lines + borders)
                Constraint::Min(10),     // Middle: Task status + workflow + trend
                Constraint::Length(6),   // Bottom: Alerts
            ])
            .split(area);

        // Render cluster summary at top
        self.render_cluster_summary(frame, main_chunks[0], theme);

        // Middle section: split horizontally into 3 parts
        let middle_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Min(30),   // Left: Task status table
                Constraint::Min(28),   // Middle: Workflow summary
                Constraint::Fill(1),   // Right: Utilization trend (expands on wide screens)
            ])
            .split(main_chunks[1]);

        self.render_task_status(frame, middle_chunks[0], theme);
        self.render_workflow_summary(frame, middle_chunks[1], theme);
        self.render_utilization_trend(frame, middle_chunks[2], theme);

        // Render alerts at bottom
        self.render_alerts(frame, main_chunks[2], theme);

        Ok(())
    }
}
