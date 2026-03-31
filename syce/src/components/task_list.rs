use crate::{
    action::Action,
    components::{filter_sidebar, Component},
    errors::Result,
    state::AppState,
    theme::Theme,
};
use chrono::Utc;
use ratatui::{prelude::*, widgets::*};

pub struct TaskList<'a> {
    state: &'a AppState,
}

impl<'a> TaskList<'a> {
    pub fn new(state: &'a AppState) -> Self {
        Self { state }
    }

    /// Render the task table
    fn render_table(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let worker_label = self.state.task_list_worker_id
            .as_deref()
            .unwrap_or("All Workers");

        let title = format!(
            " Tasks: {} ({}) ",
            worker_label,
            self.state.task_list_rows.len(),
        );

        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        if self.state.task_list_rows.is_empty() {
            let msg = if self.state.has_task_list_filters() {
                "No tasks match the current filters."
            } else {
                "No tasks found."
            };
            let placeholder = Paragraph::new(msg)
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        }

        let header_style = Style::default().fg(theme.accent).bold();
        let header = Row::new(vec![
            Cell::from("Task Name"),
            Cell::from("Queue"),
            Cell::from("Status"),
            Cell::from("Error Code"),
            Cell::from("Retry"),
            Cell::from("Age"),
        ])
        .style(header_style)
        .height(1);

        let now = Utc::now();

        let rows: Vec<Row> = self.state.task_list_rows.iter().enumerate().map(|(idx, task)| {
            let is_selected = self.state.task_list_selected == Some(idx);

            let status_color = Self::status_color(&task.status, theme);

            let retry_str = if task.retry_count > 0 {
                format!("{}/{}", task.retry_count, task.max_retries)
            } else if task.max_retries > 0 {
                format!("0/{}", task.max_retries)
            } else {
                "-".to_string()
            };

            let age = Self::format_age(now, task);

            let error_code_display = task.error_code.as_deref().unwrap_or("-");

            let style = if is_selected {
                Style::default().bg(theme.surface_alt).fg(theme.text).bold()
            } else {
                Style::default().fg(theme.text)
            };

            let error_cell = if task.error_code.is_some() {
                Cell::from(error_code_display.to_string()).style(Style::default().fg(theme.error))
            } else {
                Cell::from(error_code_display.to_string())
            };

            let retry_cell = if task.retry_count > 0 {
                Cell::from(retry_str).style(Style::default().fg(Color::Yellow))
            } else {
                Cell::from(retry_str)
            };

            Row::new(vec![
                Cell::from(task.task_name.clone()),
                Cell::from(task.queue_name.clone()),
                Cell::from(task.status.clone()).style(Style::default().fg(status_color)),
                error_cell,
                retry_cell,
                Cell::from(age),
            ])
            .style(style)
        }).collect();

        let widths = [
            Constraint::Min(20),        // Task Name
            Constraint::Length(16),      // Queue
            Constraint::Length(11),      // Status
            Constraint::Length(28),      // Error Code
            Constraint::Length(7),       // Retry
            Constraint::Length(8),       // Age
        ];

        let table = Table::new(rows, widths)
            .header(header)
            .block(block)
            .row_highlight_style(Style::default())
            .column_spacing(1);

        let mut table_state = TableState::default();
        table_state.select(self.state.task_list_selected);

        frame.render_stateful_widget(table, area, &mut table_state);
    }

    fn status_color(status: &str, theme: &Theme) -> Color {
        match status {
            "PENDING" => Color::Yellow,
            "CLAIMED" => Color::Cyan,
            "RUNNING" => Color::Blue,
            "COMPLETED" => theme.success,
            "FAILED" => theme.error,
            "CANCELLED" => theme.muted,
            "EXPIRED" => Color::DarkGray,
            _ => theme.text,
        }
    }

    fn format_age(now: chrono::DateTime<Utc>, task: &crate::models::TaskListRow) -> String {
        let ts = match task.status.as_str() {
            "COMPLETED" => task.completed_at.unwrap_or(task.enqueued_at),
            "FAILED" => task.failed_at.unwrap_or(task.enqueued_at),
            "RUNNING" => task.started_at.unwrap_or(task.enqueued_at),
            _ => task.enqueued_at,
        };
        let secs = now.signed_duration_since(ts).num_seconds();

        if secs < 60 {
            format!("{}s", secs)
        } else if secs < 3600 {
            format!("{}m", secs / 60)
        } else if secs < 86400 {
            format!("{}h", secs / 3600)
        } else {
            format!("{}d", secs / 86400)
        }
    }

    /// Render help hints
    fn render_help_hints(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let hints = if self.state.sidebar_section != crate::state::SidebarSection::None {
            " ↑↓ Navigate | Enter/Space: Toggle | Esc: Exit section"
        } else {
            " ↑↓/jk Navigate | Enter: Detail | Esc: Back | [m]tasks [u]queues [d]errors"
        };

        let block = Block::default()
            .borders(Borders::TOP)
            .border_style(theme.toolbar_border_style());

        let paragraph = Paragraph::new(hints)
            .style(theme.toolbar_style().fg(theme.muted))
            .block(block);

        frame.render_widget(paragraph, area);
    }
}

impl<'a> Component for TaskList<'a> {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        // Vertical: main content + help hints
        let v_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(8),     // Main area
                Constraint::Length(2),   // Help hints
            ])
            .split(area);

        // Horizontal: sidebar + table
        let h_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(18),  // Sidebar (scales with terminal width)
                Constraint::Percentage(82),  // Table
            ])
            .split(v_chunks[0]);

        filter_sidebar::render(frame, h_chunks[0], self.state, theme, true);
        self.render_table(frame, h_chunks[1], theme);
        self.render_help_hints(frame, v_chunks[1], theme);

        Ok(())
    }
}
