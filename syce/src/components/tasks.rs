use crate::{
    action::{Action, TaskStatus},
    components::Component,
    errors::Result,
    state::AppState,
    theme::Theme,
};
use ratatui::{prelude::*, widgets::*};

pub struct Tasks<'a> {
    state: &'a AppState,
}

/// Represents a row in the task table - either a worker row or an expanded task ID
#[derive(Clone)]
enum TableRowKind {
    Worker { index: usize, is_total: bool },
    TaskId { task_id: String, tid_index: usize },
}

impl<'a> Tasks<'a> {
    pub fn new(state: &'a AppState) -> Self {
        Self { state }
    }

    /// Render the filter bar at the top
    fn render_filter_bar(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let filter = &self.state.task_status_filter;

        let mut spans = vec![
            Span::styled(" Filter: ", Style::default().fg(theme.muted)),
        ];

        for status in TaskStatus::all() {
            let is_selected = filter.is_selected(&status);
            let key = match status {
                TaskStatus::Pending => "p",
                TaskStatus::Claimed => "c",
                TaskStatus::Running => "r",
                TaskStatus::Completed => "o",
                TaskStatus::Failed => "f",
            };

            let style = if is_selected {
                Style::default().fg(theme.background).bg(self.status_color(&status, theme)).bold()
            } else {
                Style::default().fg(theme.muted)
            };

            spans.push(Span::styled(format!("[{}]{} ", key, status.label()), style));
        }

        spans.push(Span::styled(" | ", Style::default().fg(theme.border)));
        spans.push(Span::styled("[a]ll [n]one ", Style::default().fg(theme.muted)));

        let block = Block::default()
            .borders(Borders::BOTTOM)
            .border_style(theme.toolbar_border_style());

        let paragraph = Paragraph::new(Line::from(spans))
            .style(theme.toolbar_style())
            .block(block);

        frame.render_widget(paragraph, area);
    }

    fn status_color(&self, status: &TaskStatus, theme: &Theme) -> Color {
        match status {
            TaskStatus::Pending => Color::Yellow,
            TaskStatus::Claimed => Color::Cyan,
            TaskStatus::Running => Color::Blue,
            TaskStatus::Completed => theme.success,
            TaskStatus::Failed => theme.error,
        }
    }

    /// Build the list of displayable rows (workers + expanded task IDs)
    fn build_row_list(&self) -> Vec<TableRowKind> {
        let mut rows = Vec::new();

        for (idx, row) in self.state.task_aggregation.iter().enumerate() {
            let is_total = row.worker_id == "TOTAL";
            rows.push(TableRowKind::Worker { index: idx, is_total });

            // If this row is expanded, add task ID rows
            if self.state.is_row_expanded(idx) {
                let task_ids = self.state.get_expanded_task_ids();
                for (tid_idx, task_id) in task_ids.into_iter().enumerate() {
                    rows.push(TableRowKind::TaskId { task_id, tid_index: tid_idx });
                }
            }
        }

        rows
    }

    /// Find the selected row index in the flattened row list
    fn find_selected_row(&self, row_list: &[TableRowKind]) -> Option<usize> {
        for (i, row_kind) in row_list.iter().enumerate() {
            match row_kind {
                TableRowKind::Worker { index, .. } => {
                    if self.state.expanded_worker_index.is_none()
                        && self.state.selected_task_index == Some(*index)
                    {
                        return Some(i);
                    }
                }
                TableRowKind::TaskId { tid_index, .. } => {
                    if self.state.selected_task_id_index == Some(*tid_index) {
                        return Some(i);
                    }
                }
            }
        }
        None
    }

    /// Render the worker task aggregation table with expandable rows
    fn render_aggregation_table(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Task Distribution by Worker")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        if self.state.task_aggregation.is_empty() {
            let msg = if self.state.task_status_filter.selected.is_empty() {
                "No statuses selected. Press [a] to select all."
            } else {
                "No tasks found for selected statuses."
            };
            let placeholder = Paragraph::new(msg)
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        }

        // Build header
        let header_style = Style::default().fg(theme.accent).bold();
        let header = Row::new(vec![
            Cell::from("Worker ID"),
            Cell::from("Total"),
            Cell::from("Pending"),
            Cell::from("Claimed"),
            Cell::from("Running"),
            Cell::from("Completed"),
            Cell::from("Failed"),
        ])
        .style(header_style)
        .height(1);

        // Build row list (workers + expanded task IDs)
        let row_list = self.build_row_list();
        let selected_idx = self.find_selected_row(&row_list);

        // Build table rows
        let rows: Vec<Row> = row_list
            .iter()
            .map(|row_kind| {
                match row_kind {
                    TableRowKind::Worker { index, is_total } => {
                        let agg_row = &self.state.task_aggregation[*index];
                        let is_selected = self.state.selected_task_index == Some(*index);
                        let is_expanded = self.state.is_row_expanded(*index);

                        // Row prefix (expand indicator)
                        let prefix = if *is_total {
                            "  "
                        } else if is_expanded {
                            "▼ "
                        } else {
                            "▶ "
                        };

                        let style = if *is_total {
                            Style::default()
                                .fg(theme.accent)
                                .bold()
                                .add_modifier(Modifier::UNDERLINED)
                        } else if is_selected && self.state.expanded_worker_index.is_none() {
                            Style::default()
                                .bg(theme.surface_alt)
                                .fg(theme.text)
                                .bold()
                        } else {
                            Style::default().fg(theme.text)
                        };

                        let worker_display = format!("{}{}", prefix, &agg_row.worker_id);
                        Row::new(vec![
                            Cell::from(worker_display),
                            Cell::from(agg_row.total_count.to_string()),
                            Cell::from(agg_row.pending_count.to_string()),
                            Cell::from(agg_row.claimed_count.to_string()),
                            Cell::from(agg_row.running_count.to_string()),
                            Cell::from(agg_row.completed_count.to_string()),
                            Cell::from(agg_row.failed_count.to_string()),
                        ])
                        .style(style)
                    }
                    TableRowKind::TaskId { task_id, tid_index } => {
                        let is_tid_selected = self.state.selected_task_id_index == Some(*tid_index);

                        // Check for search highlight on this task (appended to end)
                        let pointer: &str = self.state.search_highlight.as_ref()
                            .filter(|h| h.matches(task_id))
                            .map_or("", |h| h.pointer());

                        let indicator = if is_tid_selected { "→" } else { " " };

                        let style = if is_tid_selected {
                            Style::default()
                                .bg(theme.accent)
                                .fg(theme.background)
                                .bold()
                        } else {
                            Style::default().fg(theme.muted)
                        };

                        // Task ID row spans across with indentation, pointer appended at end
                        let cell_content = if pointer.is_empty() {
                            format!("    {} {}", indicator, task_id)
                        } else {
                            format!("    {} {} {}", indicator, task_id, pointer)
                        };

                        Row::new(vec![
                            Cell::from(cell_content),
                            Cell::from(""),
                            Cell::from(""),
                            Cell::from(""),
                            Cell::from(""),
                            Cell::from(""),
                            Cell::from(""),
                        ])
                        .style(style)
                    }
                }
            })
            .collect();

        // Column constraints
        let widths = [
            Constraint::Min(40),        // Worker ID (UUID + prefix)
            Constraint::Length(8),      // Total
            Constraint::Length(8),      // Pending
            Constraint::Length(8),      // Claimed
            Constraint::Length(8),      // Running
            Constraint::Length(10),     // Completed
            Constraint::Length(8),      // Failed
        ];

        let table = Table::new(rows, widths)
            .header(header)
            .block(block)
            .row_highlight_style(Style::default()) // We handle highlighting manually per-row
            .column_spacing(1);

        // Use TableState for scrolling
        let mut table_state = TableState::default();
        table_state.select(selected_idx);

        frame.render_stateful_widget(table, area, &mut table_state);
    }

    /// Render help hints at the bottom
    fn render_help_hints(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let hints = if self.state.expanded_worker_index.is_some() {
            " ↑↓ Select task | Enter: View details | Esc: Collapse"
        } else {
            " ↑↓ Select worker | Enter: Expand | p/c/r/o/f: Toggle filter"
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

impl<'a> Component for Tasks<'a> {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2),  // Filter bar (with border)
                Constraint::Min(10),    // Main table
                Constraint::Length(2),  // Help hints (with border)
            ])
            .split(area);

        self.render_filter_bar(frame, chunks[0], theme);
        self.render_aggregation_table(frame, chunks[1], theme);
        self.render_help_hints(frame, chunks[2], theme);

        Ok(())
    }
}
