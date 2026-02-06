use crate::{
    action::{Action, WorkflowStatus},
    components::Component,
    errors::Result,
    state::AppState,
    theme::Theme,
};
use ratatui::{prelude::*, widgets::*};

pub struct Workflows<'a> {
    state: &'a AppState,
}

impl<'a> Workflows<'a> {
    pub fn new(state: &'a AppState) -> Self {
        Self { state }
    }

    /// Get color for workflow status
    fn status_color(&self, status: &str, theme: &Theme) -> Color {
        if status.eq_ignore_ascii_case("PENDING") {
            Color::Yellow
        } else if status.eq_ignore_ascii_case("RUNNING") {
            Color::Blue
        } else if status.eq_ignore_ascii_case("COMPLETED") {
            theme.success
        } else if status.eq_ignore_ascii_case("FAILED") {
            theme.error
        } else if status.eq_ignore_ascii_case("PAUSED") {
            Color::Magenta
        } else if status.eq_ignore_ascii_case("CANCELLED") {
            theme.muted
        } else {
            theme.text
        }
    }

    /// Render the filter bar at the top
    fn render_filter_bar(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let filter = &self.state.workflow_status_filter;

        let mut spans = vec![Span::styled(" Filter: ", Style::default().fg(theme.muted))];

        for status in WorkflowStatus::all() {
            let is_selected = filter.is_selected(&status);
            let key = match status {
                WorkflowStatus::Pending => "p",
                WorkflowStatus::Running => "r",
                WorkflowStatus::Completed => "o",
                WorkflowStatus::Failed => "f",
                WorkflowStatus::Paused => "u",
                WorkflowStatus::Cancelled => "x",
            };

            let style = if is_selected {
                Style::default()
                    .fg(theme.background)
                    .bg(self.status_color(status.db_value(), theme))
                    .bold()
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

    /// Render the workflow list table
    fn render_workflow_table(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Workflows")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        if self.state.workflow_list.is_empty() {
            let msg = if self.state.workflow_status_filter.selected.is_empty() {
                "No statuses selected. Press [a] to select all."
            } else {
                "No workflows found for selected statuses."
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
            Cell::from("ID"),
            Cell::from("Name"),
            Cell::from("Status"),
            Cell::from("Progress"),
            Cell::from("Duration"),
            Cell::from("Created"),
        ])
        .style(header_style)
        .height(1);

        // Build table rows
        let rows: Vec<Row> = self
            .state
            .workflow_list
            .iter()
            .enumerate()
            .map(|(idx, wf)| {
                let is_selected = self.state.selected_workflow_index == Some(idx);

                let status_color = self.status_color(&wf.status, theme);

                let style = if is_selected {
                    Style::default()
                        .bg(theme.surface_alt)
                        .fg(theme.text)
                        .bold()
                } else {
                    Style::default().fg(theme.text)
                };

                // Check if this workflow should show the search pointer (appended to last cell)
                let pointer: &str = self.state.search_highlight.as_ref()
                    .filter(|h| h.matches(&wf.id))
                    .map_or("", |h| h.pointer());

                // Format progress as "completed/total"
                let progress = wf.progress_str();

                // Format duration
                let duration = wf.duration_str();

                // Format created timestamp with optional pointer
                let created = if pointer.is_empty() {
                    wf.created_at.format("%m-%d %H:%M").to_string()
                } else {
                    format!("{} {}", wf.created_at.format("%m-%d %H:%M"), pointer)
                };

                Row::new(vec![
                    Cell::from(wf.short_id().to_string()),
                    Cell::from(wf.name.clone()),
                    Cell::from(Span::styled(
                        wf.status.clone(),
                        Style::default().fg(status_color).bold(),
                    )),
                    Cell::from(progress),
                    Cell::from(duration),
                    Cell::from(created),
                ])
                .style(style)
            })
            .collect();

        // Column constraints
        let widths = [
            Constraint::Length(10),  // ID (truncated)
            Constraint::Min(20),     // Name
            Constraint::Length(12),  // Status
            Constraint::Length(10),  // Progress
            Constraint::Length(12),  // Duration
            Constraint::Length(16),  // Created (with space for pointer)
        ];

        let table = Table::new(rows, widths)
            .header(header)
            .block(block)
            .row_highlight_style(Style::default())
            .column_spacing(1);

        // Use TableState for scrolling
        let mut table_state = TableState::default();
        table_state.select(self.state.selected_workflow_index);

        frame.render_stateful_widget(table, area, &mut table_state);
    }

    /// Render help hints at the bottom
    fn render_help_hints(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let hints = " ↑↓ Navigate | PgUp/PgDn Page | Home/End Jump | Enter: View details | p/r/o/f/u/x: Filter";

        let block = Block::default()
            .borders(Borders::TOP)
            .border_style(theme.toolbar_border_style());

        let paragraph = Paragraph::new(hints)
            .style(theme.toolbar_style().fg(theme.muted))
            .block(block);

        frame.render_widget(paragraph, area);
    }
}

impl<'a> Component for Workflows<'a> {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2), // Filter bar (with border)
                Constraint::Min(10),   // Main table
                Constraint::Length(2), // Help hints (with border)
            ])
            .split(area);

        self.render_filter_bar(frame, chunks[0], theme);
        self.render_workflow_table(frame, chunks[1], theme);
        self.render_help_hints(frame, chunks[2], theme);

        Ok(())
    }
}
