use std::borrow::Cow;

use crate::{action::Action, components::Component, errors::Result, models::TaskDetail, state::SearchHighlight, theme::Theme};
use ratatui::{
    prelude::*,
    widgets::{Block, BorderType, Borders, Clear, Padding, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap},
};

pub struct TaskDetailPanel<'a> {
    task: &'a TaskDetail,
    scroll_offset: u16,
    highlight: Option<&'a SearchHighlight>,
    /// Clamped scroll value computed during draw, readable after render.
    effective_scroll: u16,
}

pub struct SearchableLine {
    pub display: String,
    pub search_text: String,
}

struct DetailLine {
    display: Line<'static>,
    display_text: String,
    search_text: String,
}

impl DetailLine {
    fn new(display: Line<'static>) -> Self {
        let display_text = line_to_string(&display);
        Self {
            display,
            search_text: display_text.clone(),
            display_text,
        }
    }
}

fn line_to_string(line: &Line) -> String {
    let mut text = String::new();
    for span in &line.spans {
        text.push_str(span.content.as_ref());
    }
    text
}

impl<'a> TaskDetailPanel<'a> {
    pub fn new(task: &'a TaskDetail, scroll_offset: u16, highlight: Option<&'a SearchHighlight>) -> Self {
        Self { task, scroll_offset, highlight, effective_scroll: 0 }
    }

    /// Return the clamped scroll value computed during the last draw call.
    pub fn effective_scroll(&self) -> u16 {
        self.effective_scroll
    }

    /// Calculate centered rect for the detail modal
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

    fn format_timestamp(dt: &Option<chrono::DateTime<chrono::Utc>>) -> Cow<'static, str> {
        match dt {
            Some(t) => Cow::Owned(t.format("%Y-%m-%d %H:%M:%S UTC").to_string()),
            None => Cow::Borrowed("-"),
        }
    }

    fn status_color(status: &str, theme: &Theme) -> Color {
        if status.eq_ignore_ascii_case("PENDING") {
            Color::Yellow
        } else if status.eq_ignore_ascii_case("CLAIMED") {
            Color::Cyan
        } else if status.eq_ignore_ascii_case("RUNNING") {
            Color::Blue
        } else if status.eq_ignore_ascii_case("COMPLETED") {
            theme.success
        } else if status.eq_ignore_ascii_case("FAILED") {
            theme.error
        } else if status.eq_ignore_ascii_case("CANCELLED") {
            theme.muted
        } else if status.eq_ignore_ascii_case("REQUEUED") {
            Color::Magenta
        } else {
            theme.text
        }
    }

    fn format_json(json_str: &Option<String>) -> Vec<String> {
        match json_str {
            Some(s) if !s.is_empty() => {
                // Try to pretty-print JSON with recursive string parsing
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                    let expanded = Self::expand_nested_json(parsed);
                    if let Ok(pretty) = serde_json::to_string_pretty(&expanded) {
                        return pretty.lines().map(String::from).collect();
                    }
                }
                // Fallback: show raw string (split long lines)
                s.lines().map(String::from).collect()
            }
            _ => vec!["-".to_string()],
        }
    }

    /// Recursively expand JSON strings that contain escaped JSON
    fn expand_nested_json(value: serde_json::Value) -> serde_json::Value {
        use serde_json::Value;

        match value {
            Value::String(s) => {
                // Try to parse the string as JSON
                if s.starts_with('{') || s.starts_with('[') {
                    if let Ok(inner) = serde_json::from_str::<Value>(&s) {
                        return Self::expand_nested_json(inner);
                    }
                }
                Value::String(s)
            }
            Value::Array(arr) => {
                Value::Array(arr.into_iter().map(Self::expand_nested_json).collect())
            }
            Value::Object(obj) => {
                Value::Object(
                    obj.into_iter()
                        .map(|(k, v)| (k, Self::expand_nested_json(v)))
                        .collect(),
                )
            }
            other => other,
        }
    }

    fn build_detail_lines(&self, theme: &Theme) -> Vec<DetailLine> {
        let mut lines: Vec<DetailLine> = Vec::new();

        // Header section
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Task Name:    ", Style::default().fg(theme.muted)),
            Span::styled(self.task.task_name.clone(), Style::default().fg(theme.accent).bold()),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Queue:        ", Style::default().fg(theme.muted)),
            Span::styled(self.task.queue_name.clone(), Style::default().fg(theme.text)),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Status:       ", Style::default().fg(theme.muted)),
            Span::styled(
                self.task.status.clone(),
                Style::default().fg(Self::status_color(&self.task.status, theme)).bold(),
            ),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Priority:     ", Style::default().fg(theme.muted)),
            Span::styled(format!("{}", self.task.priority), Style::default().fg(theme.text)),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Retries:      ", Style::default().fg(theme.muted)),
            Span::styled(
                format!("{} / {}", self.task.retry_count, self.task.max_retries),
                Style::default().fg(theme.text),
            ),
        ])));

        lines.push(DetailLine::new(Line::from("")));

        // Worker section
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Worker Info", Style::default().fg(theme.accent).bold()),
        ])));
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Worker ID:  ", Style::default().fg(theme.muted)),
            Span::styled(
                self.task
                    .claimed_by_worker_id
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
                Style::default().fg(theme.text),
            ),
        ])));
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Hostname:   ", Style::default().fg(theme.muted)),
            Span::styled(
                self.task
                    .worker_hostname
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
                Style::default().fg(theme.text),
            ),
        ])));
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  PID:        ", Style::default().fg(theme.muted)),
            Span::styled(
                self.task.worker_pid.map(|p| p.to_string()).unwrap_or_else(|| "-".to_string()),
                Style::default().fg(theme.text),
            ),
        ])));

        lines.push(DetailLine::new(Line::from("")));

        // Timestamps section
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Timeline", Style::default().fg(theme.accent).bold()),
        ])));
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Created:    ", Style::default().fg(theme.muted)),
            Span::styled(
                self.task.created_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                Style::default().fg(theme.text),
            ),
        ])));
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Sent:       ", Style::default().fg(theme.muted)),
            Span::styled(Self::format_timestamp(&self.task.sent_at), Style::default().fg(theme.text)),
        ])));
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Claimed:    ", Style::default().fg(theme.muted)),
            Span::styled(Self::format_timestamp(&self.task.claimed_at), Style::default().fg(theme.text)),
        ])));
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Started:    ", Style::default().fg(theme.muted)),
            Span::styled(Self::format_timestamp(&self.task.started_at), Style::default().fg(theme.text)),
        ])));
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Completed:  ", Style::default().fg(theme.muted)),
            Span::styled(Self::format_timestamp(&self.task.completed_at), Style::default().fg(theme.text)),
        ])));
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Failed:     ", Style::default().fg(theme.muted)),
            Span::styled(Self::format_timestamp(&self.task.failed_at), Style::default().fg(theme.text)),
        ])));

        lines.push(DetailLine::new(Line::from("")));

        // Arguments section
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Arguments", Style::default().fg(theme.accent).bold()),
        ])));
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  args:   ", Style::default().fg(theme.muted)),
        ])));
        for arg_line in Self::format_json(&self.task.args) {
            lines.push(DetailLine::new(Line::from(vec![
                Span::styled(format!("    {}", arg_line), Style::default().fg(theme.text)),
            ])));
        }
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  kwargs: ", Style::default().fg(theme.muted)),
        ])));
        for kwarg_line in Self::format_json(&self.task.kwargs) {
            lines.push(DetailLine::new(Line::from(vec![
                Span::styled(format!("    {}", kwarg_line), Style::default().fg(theme.text)),
            ])));
        }

        lines.push(DetailLine::new(Line::from("")));

        // Result/Error section
        if self.task.status.to_uppercase() == "COMPLETED" {
            lines.push(DetailLine::new(Line::from(vec![
                Span::styled("Result", Style::default().fg(theme.success).bold()),
            ])));
            for result_line in Self::format_json(&self.task.result) {
                lines.push(DetailLine::new(Line::from(vec![
                    Span::styled(format!("  {}", result_line), Style::default().fg(theme.text)),
                ])));
            }
        } else if self.task.status.to_uppercase() == "FAILED" {
            lines.push(DetailLine::new(Line::from(vec![
                Span::styled("Error", Style::default().fg(theme.error).bold()),
            ])));
            for error_line in Self::format_json(&self.task.failed_reason) {
                lines.push(DetailLine::new(Line::from(vec![
                    Span::styled(format!("  {}", error_line), Style::default().fg(theme.error)),
                ])));
            }
        }

        lines
    }

    pub fn build_search_lines(&self, theme: &Theme) -> Vec<SearchableLine> {
        self.build_detail_lines(theme)
            .into_iter()
            .map(|line| SearchableLine {
                display: line.display_text,
                search_text: line.search_text,
            })
            .collect()
    }
}

impl<'a> Component for TaskDetailPanel<'a> {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        let popup_area = Self::centered_rect(80, 85, area);

        // Clear the background
        frame.render_widget(Clear, popup_area);

        let title = format!(" Task: {} - Esc: close | ↑↓: scroll | []: prev/next | y: copy ", self.task.id);
        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.accent))
            .border_type(BorderType::Rounded)
            .padding(Padding::uniform(1))
            .style(Style::default().bg(theme.surface));

        let inner = block.inner(popup_area);
        frame.render_widget(block, popup_area);

        let lines: Vec<Line> = self
            .build_detail_lines(theme)
            .into_iter()
            .enumerate()
            .map(|(idx, line)| {
                // Check if this line should show the search pointer (appended to end)
                if let Some(highlight) = &self.highlight {
                    if highlight.matches_line(idx) {
                        let mut spans = line.display.spans;
                        spans.push(Span::styled(
                            format!(" {}", highlight.pointer()),
                            Style::default().fg(theme.accent),
                        ));
                        return Line::from(spans);
                    }
                }
                line.display
            })
            .collect();

        // Calculate total content height for scrollbar
        let content_height = lines.len() as u16;
        let visible_height = inner.height;

        // Apply scroll offset (clamp to valid range)
        let scroll = self.scroll_offset.min(content_height.saturating_sub(visible_height));
        self.effective_scroll = scroll;
        let paragraph = Paragraph::new(lines)
            .style(Style::default().bg(theme.background).fg(theme.text))
            .scroll((scroll, 0))
            .wrap(Wrap { trim: false });

        frame.render_widget(paragraph, inner);

        // Render scrollbar if content overflows
        if content_height > visible_height {
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(Some("▲"))
                .end_symbol(Some("▼"))
                .track_symbol(Some("│"))
                .thumb_symbol("█");

            let mut scrollbar_state = ScrollbarState::new(content_height.saturating_sub(visible_height) as usize)
                .position(scroll as usize);

            frame.render_stateful_widget(
                scrollbar,
                inner.inner(Margin { vertical: 1, horizontal: 0 }),
                &mut scrollbar_state,
            );
        }

        Ok(())
    }
}
