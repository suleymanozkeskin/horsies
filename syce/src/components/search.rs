use crate::{action::SearchMatch, errors::Result, state::SearchState, theme::Theme};
use ratatui::{
    prelude::*,
    widgets::{Block, BorderType, Borders, Clear, Padding, Paragraph},
};

/// Reusable search modal component
pub struct SearchModal<'a> {
    state: &'a SearchState,
    context_label: &'a str,
}

struct SearchLayout {
    main_width: usize,
    secondary_width: usize,
}

impl<'a> SearchModal<'a> {
    pub fn new(state: &'a SearchState, context_label: &'a str) -> Self {
        Self {
            state,
            context_label,
        }
    }

    /// Calculate centered rect for the search modal
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

    /// Render the search input field
    fn render_input(&self, theme: &Theme) -> Paragraph<'a> {
        // Build the input line with cursor
        let query = &self.state.query;
        let cursor_pos = self.state.cursor_position;

        let mut spans = vec![
            Span::styled("/  ", Style::default().fg(theme.accent).bold()),
        ];

        if query.is_empty() {
            spans.push(Span::styled(
                "Type to search...",
                Style::default().fg(theme.muted).italic(),
            ));
        } else {
            // Text before cursor
            if cursor_pos > 0 {
                spans.push(Span::styled(
                    &query[..cursor_pos],
                    Style::default().fg(theme.text),
                ));
            }
            // Cursor (block character or underscore)
            let cursor_char = if cursor_pos < query.len() {
                query.chars().nth(cursor_pos).unwrap_or(' ')
            } else {
                ' '
            };
            spans.push(Span::styled(
                cursor_char.to_string(),
                Style::default().fg(theme.background).bg(theme.accent),
            ));
            // Text after cursor
            if cursor_pos + 1 < query.len() {
                spans.push(Span::styled(
                    &query[cursor_pos + 1..],
                    Style::default().fg(theme.text),
                ));
            }
        }

        Paragraph::new(Line::from(spans))
    }

    /// Render a single search result line
    fn render_match_line(
        &self,
        search_match: &SearchMatch,
        is_selected: bool,
        theme: &Theme,
        layout: &SearchLayout,
    ) -> Line<'static> {
        let (prefix, main_text, secondary_text, status_text, status_color) = match search_match {
            SearchMatch::Worker {
                worker_id,
                hostname,
                status,
            } => {
                let color = Self::status_color(status, theme);
                (
                    "W",
                    worker_id.clone(),
                    hostname.clone(),
                    status.clone(),
                    color,
                )
            }
            SearchMatch::Task {
                task_id,
                worker_id,
                status,
            } => {
                let color = Self::status_color(status, theme);
                (
                    "T",
                    Self::truncate_id(task_id, 20),
                    worker_id.clone(),
                    status.clone(),
                    color,
                )
            }
            SearchMatch::Workflow {
                workflow_id,
                name,
                status,
            } => {
                let color = Self::status_color(status, theme);
                let display_name = if name.is_empty() {
                    "-".to_string()
                } else {
                    name.clone()
                };
                (
                    "F",
                    Self::truncate_id(workflow_id, 20),
                    display_name,
                    status.clone(),
                    color,
                )
            }
            SearchMatch::ModalLine {
                line_number,
                content,
            } => {
                let label = Self::modal_label(content, *line_number);
                (
                    "L",
                    label,
                    content.clone(),
                    String::new(),
                    theme.muted,
                )
            }
        };

        let bg_color = if is_selected {
            theme.surface_alt
        } else {
            theme.surface
        };

        let prefix_style = if is_selected {
            Style::default().fg(theme.accent).bg(bg_color).bold()
        } else {
            Style::default().fg(theme.muted).bg(bg_color)
        };

        let main_style = if is_selected {
            Style::default().fg(theme.text).bg(bg_color).bold()
        } else {
            Style::default().fg(theme.text).bg(bg_color)
        };

        let secondary_style = Style::default().fg(theme.muted).bg(bg_color);
        let status_style = Style::default().fg(status_color).bg(bg_color);

        let indicator = if is_selected { " > " } else { "   " };
        let main_fitted = Self::fit_to_width(&main_text, layout.main_width);
        let secondary_fitted = Self::fit_to_width(&secondary_text, layout.secondary_width);

        let mut spans = vec![
            Span::styled(indicator, prefix_style),
            Span::styled(format!("[{}] ", prefix), prefix_style),
            Span::styled(main_fitted, main_style),
            Span::styled(secondary_fitted, secondary_style),
        ];

        if !status_text.is_empty() {
            spans.push(Span::styled(status_text, status_style));
        }

        Line::from(spans)
    }

    fn status_color(status: &str, theme: &Theme) -> Color {
        match status.to_uppercase().as_str() {
            "PENDING" => Color::Yellow,
            "CLAIMED" => Color::Cyan,
            "RUNNING" => Color::Blue,
            "COMPLETED" => theme.success,
            "FAILED" => theme.error,
            "PAUSED" => Color::Magenta,
            "CANCELLED" => theme.muted,
            "IDLE" => theme.muted,
            _ => theme.text,
        }
    }

    fn truncate_id(id: &str, max_len: usize) -> String {
        if max_len == 0 {
            return String::new();
        }
        let char_count = id.chars().count();
        if char_count <= max_len {
            id.to_string()
        } else if max_len <= 3 {
            id.chars().take(max_len).collect()
        } else {
            let truncated: String = id.chars().take(max_len - 3).collect();
            format!("{}...", truncated)
        }
    }

    fn truncate_str(s: &str, max_len: usize) -> String {
        if max_len == 0 {
            return String::new();
        }
        let trimmed = s.trim();
        let char_count = trimmed.chars().count();
        if char_count <= max_len {
            trimmed.to_string()
        } else if max_len <= 3 {
            trimmed.chars().take(max_len).collect()
        } else {
            let truncated: String = trimmed.chars().take(max_len - 3).collect();
            format!("{}...", truncated)
        }
    }

    fn modal_dimensions(area: Rect) -> (u16, u16) {
        let percent_x = match area.width {
            0..=80 => 95,
            81..=120 => 88,
            121..=160 => 78,
            _ => 70,
        };
        let percent_y = match area.height {
            0..=24 => 90,
            25..=35 => 75,
            36..=50 => 65,
            _ => 55,
        };
        (percent_x, percent_y)
    }

    fn compute_layout(width: u16, matches: &[SearchMatch]) -> SearchLayout {
        let indicator_width = 3usize;
        let prefix_width = 4usize;
        let max_status = matches
            .iter()
            .map(|m| match m {
                SearchMatch::Worker { status, .. } => status.len(),
                SearchMatch::Task { status, .. } => status.len(),
                SearchMatch::Workflow { status, .. } => status.len(),
                SearchMatch::ModalLine { .. } => 0,
            })
            .max()
            .unwrap_or(0);

        let available = width as usize;
        let reserved = indicator_width + prefix_width + max_status;
        let available_for_text = available.saturating_sub(reserved);

        let min_main = 10usize;
        let min_secondary = 10usize;

        if available_for_text == 0 {
            return SearchLayout {
                main_width: 0,
                secondary_width: 0,
            };
        }

        let mut main_width = ((available_for_text as f32) * 0.45) as usize;
        if main_width < min_main {
            main_width = min_main.min(available_for_text);
        }

        let mut secondary_width = available_for_text.saturating_sub(main_width);
        if secondary_width < min_secondary && available_for_text > min_secondary {
            secondary_width = min_secondary;
            main_width = available_for_text.saturating_sub(secondary_width);
        }

        SearchLayout {
            main_width,
            secondary_width,
        }
    }

    fn fit_to_width(text: &str, width: usize) -> String {
        if width == 0 {
            return String::new();
        }
        let truncated = Self::truncate_str(text, width);
        format!("{:<width$}", truncated, width = width)
    }

    fn modal_label(content: &str, line_number: usize) -> String {
        Self::extract_modal_label(content).unwrap_or_else(|| format!("Line {}", line_number))
    }

    fn extract_modal_label(content: &str) -> Option<String> {
        let trimmed = content.trim_start();
        if let Some(rest) = trimmed.strip_prefix("Layer ") {
            let level = rest.split_whitespace().next()?;
            return Some(format!("Layer {}", level));
        }
        if let Some(rest) = trimmed.strip_prefix("Task ") {
            let task_id = rest.split_whitespace().next()?.trim_end_matches(':');
            return Some(format!("Task {}", task_id));
        }
        if trimmed.starts_with('[') {
            if let Some(end) = trimmed.find(']') {
                let inside = &trimmed[1..end];
                if !inside.is_empty() && inside.chars().all(|c| c.is_ascii_digit()) {
                    return Some(format!("Task {}", inside));
                }
            }
        }
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        let (percent_x, percent_y) = Self::modal_dimensions(area);
        let popup_area = Self::centered_rect(percent_x, percent_y, area);

        // Clear the background
        frame.render_widget(Clear, popup_area);

        // Main block
        let title = format!(" Search: {} ", self.context_label);
        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.accent))
            .border_type(BorderType::Rounded)
            .padding(Padding::horizontal(1))
            .style(Style::default().bg(theme.surface));

        let inner = block.inner(popup_area);
        frame.render_widget(block, popup_area);

        // Split inner area: input (1 line) + separator (1 line) + results + footer (1 line)
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // Input field
                Constraint::Length(1), // Separator
                Constraint::Min(1),    // Results
                Constraint::Length(1), // Footer
            ])
            .split(inner);

        // Render input field
        let input = self.render_input(theme);
        frame.render_widget(input, chunks[0]);

        // Render separator line
        let separator = Paragraph::new(Line::from(vec![Span::styled(
            "─".repeat(chunks[1].width as usize),
            Style::default().fg(theme.border),
        )]));
        frame.render_widget(separator, chunks[1]);

        // Render results
        if self.state.matches.is_empty() {
            let empty_msg = if self.state.has_query() {
                "No matches found"
            } else {
                "Start typing to search"
            };
            let empty = Paragraph::new(empty_msg)
                .style(Style::default().fg(theme.muted).italic())
                .alignment(Alignment::Center);
            frame.render_widget(empty, chunks[2]);
        } else {
            let max_visible = chunks[2].height as usize;
            let selected = self.state.selected_index;
            let layout = Self::compute_layout(chunks[2].width, &self.state.matches);

            // Calculate scroll offset to keep selected item visible
            let scroll_offset = if selected >= max_visible {
                selected - max_visible + 1
            } else {
                0
            };

            let visible_matches: Vec<Line> = self
                .state
                .matches
                .iter()
                .enumerate()
                .skip(scroll_offset)
                .take(max_visible)
                .map(|(idx, m)| self.render_match_line(m, idx == selected, theme, &layout))
                .collect();

            let results = Paragraph::new(visible_matches);
            frame.render_widget(results, chunks[2]);
        }

        // Render footer with hints
        let match_count = self.state.matches.len();
        let footer_text = format!(
            " {} match{}  |  {} navigate  Enter select  Esc close ",
            match_count,
            if match_count == 1 { "" } else { "es" },
            "\u{2191}\u{2193}", // ↑↓
        );
        let footer = Paragraph::new(footer_text)
            .style(Style::default().fg(theme.muted))
            .alignment(Alignment::Center);
        frame.render_widget(footer, chunks[3]);

        Ok(())
    }
}
