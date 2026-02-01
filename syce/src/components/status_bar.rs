use crate::{
    action::Action,
    components::Component,
    errors::Result,
    state::AppState,
    theme::Theme,
};
use ratatui::{prelude::*, widgets::*};

pub struct StatusBar<'a> {
    state: &'a AppState,
}

impl<'a> StatusBar<'a> {
    pub fn new(state: &'a AppState) -> Self {
        Self { state }
    }

    /// Build the left section (tab indicator + loading status)
    fn build_left_section(&self, theme: &Theme) -> Line<'static> {
        let tab_name = format!(" {} ", self.state.current_tab);

        let mut spans = vec![
            Span::styled(tab_name, theme.accent_style().bold()),
            Span::raw(" "),
        ];

        // Show loading indicators
        let loading_sources: Vec<String> = self
            .state
            .loading
            .iter()
            .filter(|(_, &is_loading)| is_loading)
            .map(|(source, _)| format!("{}", source))
            .collect();

        if !loading_sources.is_empty() {
            spans.push(Span::styled(
                "⟳ ",
                Style::default().fg(theme.accent),
            ));
            spans.push(Span::styled(
                format!("Loading: {}", loading_sources.join(", ")),
                theme.muted_style(),
            ));
        }

        Line::from(spans)
    }

    /// Build the center section (error indicator - just shows there's an error)
    fn build_center_section(&self, theme: &Theme) -> Option<Line<'static>> {
        let error_count = self.state.errors.len();
        if error_count > 0 {
            let msg = if error_count == 1 {
                "1 error".to_string()
            } else {
                format!("{} errors", error_count)
            };
            let spans = vec![
                Span::styled("✖ ", Style::default().fg(theme.error)),
                Span::styled(msg, Style::default().fg(theme.error).bold()),
                Span::styled(" (press ", theme.muted_style()),
                Span::styled("e", Style::default().fg(theme.error).bold()),
                Span::styled(" to view)", theme.muted_style()),
            ];
            return Some(Line::from(spans));
        }
        None
    }

    /// Build the right section (keybinding hints)
    fn build_right_section(&self, theme: &Theme) -> Line<'static> {
        let hints = vec![
            ("/", "search"),
            ("1-5", "tabs"),
            ("t", "theme"),
            ("q", "quit"),
        ];

        let mut spans = Vec::new();
        for (i, (key, desc)) in hints.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw(" │ "));
            }
            spans.push(Span::styled(*key, theme.accent_style().bold()));
            spans.push(Span::raw(":"));
            spans.push(Span::styled(*desc, theme.muted_style()));
        }
        spans.push(Span::raw(" "));

        Line::from(spans)
    }
}

impl<'a> Component for StatusBar<'a> {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        // Split area into three sections: left, center, right
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(30),  // Left: tab + loading
                Constraint::Min(0),      // Center: errors (takes remaining space)
                Constraint::Length(42),  // Right: keybindings
            ])
            .split(area);

        // Render left section
        let left = self.build_left_section(theme);
        let left_paragraph = Paragraph::new(left)
            .style(Style::default().bg(theme.background).fg(theme.text))
            .alignment(Alignment::Left);
        frame.render_widget(left_paragraph, chunks[0]);

        // Render center section (errors)
        if let Some(error_line) = self.build_center_section(theme) {
            let center_paragraph = Paragraph::new(error_line)
                .style(Style::default().bg(theme.background))
                .alignment(Alignment::Center);
            frame.render_widget(center_paragraph, chunks[1]);
        }

        // Render right section
        let right = self.build_right_section(theme);
        let right_paragraph = Paragraph::new(right)
            .style(Style::default().bg(theme.background).fg(theme.text))
            .alignment(Alignment::Right);
        frame.render_widget(right_paragraph, chunks[2]);

        Ok(())
    }
}
