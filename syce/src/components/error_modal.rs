use crate::{action::Action, components::Component, errors::Result, theme::Theme};
use ratatui::{
    prelude::*,
    widgets::{Block, BorderType, Borders, Clear, Padding, Paragraph, Wrap},
};
use std::collections::HashMap;

use crate::action::DataSource;

pub struct ErrorModal<'a> {
    errors: &'a HashMap<DataSource, String>,
}

impl<'a> ErrorModal<'a> {
    pub fn new(errors: &'a HashMap<DataSource, String>) -> Self {
        Self { errors }
    }

    /// Calculate centered rect for the modal
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

}

impl<'a> Component for ErrorModal<'a> {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        // Size based on number of errors
        let height = (self.errors.len() * 4 + 4).min(60) as u16;
        let popup_area = Self::centered_rect(70, height.max(20), area);

        // Clear the background
        frame.render_widget(Clear, popup_area);

        let error_count = self.errors.len();
        let title = if error_count == 1 {
            " Error - Esc: close | y: copy | c: clear ".to_string()
        } else {
            format!(" {} Errors - Esc: close | y: copy | c: clear ", error_count)
        };

        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.error))
            .border_type(BorderType::Rounded)
            .padding(Padding::uniform(1))
            .style(Style::default().bg(theme.surface));

        let inner = block.inner(popup_area);
        frame.render_widget(block, popup_area);

        // Build content lines
        let mut lines: Vec<Line> = Vec::new();

        for (i, (source, message)) in self.errors.iter().enumerate() {
            if i > 0 {
                lines.push(Line::from(""));
            }

            // Source header
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{}", source),
                    Style::default().fg(theme.error).bold(),
                ),
            ]));

            // Error message (may be multiline)
            for msg_line in message.lines() {
                lines.push(Line::from(vec![
                    Span::styled("  ", Style::default()),
                    Span::styled(msg_line.to_string(), Style::default().fg(theme.text)),
                ]));
            }
        }

        let paragraph = Paragraph::new(lines)
            .style(Style::default().bg(theme.background).fg(theme.text))
            .wrap(Wrap { trim: false });

        frame.render_widget(paragraph, inner);

        Ok(())
    }
}
