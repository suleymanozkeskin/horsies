use crate::{action::Action, components::Component, errors::Result, theme::Theme};
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

pub struct HelpOverlay;

impl HelpOverlay {
    pub fn new() -> Self {
        Self
    }

    /// Calculate centered rect for the help modal
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

impl Component for HelpOverlay {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        let popup_area = Self::centered_rect(60, 70, area);

        // Clear the background
        frame.render_widget(Clear, popup_area);

        let block = Block::default()
            .title(" Help - Press ? or Esc to close ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.accent))
            .style(Style::default().bg(theme.background));

        let inner = block.inner(popup_area);
        frame.render_widget(block, popup_area);

        let help_text = vec![
            Line::from(vec![
                Span::styled("Global", Style::default().fg(theme.accent).bold()),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("  /        ", Style::default().fg(theme.warning)),
                Span::raw("Search (context-aware)"),
            ]),
            Line::from(vec![
                Span::styled("  ?        ", Style::default().fg(theme.warning)),
                Span::raw("Toggle this help"),
            ]),
            Line::from(vec![
                Span::styled("  q, Esc   ", Style::default().fg(theme.warning)),
                Span::raw("Quit application"),
            ]),
            Line::from(vec![
                Span::styled("  r        ", Style::default().fg(theme.warning)),
                Span::raw("Refresh current tab"),
            ]),
            Line::from(vec![
                Span::styled("  t        ", Style::default().fg(theme.warning)),
                Span::raw("Cycle theme"),
            ]),
            Line::from(vec![
                Span::styled("  Ctrl+z   ", Style::default().fg(theme.warning)),
                Span::raw("Suspend to background"),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("Tab Navigation", Style::default().fg(theme.accent).bold()),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("  1        ", Style::default().fg(theme.warning)),
                Span::raw("Dashboard"),
            ]),
            Line::from(vec![
                Span::styled("  2        ", Style::default().fg(theme.warning)),
                Span::raw("Workers"),
            ]),
            Line::from(vec![
                Span::styled("  3        ", Style::default().fg(theme.warning)),
                Span::raw("Tasks"),
            ]),
            Line::from(vec![
                Span::styled("  4        ", Style::default().fg(theme.warning)),
                Span::raw("Workflows"),
            ]),
            Line::from(vec![
                Span::styled("  5        ", Style::default().fg(theme.warning)),
                Span::raw("Maintenance"),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("Workers Tab", Style::default().fg(theme.accent).bold()),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("  Up/Down  ", Style::default().fg(theme.warning)),
                Span::raw("Select worker"),
            ]),
            Line::from(vec![
                Span::styled("  [ ]      ", Style::default().fg(theme.warning)),
                Span::raw("Cycle time window (5m/30m/1h/6h/24h)"),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("Tasks Tab", Style::default().fg(theme.accent).bold()),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("  Up/Down  ", Style::default().fg(theme.warning)),
                Span::raw("Select worker / task ID"),
            ]),
            Line::from(vec![
                Span::styled("  Enter    ", Style::default().fg(theme.warning)),
                Span::raw("Expand row / view details"),
            ]),
            Line::from(vec![
                Span::styled("  Esc      ", Style::default().fg(theme.warning)),
                Span::raw("Collapse expanded row"),
            ]),
            Line::from(vec![
                Span::styled("  y        ", Style::default().fg(theme.warning)),
                Span::raw("Copy task JSON to clipboard (in detail view)"),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("  Filter Keys", Style::default().fg(theme.accent).bold()),
            ]),
            Line::from(vec![
                Span::styled("  p c u o f", Style::default().fg(theme.warning)),
                Span::raw("Toggle Pending/Claimed/rUnning/cOmpleted/Failed"),
            ]),
            Line::from(vec![
                Span::styled("  a        ", Style::default().fg(theme.warning)),
                Span::raw("Select all statuses"),
            ]),
            Line::from(vec![
                Span::styled("  n        ", Style::default().fg(theme.warning)),
                Span::raw("Clear all statuses"),
            ]),
        ];

        let paragraph = Paragraph::new(help_text)
            .style(Style::default().bg(theme.background).fg(theme.text))
            .wrap(Wrap { trim: false });

        frame.render_widget(paragraph, inner);

        Ok(())
    }
}
