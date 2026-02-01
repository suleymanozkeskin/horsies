use crate::{action::Action, components::Component, errors::Result, theme::Theme};
use ratatui::{prelude::*, widgets::*};

#[derive(Default)]
pub struct Home;

impl Home {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Component for Home {
    fn update(&mut self, action: Action) -> Result<Option<Action>> {
        match action {
            Action::Tick => {
                // add any logic here that should run on every tick
            }
            Action::Render => {
                // add any logic here that should run on every render
            }
            _ => {}
        }
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        let title = Line::from(vec![
            Span::styled("syce", theme.accent_style()),
            Span::raw(" "),
            Span::styled(format!("({})", theme.flavor.label()), theme.muted_style()),
        ]);

        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        let body = vec![
            Line::styled(
                "Monitoring TUI coming soon.",
                Style::default().fg(theme.text),
            ),
            Line::styled("Press t to cycle Catppuccin flavors.", theme.muted_style()),
        ];

        let paragraph = Paragraph::new(body)
            .style(Style::default().fg(theme.text).bg(theme.background))
            .block(block)
            .alignment(Alignment::Left)
            .wrap(Wrap { trim: true });
        frame.render_widget(paragraph, area);
        Ok(())
    }
}
