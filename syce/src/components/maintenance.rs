use crate::{
    action::Action,
    components::Component,
    errors::Result,
    state::AppState,
    theme::Theme,
};
use ratatui::{prelude::*, widgets::*};

pub struct Maintenance<'a> {
    state: &'a AppState,
}

impl<'a> Maintenance<'a> {
    pub fn new(state: &'a AppState) -> Self {
        Self { state }
    }

    /// Render the snapshot age distribution histogram
    fn render_snapshot_age_histogram(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let block = Block::default()
            .title("Snapshot Age Distribution")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.border))
            .style(Style::default().bg(theme.background));

        if self.state.snapshot_age_dist.is_empty() {
            let placeholder = Paragraph::new("No snapshot data available")
                .style(theme.muted_style())
                .alignment(Alignment::Center)
                .block(block);
            frame.render_widget(placeholder, area);
            return;
        }

        let inner = block.inner(area);
        frame.render_widget(block, area);

        // Split into two sections: table and chart
        let sections = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(40), // Left: Table with data
                Constraint::Percentage(60), // Right: Bar chart
            ])
            .split(inner);

        // Render table on the left
        self.render_age_table(frame, sections[0], theme);

        // Render bar chart on the right
        self.render_age_chart(frame, sections[1], theme);
    }

    /// Render table with snapshot age buckets
    fn render_age_table(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        let rows: Vec<Row> = self
            .state
            .snapshot_age_dist
            .iter()
            .map(|bucket| {
                Row::new(vec![
                    Cell::from(bucket.age_bucket.clone()),
                    Cell::from(format!("{}", bucket.snapshot_count)),
                    Cell::from(format!("{}", bucket.unique_workers)),
                ])
                .style(Style::default().fg(theme.text))
            })
            .collect();

        let widths = [
            Constraint::Percentage(40), // Age Bucket
            Constraint::Percentage(30), // Snapshots
            Constraint::Percentage(30), // Workers
        ];

        let table = Table::new(rows, widths)
            .header(
                Row::new(vec!["Age Bucket", "Snapshots", "Workers"])
                    .style(Style::default().fg(theme.accent).bold())
                    .bottom_margin(1),
            )
            .style(Style::default().bg(theme.background).fg(theme.text));

        frame.render_widget(table, area);
    }

    /// Render bar chart visualization
    fn render_age_chart(&self, frame: &mut Frame, area: Rect, theme: &Theme) {
        if self.state.snapshot_age_dist.is_empty() {
            return;
        }

        // Convert to (x, y) coordinates for chart
        let data: Vec<(f64, f64)> = self
            .state
            .snapshot_age_dist
            .iter()
            .enumerate()
            .map(|(i, bucket)| (i as f64, bucket.snapshot_count as f64))
            .collect();

        let max_count = self
            .state
            .snapshot_age_dist
            .iter()
            .map(|b| b.snapshot_count)
            .max()
            .unwrap_or(100) as f64;

        // Create dataset
        let dataset = Dataset::default()
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(theme.accent))
            .data(&data);

        // Create X-axis labels from age buckets
        let x_labels: Vec<Span> = self
            .state
            .snapshot_age_dist
            .iter()
            .map(|b| Span::raw(b.age_bucket.clone()))
            .collect();

        let x_axis = Axis::default()
            .title("Age")
            .style(Style::default().fg(theme.text))
            .bounds([0.0, (self.state.snapshot_age_dist.len().saturating_sub(1)) as f64])
            .labels(x_labels);

        let y_max = (max_count * 1.2).max(10.0);
        let y_axis = Axis::default()
            .title("Count")
            .style(Style::default().fg(theme.text))
            .bounds([0.0, y_max])
            .labels(vec![
                Span::raw("0"),
                Span::raw(format!("{:.0}", y_max / 2.0)),
                Span::raw(format!("{:.0}", y_max)),
            ]);

        let chart = Chart::new(vec![dataset])
            .x_axis(x_axis)
            .y_axis(y_axis);

        frame.render_widget(chart, area);
    }
}

impl<'a> Component for Maintenance<'a> {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        self.render_snapshot_age_histogram(frame, area, theme);
        Ok(())
    }
}
