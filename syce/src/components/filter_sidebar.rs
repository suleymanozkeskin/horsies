use crate::{
    action::TaskStatus,
    models::FilterValue,
    state::{AppState, SidebarSection},
    theme::Theme,
};
use ratatui::{prelude::*, widgets::*};
use std::collections::HashSet;

/// Render the shared filter sidebar used in both Layer 1 (aggregation) and Layer 2 (task list).
/// When `show_value_filters` is true, distinct task_name/queue/error lists are shown (Layer 2).
pub fn render(
    frame: &mut Frame,
    area: Rect,
    state: &AppState,
    theme: &Theme,
    show_value_filters: bool,
) {
    let block = Block::default()
        .title(" Filters ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.border))
        .style(Style::default().bg(theme.background));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let mut lines: Vec<Line> = Vec::new();
    let w = inner.width as usize;

    // ── Status ──
    lines.push(section_header("Status", theme));
    lines.push(Line::from(""));

    let filter = &state.task_status_filter;
    for status in TaskStatus::all() {
        let is_selected = filter.is_selected(&status);
        let key = match status {
            TaskStatus::Pending => "p",
            TaskStatus::Claimed => "c",
            TaskStatus::Running => "r",
            TaskStatus::Completed => "o",
            TaskStatus::Failed => "f",
            TaskStatus::Cancelled => "x",
            TaskStatus::Expired => "e",
        };

        let color = status_color(&status, theme);
        if is_selected {
            lines.push(Line::from(vec![
                Span::styled("  ", Style::default()),
                Span::styled(format!("[{}]", key), Style::default().fg(color).bold()),
                Span::styled(format!(" {}", status.label()), Style::default().fg(theme.text)),
            ]));
        } else {
            lines.push(Line::from(vec![
                Span::styled("  ", Style::default()),
                Span::styled(format!("[{}] {}", key, status.label()), Style::default().fg(theme.muted)),
            ]));
        }
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("  [a]ll  [n]one", Style::default().fg(theme.muted)),
    ]));

    // ── Retried ──
    lines.push(Line::from(""));
    let retried_style = if state.retried_only_filter {
        Style::default().fg(theme.background).bg(Color::Yellow).bold()
    } else {
        Style::default().fg(theme.muted)
    };
    lines.push(Line::from(vec![
        Span::styled("  ", Style::default()),
        Span::styled("[i] Retried", retried_style),
    ]));

    // ── Distinct value sections (Layer 2 only) ──
    if show_value_filters {
        render_value_section(
            &mut lines, "m", "Tasks",
            &state.distinct_task_names,
            &state.selected_task_names,
            state.sidebar_section == SidebarSection::TaskNames,
            state.sidebar_cursor, theme, w,
        );

        render_value_section(
            &mut lines, "u", "Queues",
            &state.distinct_queues,
            &state.selected_queues,
            state.sidebar_section == SidebarSection::Queues,
            state.sidebar_cursor, theme, w,
        );

        render_value_section(
            &mut lines, "d", "Errors",
            &state.distinct_errors,
            &state.selected_errors,
            state.sidebar_section == SidebarSection::Errors,
            state.sidebar_cursor, theme, w,
        );
    }

    let paragraph = Paragraph::new(lines);
    frame.render_widget(paragraph, inner);
}

fn section_header<'a>(label: &str, theme: &Theme) -> Line<'a> {
    Line::from(Span::styled(
        format!(" {}", label),
        Style::default().fg(theme.accent).bold(),
    ))
}

fn render_value_section(
    lines: &mut Vec<Line<'_>>,
    key: &str,
    label: &str,
    values: &[FilterValue],
    selected: &HashSet<String>,
    is_focused: bool,
    cursor: usize,
    theme: &Theme,
    max_width: usize,
) {
    if values.is_empty() {
        return;
    }

    // Separator + header
    lines.push(Line::from(""));

    let header_style = if is_focused {
        Style::default().fg(theme.accent).bold()
    } else {
        Style::default().fg(theme.accent)
    };

    let badge = if selected.is_empty() {
        String::new()
    } else {
        format!(" {}", selected.len())
    };

    lines.push(Line::from(vec![
        Span::styled(format!(" [{}] {}", key, label), header_style),
        Span::styled(badge, Style::default().fg(Color::Yellow).bold()),
    ]));
    lines.push(Line::from(""));

    // Layout: "  ● name  count"
    // Overhead: indent(2) + bullet(1) + space(1) + gap(1) = 5 fixed chars before name
    // Count gets up to 6 chars + 1 space separator
    let count_reserve = 7;
    let name_width = max_width.saturating_sub(5 + count_reserve);

    for (i, fv) in values.iter().enumerate() {
        let is_selected = selected.contains(&fv.value);
        let is_cursor = is_focused && i == cursor;

        let bullet = if is_selected { "●" } else { "○" };

        let name = if fv.value.len() > name_width {
            format!("{}..", &fv.value[..name_width.saturating_sub(2)])
        } else {
            fv.value.clone()
        };

        let count_str = format!("{}", fv.count);
        // Pad between name and count to right-align
        let used = name.len() + count_str.len();
        let available = name_width + count_reserve;
        let pad = available.saturating_sub(used);

        let style = if is_cursor {
            Style::default().fg(theme.background).bg(theme.accent).bold()
        } else if is_selected {
            Style::default().fg(theme.text).bold()
        } else {
            Style::default().fg(theme.muted)
        };

        let count_style = if is_cursor {
            style
        } else {
            Style::default().fg(theme.muted)
        };

        lines.push(Line::from(vec![
            Span::styled(format!("  {} ", bullet), style),
            Span::styled(name, style),
            Span::styled(" ".repeat(pad), Style::default()),
            Span::styled(count_str, count_style),
        ]));
    }
}

fn status_color(status: &TaskStatus, theme: &Theme) -> Color {
    match status {
        TaskStatus::Pending => Color::Yellow,
        TaskStatus::Claimed => Color::Cyan,
        TaskStatus::Running => Color::Blue,
        TaskStatus::Completed => theme.success,
        TaskStatus::Failed => theme.error,
        TaskStatus::Cancelled => theme.muted,
        TaskStatus::Expired => Color::DarkGray,
    }
}
