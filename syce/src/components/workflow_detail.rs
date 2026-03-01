use std::borrow::Cow;

use crate::{
    action::Action,
    components::Component,
    errors::Result,
    models::{WorkflowRow, WorkflowTaskRow},
    state::SearchHighlight,
    theme::Theme,
};
use ratatui::{
    prelude::*,
    widgets::{Block, BorderType, Borders, Clear, Padding, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
};

pub struct WorkflowDetailPanel<'a> {
    workflow: &'a WorkflowRow,
    tasks: &'a [WorkflowTaskRow],
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

    fn with_search_text(display: Line<'static>, search_text: String) -> Self {
        let display_text = line_to_string(&display);
        Self {
            display,
            search_text,
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

impl<'a> WorkflowDetailPanel<'a> {
    pub fn new(workflow: &'a WorkflowRow, tasks: &'a [WorkflowTaskRow], scroll_offset: u16, highlight: Option<&'a SearchHighlight>) -> Self {
        Self {
            workflow,
            tasks,
            highlight,
            scroll_offset,
            effective_scroll: 0,
        }
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
        if status.eq_ignore_ascii_case("PENDING") || status.eq_ignore_ascii_case("READY") {
            Color::Yellow
        } else if status.eq_ignore_ascii_case("ENQUEUED") {
            Color::Cyan
        } else if status.eq_ignore_ascii_case("RUNNING") {
            Color::Blue
        } else if status.eq_ignore_ascii_case("COMPLETED") {
            theme.success
        } else if status.eq_ignore_ascii_case("FAILED") {
            theme.error
        } else if status.eq_ignore_ascii_case("SKIPPED") {
            theme.muted
        } else if status.eq_ignore_ascii_case("PAUSED") {
            Color::Magenta
        } else if status.eq_ignore_ascii_case("CANCELLED") {
            theme.muted
        } else {
            theme.text
        }
    }

    /// Calculate topological level for each task (depth in DAG)
    fn calculate_levels(&self) -> Vec<(usize, &WorkflowTaskRow)> {
        use std::collections::HashMap;

        // Build index map for quick lookup
        let index_map: HashMap<i32, usize> = self
            .tasks
            .iter()
            .enumerate()
            .map(|(i, t)| (t.task_index, i))
            .collect();

        // Calculate level for each task
        let mut levels: Vec<usize> = vec![0; self.tasks.len()];

        // Iterate until no changes (handles any DAG structure)
        let mut changed = true;
        while changed {
            changed = false;
            for (i, task) in self.tasks.iter().enumerate() {
                if let Some(deps) = &task.dependencies {
                    for dep_idx in deps {
                        if let Some(&dep_pos) = index_map.get(dep_idx) {
                            let new_level = levels[dep_pos] + 1;
                            if new_level > levels[i] {
                                levels[i] = new_level;
                                changed = true;
                            }
                        }
                    }
                }
            }
        }

        // Pair levels with tasks
        self.tasks.iter().enumerate().map(|(i, t)| (levels[i], t)).collect()
    }

    /// Get status icon for a task
    fn status_icon(status: &str) -> &'static str {
        match status {
            "COMPLETED" => "✓",
            "FAILED" => "✗",
            "RUNNING" => "▶",
            "PENDING" | "READY" => "○",
            "ENQUEUED" => "◎",
            "SKIPPED" => "⊘",
            _ => "?",
        }
    }

    /// Build ASCII DAG visualization with level-based grouping
    fn build_dag_lines(&self, theme: &Theme) -> Vec<DetailLine> {
        let mut lines = Vec::new();

        if self.tasks.is_empty() {
            lines.push(DetailLine::new(Line::from(vec![Span::styled(
                "  No tasks in workflow",
                Style::default().fg(theme.muted),
            )])));
            return lines;
        }

        // Calculate levels and group tasks
        let task_levels = self.calculate_levels();
        let max_level = task_levels.iter().map(|(l, _)| *l).max().unwrap_or(0);

        // Group tasks by level
        let mut levels_grouped: Vec<Vec<&WorkflowTaskRow>> = vec![Vec::new(); max_level + 1];
        for (level, task) in &task_levels {
            levels_grouped[*level].push(*task);
        }

        // Output task index for marking with ★
        let output_idx = self.workflow.output_task_index;

        // Render each level
        for (level, tasks_at_level) in levels_grouped.iter().enumerate() {
            if tasks_at_level.is_empty() {
                continue;
            }

            // Calculate layer duration from task timestamps
            let layer_start = tasks_at_level
                .iter()
                .filter_map(|t| t.started_at)
                .min();
            let layer_end = tasks_at_level
                .iter()
                .filter_map(|t| t.completed_at)
                .max();
            let layer_duration = match (layer_start, layer_end) {
                (Some(start), Some(end)) => {
                    let millis = (end - start).num_milliseconds();
                    if millis < 1000 {
                        format!(" \u{1F551} {}ms", millis)
                    } else {
                        let secs = millis / 1000;
                        if secs < 60 {
                            format!(" \u{1F551} {}s", secs)
                        } else if secs < 3600 {
                            format!(" \u{1F551} {}m{}s", secs / 60, secs % 60)
                        } else {
                            format!(" \u{1F551} {}h{}m", secs / 3600, (secs % 3600) / 60)
                        }
                    }
                }
                _ => String::new(), // No duration if layer hasn't completed
            };

            // Level header
            let parallel_hint = if tasks_at_level.len() > 1 {
                format!(" ({} parallel)", tasks_at_level.len())
            } else {
                String::new()
            };

            lines.push(DetailLine::new(Line::from(vec![
                Span::styled(
                    format!("  Layer {} ", level),
                    Style::default().fg(theme.accent).bold(),
                ),
                Span::styled(
                    format!("───────────────────{}", parallel_hint),
                    Style::default().fg(theme.muted),
                ),
                Span::styled(
                    layer_duration,
                    Style::default().fg(theme.text),
                ),
            ])));

            // Render tasks at this level
            for task in tasks_at_level {
                let status_color = Self::status_color(&task.status, theme);
                let status_icon = Self::status_icon(&task.status);

                // Check if this is the output task
                let is_output = output_idx == Some(task.task_index);
                let output_marker = if is_output { " ★" } else { "" };

                // Main task line
                let task_line = Line::from(vec![
                    Span::styled(
                        format!("    [{}] ", task.task_index),
                        Style::default().fg(theme.accent),
                    ),
                    Span::styled(status_icon, Style::default().fg(status_color).bold()),
                    Span::styled(" ", Style::default()),
                    Span::styled(task.task_name.clone(), Style::default().fg(theme.text).bold()),
                    Span::styled(output_marker, Style::default().fg(Color::Yellow).bold()),
                ]);
                let mut task_search_text = format!("Task {}", task.task_index);
                task_search_text.push(' ');
                task_search_text.push_str(&line_to_string(&task_line));
                if let Some(task_id) = task.task_id.as_deref() {
                    task_search_text.push(' ');
                    task_search_text.push_str(task_id);
                }
                task_search_text.push(' ');
                task_search_text.push_str(&task.status);
                lines.push(DetailLine::with_search_text(task_line, task_search_text));

                // Dependencies line (if any)
                if let Some(deps) = &task.dependencies {
                    if !deps.is_empty() {
                        let deps_str = deps
                            .iter()
                            .map(|d| d.to_string())
                            .collect::<Vec<_>>()
                            .join(", ");

                        // Join type info
                        let join_info = match task.join_type.as_str() {
                            "any" => {
                                format!("  join: ANY (need 1/{} success)", deps.len())
                            }
                            "quorum" => {
                                let min = task.min_success.unwrap_or(1);
                                format!("  join: QUORUM ({}/{})", min, deps.len())
                            }
                            _ => String::new(), // "all" is default
                        };

                        lines.push(DetailLine::new(Line::from(vec![
                            Span::styled("        ◀── ", Style::default().fg(theme.muted)),
                            Span::styled(format!("deps: [{}]", deps_str), Style::default().fg(theme.muted)),
                            Span::styled(join_info, Style::default().fg(Color::Cyan)),
                        ])));
                    }
                }

                // Context sources (if different from deps or notable)
                if let Some(ctx_from) = &task.workflow_ctx_from {
                    if !ctx_from.is_empty() {
                        // Shorten if too many
                        let ctx_display = if ctx_from.len() > 3 {
                            format!("[{}, ... +{}]", ctx_from[..2].join(", "), ctx_from.len() - 2)
                        } else {
                            format!("[{}]", ctx_from.join(", "))
                        };
                        lines.push(DetailLine::new(Line::from(vec![
                            Span::styled("        ctx: ", Style::default().fg(theme.muted)),
                            Span::styled(ctx_display, Style::default().fg(Color::Magenta)),
                        ])));
                    }
                }

                // Args from (data flow)
                if let Some(args_from) = &task.args_from {
                    if !args_from.is_null() && args_from.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
                        let args_str = serde_json::to_string(args_from).unwrap_or_default();
                        // Truncate if too long
                        let display = if args_str.len() > 50 {
                            format!("{}...", &args_str[..47])
                        } else {
                            args_str
                        };
                        lines.push(DetailLine::new(Line::from(vec![
                            Span::styled("        args: ", Style::default().fg(theme.muted)),
                            Span::styled(display, Style::default().fg(Color::Cyan)),
                        ])));
                    }
                }

                // Annotations (allow_failed_deps, non-default priority)
                let mut annotations = Vec::new();
                if task.allow_failed_deps {
                    annotations.push("allow_failed".to_string());
                }
                if task.priority != 100 {
                    annotations.push(format!("pri:{}", task.priority));
                }
                if !annotations.is_empty() {
                    lines.push(DetailLine::new(Line::from(vec![
                        Span::styled("        ", Style::default()),
                        Span::styled(
                            format!("({})", annotations.join(", ")),
                            Style::default().fg(theme.muted).italic(),
                        ),
                    ])));
                }

                // Status details for terminal states
                if task.status == "COMPLETED" {
                    lines.push(DetailLine::new(Line::from(vec![
                        Span::styled("        ", Style::default()),
                        Span::styled("status: ", Style::default().fg(theme.muted)),
                        Span::styled("COMPLETED", Style::default().fg(theme.success)),
                    ])));
                } else if task.status == "FAILED" {
                    // Extract error from result field (backend writes error info there, not to error column)
                    let error_summary = task.result.as_ref().and_then(|r| {
                        serde_json::from_str::<serde_json::Value>(r).ok().and_then(|v| {
                            v.get("err").and_then(|err| {
                                let code = err.get("error_code").and_then(|c| c.as_str());
                                let msg = err.get("message").and_then(|m| m.as_str());
                                match (code, msg) {
                                    (Some(c), Some(m)) => Some(format!("{}: {}", c, m)),
                                    (Some(c), None) => Some(c.to_string()),
                                    (None, Some(m)) => Some(m.to_string()),
                                    (None, None) => None,
                                }
                            })
                        })
                    }).or_else(|| task.error.clone());

                    let display = match &error_summary {
                        Some(s) => {
                            if s.len() > 60 {
                                format!("{}...", &s[..57])
                            } else {
                                s.clone()
                            }
                        }
                        None => "FAILED".to_string(),
                    };
                    lines.push(DetailLine::new(Line::from(vec![
                        Span::styled("        ", Style::default()),
                        Span::styled("error: ", Style::default().fg(theme.error)),
                        Span::styled(display, Style::default().fg(theme.error)),
                    ])));
                } else if task.status == "RUNNING" {
                    lines.push(DetailLine::new(Line::from(vec![
                        Span::styled("        ", Style::default()),
                        Span::styled("status: ", Style::default().fg(theme.muted)),
                        Span::styled("RUNNING", Style::default().fg(Color::Blue).bold()),
                    ])));
                } else if task.status == "SKIPPED" {
                    lines.push(DetailLine::new(Line::from(vec![
                        Span::styled("        ", Style::default()),
                        Span::styled("status: ", Style::default().fg(theme.muted)),
                        Span::styled("SKIPPED", Style::default().fg(theme.muted)),
                    ])));
                } else if task.status == "ENQUEUED" || task.status == "READY" {
                    let color = Self::status_color(&task.status, theme);
                    lines.push(DetailLine::new(Line::from(vec![
                        Span::styled("        ", Style::default()),
                        Span::styled("status: ", Style::default().fg(theme.muted)),
                        Span::styled(task.status.clone(), Style::default().fg(color)),
                    ])));
                }
            }

            // Show flow indicator between levels (except after last level)
            if level < max_level {
                let next_level_has_deps = levels_grouped
                    .get(level + 1)
                    .map(|tasks| {
                        tasks.iter().any(|t| {
                            t.dependencies
                                .as_ref()
                                .map(|d| !d.is_empty())
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false);

                if next_level_has_deps {
                    lines.push(DetailLine::new(Line::from(vec![
                        Span::styled("        │", Style::default().fg(theme.muted)),
                    ])));
                    lines.push(DetailLine::new(Line::from(vec![
                        Span::styled("        ▼", Style::default().fg(theme.muted)),
                    ])));
                }
            }
        }

        lines
    }

    /// Format success policy for display
    fn format_success_policy(&self) -> Option<String> {
        self.workflow.success_policy.as_ref().map(|policy| {
            serde_json::to_string(policy).unwrap_or_else(|_| "invalid".to_string())
        })
    }

    fn build_detail_lines(&self, theme: &Theme) -> Vec<DetailLine> {
        let mut lines: Vec<DetailLine> = Vec::new();

        // Header section
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Name:         ", Style::default().fg(theme.muted)),
            Span::styled(
                self.workflow.name.clone(),
                Style::default().fg(theme.accent).bold(),
            ),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("ID:           ", Style::default().fg(theme.muted)),
            Span::styled(self.workflow.id.clone(), Style::default().fg(theme.text)),
        ])));

        let status_color = Self::status_color(&self.workflow.status, theme);
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Status:       ", Style::default().fg(theme.muted)),
            Span::styled(
                self.workflow.status.clone(),
                Style::default().fg(status_color).bold(),
            ),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("On Error:     ", Style::default().fg(theme.muted)),
            Span::styled(
                self.workflow.on_error.clone(),
                Style::default().fg(theme.text),
            ),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Progress:     ", Style::default().fg(theme.muted)),
            Span::styled(
                self.workflow.progress_str(),
                Style::default().fg(theme.text),
            ),
            Span::styled(
                format!(" ({:.0}%)", self.workflow.progress_pct() * 100.0),
                Style::default().fg(theme.muted),
            ),
        ])));

        // Show success rate separately for clarity
        let success_color = if self.workflow.success_pct() >= 1.0 {
            theme.success
        } else if self.workflow.success_pct() >= 0.5 {
            Color::Yellow
        } else {
            theme.error
        };
        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("Success:      ", Style::default().fg(theme.muted)),
            Span::styled(
                self.workflow.success_str(),
                Style::default().fg(success_color),
            ),
            Span::styled(
                format!(" ({:.0}%)", self.workflow.success_pct() * 100.0),
                Style::default().fg(theme.muted),
            ),
        ])));

        // Show output task index if specified
        if let Some(output_idx) = self.workflow.output_task_index {
            lines.push(DetailLine::new(Line::from(vec![
                Span::styled("Output Task:  ", Style::default().fg(theme.muted)),
                Span::styled(
                    format!("[{}]", output_idx),
                    Style::default().fg(theme.accent),
                ),
            ])));
        }

        // Show success policy if specified
        if let Some(policy_str) = self.format_success_policy() {
            lines.push(DetailLine::new(Line::from(vec![
                Span::styled("Success Policy: ", Style::default().fg(theme.muted)),
                Span::styled(policy_str, Style::default().fg(Color::Cyan)),
            ])));
        }

        lines.push(DetailLine::new(Line::from("")));

        // Timeline section
        lines.push(DetailLine::new(Line::from(vec![Span::styled(
            "Timeline",
            Style::default().fg(theme.accent).bold(),
        )])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Sent:       ", Style::default().fg(theme.muted)),
            Span::styled(
                self.workflow.sent_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                Style::default().fg(theme.text),
            ),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Created(DB):", Style::default().fg(theme.muted)),
            Span::styled(
                self.workflow.created_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                Style::default().fg(theme.text),
            ),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Started:    ", Style::default().fg(theme.muted)),
            Span::styled(
                Self::format_timestamp(&self.workflow.started_at),
                Style::default().fg(theme.text),
            ),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Completed:  ", Style::default().fg(theme.muted)),
            Span::styled(
                Self::format_timestamp(&self.workflow.completed_at),
                Style::default().fg(theme.text),
            ),
        ])));

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Duration:   ", Style::default().fg(theme.muted)),
            Span::styled(
                self.workflow.duration_str(),
                Style::default().fg(theme.text),
            ),
        ])));

        lines.push(DetailLine::new(Line::from("")));

        // Task counts section
        lines.push(DetailLine::new(Line::from(vec![Span::styled(
            "Task Counts",
            Style::default().fg(theme.accent).bold(),
        )])));

        let total = self.workflow.total_tasks.unwrap_or(0);
        let completed = self.workflow.completed_tasks.unwrap_or(0);
        let running = self.workflow.running_tasks.unwrap_or(0);
        let failed = self.workflow.failed_tasks.unwrap_or(0);
        let pending = self.workflow.pending_tasks.unwrap_or(0);

        lines.push(DetailLine::new(Line::from(vec![
            Span::styled("  Total: ", Style::default().fg(theme.muted)),
            Span::styled(format!("{}", total), Style::default().fg(theme.text)),
            Span::styled("  Completed: ", Style::default().fg(theme.muted)),
            Span::styled(format!("{}", completed), Style::default().fg(theme.success)),
            Span::styled("  Running: ", Style::default().fg(theme.muted)),
            Span::styled(format!("{}", running), Style::default().fg(Color::Blue)),
            Span::styled("  Failed: ", Style::default().fg(theme.muted)),
            Span::styled(format!("{}", failed), Style::default().fg(theme.error)),
            Span::styled("  Pending: ", Style::default().fg(theme.muted)),
            Span::styled(format!("{}", pending), Style::default().fg(Color::Yellow)),
        ])));

        lines.push(DetailLine::new(Line::from("")));

        // DAG Visualization section
        lines.push(DetailLine::new(Line::from(vec![Span::styled(
            "Task DAG",
            Style::default().fg(theme.accent).bold(),
        )])));

        // Add DAG lines
        let dag_lines = self.build_dag_lines(theme);
        lines.extend(dag_lines);

        // Show workflow result/error for terminal and paused states
        if self.workflow.status == "COMPLETED" || self.workflow.status == "FAILED" || self.workflow.status == "PAUSED" {
            lines.push(DetailLine::new(Line::from("")));

            if self.workflow.status == "COMPLETED" {
                if let Some(result) = &self.workflow.result {
                    lines.push(DetailLine::new(Line::from(vec![Span::styled(
                        "Result",
                        Style::default().fg(theme.success).bold(),
                    )])));
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(result) {
                        if let Ok(pretty) = serde_json::to_string_pretty(&parsed) {
                            for line in pretty.lines() {
                                lines.push(DetailLine::new(Line::from(vec![
                                    Span::styled(format!("  {}", line), Style::default().fg(theme.text)),
                                ])));
                            }
                        } else {
                            lines.push(DetailLine::new(Line::from(vec![
                                Span::styled(format!("  {}", result), Style::default().fg(theme.text)),
                            ])));
                        }
                    } else {
                        lines.push(DetailLine::new(Line::from(vec![
                            Span::styled(format!("  {}", result), Style::default().fg(theme.text)),
                        ])));
                    }
                }
            } else {
                // FAILED or PAUSED — show error and/or result
                let error_color = if self.workflow.status == "PAUSED" { Color::Magenta } else { theme.error };
                let header = if self.workflow.status == "PAUSED" { "Paused Error" } else { "Error" };

                let has_error = self.workflow.error.as_ref().is_some_and(|s| !s.is_empty());
                let has_result = self.workflow.result.as_ref().is_some_and(|s| !s.is_empty());

                if has_error || has_result {
                    lines.push(DetailLine::new(Line::from(vec![Span::styled(
                        header,
                        Style::default().fg(error_color).bold(),
                    )])));
                }

                if let Some(error) = &self.workflow.error {
                    if !error.is_empty() {
                        for line in error.lines() {
                            lines.push(DetailLine::new(Line::from(vec![
                                Span::styled(format!("  {}", line), Style::default().fg(error_color)),
                            ])));
                        }
                    }
                }

                if let Some(result) = &self.workflow.result {
                    if !result.is_empty() {
                        if has_error {
                            lines.push(DetailLine::new(Line::from("")));
                            lines.push(DetailLine::new(Line::from(vec![Span::styled(
                                "  Result:",
                                Style::default().fg(error_color).bold(),
                            )])));
                        }
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(result) {
                            if let Ok(pretty) = serde_json::to_string_pretty(&parsed) {
                                for line in pretty.lines() {
                                    lines.push(DetailLine::new(Line::from(vec![
                                        Span::styled(format!("    {}", line), Style::default().fg(error_color)),
                                    ])));
                                }
                            } else {
                                lines.push(DetailLine::new(Line::from(vec![
                                    Span::styled(format!("    {}", result), Style::default().fg(error_color)),
                                ])));
                            }
                        } else {
                            lines.push(DetailLine::new(Line::from(vec![
                                Span::styled(format!("    {}", result), Style::default().fg(error_color)),
                            ])));
                        }
                    }
                }
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

impl<'a> Component for WorkflowDetailPanel<'a> {
    fn update(&mut self, _action: Action) -> Result<Option<Action>> {
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect, theme: &Theme) -> Result<()> {
        let popup_area = Self::centered_rect(85, 85, area);

        // Clear the background
        frame.render_widget(Clear, popup_area);

        let title = format!(
            " Workflow: {} - Esc: close | ↑↓: scroll | []: prev/next | y: copy ",
            self.workflow.short_id()
        );
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
        let scroll = self
            .scroll_offset
            .min(content_height.saturating_sub(visible_height));
        self.effective_scroll = scroll;
        let paragraph = Paragraph::new(lines)
            .style(Style::default().bg(theme.background).fg(theme.text))
            .scroll((scroll, 0));

        frame.render_widget(paragraph, inner);

        // Render scrollbar if content overflows
        if content_height > visible_height {
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(Some("▲"))
                .end_symbol(Some("▼"))
                .track_symbol(Some("│"))
                .thumb_symbol("█");

            let mut scrollbar_state = ScrollbarState::new(
                content_height.saturating_sub(visible_height) as usize,
            )
            .position(scroll as usize);

            frame.render_stateful_widget(
                scrollbar,
                inner.inner(Margin {
                    vertical: 1,
                    horizontal: 0,
                }),
                &mut scrollbar_state,
            );
        }

        Ok(())
    }
}
