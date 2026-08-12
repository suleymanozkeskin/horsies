//! Generated staged readers over static history-leaf references.

use std::collections::HashSet;

use chrono::{DateTime, Timelike, Utc};

use crate::core::history::commands::is_safe_identifier;
use crate::core::history::errors::HistoryError;
use crate::core::history::names::{
    HEARTBEAT_CLASS_KEY, LIVE_TASKS, TASK_DETAIL_FUNCTION, TASK_HISTORY_PARENT,
    TASK_LOOKUP_FUNCTION, TASK_LOOKUP_TYPE, TASK_PROVENANCE_FUNCTION, TASK_PROVENANCE_TYPE,
};
use crate::core::history::partitions::catalog::LeafCatalogRow;

pub const CLOCK_BOUND_SECONDS: i64 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupLeaf {
    relation_name: String,
    lower_anchor: DateTime<Utc>,
    upper_anchor: DateTime<Utc>,
    min_birth_at: Option<DateTime<Utc>>,
}

impl LookupLeaf {
    pub fn new(
        relation_name: impl Into<String>,
        lower_anchor: DateTime<Utc>,
        upper_anchor: DateTime<Utc>,
        min_birth_at: Option<DateTime<Utc>>,
    ) -> Result<Self, HistoryError> {
        let relation_name = relation_name.into();
        if !is_safe_identifier(&relation_name) {
            return Err(HistoryError::contract(format!(
                "lookup leaf name is not a safe identifier: {relation_name:?}"
            )));
        }
        if lower_anchor >= upper_anchor {
            return Err(HistoryError::contract(
                "lookup leaf bounds must be increasing",
            ));
        }
        Ok(Self {
            relation_name,
            lower_anchor,
            upper_anchor,
            min_birth_at,
        })
    }

    pub fn relation_name(&self) -> &str {
        &self.relation_name
    }

    pub fn lower_anchor(&self) -> DateTime<Utc> {
        self.lower_anchor
    }

    pub fn upper_anchor(&self) -> DateTime<Utc> {
        self.upper_anchor
    }

    pub fn min_birth_at(&self) -> Option<DateTime<Utc>> {
        self.min_birth_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupManifest {
    leaves: Vec<LookupLeaf>,
    birth_floor: Option<DateTime<Utc>>,
}

impl LookupManifest {
    pub fn new(
        leaves: Vec<LookupLeaf>,
        birth_floor: Option<DateTime<Utc>>,
    ) -> Result<Self, HistoryError> {
        let mut names = HashSet::with_capacity(leaves.len());
        if leaves
            .iter()
            .any(|leaf| !names.insert(leaf.relation_name.clone()))
        {
            return Err(HistoryError::contract(
                "lookup manifest relation names must be distinct",
            ));
        }
        Ok(Self {
            leaves,
            birth_floor,
        })
    }

    pub fn leaves(&self) -> &[LookupLeaf] {
        &self.leaves
    }

    pub fn birth_floor(&self) -> Option<DateTime<Utc>> {
        self.birth_floor
    }
}

pub fn manifest_from_catalog(
    rows: &[LeafCatalogRow],
    absent_relations: &HashSet<String>,
) -> Result<LookupManifest, HistoryError> {
    let mut ordered: Vec<&LeafCatalogRow> = rows
        .iter()
        .filter(|row| row.class_key != HEARTBEAT_CLASS_KEY)
        .collect();
    ordered.sort_by(|left, right| {
        left.lower_anchor
            .cmp(&right.lower_anchor)
            .then_with(|| left.leaf_name.cmp(&right.leaf_name))
    });
    let mut names = HashSet::with_capacity(ordered.len());
    if ordered
        .iter()
        .any(|row| !names.insert(row.leaf_name.as_str()))
    {
        return Err(HistoryError::contract(
            "lookup manifest relation names must be distinct",
        ));
    }
    let birth_floor = if !ordered.is_empty() && ordered.iter().all(|row| row.min_birth_verified) {
        ordered.iter().filter_map(|row| row.min_birth_at).min()
    } else {
        None
    };
    let leaves = ordered
        .into_iter()
        .filter(|row| !absent_relations.contains(&row.leaf_name))
        .map(|row| {
            LookupLeaf::new(
                row.leaf_name.clone(),
                row.lower_anchor,
                row.upper_anchor,
                row.min_birth_at,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    LookupManifest::new(leaves, birth_floor)
}

pub fn render_staged_lookup_function(manifest: &LookupManifest) -> String {
    staged_function(
        TASK_LOOKUP_FUNCTION,
        TASK_LOOKUP_TYPE,
        "",
        &[
            "v_task_id uuid;",
            "v_fingerprint_version smallint;",
            "v_fingerprint bytea;",
        ],
        &identity_probe(LIVE_TASKS, "LIVE", "id"),
        |relation| identity_probe(relation, "HISTORY", "task_id"),
        manifest,
        Absence::Composite("NULL, NULL, NULL, NULL"),
    )
}

pub fn render_staged_provenance_function(manifest: &LookupManifest) -> String {
    let live_probe = format!(
        "\n        IF p_include_live THEN\n{}\n        END IF;\n",
        provenance_live_probe()
    );
    staged_function(
        TASK_PROVENANCE_FUNCTION,
        TASK_PROVENANCE_TYPE,
        ", p_include_live boolean DEFAULT TRUE",
        &[
            "v_task_id uuid;",
            "v_status text;",
            "v_terminal_at timestamptz;",
            "v_kind text;",
        ],
        &live_probe,
        provenance_history_probe,
        manifest,
        Absence::Composite("NULL, NULL, NULL, NULL, NULL"),
    )
}

pub fn render_staged_detail_function(manifest: &LookupManifest) -> String {
    let live_probe = format!(
        "\n        IF EXISTS (SELECT 1 FROM {LIVE_TASKS} WHERE id = p_task_id) THEN\n            RETURN QUERY SELECT\n                'LIVE'::text, NULL::{TASK_HISTORY_PARENT};\n            RETURN;\n        END IF;\n"
    );
    staged_function(
        TASK_DETAIL_FUNCTION,
        &format!("TABLE (location text, task_row {TASK_HISTORY_PARENT})"),
        "",
        &[&format!("v_row {TASK_HISTORY_PARENT}%ROWTYPE;")],
        &live_probe,
        |relation| {
            format!(
                "\n        SELECT h.* INTO v_row FROM {relation} h WHERE h.task_id = p_task_id;\n        IF FOUND THEN\n            RETURN QUERY SELECT 'HISTORY'::text, v_row;\n            RETURN;\n        END IF;\n"
            )
        },
        manifest,
        Absence::Statement("RETURN;"),
    )
}

enum Absence<'a> {
    Composite(&'a str),
    Statement(&'a str),
}

#[allow(clippy::too_many_arguments)]
fn staged_function(
    function_name: &str,
    return_type: &str,
    extra_parameters: &str,
    declares: &[&str],
    live_probe: &str,
    history_probe: impl Fn(&str) -> String,
    manifest: &LookupManifest,
    absence_form: Absence<'_>,
) -> String {
    let absence = match absence_form {
        Absence::Composite(values) => {
            format!("RETURN ROW(FALSE, {values})::{return_type};")
        }
        Absence::Statement(statement) => statement.to_owned(),
    };
    let finite_section = if manifest.leaves.is_empty() {
        String::new()
    } else {
        let floor_check = manifest.birth_floor.map_or_else(String::new, |floor| {
            format!(
                "\n            IF v_birth_at < {} THEN\n                {}\n            END IF;\n",
                timestamp_literal(floor),
                absence
            )
        });
        let pruned = manifest
            .leaves
            .iter()
            .map(|leaf| pruned_probe(leaf, &history_probe(leaf.relation_name())))
            .collect::<Vec<_>>()
            .join("\n");
        let fallback = manifest
            .leaves
            .iter()
            .rev()
            .map(|leaf| fallback_probe(leaf, &history_probe(leaf.relation_name())))
            .collect::<Vec<_>>()
            .join("\n");
        let legacy = manifest
            .leaves
            .iter()
            .rev()
            .map(|leaf| history_probe(leaf.relation_name()))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "\n        v_uuid_bytes := uuid_send(p_task_id);\n        IF (get_byte(v_uuid_bytes, 6) >> 4) = 7\n           AND (get_byte(v_uuid_bytes, 8) & 192) = 128\n        THEN\n            v_birth_milliseconds :=\n                (get_byte(v_uuid_bytes, 0)::bigint << 40)\n                | (get_byte(v_uuid_bytes, 1)::bigint << 32)\n                | (get_byte(v_uuid_bytes, 2)::bigint << 24)\n                | (get_byte(v_uuid_bytes, 3)::bigint << 16)\n                | (get_byte(v_uuid_bytes, 4)::bigint << 8)\n                | get_byte(v_uuid_bytes, 5)::bigint;\n            v_birth_at := to_timestamp(\n                v_birth_milliseconds::double precision / 1000.0\n            );\n{floor_check}\n            v_effective_birth :=\n                v_birth_at - INTERVAL '{CLOCK_BOUND_SECONDS} seconds';\n{pruned}\n            -- Birth time is an optimization hint, not an integrity\n            -- constraint. Probe every leaf skipped above before declaring\n            -- absence so a caller-clock violation cannot hide a retained row.\n{fallback}\n        ELSE\n{legacy}\n        END IF;\n"
        )
    };
    let declare_block = declares
        .iter()
        .map(|declare| format!("        {declare}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "\n    CREATE OR REPLACE FUNCTION {function_name}(p_task_id uuid{extra_parameters})\n    RETURNS {return_type}\n    LANGUAGE plpgsql\n    STABLE\n    AS $function$\n    DECLARE\n{declare_block}\n        v_uuid_bytes bytea;\n        v_birth_milliseconds bigint;\n        v_birth_at timestamptz;\n        v_effective_birth timestamptz;\n    BEGIN\n{live_probe}\n{finite_section}\n        {absence}\n    END\n    $function$\n    "
    )
}

fn pruned_probe(leaf: &LookupLeaf, probe: &str) -> String {
    format!(
        "\n            IF v_effective_birth < {} THEN\n{probe}\n            END IF;\n",
        timestamp_literal(leaf.upper_anchor)
    )
}

fn fallback_probe(leaf: &LookupLeaf, probe: &str) -> String {
    format!(
        "\n            IF v_effective_birth >= {} THEN\n{probe}\n            END IF;\n",
        timestamp_literal(leaf.upper_anchor)
    )
}

fn identity_probe(relation: &str, location: &str, id_column: &str) -> String {
    debug_assert!(is_safe_identifier(relation));
    format!(
        "\n        SELECT {id_column}, command_fingerprint_version, command_fingerprint\n        INTO v_task_id, v_fingerprint_version, v_fingerprint\n        FROM {relation}\n        WHERE {id_column} = p_task_id;\n        IF FOUND THEN\n            RETURN ROW(\n                TRUE, '{location}', v_task_id,\n                v_fingerprint_version, v_fingerprint\n            )::{TASK_LOOKUP_TYPE};\n        END IF;\n"
    )
}

fn provenance_live_probe() -> String {
    format!(
        "\n        SELECT id INTO v_task_id\n        FROM {LIVE_TASKS}\n        WHERE id = p_task_id;\n        IF FOUND THEN\n            RETURN ROW(\n                TRUE, 'LIVE', v_task_id, NULL, NULL, NULL\n            )::{TASK_PROVENANCE_TYPE};\n        END IF;\n"
    )
}

fn provenance_history_probe(relation: &str) -> String {
    debug_assert!(is_safe_identifier(relation));
    format!(
        "\n        SELECT task_id, status, terminal_at, terminalization_kind\n        INTO v_task_id, v_status, v_terminal_at, v_kind\n        FROM {relation}\n        WHERE task_id = p_task_id;\n        IF FOUND THEN\n            RETURN ROW(\n                TRUE, 'HISTORY', v_task_id,\n                v_status, v_terminal_at, v_kind\n            )::{TASK_PROVENANCE_TYPE};\n        END IF;\n"
    )
}

fn timestamp_literal(value: DateTime<Utc>) -> String {
    let mut rendered = value.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = value.nanosecond() / 1_000;
    if micros != 0 {
        rendered.push_str(&format!(".{micros:06}"));
    }
    format!("TIMESTAMPTZ '{rendered}Z'")
}
