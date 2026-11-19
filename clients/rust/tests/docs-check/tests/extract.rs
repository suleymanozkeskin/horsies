//! The extraction of the `rust` blocks: one test per outcome of reading a
//! page, and the module files of a chain.

#[allow(dead_code)]
#[path = "../extract.rs"]
mod extract;

use std::path::Path;

use extract::{
    module_name, module_text, page_modules, read_blocks, Block, BlockMode, ExtractError, SkipKind,
};

const PAGE: &str = "website/src/content/docs/rust/page.md";
const MDX_PAGE: &str = "website/src/content/docs/rust/page.mdx";

fn modes(text: &str) -> Vec<(usize, BlockMode)> {
    read_blocks(PAGE, text)
        .unwrap_or_else(|error| panic!("the page reads: {error}"))
        .into_iter()
        .map(|block| (block.line, block.mode))
        .collect()
}

fn refusal(text: &str) -> ExtractError {
    match read_blocks(PAGE, text) {
        Err(error) => error,
        Ok(blocks) => panic!("the page reads with {} blocks", blocks.len()),
    }
}

#[test]
fn a_block_without_a_marker_is_chained() {
    assert_eq!(
        modes("Text.\n\n```rust\nfn a() {}\n```\n"),
        vec![(3, BlockMode::Chained)]
    );
}

#[test]
fn alone_builds_the_block_in_a_module_of_its_own() {
    assert_eq!(
        modes("<!-- rust-check: alone -->\n```rust\nfn a() {}\n```\n"),
        vec![(2, BlockMode::Alone)]
    );
}

#[test]
fn each_skip_kind_is_read_with_its_reason() {
    for (kind, mode) in [
        ("elided", SkipKind::Elided),
        ("partial", SkipKind::Partial),
        ("defect", SkipKind::Defect),
    ] {
        let text =
            format!("<!-- rust-check: skip {kind} -- the reason -->\n```rust\nlet a = 1;\n```\n");
        assert_eq!(modes(&text), vec![(2, BlockMode::Skip(mode))], "{kind}");
    }
}

#[test]
fn the_mdx_marker_form_is_read() {
    let blocks = read_blocks(
        MDX_PAGE,
        "{/* rust-check: skip elided -- a signature */}\n```rust\nfn a();\n```\n",
    )
    .unwrap_or_else(|error| panic!("the page reads: {error}"));
    assert_eq!(blocks[0].mode, BlockMode::Skip(SkipKind::Elided));
}

#[test]
fn a_skip_without_a_kind_is_refused() {
    assert!(matches!(
        refusal("<!-- rust-check: skip -- why -->\n```rust\n```\n"),
        ExtractError::SkipWithoutKind { line: 1, .. }
    ));
}

#[test]
fn a_skip_without_a_reason_is_refused() {
    assert!(matches!(
        refusal("<!-- rust-check: skip partial -->\n```rust\n```\n"),
        ExtractError::SkipWithoutReason { line: 1, .. }
    ));
}

#[test]
fn an_unknown_marker_is_refused() {
    for marker in [
        "<!-- rust-check: chained -->",
        "<!-- rust-check: skip later -- why -->",
        "<!-- rust-check alone -->",
    ] {
        let text = format!("{marker}\n```rust\n```\n");
        assert!(
            matches!(refusal(&text), ExtractError::UnknownMarker { line: 1, .. }),
            "{marker}"
        );
    }
}

#[test]
fn a_marker_with_no_rust_fence_under_it_is_refused() {
    for text in [
        "<!-- rust-check: alone -->\n```python\nx = 1\n```\n",
        "<!-- rust-check: alone -->\nText.\n```rust\n```\n",
        "<!-- rust-check: alone -->\n<!-- rust-check: alone -->\n```rust\n```\n",
        "Text.\n<!-- rust-check: alone -->\n",
    ] {
        assert!(
            matches!(refusal(text), ExtractError::MarkerWithoutRustFence { .. }),
            "{text:?}"
        );
    }
}

#[test]
fn blank_lines_between_a_marker_and_its_fence_are_allowed() {
    assert_eq!(
        modes("<!-- rust-check: alone -->\n\n```rust\nfn a() {}\n```\n"),
        vec![(3, BlockMode::Alone)]
    );
}

#[test]
fn a_fence_that_is_never_closed_is_refused() {
    assert!(matches!(
        refusal("Text.\n```rust\nfn a() {}\n"),
        ExtractError::UnclosedFence { line: 2, .. }
    ));
}

#[test]
fn the_markers_of_the_python_check_and_other_fences_are_ignored() {
    let text = "<!-- check: skip elided -- pseudo-code -->\n```python\nx = ???\n```\n\
                ```toml\n[a]\n```\n```rust\nfn a() {}\n```\n";
    assert_eq!(modes(text), vec![(8, BlockMode::Chained)]);
}

#[test]
fn the_code_of_an_indented_fence_loses_the_fence_indent() {
    let blocks = read_blocks(
        PAGE,
        "1. Step:\n\n   ```rust\n   fn a() {\n       b();\n   }\n   ```\n",
    )
    .unwrap_or_else(|error| panic!("the page reads: {error}"));
    assert_eq!(blocks[0].code, "fn a() {\n    b();\n}");
}

fn block(line: usize, mode: BlockMode) -> Block {
    Block {
        page: PAGE.to_owned(),
        line,
        mode,
        code: format!("fn at_{line}() {{}}"),
    }
}

#[test]
fn a_chained_block_is_inside_the_module_of_the_chained_block_before_it() {
    let modules = page_modules(&[
        block(3, BlockMode::Chained),
        block(9, BlockMode::Alone),
        block(15, BlockMode::Skip(SkipKind::Partial)),
        block(21, BlockMode::Chained),
    ]);
    let shape: Vec<(usize, bool, Option<String>)> = modules
        .iter()
        .map(|module| (module.line, module.nested, module.next.clone()))
        .collect();
    assert_eq!(
        shape,
        vec![
            (3, false, Some(module_name(PAGE, 21))),
            (9, false, None),
            (21, true, None),
        ]
    );
}

#[test]
fn the_module_name_is_the_page_path_and_the_line() {
    assert_eq!(
        module_name("crates/horsies/.agents/skills/horsies/SKILL.md", 88),
        "crates_horsies_agents_skills_horsies_skill__line88"
    );
}

#[test]
fn a_module_file_names_its_page_and_ends_with_its_chain() {
    let modules = page_modules(&[block(3, BlockMode::Chained), block(21, BlockMode::Chained)]);
    let folder = Path::new("/out/blocks");
    let first = module_text(&modules[0], folder);
    assert!(first.starts_with(&format!(
        "// The rust block at line 3 of {PAGE}: line L of this file is line L + 2 of the page.\n\
         fn at_3() {{}}\n"
    )));
    assert!(first.ends_with(&format!(
        "#[path = \"/out/blocks/{name}.rs\"]\nmod {name};\n",
        name = module_name(PAGE, 21)
    )));
    let second = module_text(&modules[1], folder);
    assert!(second.ends_with("fn at_21() {}\nuse super::*;\n"));
}
