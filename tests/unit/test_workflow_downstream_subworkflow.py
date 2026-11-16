"""The downstream sub-workflow fact that the start writes on each node row."""

from __future__ import annotations

from typing import Any

from horsies.core.workflows.sql import with_downstream_subworkflow


def _node(index: int, deps: list[int], *, sub: bool = False) -> dict[str, Any]:
    return {'idx': index, 'deps': deps, 'is_subworkflow': sub, 'name': f'n{index}'}


def _facts(nodes: list[dict[str, Any]]) -> dict[int, str]:
    return {
        params['idx']: params['downstream_subworkflow']
        for params in with_downstream_subworkflow(nodes)
    }


def test_every_ancestor_of_a_sub_workflow_node_is_reachable() -> None:
    nodes = [
        _node(0, []),
        _node(1, [0]),
        _node(2, [1], sub=True),
        _node(3, []),
        _node(4, [3]),
        _node(5, [0]),
    ]
    assert _facts(nodes) == {
        0: 'REACHABLE',
        1: 'REACHABLE',
        2: 'NONE',
        3: 'NONE',
        4: 'NONE',
        5: 'NONE',
    }


def test_a_graph_with_no_sub_workflow_node_is_none_everywhere() -> None:
    nodes = [_node(0, []), _node(1, [0]), _node(2, [0, 1])]
    assert set(_facts(nodes).values()) == {'NONE'}


def test_a_diamond_visits_each_node_once_and_keeps_the_other_params() -> None:
    nodes = [
        _node(0, []),
        _node(1, [0]),
        _node(2, [0]),
        _node(3, [1, 2], sub=True),
    ]
    rows = with_downstream_subworkflow(nodes)
    assert [row['downstream_subworkflow'] for row in rows] == [
        'REACHABLE',
        'REACHABLE',
        'REACHABLE',
        'NONE',
    ]
    assert [row['name'] for row in rows] == ['n0', 'n1', 'n2', 'n3']
    assert all('downstream_subworkflow' not in node for node in nodes)
