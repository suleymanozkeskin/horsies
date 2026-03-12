"""Generate a simple coverage badge SVG from a coverage.py JSON report."""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path


def badge_color(percent: float) -> str:
    if percent >= 90:
        return '#4c1'
    if percent >= 80:
        return '#97ca00'
    if percent >= 70:
        return '#a4a61d'
    if percent >= 60:
        return '#dfb317'
    if percent >= 50:
        return '#fe7d37'
    return '#e05d44'


def text_width(text: str) -> int:
    """Return an approximation close enough for a small SVG badge."""
    return max(40, int(math.ceil(len(text) * 6.8 + 10)))


def render_svg(value: str, color: str) -> str:
    label = 'coverage'
    label_width = text_width(label)
    value_width = text_width(value)
    total_width = label_width + value_width
    label_center = label_width / 2
    value_center = label_width + (value_width / 2)

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="20" role="img" aria-label="{label}: {value}">
  <title>{label}: {value}</title>
  <linearGradient id="smooth" x2="0" y2="100%">
    <stop offset="0" stop-color="#fff" stop-opacity=".7"/>
    <stop offset=".1" stop-color="#aaa" stop-opacity=".1"/>
    <stop offset=".9" stop-color="#000" stop-opacity=".3"/>
    <stop offset="1" stop-color="#000" stop-opacity=".5"/>
  </linearGradient>
  <mask id="round">
    <rect width="{total_width}" height="20" rx="3" fill="#fff"/>
  </mask>
  <g mask="url(#round)">
    <rect width="{label_width}" height="20" fill="#555"/>
    <rect x="{label_width}" width="{value_width}" height="20" fill="{color}"/>
    <rect width="{total_width}" height="20" fill="url(#smooth)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="11">
    <text x="{label_center}" y="15" fill="#010101" fill-opacity=".3">{label}</text>
    <text x="{label_center}" y="14">{label}</text>
    <text x="{value_center}" y="15" fill="#010101" fill-opacity=".3">{value}</text>
    <text x="{value_center}" y="14">{value}</text>
  </g>
</svg>
"""


def read_coverage_percent(path: Path) -> float:
    data = json.loads(path.read_text())
    totals = data['totals']
    percent = totals.get('percent_covered')
    if percent is None:
        display = totals.get('percent_covered_display')
        if display is None:
            raise KeyError('coverage totals missing percent_covered fields')
        percent = float(str(display).rstrip('%'))
    return float(percent)


def main() -> int:
    if len(sys.argv) != 3:
        print(
            'usage: update_coverage_badge.py <coverage.json> <output.svg>',
            file=sys.stderr,
        )
        return 2

    coverage_json = Path(sys.argv[1])
    output_svg = Path(sys.argv[2])

    percent = read_coverage_percent(coverage_json)
    display = f'{percent:.0f}%'
    svg = render_svg(display, badge_color(percent))

    output_svg.parent.mkdir(parents=True, exist_ok=True)
    output_svg.write_text(svg)
    print(f'wrote {display} badge to {output_svg}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
