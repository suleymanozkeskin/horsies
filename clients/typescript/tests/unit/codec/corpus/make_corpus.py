# pyright: strict
'''Write the corpus files of the TypeScript codec tests: Python's stored text of floats, decimals, date-times
and times of day; the decimal texts that the Python client accepts and refuses; Python's whitespace and digit
tables of `Decimal`.

Run from the repository root. The command installs only pydantic, in a cached environment; it builds no part
of the repository:

    uv run --no-project --python 3.13 --with pydantic==2.13.4 --with pydantic-core==2.46.4 \
        python -I -B clients/typescript/tests/unit/codec/corpus/make_corpus.py clients/typescript/tests/unit/codec/corpus

The steps are those of the Python client's typed encode (`clients/python/horsies/core/codec/typed.py`):
`TypeAdapter(T).validate_python`, `dump_python(mode='json')`, then the text of `dumps_json`
(`json.dumps(ensure_ascii=False, separators=(',', ':'), allow_nan=False)`). The script does not import horsies.
One fixed seed: the same Python and pydantic versions write the same files.

Exit status: 0 when the files are written; 1 when pydantic and `str(Decimal)` disagree on a corpus text (no
file is written then); 2 for a usage error.
'''

import datetime as dt
import json
import random
import struct
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
import unicodedata
from typing import Final, Literal, TypedDict

import pydantic
import pydantic_core
from pydantic import TypeAdapter

SEED: Final = 20261125
FLOAT_COUNT: Final = 10_000
RANDOM_BITS_UNTIL: Final = 6_250
SHORT_DECIMALS_UNTIL: Final = 7_500
LARGE_INTEGERS_UNTIL: Final = 8_750
NEIGHBOUR_STEPS: Final = 20
DECIMAL_COUNT: Final = 2_000
DATETIME_COUNT: Final = 2_000
TIME_COUNT: Final = 1_000
DECIMAL_FUZZ_COUNT: Final = 4_000
DECIMAL_FUZZ_SEED: Final = SEED + 1
CODE_POINT_END: Final = 0x110000
DIGITS_PER_RUN: Final = 10
MAX_ADJUSTED_EXPONENT: Final = 999_999_999_999_999_999
MIN_EXPONENT: Final = -1_999_999_999_999_999_997
FLOAT_BYTES: Final = 8

FLOAT: Final = TypeAdapter(float)
DECIMAL: Final = TypeAdapter(Decimal)
DATETIME: Final = TypeAdapter(dt.datetime)
TIME: Final = TypeAdapter(dt.time)

NAMED_FLOATS: Final = [
    0.0,
    -0.0,
    5e-324,
    -5e-324,
    2.2250738585072014e-308,
    1.7976931348623157e308,
    -1.7976931348623157e308,
    1e-5,
    1e-4,
    1e15,
    1e16,
    9007199254740991.0,
    9007199254740992.0,
    123.0,
    0.1,
    0.30000000000000004,
]
BOUNDARY_FLOATS: Final = [1e-5, 1e-4, 1e15, 1e16, 1e-7, 1e22]
NAMED_DECIMALS: Final = [
    '1.10',
    '1E+3',
    '-0',
    '0',
    '0.000',
    '0E-7',
    '0E+5',
    '-0.0',
    '.5',
    '5.',
    '+5',
    '007.50',
    '1e-7',
    '0.0000001',
    '0.000001',
    '123456789012345678901234567890',
    '1E+999999999999999999',
    '-1.0E-10',
]

SPECIAL_WORDS: Final = [
    'inf',
    'Infinity',
    'INF',
    'nan',
    'NaN',
    'snan',
    'sNaN',
    'infinit',
    'nana',
    'in',
    'NaN-1',
]
JUNK: Final = [
    '',
    '.',
    '..',
    'e',
    'E',
    '+',
    '-',
    'e+',
    '1e',
    'x',
    ' ',
    '\x00',
    '\t',
    '1.2.3',
    '0x1',
    '++1',
    '--1',
    '1e1e1',
]
SURROGATE_TEXTS: Final = ['\ud800', '1\udc00', '\U0001f40e', '1\U0001f40e']

type ZoneLabel = Literal['naive', 'utc'] | int


class DateTimeRow(TypedDict):
    y: int
    mo: int
    d: int
    h: int
    mi: int
    s: int
    us: int
    tz: ZoneLabel
    text: str


class TimeRow(TypedDict):
    h: int
    mi: int
    s: int
    us: int
    text: str


@dataclass(frozen=True, slots=True)
class CorpusRefused:
    '''Pydantic and `str(Decimal)` disagree on a corpus text: the corpus would not pin one text.'''

    reason: str


def stored_text[T](adapter: TypeAdapter[T], value: object) -> str:
    '''Python's stored text of `value` under the adapter's type.'''
    validated = adapter.validate_python(value)
    dumped = adapter.dump_python(validated, mode='json')
    return json.dumps(
        dumped, ensure_ascii=False, separators=(',', ':'), allow_nan=False
    )


def float_bits(value: float) -> str:
    return struct.pack('>d', value).hex()


def float_of_bits(bits: int) -> float:
    (value,) = struct.unpack('>d', bits.to_bytes(FLOAT_BYTES, 'big'))
    return float(value)


def neighbour(base: float, step: int) -> float:
    '''The float `step` representable values away from `base`, by the signed bit pattern.'''
    bits = int.from_bytes(struct.pack('>d', base), 'big', signed=True) + step
    (value,) = struct.unpack('>d', bits.to_bytes(FLOAT_BYTES, 'big', signed=True))
    return float(value)


def float_values(rng: random.Random) -> list[float]:
    values = list(NAMED_FLOATS)
    while len(values) < RANDOM_BITS_UNTIL:
        candidate = float_of_bits(rng.getrandbits(64))
        if candidate == candidate and abs(candidate) != float('inf'):
            values.append(candidate)
    while len(values) < SHORT_DECIMALS_UNTIL:
        whole = rng.randint(0, 10 ** rng.randint(0, 12))
        digits = rng.randint(0, 9)
        fraction = rng.randint(0, 10**digits - 1) if digits else 0
        text = f'{whole}.{fraction:0{digits}d}' if digits else str(whole)
        values.append(float(('-' if rng.random() < 0.5 else '') + text))
    while len(values) < LARGE_INTEGERS_UNTIL:
        values.append(
            float(rng.randint(-(2**60), 2**60) * rng.choice([1, 1, 1, 10, 1000]))
        )
    while len(values) < FLOAT_COUNT:
        candidate = neighbour(
            rng.choice(BOUNDARY_FLOATS), rng.randint(-NEIGHBOUR_STEPS, NEIGHBOUR_STEPS)
        )
        values.append(-candidate if rng.random() < 0.5 else candidate)
    return values


def decimal_body(rng: random.Random) -> str:
    zeros = rng.random() < 0.15
    integer = ''.join(
        '0' if zeros else str(rng.randint(0, 9)) for _ in range(rng.randint(0, 25))
    )
    fraction = ''.join(
        '0' if zeros else str(rng.randint(0, 9)) for _ in range(rng.randint(0, 25))
    )
    if rng.random() < 0.3:
        fraction += '0' * rng.randint(1, 6)
    match (integer, fraction, rng.random() < 0.3):
        case ('', '', _):
            return '0'
        case (_, '', True):
            return integer + '.'
        case ('', _, _):
            return '.' + fraction
        case (_, '', False):
            return integer
        case _:
            return integer + '.' + fraction


def decimal_texts(rng: random.Random) -> list[str]:
    texts = list(NAMED_DECIMALS)
    while len(texts) < DECIMAL_COUNT:
        body = decimal_body(rng)
        if rng.random() < 0.5:
            exponent = str(
                rng.randint(0, 10 ** rng.choice([1, 1, 2, 3, 6, 12, 18]) - 1)
            )
            body += rng.choice(['e', 'E']) + rng.choice(['', '+', '-']) + exponent
        texts.append(rng.choice(['', '', '-', '+']) + body)
    return texts


def zone_of(rng: random.Random) -> tuple[ZoneLabel, dt.tzinfo | None]:
    match rng.random():
        case choice if choice < 0.4:
            return 'naive', None
        case choice if choice < 0.55:
            return 'utc', dt.timezone.utc
        case _:
            minutes = (
                0 if rng.random() < 0.05 else rng.randint(-(23 * 60 + 59), 23 * 60 + 59)
            )
            return minutes, dt.timezone(dt.timedelta(minutes=minutes))


def datetime_rows(rng: random.Random) -> list[DateTimeRow]:
    rows: list[DateTimeRow] = []
    years = [1, 9999] + [rng.randint(1, 9999) for _ in range(DATETIME_COUNT - 2)]
    for year in years:
        month = rng.randint(1, 12)
        day = rng.randint(1, 28 if month == 2 else 30)
        microsecond = (
            0
            if rng.random() < 0.5
            else rng.choice([rng.randint(1, 999_999), rng.randint(1, 999) * 1000])
        )
        zone_label, zone = zone_of(rng)
        value = dt.datetime(
            year,
            month,
            day,
            rng.randint(0, 23),
            rng.randint(0, 59),
            rng.randint(0, 59),
            microsecond,
            tzinfo=zone,
        )
        rows.append(
            {
                'y': value.year,
                'mo': value.month,
                'd': value.day,
                'h': value.hour,
                'mi': value.minute,
                's': value.second,
                'us': value.microsecond,
                'tz': zone_label,
                'text': stored_text(DATETIME, value),
            }
        )
    return rows


def time_rows(rng: random.Random) -> list[TimeRow]:
    rows: list[TimeRow] = []
    for _ in range(TIME_COUNT):
        microsecond = 0 if rng.random() < 0.5 else rng.randint(1, 999_999)
        value = dt.time(
            rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59), microsecond
        )
        rows.append(
            {
                'h': value.hour,
                'mi': value.minute,
                's': value.second,
                'us': value.microsecond,
                'text': stored_text(TIME, value),
            }
        )
    return rows


def decimal_lines(rng: random.Random) -> list[str] | CorpusRefused:
    '''Each text with Python's `str(Decimal(text))`. Pydantic must accept it and write the same text.'''
    lines: list[str] = []
    for text in decimal_texts(rng):
        try:
            python = str(Decimal(text))
        except InvalidOperation:
            return CorpusRefused(f'Decimal refused the corpus text {text!r}')
        pydantic_text = json.loads(stored_text(DECIMAL, text))
        if pydantic_text != python:
            return CorpusRefused(
                f'pydantic writes {pydantic_text!r} and str(Decimal) writes {python!r}'
            )
        lines.append(json.dumps([text, python]))
    return lines


def python_spaces() -> list[int]:
    return [code for code in range(CODE_POINT_END) if chr(code).isspace()]


def python_digit_zeros() -> list[int] | CorpusRefused:
    '''The digit zero of each run of Unicode decimal digits. Each run must hold 0 to 9 at the next 10 code points.'''
    zeros = [
        code
        for code in range(CODE_POINT_END)
        if unicodedata.decimal(chr(code), -1) == 0
    ]
    for zero in zeros:
        for offset in range(DIGITS_PER_RUN):
            if unicodedata.decimal(chr(zero + offset), -1) != offset:
                return CorpusRefused(f'the digit run at {zero:#x} is not 0 to 9')
    return zeros


def exponent_near_bound(rng: random.Random) -> str:
    '''An exponent text near one of the bounds of Python's decimal, with leading zeros at times.'''
    bound = rng.choice(
        [
            MAX_ADJUSTED_EXPONENT,
            MIN_EXPONENT,
            MAX_ADJUSTED_EXPONENT + 1,
            MIN_EXPONENT - 1,
        ]
    )
    value = bound + rng.randint(-30, 30)
    sign = '-' if value < 0 else rng.choice(['', '+'])
    return sign + '0' * rng.choice([0, 0, 0, 1, 3]) + str(abs(value))


def decorated(
    rng: random.Random, text: str, digits: list[str], spaces: list[str]
) -> str:
    '''`text` with underscores, Unicode digits in place of ASCII digits, and whitespace at the ends.'''
    out: list[str] = []
    for character in text:
        if rng.random() < 0.1:
            out.append('_')
        if character.isdigit() and character.isascii() and rng.random() < 0.3:
            out.append(rng.choice(digits)[int(character)])
        else:
            out.append(character)
    return (
        rng.choice(['', '', rng.choice(spaces)])
        + ''.join(out)
        + rng.choice(['', '', rng.choice(spaces)])
    )


def fuzz_decimal_text(rng: random.Random, digits: list[str], spaces: list[str]) -> str:
    body = decimal_body(rng)
    sign = rng.choice(['', '', '-', '+'])
    match rng.randrange(6):
        case 0:
            return sign + body + rng.choice(['e', 'E']) + exponent_near_bound(rng)
        case 1:
            return decorated(
                rng,
                sign + body + rng.choice(['', 'e' + str(rng.randint(-99, 99))]),
                digits,
                spaces,
            )
        case 2:
            at = rng.randint(0, len(body))
            return sign + body[:at] + rng.choice(JUNK + spaces) + body[at:]
        case 3:
            word = rng.choice(SPECIAL_WORDS) + rng.choice(
                ['', '', str(rng.randint(0, 99))]
            )
            return decorated(rng, sign + word, digits, spaces)
        case 4:
            at = rng.randint(0, len(body))
            return body[:at] + rng.choice(SURROGATE_TEXTS) + body[at:]
        case _:
            return (
                sign
                + body
                + rng.choice(['e', 'E'])
                + rng.choice(['', '+', '-'])
                + '0' * rng.randint(0, 25)
                + '7'
            )


def decimal_fuzz_lines(digit_zeros: list[int]) -> list[str]:
    '''Each fuzz text with the Python client's stored text of it, or `null` when Pydantic refuses it.'''
    rng = random.Random(DECIMAL_FUZZ_SEED)
    digits = [
        ''.join(chr(zero + offset) for offset in range(DIGITS_PER_RUN))
        for zero in digit_zeros
    ]
    spaces = [chr(code) for code in python_spaces()]
    lines: list[str] = []
    for _ in range(DECIMAL_FUZZ_COUNT):
        text = fuzz_decimal_text(rng, digits, spaces)
        try:
            stored: str | None = json.loads(stored_text(DECIMAL, text))
        except pydantic.ValidationError:
            stored = None
        lines.append(json.dumps([text, stored]))
    return lines


def write_lines(path: Path, lines: list[str]) -> None:
    path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def write_corpus(folder: Path) -> CorpusRefused | None:
    '''Write the corpus files to `folder`. Returns the refusal, and writes no file, when the texts disagree.'''
    rng = random.Random(SEED)
    floats = float_values(rng)
    decimals = decimal_lines(rng)
    if isinstance(decimals, CorpusRefused):
        return decimals
    datetimes = datetime_rows(rng)
    times = time_rows(rng)
    digit_zeros = python_digit_zeros()
    if isinstance(digit_zeros, CorpusRefused):
        return digit_zeros
    fuzz = decimal_fuzz_lines(digit_zeros)
    write_lines(
        folder / 'floats.txt',
        [f'{float_bits(value)} {stored_text(FLOAT, value)}' for value in floats],
    )
    write_lines(folder / 'decimals.jsonl', decimals)
    write_lines(folder / 'datetimes.jsonl', [json.dumps(row) for row in datetimes])
    write_lines(folder / 'times.jsonl', [json.dumps(row) for row in times])
    write_lines(folder / 'decimal-fuzz.jsonl', fuzz)
    tables = {
        'unicode': unicodedata.unidata_version,
        'spaces': python_spaces(),
        'digit_zeros': digit_zeros,
    }
    (folder / 'decimal-tables.json').write_text(
        json.dumps(tables) + '\n', encoding='utf-8'
    )
    facts = {
        'python': sys.version.split()[0],
        'pydantic': pydantic.VERSION,
        'pydantic_core': pydantic_core.__version__,
        'seed': SEED,
        'floats': len(floats),
        'decimals': len(decimals),
        'datetimes': len(datetimes),
        'times': len(times),
        'decimal_fuzz': len(fuzz),
        'decimal_fuzz_refused': sum(1 for line in fuzz if line.endswith(', null]')),
    }
    (folder / 'corpus.json').write_text(
        json.dumps(facts, indent=1) + '\n', encoding='utf-8'
    )
    print(json.dumps(facts))
    return None


def main(arguments: list[str]) -> int:
    match arguments:
        case [folder]:
            refused = write_corpus(Path(folder))
            if refused is None:
                return 0
            print(f'make_corpus: {refused.reason}', file=sys.stderr)
            return 1
        case _:
            print('usage: make_corpus.py <output folder>', file=sys.stderr)
            return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
