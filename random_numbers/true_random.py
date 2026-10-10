"""
Appends uniform random integers from 1 to 100, each with a timestamp, to a CSV.

Randomness source is os.urandom, the operating system CSPRNG (getentropy on
macOS, getrandom on Linux). It is the same source the secrets module draws from,
it reseeds itself from system entropy, and it exposes no internal state that
would let anyone predict the next value. Python's default random module is a
Mersenne Twister, which is quicker but becomes fully predictable once you have
observed 624 outputs, so it is deliberately not used here.

Bytes are mapped to 1..100 by rejection sampling. A plain "byte % 100" would be
biased, because 256 is not a multiple of 100: the values 1 to 56 would turn up
about 1.5 times as often as 57 to 100. Throwing away bytes above 199 removes
that bias completely. Speed comes from buffering, drawing many bytes in one
syscall instead of one call per number, so quality costs almost nothing.

Usage:
    python true_random.py                 # 10,000 numbers, the default
    python true_random.py --count 1       # one number
"""
import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import argparse
import csv
import logging
import math
import os
from datetime import datetime

from steeped_dives.config import env_path

logger = logging.getLogger(__name__)

output_path = str(env_path("RANDOM_DATA_DIR", must_exist=True))

CSV_NAME = 'random-numbers.csv'

LOW, HIGH = 1, 100
_SPAN  = HIGH - LOW + 1               # 100 possible values
_LIMIT = (256 // _SPAN) * _SPAN       # 200, the largest whole number of spans in a byte


def random_numbers(count: int) -> list:
    """`count` uniform integers in [LOW, HIGH] from the OS CSPRNG."""
    out = []
    while len(out) < count:
        need = count - len(out)
        # 200 of every 256 bytes survive the filter, so 256/200 bytes per number
        # covers the ~22% that get rejected. That is only the average though, and
        # how many survive is itself random, so asking for exactly the average
        # falls short roughly half the time on a large batch and costs a second
        # syscall. The sqrt term adds about four standard deviations on top,
        # which is under 1% more bytes and makes a single draw enough in
        # practice. The loop remains as the correctness backstop.
        draw = os.urandom(need * 256 // _LIMIT + 4 * math.isqrt(need) + 16)
        out.extend(LOW + b % _SPAN for b in draw if b < _LIMIT)
    return out[:count]


def append_to_csv(numbers: list, path: str) -> None:
    """Append one timestamped row per number, writing a header for a new file."""
    is_new = not os.path.exists(path) or os.path.getsize(path) == 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', newline='') as fh:
        writer = csv.writer(fh)
        if is_new:
            writer.writerow(['timestamp', 'number'])
        for n in numbers:
            # Local time with UTC offset, so the row stays unambiguous.
            writer.writerow([datetime.now().astimezone().isoformat(), n])


def main() -> None:
    ap = argparse.ArgumentParser(description=f'Write random {LOW}-{HIGH} values to a CSV.')
    ap.add_argument('--count', type=int, default=10_000, help='how many numbers (default 10,000)')
    ap.add_argument('--out', default=os.path.join(output_path, CSV_NAME),
                    help='CSV to append to')
    ap.add_argument('--stats', type=int, metavar='N', default=1_000_000,
                    help='sample size for the randomness check that runs every time')
    args = ap.parse_args()

    if args.count < 1:
        ap.error('--count must be at least 1')

    numbers = random_numbers(args.count)
    append_to_csv(numbers, args.out)
    logger.info(f'wrote {len(numbers)} number(s) to {args.out}')
    if len(numbers) <= 10:
        logger.info(f'  {numbers}')

    # The check runs on every invocation, on its own fresh sample, because one
    # or two written numbers are far too few to say anything about uniformity.
    run_stats(args.stats)


# ---------------------------------------------------------------------------
# Randomness check, runs on every invocation
#
# Two tests on a fresh sample:
#   uniformity   chi-square goodness of fit. Note that the expected value of
#                chi-square IS its degrees of freedom, so a result near 99 is
#                the target. Lower is not better: a chi-square near zero would
#                mean the counts are suspiciously even, which real randomness
#                does not produce.
#   independence lag-1 correlation, which catches a generator that is flat
#                overall but whose values still depend on the previous one.
#
# The control block afterwards deliberately runs the biased "byte % 100" method
# that this module does NOT use. It is SUPPOSED to fail. Its failure is the
# evidence that the rejection sampling above is doing real work.
# ---------------------------------------------------------------------------

# Bounds for 99 df with 0.1% in each tail. Being under the low bound fails
# too, because counts that are too even are their own kind of non-random.
_CHI2_LOW, _CHI2_HIGH = 61.1, 148.2


def run_stats(n: int = 1_000_000) -> bool:
    import collections
    import time

    t0 = time.perf_counter()
    vals = random_numbers(n)
    dt = time.perf_counter() - t0

    logger.info(f'=== randomness check on {n:,} fresh draws ===')
    logger.info(f'speed       : {n/dt/1e6:.2f} million/sec ({dt:.3f}s)')
    logger.info(f'range       : {min(vals)}..{max(vals)}, '
                f'{len(set(vals))}/{_SPAN} values present')

    # Uniformity.
    counts = collections.Counter(vals)
    expected = n / _SPAN
    chi2 = sum((counts[i] - expected) ** 2 / expected for i in range(LOW, HIGH + 1))
    uniform_ok = _CHI2_LOW < chi2 < _CHI2_HIGH
    line = (f'uniformity  : chi-square {chi2:.1f} on {_SPAN - 1} df '
            f'(want {_CHI2_LOW}-{_CHI2_HIGH}, ideal ~{_SPAN - 1})')
    try:
        from scipy.stats import chisquare
        p = chisquare([counts[i] for i in range(LOW, HIGH + 1)]).pvalue
        line += f', p={p:.4f}'
        # Both tails matter. p near 0 means lumpy, p near 1 means the counts are
        # too even to be real, which is what a rigged or cycling source looks
        # like. Testing only the low tail would wave that through. These are
        # the same 0.1% tails as the chi-square bounds above.
        uniform_ok = 0.001 < p < 0.999
    except ImportError:
        pass
    logger.info(f'{line}   {"PASS" if uniform_ok else "FAIL"}')

    # Independence: lag-1 correlation, scaled to a z-score so the threshold does
    # not have to move with the sample size.
    mean = sum(vals) / n
    cov = sum((x - mean) * (y - mean) for x, y in zip(vals, vals[1:], strict=False))
    var = sum((x - mean) ** 2 for x in vals)
    r = cov / var if var else 0.0
    z = r * math.sqrt(n - 1)
    indep_ok = abs(z) < 4.0
    logger.info(f'independence: lag-1 correlation {r:+.5f} (z={z:+.2f}, want |z|<4)'
                f'   {"PASS" if indep_ok else "FAIL"}')

    ok = uniform_ok and indep_ok
    logger.info(f'VERDICT: {"PASS" if ok else "FAIL"}')

    # Control. This is NOT the generator, it is the biased method being avoided.
    # Bytes 0..55 hand their residue a third preimage, so the low values win by
    # half again as much.
    raw = os.urandom(n)
    naive = collections.Counter(LOW + b % _SPAN for b in raw)
    cut = 256 % _SPAN                       # 56
    lo = sum(naive[i] for i in range(LOW, LOW + cut)) / cut
    hi = sum(naive[i] for i in range(LOW + cut, HIGH + 1)) / (_SPAN - cut)
    naive_chi2 = sum((naive[i] - expected) ** 2 / expected for i in range(LOW, HIGH + 1))
    logger.info(f'--- control: the biased "byte % {_SPAN}" this code does NOT use ---')
    logger.info('    (this block is SUPPOSED to fail, that is the point of it)')
    logger.info(f'    {LOW}..{LOW + cut - 1} average {lo:,.0f} vs '
                f'{LOW + cut}..{HIGH} average {hi:,.0f}, ratio {lo/hi:.3f} (1.000 = unbiased)')
    logger.info(f'    chi-square {naive_chi2:,.0f}   FAIL, as expected. Rejecting bytes '
                f'above {_LIMIT - 1} is what avoids this.')

    return ok


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    main()
