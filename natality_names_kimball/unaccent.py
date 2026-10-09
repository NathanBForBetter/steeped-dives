"""Fold accented letters to their plain ASCII counterparts.

The character map is built once on the driver, then pushed into a single
native translate() call, so no Python runs per row - a Python UDF would
serialize every value out to a worker process and back, which is the slow
way to do this in local PySpark.
"""
import logging
import unicodedata

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

## letters NFKD will not decompose: these are distinct letters rather than
## accented forms, so the fold is by convention, not by decomposition
## an empty value means "drop the character"
## these always apply, whether or not they fall in FOLD_RANGES below
NON_DECOMPOSABLE = {
    'Æ': 'AE', 'æ': 'ae', 'Œ': 'OE', 'œ': 'oe', 'ß': 'ss',
    'Þ': 'Th', 'þ': 'th', 'Ð': 'D', 'ð': 'd', 'Đ': 'D', 'đ': 'd',
    'Ø': 'O', 'ø': 'o', 'Ł': 'L', 'ł': 'l', 'Ŋ': 'N', 'ŋ': 'n',
    'Ħ': 'H', 'ħ': 'h', 'Ŧ': 'T', 'ŧ': 't', 'Ə': 'E', 'ə': 'e',
    'Ɛ': 'E', 'ɛ': 'e', 'Ɔ': 'O', 'ɔ': 'o', 'Ʃ': 'S', 'ʃ': 's',
    'Ʒ': 'Z', 'ʒ': 'z', 'Ƿ': 'W', 'ƿ': 'w', 'Ȝ': 'Y', 'ȝ': 'y',
    'ı': 'i', 'İ': 'I', 'ĸ': 'k', 'ſ': 's',
    ## okina and friends: a transliteration library renders these as a
    ## backtick, which is worse than an apostrophe in a name
    'ʻ': "'", 'ʼ': "'", 'ʹ': "'", 'ʾ': "'", 'ʿ': "'", 'ʔ': "'", 'ʕ': "'",
    '‘': "'", '’': "'", '`': "'",
    ## Catalan punt volat separates a geminate l ('Gal·la'), it is a
    ## typographic mark rather than a letter, so the plain spelling drops it
    '·': '', '­': '',
}

## Latin-1 Supplement + Latin Extended-A/B, IPA Extensions (schwa lives
## here), and Latin Extended Additional - the blocks name data turns up in
FOLD_RANGES = ((0x00C0, 0x024F), (0x0250, 0x02AF), (0x1E00, 0x1EFF))

## a fold is only believable if it comes back as a letter - this keeps the
## library from turning an interpunct into '*' or an okina into '`'
PLAUSIBLE_EXTRAS = set(" '-")


def _transliterate(char: str) -> str:
    """Best ASCII spelling of one character, or '' if there is none."""
    ## NFKD splits 'é' into 'e' + combining acute; dropping the non-ascii
    ## parts leaves the bare letter behind
    decomposed = unicodedata.normalize('NFKD', char)
    folded = ''.join(c for c in decomposed if ord(c) < 128)
    if folded:
        return folded

    ## anything left is outside the tables above - let a transliteration
    ## library try, but only take letter-like answers
    try:
        from text_unidecode import unidecode
    except ImportError:
        try:
            from unidecode import unidecode
        except ImportError:
            return ''

    folded = ''.join(c for c in unidecode(char) if ord(c) < 128).strip()
    if folded and all(c.isalpha() or c in PLAUSIBLE_EXTRAS for c in folded):
        return folded
    return ''


def build_char_map():
    """Split the fold into 1:1 pairs (translate) and the rest (regex)."""
    candidates = set(NON_DECOMPOSABLE)
    for start, end in FOLD_RANGES:
        candidates.update(chr(codepoint) for codepoint in range(start, end + 1))

    single, multi = {}, {}
    for char in sorted(candidates):
        if char in NON_DECOMPOSABLE:
            ## listed by hand, so trust it - including '' meaning "drop"
            folded = NON_DECOMPOSABLE[char]
        else:
            folded = _transliterate(char)
            ## no known spelling: leave the character alone rather than
            ## quietly deleting something nobody has looked at
            if not folded or folded == char:
                continue

        if len(folded) == 1:
            single[char] = folded
        else:
            multi[char] = folded
    return single, multi


SINGLE_CHAR_MAP, MULTI_CHAR_MAP = build_char_map()


def unaccent(column):
    """Column expression folding accents in `column`, evaluated in the JVM."""
    expression = F.col(column) if isinstance(column, str) else column

    ## translate() is strictly 1:1, so expansions ('Æ' -> 'AE') and drops
    ## run first - \Q..\E stops Java reading the char as a regex token
    for char, replacement in sorted(MULTI_CHAR_MAP.items()):
        expression = F.regexp_replace(expression, f"\\Q{char}\\E", replacement)

    return F.translate(
        expression,
        ''.join(SINGLE_CHAR_MAP.keys()),
        ''.join(SINGLE_CHAR_MAP.values()),
    )
