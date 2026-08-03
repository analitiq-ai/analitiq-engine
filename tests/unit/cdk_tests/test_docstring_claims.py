"""The claims CDK docstrings make about the code beside them, checked.

A docstring here is not decoration: it is where the reason for a rule
lives, and a later change is read against it. Two of its claims can be
checked mechanically rather than by a reader noticing, so they are:

* a docstring that counts what follows -- "Two things are driven:", "Three
  substitutions are the kit's own:" -- must count the list it introduces.
  ``check_api_read_advances`` said three and listed four, so the drive a
  reader would go looking for was the one that did not exist;
* a docstring naming a function as the ONE reader of a module-private
  pattern must be right about it, because the whole value of the claim is
  that a second reader cannot drift from the first.

Both are source-level, so they hold for a docstring nothing imports.
"""

from __future__ import annotations

import ast
import re
import textwrap
from pathlib import Path

import pytest

#: The CDK package source, walked as text.
_CDK = Path(__file__).resolve().parents[3] / "cdk" / "cdk"

#: How a docstring spells a count. Capitalized only: a lowercase "one" is
#: an article ("the one declaration that ends a traversal"), not a tally.
_COUNTS = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5,
    "Six": 6,
    "Seven": 7,
}

_COUNT_WORD = re.compile(r"\b(%s)\b" % "|".join(_COUNTS))
_BULLET = re.compile(r"\s*\* ")


def _documented(path: Path) -> list[tuple[str, str]]:
    """Every docstring in *path*, labelled by what carries it."""
    tree = ast.parse(path.read_text())
    carriers: list[ast.AST] = [tree]
    carriers.extend(
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    )
    documented = []
    for node in carriers:
        doc = ast.get_docstring(node, clean=False)
        if doc:
            documented.append((getattr(node, "name", "<module>"), doc))
    return documented


def _counted_claims(doc: str) -> list[tuple[str, int, int]]:
    """Return (claim, counted, listed) for every count a bullet list follows."""
    paragraphs = re.split(r"\n\s*\n", textwrap.dedent(doc))
    claims = []
    for paragraph, following in zip(paragraphs, paragraphs[1:]):
        claim = " ".join(paragraph.split())
        words = _COUNT_WORD.findall(claim)
        bullets = [line for line in following.splitlines() if _BULLET.match(line)]
        # The count that introduces the list is the last one before the
        # colon; an earlier sentence in the same paragraph may count
        # something else entirely.
        if claim.endswith(":") and words and bullets:
            claims.append((claim, _COUNTS[words[-1]], len(bullets)))
    return claims


@pytest.mark.parametrize(
    "path", sorted(_CDK.rglob("*.py")), ids=lambda path: path.name
)
def test_a_counted_docstring_claim_matches_the_list_it_introduces(path: Path) -> None:
    """Say three and list four and the reader hunts for a rule nobody wrote."""
    mismatched = [
        f"{path.relative_to(_CDK)}:{name}: {claim!r} is followed by "
        f"{listed} bullet(s), not {counted}"
        for name, doc in _documented(path)
        for claim, counted, listed in _counted_claims(doc)
        if counted != listed
    ]
    assert mismatched == []


def test_one_function_reads_the_path_placeholder_grammar() -> None:
    """``path_placeholders`` says it is the one reader; nothing else may be.

    The claim is what keeps a second answer to "what is a placeholder" out
    of the module -- and a substitution matching for itself was exactly
    that second answer, sitting under a docstring saying it asked.
    """
    module = _CDK / "api" / "request.py"
    tree = ast.parse(module.read_text())
    readers = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(
            isinstance(child, ast.Name) and child.id == "_PLACEHOLDER"
            for child in ast.walk(node)
        )
    }
    assert readers == {"path_placeholders"}
