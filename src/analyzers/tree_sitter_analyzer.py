"""Multi-language AST parsing using tree-sitter with LanguageRouter."""

from pathlib import Path
from typing import Any

import tree_sitter_javascript
import tree_sitter_python
import tree_sitter_sql
import tree_sitter_typescript
import tree_sitter_yaml
from tree_sitter import Language, Node, Parser, Tree

from src.models import ModuleNode


# Grammar registry: extension -> (Language, language_name)
_LANGUAGES: dict[str, tuple[Any, str]] = {
    ".py": (Language(tree_sitter_python.language()), "python"),
    ".yaml": (Language(tree_sitter_yaml.language()), "yaml"),
    ".yml": (Language(tree_sitter_yaml.language()), "yaml"),
    ".js": (Language(tree_sitter_javascript.language()), "javascript"),
    ".jsx": (Language(tree_sitter_javascript.language()), "javascript"),
    ".ts": (Language(tree_sitter_typescript.language_typescript()), "typescript"),
    ".tsx": (Language(tree_sitter_typescript.language_tsx()), "typescript"),
    ".sql": (Language(tree_sitter_sql.language()), "sql"),
}

# SQL keywords that tree-sitter may mis-parse as table identifiers in broken SQL
_SQL_KEYWORDS: frozenset[str] = frozenset({
    "SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR", "NOT", "IN", "IS",
    "NULL", "AS", "BY", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION",
    "ALL", "INNER", "LEFT", "RIGHT", "OUTER", "FULL", "CROSS", "WITH", "INSERT",
    "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "VIEW", "INDEX",
    "SET", "INTO", "VALUES", "DISTINCT", "EXISTS", "BETWEEN", "LIKE", "CASE",
    "WHEN", "THEN", "ELSE", "END", "OVER", "PARTITION", "ROWS", "RANGE",
})


def ts_fallback_extract_sql_tables(source: bytes) -> set[str]:
    """
    Fallback SQL table extractor using tree-sitter.

    Called when sqlglot fails to parse a SQL file. Walks the tree-sitter AST
    and collects all `object_reference` nodes that are direct children of
    `relation` nodes (i.e. FROM/JOIN targets). SQL keywords are filtered out
    to avoid false positives from error-recovery nodes.

    Returns a set of table name strings (may include schema-qualified names
    like 'schema.table'). Results are best-effort on malformed SQL.
    """
    lang = Language(tree_sitter_sql.language())
    parser = Parser(lang)
    tree = parser.parse(source)

    tables: set[str] = set()

    def _walk(node: Node) -> None:
        if node.type == "relation":
            for child in node.children:
                if child.type == "object_reference":
                    parts = [
                        c.text.decode("utf-8", errors="replace")
                        for c in child.children
                        if c.type == "identifier"
                    ]
                    name = ".".join(parts)
                    if name and name.upper() not in _SQL_KEYWORDS:
                        tables.add(name)
        for child in node.children:
            _walk(child)

    _walk(tree.root_node)
    return tables


class LanguageRouter:
    """Select tree-sitter grammar based on file extension."""

    def __init__(self) -> None:
        self._parsers: dict[str, Parser] = {}
        for ext, (lang, name) in _LANGUAGES.items():
            p = Parser(lang)
            self._parsers[ext] = p

    def get_language(self, path: str | Path) -> str:
        """Return language name for file, or 'unknown' if unsupported."""
        ext = Path(path).suffix.lower()
        if ext in _LANGUAGES:
            return _LANGUAGES[ext][1]
        return "unknown"

    def supports(self, path: str | Path) -> bool:
        """Return True if we have a grammar for this file."""
        return self.get_language(path) != "unknown"

    def parse(self, path: str | Path, source: bytes) -> Tree | None:
        """Parse source bytes. Returns Tree or None if unsupported."""
        ext = Path(path).suffix.lower()
        if ext not in self._parsers:
            return None
        return self._parsers[ext].parse(source)


def _get_text(node: Node, source: bytes) -> str:
    return source[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


def _get_identifier_text(node: Node, source: bytes) -> str:
    """Get text for identifier/dotted_name node."""
    if node.type == "identifier":
        return _get_text(node, source)
    if node.type == "dotted_name":
        return _get_text(node, source)
    # Fallback
    return _get_text(node, source)


def _find_child(node: Node, *types: str) -> Node | None:
    for c in node.children:
        if c.type in types:
            return c
    return None


def _extract_python_imports(tree: Tree, source: bytes) -> list[tuple[str, str | None]]:
    """Extract (module, imported_name) from Python AST. imported_name is None for 'import x'."""
    imports: list[tuple[str, str | None]] = []

    def walk(n: Node) -> None:
        if n.type == "import_statement":
            # import foo.bar -> module=foo.bar, name=None
            name_node = _find_child(n, "dotted_name")
            if name_node:
                imports.append((_get_identifier_text(name_node, source), None))
        elif n.type == "import_from_statement":
            # from foo.bar import x, y  OR  from . import utils
            module_name = ""
            seen_import = False
            for c in n.children:
                if c.type == "relative_import":
                    module_name = _get_text(c, source).strip()
                elif c.type == "dotted_name":
                    text = _get_identifier_text(c, source)
                    if not seen_import:
                        # First dotted_name is module (after 'from')
                        module_name = module_name or text
                    else:
                        # After 'import': imported names
                        imports.append((module_name or "?", text))
                elif c.type == "identifier" and seen_import:
                    imports.append((module_name or "?", _get_text(c, source)))
                elif _get_text(c, source).strip() == "import":
                    seen_import = True
        for c in n.children:
            walk(c)

    walk(tree.root_node)
    return imports


def _extract_python_functions(tree: Tree, source: bytes) -> list[dict[str, Any]]:
    """Extract function definitions: name, signature text, decorators, is_public."""
    functions: list[dict[str, Any]] = []

    def _process_function(n: Node, decorators: list[str]) -> None:
        name_node = _find_child(n, "identifier")
        if name_node:
            name = _get_text(name_node, source)
            params_node = _find_child(n, "parameters")
            sig = _get_text(params_node, source) if params_node else "()"
            functions.append({
                "name": name,
                "signature": sig,
                "decorators": decorators,
                "is_public_api": not name.startswith("_"),
                "line_range": (n.start_point[0] + 1, n.end_point[0] + 1),
            })
        # Walk nested functions inside the body
        body = _find_child(n, "block")
        if body:
            walk(body)

    def walk(n: Node) -> None:
        if n.type == "decorated_definition":
            # Collect all decorator texts, then process the inner function_definition
            decs: list[str] = []
            inner: Node | None = None
            for c in n.children:
                if c.type == "decorator":
                    decs.append(_get_text(c, source).strip())
                elif c.type == "function_definition":
                    inner = c
            if inner is not None:
                _process_function(inner, decs)
            return  # body already walked by _process_function
        if n.type == "function_definition":
            _process_function(n, [])
            return  # body already walked by _process_function
        for c in n.children:
            walk(c)

    walk(tree.root_node)
    return functions


def _extract_python_classes(tree: Tree, source: bytes) -> list[dict[str, Any]]:
    """Extract class definitions: name, base classes."""
    classes: list[dict[str, Any]] = []

    def walk(n: Node) -> None:
        if n.type == "class_definition":
            name_node = _find_child(n, "identifier")
            if name_node:
                name = _get_text(name_node, source)
                # inheritance_list: ( object )
                base_node = _find_child(n, "argument_list")
                bases = _get_text(base_node, source) if base_node else ""
                classes.append({
                    "name": name,
                    "bases": bases,
                    "is_public_api": not name.startswith("_"),
                    "line_range": (n.start_point[0] + 1, n.end_point[0] + 1),
                })
        for c in n.children:
            walk(c)

    walk(tree.root_node)
    return classes


def _count_lines_and_comments(source: bytes) -> tuple[int, int]:
    """Return (total_lines, comment_lines)."""
    lines = source.decode("utf-8", errors="replace").splitlines()
    total = len(lines)
    comment = sum(1 for L in lines if L.strip().startswith("#"))
    return total, comment


def analyze_module(
    path: str | Path,
    source: bytes,
    router: LanguageRouter | None = None,
) -> ModuleNode | None:
    """
    Analyze a module and return a ModuleNode. Returns None if language unsupported.
    """
    path = Path(path)
    router = router or LanguageRouter()
    if not router.supports(path):
        return None

    lang = router.get_language(path)
    path_str = str(path)

    # Parse
    tree = router.parse(path, source)
    if tree is None:
        return None

    # Python-specific extraction
    if lang == "python":
        imports = _extract_python_imports(tree, source)
        functions = _extract_python_functions(tree, source)
        classes = _extract_python_classes(tree, source)
        total_lines, comment_lines = _count_lines_and_comments(source)
        comment_ratio = comment_lines / total_lines if total_lines else 0.0

        return ModuleNode(
            path=path_str,
            language=lang,
            complexity_score=float(total_lines),  # Simplified; cyclomatic would need more
            purpose_statement=None,  # Filled by Semanticist
            domain_cluster=None,
            change_velocity_30d=None,  # Filled by Surveyor via git
            is_dead_code_candidate=False,  # Filled by Surveyor
            last_modified=None,
        )
    elif lang == "yaml":
        # YAML: minimal extraction; DAG config parser handles structure
        total_lines, _ = _count_lines_and_comments(source)
        return ModuleNode(
            path=path_str,
            language=lang,
            complexity_score=float(total_lines),
            purpose_statement=None,
            domain_cluster=None,
            change_velocity_30d=None,
            is_dead_code_candidate=False,
            last_modified=None,
        )
    elif lang in ("javascript", "typescript"):
        # JS/TS: basic ModuleNode; import/function extraction can be extended later
        total_lines, _ = _count_lines_and_comments(source)
        return ModuleNode(
            path=path_str,
            language=lang,
            complexity_score=float(total_lines),
            purpose_statement=None,
            domain_cluster=None,
            change_velocity_30d=None,
            is_dead_code_candidate=False,
            last_modified=None,
        )

    return None


def extract_module_info(
    path: str | Path,
    source: bytes,
    router: LanguageRouter | None = None,
) -> dict[str, Any]:
    """
    Extract detailed module info for graph building: imports, functions, classes.
    Returns dict with keys: imports, functions, classes, module_node, language.
    """
    path = Path(path)
    router = router or LanguageRouter()
    if not router.supports(path):
        return {"imports": [], "functions": [], "classes": [], "module_node": None, "language": "unknown"}

    lang = router.get_language(path)
    tree = router.parse(path, source)
    if tree is None:
        return {"imports": [], "functions": [], "classes": [], "module_node": None, "language": lang}

    result: dict[str, Any] = {
        "imports": [],
        "functions": [],
        "classes": [],
        "module_node": None,
        "language": lang,
    }

    if lang == "python":
        result["imports"] = _extract_python_imports(tree, source)
        result["functions"] = _extract_python_functions(tree, source)
        result["classes"] = _extract_python_classes(tree, source)

    result["module_node"] = analyze_module(path, source, router)
    return result
