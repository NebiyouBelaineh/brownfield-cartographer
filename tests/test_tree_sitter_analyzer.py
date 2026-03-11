"""Tests for src/analyzers/tree_sitter_analyzer.py."""

import pytest

from src.analyzers.tree_sitter_analyzer import (
    LanguageRouter,
    _count_lines_and_comments,
    _extract_python_classes,
    _extract_python_functions,
    _extract_python_imports,
    analyze_module,
    extract_module_info,
)
from src.models.nodes import ModuleNode


# ---------------------------------------------------------------------------
# LanguageRouter
# ---------------------------------------------------------------------------


class TestLanguageRouter:
    def setup_method(self):
        self.router = LanguageRouter()

    def test_python_detected(self):
        assert self.router.get_language("foo.py") == "python"

    def test_yaml_detected(self):
        assert self.router.get_language("config.yaml") == "yaml"

    def test_yml_detected(self):
        assert self.router.get_language("config.yml") == "yaml"

    def test_javascript_detected(self):
        assert self.router.get_language("app.js") == "javascript"

    def test_jsx_detected(self):
        assert self.router.get_language("component.jsx") == "javascript"

    def test_typescript_detected(self):
        assert self.router.get_language("main.ts") == "typescript"

    def test_tsx_detected(self):
        assert self.router.get_language("App.tsx") == "typescript"

    def test_unknown_extension(self):
        assert self.router.get_language("script.rb") == "unknown"

    def test_sql_is_unknown(self):
        assert self.router.get_language("query.sql") == "unknown"

    def test_supports_python(self):
        assert self.router.supports("foo.py") is True

    def test_supports_yaml(self):
        assert self.router.supports("config.yaml") is True

    def test_not_supports_sql(self):
        assert self.router.supports("query.sql") is False

    def test_not_supports_ruby(self):
        assert self.router.supports("app.rb") is False

    def test_parse_python_returns_tree(self):
        src = b"x = 1\n"
        tree = self.router.parse("foo.py", src)
        assert tree is not None

    def test_parse_unsupported_returns_none(self):
        tree = self.router.parse("script.rb", b"puts 'hello'")
        assert tree is None

    def test_parse_yaml_returns_tree(self):
        src = b"key: value\n"
        tree = self.router.parse("config.yaml", src)
        assert tree is not None

    def test_case_insensitive_extension(self):
        assert self.router.get_language("FOO.PY") == "python"


# ---------------------------------------------------------------------------
# _count_lines_and_comments
# ---------------------------------------------------------------------------


class TestCountLinesAndComments:
    def test_empty_source(self):
        total, comments = _count_lines_and_comments(b"")
        assert total == 0
        assert comments == 0

    def test_single_line_no_comment(self):
        total, comments = _count_lines_and_comments(b"x = 1\n")
        assert total == 1
        assert comments == 0

    def test_comment_line(self):
        src = b"# This is a comment\nx = 1\n"
        total, comments = _count_lines_and_comments(src)
        assert total == 2
        assert comments == 1

    def test_multiple_comments(self):
        src = b"# a\n# b\nx = 1\n"
        total, comments = _count_lines_and_comments(src)
        assert total == 3
        assert comments == 2

    def test_inline_comment_not_counted(self):
        # Inline comments like "x = 1  # note" should not be counted
        src = b"x = 1  # inline\n"
        total, comments = _count_lines_and_comments(src)
        assert comments == 0  # doesn't start with #


# ---------------------------------------------------------------------------
# _extract_python_imports
# ---------------------------------------------------------------------------


class TestExtractPythonImports:
    def _parse(self, code: str):
        router = LanguageRouter()
        src = code.encode()
        tree = router.parse("test.py", src)
        return _extract_python_imports(tree, src)

    def test_simple_import(self):
        imports = self._parse("import os")
        assert any(mod == "os" for mod, _ in imports)

    def test_from_import(self):
        imports = self._parse("from pathlib import Path")
        assert any(mod == "pathlib" and name == "Path" for mod, name in imports)

    def test_multiple_from_import(self):
        imports = self._parse("from os.path import join, exists")
        mods = [mod for mod, _ in imports]
        assert all(m == "os.path" for m in mods)
        names = [name for _, name in imports]
        assert "join" in names
        assert "exists" in names

    def test_dotted_import(self):
        imports = self._parse("import os.path")
        assert any("os.path" in mod for mod, _ in imports)

    def test_relative_import(self):
        imports = self._parse("from . import utils")
        assert len(imports) > 0

    def test_no_imports(self):
        imports = self._parse("x = 1\n")
        assert imports == []

    def test_multiple_imports(self):
        code = "import os\nimport sys\nfrom pathlib import Path\n"
        imports = self._parse(code)
        mods = [mod for mod, _ in imports]
        assert "os" in mods
        assert "sys" in mods
        assert "pathlib" in mods


# ---------------------------------------------------------------------------
# _extract_python_functions
# ---------------------------------------------------------------------------


class TestExtractPythonFunctions:
    def _parse(self, code: str):
        router = LanguageRouter()
        src = code.encode()
        tree = router.parse("test.py", src)
        return _extract_python_functions(tree, src)

    def test_simple_function(self):
        code = "def greet(name):\n    return f'Hello {name}'\n"
        fns = self._parse(code)
        assert len(fns) == 1
        assert fns[0]["name"] == "greet"

    def test_public_api_flag(self):
        code = "def public_fn(): pass\ndef _private(): pass\n"
        fns = self._parse(code)
        public = next(f for f in fns if f["name"] == "public_fn")
        private = next(f for f in fns if f["name"] == "_private")
        assert public["is_public_api"] is True
        assert private["is_public_api"] is False

    def test_line_range_reported(self):
        code = "def foo():\n    pass\n"
        fns = self._parse(code)
        assert fns[0]["line_range"][0] == 1  # starts at line 1

    def test_signature_extracted(self):
        code = "def foo(a, b, c=None): pass\n"
        fns = self._parse(code)
        assert "a" in fns[0]["signature"]

    def test_no_functions(self):
        fns = self._parse("x = 1\n")
        assert fns == []

    def test_nested_functions(self):
        code = "def outer():\n    def inner():\n        pass\n"
        fns = self._parse(code)
        names = [f["name"] for f in fns]
        assert "outer" in names
        assert "inner" in names

    def test_multiple_functions(self):
        code = "def a(): pass\ndef b(): pass\ndef c(): pass\n"
        fns = self._parse(code)
        assert len(fns) == 3


# ---------------------------------------------------------------------------
# _extract_python_classes
# ---------------------------------------------------------------------------


class TestExtractPythonClasses:
    def _parse(self, code: str):
        router = LanguageRouter()
        src = code.encode()
        tree = router.parse("test.py", src)
        return _extract_python_classes(tree, src)

    def test_simple_class(self):
        code = "class Foo:\n    pass\n"
        classes = self._parse(code)
        assert len(classes) == 1
        assert classes[0]["name"] == "Foo"

    def test_public_api_flag(self):
        code = "class Public: pass\nclass _Private: pass\n"
        classes = self._parse(code)
        pub = next(c for c in classes if c["name"] == "Public")
        priv = next(c for c in classes if c["name"] == "_Private")
        assert pub["is_public_api"] is True
        assert priv["is_public_api"] is False

    def test_inheritance_extracted(self):
        code = "class Child(Parent):\n    pass\n"
        classes = self._parse(code)
        assert "Parent" in classes[0]["bases"]

    def test_no_classes(self):
        classes = self._parse("x = 1\n")
        assert classes == []

    def test_multiple_classes(self):
        code = "class A: pass\nclass B: pass\n"
        classes = self._parse(code)
        assert len(classes) == 2


# ---------------------------------------------------------------------------
# analyze_module
# ---------------------------------------------------------------------------


class TestAnalyzeModule:
    def test_python_module_returns_module_node(self):
        src = b"x = 1\n"
        node = analyze_module("src/foo.py", src)
        assert isinstance(node, ModuleNode)
        assert node.language == "python"
        assert node.path == "src/foo.py"

    def test_yaml_module_returns_node(self):
        src = b"key: value\n"
        node = analyze_module("config.yaml", src)
        assert isinstance(node, ModuleNode)
        assert node.language == "yaml"

    def test_unsupported_file_returns_none(self):
        node = analyze_module("script.rb", b"puts 'hi'")
        assert node is None

    def test_complexity_score_is_line_count(self):
        src = b"a = 1\nb = 2\nc = 3\n"
        node = analyze_module("foo.py", src)
        assert node.complexity_score == 3.0

    def test_dead_code_default_false(self):
        node = analyze_module("foo.py", b"x = 1\n")
        assert node.is_dead_code_candidate is False

    def test_javascript_module(self):
        src = b"const x = 1;\n"
        node = analyze_module("app.js", src)
        assert isinstance(node, ModuleNode)
        assert node.language == "javascript"

    def test_typescript_module(self):
        src = b"const x: number = 1;\n"
        node = analyze_module("main.ts", src)
        assert isinstance(node, ModuleNode)
        assert node.language == "typescript"


# ---------------------------------------------------------------------------
# extract_module_info
# ---------------------------------------------------------------------------


class TestExtractModuleInfo:
    def test_python_module_info(self):
        code = b"import os\ndef foo(): pass\nclass Bar: pass\n"
        info = extract_module_info("mod.py", code)
        assert info["language"] == "python"
        assert len(info["imports"]) > 0
        assert len(info["functions"]) > 0
        assert len(info["classes"]) > 0
        assert isinstance(info["module_node"], ModuleNode)

    def test_unsupported_file(self):
        info = extract_module_info("script.rb", b"puts 'hi'")
        assert info["language"] == "unknown"
        assert info["imports"] == []
        assert info["module_node"] is None

    def test_yaml_file(self):
        info = extract_module_info("config.yaml", b"key: val\n")
        assert info["language"] == "yaml"
        # YAML has no import/function extraction
        assert info["imports"] == []

    def test_imports_populated_for_python(self):
        code = b"from pathlib import Path\nimport os\n"
        info = extract_module_info("x.py", code)
        mods = [m for m, _ in info["imports"]]
        assert "os" in mods
        assert "pathlib" in mods

    def test_functions_populated_for_python(self):
        code = b"def hello(): pass\ndef world(): pass\n"
        info = extract_module_info("x.py", code)
        names = [f["name"] for f in info["functions"]]
        assert "hello" in names
        assert "world" in names
