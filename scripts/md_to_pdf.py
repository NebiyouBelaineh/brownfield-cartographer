#!/usr/bin/env python3
"""
Convert Markdown to PDF: extract ```mermaid``` blocks, render them to images
via mermaid-cli (mmdc), replace blocks with image refs, then run pandoc.

Usage:
  python scripts/md_to_pdf.py input.md -o output.pdf
  python scripts/md_to_pdf.py input.md -o output.pdf --metadata docs/report/templates/pdf-metadata-final.yaml
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path


MERMAID_BLOCK_RE = re.compile(r"```mermaid\s*\n(.*?)```", re.DOTALL)
DIAGRAMS_DIR = "diagrams"


def find_mmdc() -> str | None:
    """Return the mmdc command to use: 'mmdc' if in PATH, else 'npx -p @mermaid-js/mermaid-cli mmdc'."""
    if shutil.which("mmdc"):
        return "mmdc"
    if shutil.which("npx"):
        return "npx"
    return None


def render_mermaid_to_image(
    mmdc_cmd: str,
    mermaid_content: str,
    output_path: Path,
    scale: float = 1.0,
) -> bool:
    """Write mermaid content to a temp .mmd file and run mmdc to produce an image."""
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    mmd_path = output_path.with_suffix(".mmd")
    base_args = [
        "-i", str(mmd_path),
        "-o", str(output_path),
        "-b", "transparent",
    ]
    if scale != 1.0:
        base_args.extend(["-s", str(scale)])
    try:
        mmd_path.write_text(mermaid_content.strip() + "\n", encoding="utf-8")
        if mmdc_cmd == "npx":
            proc = subprocess.run(
                [
                    "npx",
                    "-p",
                    "@mermaid-js/mermaid-cli",
                    "mmdc",
                    *base_args,
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
        else:
            proc = subprocess.run(
                ["mmdc", *base_args],
                capture_output=True,
                text=True,
                timeout=60,
            )
        if proc.returncode != 0:
            print(f"mmdc stderr: {proc.stderr}", file=sys.stderr)
            return False
        return True
    finally:
        if mmd_path.exists():
            mmd_path.unlink(missing_ok=True)


def process_markdown(
    content: str,
    build_dir: Path,
    mmdc_cmd: str,
    image_format: str = "png",
    mermaid_scale: float = 1.0,
) -> str:
    """Replace each ```mermaid ... ``` block with an image reference after rendering."""
    diagrams_dir = build_dir / DIAGRAMS_DIR
    diagrams_dir.mkdir(parents=True, exist_ok=True)

    def replacer(match: re.Match[str]) -> str:
        nonlocal index
        idx = index
        index += 1
        mermaid_src = match.group(1).strip()
        name = f"diagram_{idx:03d}"
        out_file = diagrams_dir / f"{name}.{image_format}"
        if not render_mermaid_to_image(mmdc_cmd, mermaid_src, out_file, scale=mermaid_scale):
            return f"\n\n<!-- mermaid block {idx} failed to render -->\n{match.group(0)}\n\n"
        rel = f"{DIAGRAMS_DIR}/{name}.{image_format}"
        return f"\n\n![]({rel})\n\n"

    index = 0
    return MERMAID_BLOCK_RE.sub(replacer, content)


def run_pandoc(
    input_md: Path,
    output_pdf: Path,
    metadata_file: Path | None,
    resource_path: Path,
    template: Path | str | None = None,
) -> bool:
    """Run pandoc to generate PDF from the processed markdown."""
    args = [
        "pandoc",
        str(input_md),
        "-o",
        str(output_pdf),
        "--resource-path",
        str(resource_path),
        "--pdf-engine=xelatex",
    ]
    if metadata_file and metadata_file.exists():
        args.extend(["--metadata-file", str(metadata_file)])
    if template:
        args.extend(["--template", str(template)])
    proc = subprocess.run(args, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert Markdown to PDF with Mermaid diagrams rendered as images."
    )
    parser.add_argument("input", type=Path, help="Input Markdown file")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Output PDF file")
    parser.add_argument(
        "--metadata",
        type=Path,
        default=None,
        help="Pandoc metadata YAML (e.g. for Eisvogel template)",
    )
    parser.add_argument(
        "--build-dir",
        type=Path,
        default=None,
        help="Directory for processed MD and diagram images (default: same dir as output)",
    )
    parser.add_argument(
        "--image-format",
        choices=("png", "svg"),
        default="png",
        help="Format for rendered Mermaid diagrams (default: png)",
    )
    parser.add_argument(
        "--skip-mermaid",
        action="store_true",
        help="Skip Mermaid rendering; only run pandoc (input must already have images)",
    )
    parser.add_argument(
        "--mermaid-scale",
        type=float,
        default=2.0,
        metavar="N",
        help="Scale factor for Mermaid PNG/SVG resolution (default: 2.0 for sharper PDFs)",
    )
    parser.add_argument(
        "--template",
        type=str,
        default=None,
        help="Pandoc template (e.g. eisvogel for title page)",
    )
    args = parser.parse_args()

    input_md = args.input.resolve()
    if not input_md.exists():
        print(f"Error: input file not found: {input_md}", file=sys.stderr)
        return 1

    output_pdf = args.output.resolve()
    output_pdf.parent.mkdir(parents=True, exist_ok=True)

    build_dir = args.build_dir or output_pdf.parent
    build_dir = build_dir.resolve()
    build_dir.mkdir(parents=True, exist_ok=True)

    content = input_md.read_text(encoding="utf-8")

    if not args.skip_mermaid:
        mmdc_cmd = find_mmdc()
        if mmdc_cmd is None:
            print(
                "Error: mermaid-cli not found. Install with: npm install -g @mermaid-js/mermaid-cli",
                file=sys.stderr,
            )
            return 1
        content = process_markdown(
            content, build_dir, mmdc_cmd, args.image_format, args.mermaid_scale
        )
        processed_md = build_dir / "_processed.md"
        processed_md.write_text(content, encoding="utf-8")
        pandoc_input = processed_md
        resource_path = build_dir
    else:
        pandoc_input = input_md
        resource_path = input_md.parent

    if shutil.which("pandoc") is None:
        print("Error: pandoc not found. Install pandoc to generate PDF.", file=sys.stderr)
        return 1

    if not run_pandoc(
        pandoc_input,
        output_pdf,
        args.metadata,
        resource_path,
        template=args.template,
    ):
        return 1

    print(f"Wrote {output_pdf}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
