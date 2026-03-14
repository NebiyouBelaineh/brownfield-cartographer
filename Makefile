# Markdown → PDF with Mermaid diagram rendering
#
# Requires: Python 3, pandoc, mermaid-cli (mmdc or npx)
#   npm install -g @mermaid-js/mermaid-cli   # or use npx (script will use it)
#
# Usage:
#   make pdf                          # build reports/final_report.pdf from reports/final_report.md
#   make pdf INPUT=... OUTPUT=...      # custom paths
#   make pdf INPUT=reports/final_report-diagram.md OUTPUT=reports/final_report-diagram.pdf SKIP_MERMAID=1  # diagrams already rendered

INPUT  ?= docs/reports/cartographer-final-report.md
OUTPUT ?= docs/reports/cartographer-final-report.pdf
METADATA ?= backup/templates/pdf-metadata.yaml
BUILD_DIR ?= $(dir $(OUTPUT))
MERMAID_SCALE ?= 2.0
SKIP_MERMAID ?=                                 # set to 1 to skip mermaid (use pre-rendered diagram MD)
METADATA_MODERN ?= backup/templates/pdf-metadata.yaml
TEMPLATE ?= backup/templates/eisvogel.latex
PYTHON ?= python3

.PHONY: pdf check-deps help clean

help:
	@echo "Markdown to PDF (Mermaid → images, then Pandoc)"
	@echo ""
	@echo "  make pdf              Build PDF from ${INPUT} → ${OUTPUT}"
# 	@echo "  make pdf-modern       Build PDF from docs/reports/final-report.md → docs/reports/final-report-modern.pdf (modern template)"
	@echo "  make pdf INPUT=<.md> OUTPUT=<.pdf>   Custom input and output"
	@echo "  make check-deps       Verify pandoc and mermaid-cli are available"
	@echo "  make clean            Remove generated diagrams and processed markdown under build dirs"
	@echo ""
	@echo "Variables: INPUT, OUTPUT, METADATA, BUILD_DIR, SKIP_MERMAID (set to 1 to skip mermaid)"

check-deps:
	@command -v pandoc >/dev/null 2>&1 || { echo "pandoc not found. Install: https://pandoc.org/"; exit 1; }
	@command -v mmdc >/dev/null 2>&1 || command -v npx >/dev/null 2>&1 || { echo "mermaid-cli not found. Install: npm install -g @mermaid-js/mermaid-cli"; exit 1; }
	@echo "Dependencies OK (pandoc, mermaid-cli or npx)"

pdf: $(if $(SKIP_MERMAID),,check-deps)
	@command -v pandoc >/dev/null 2>&1 || { echo "pandoc not found. Install: https://pandoc.org/"; exit 1; }
	$(PYTHON) scripts/md_to_pdf.py "$(INPUT)" -o "$(OUTPUT)" --metadata "$(METADATA)" --build-dir "$(BUILD_DIR)" --mermaid-scale "$(MERMAID_SCALE)" $(if $(SKIP_MERMAID),--skip-mermaid,)

# pdf-modern: $(if $(SKIP_MERMAID),,check-deps)
# 	@command -v pandoc >/dev/null 2>&1 || { echo "pandoc not found. Install: https://pandoc.org/"; exit 1; }
# 	$(PYTHON) scripts/md_to_pdf.py "$(INPUT)" -o "$(OUTPUT)" --metadata "$(METADATA_MODERN)" --build-dir "$(BUILD_DIR)" --font-dir "$(CURDIR)/docs/templates" --mermaid-scale "$(MERMAID_SCALE)" $(if $(SKIP_MERMAID),--skip-mermaid,)

# Remove common build artifacts (processed md and diagram images)
# Adjust if you use a different BUILD_DIR
clean:
	rm -rf docs/reports/diagrams docs/reports/_processed.md
	rm -rf docs/output/diagrams docs/output/_processed.md
	rm -rf .md_to_pdf_build
