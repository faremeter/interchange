#!/usr/bin/env bash
# shellcheck shell=bash

source "$(dirname -- "${BASH_SOURCE[0]}")/../opsh"
opsh::version::require v0.9.0
lib::import step-runner

REPODIR=$(git rev-parse --show-toplevel)
ASSETSDIR="$REPODIR/bin/gemini-discover/assets"

# Source URLs and provenance for downloaded inputs. Update assets/README.md
# in lockstep with any change here.
IMAGE_SRC_URL="https://images-assets.nasa.gov/image/PIA12235/PIA12235~thumb.jpg"
AUDIO_SRC_URL="https://upload.wikimedia.org/wikipedia/commons/f/fa/Audio_Recording_of_President_Clinton%27s_Exhange_with_Reporters_Prior_to_a_Meeting_with_Federal_Reserve_Chairman_Alan_Greenspan_-_DPLA_-_82e3da18fda445589929a687fa90211a.mp3"
TEXT_SRC_URL="https://www.gutenberg.org/files/74/74-0.txt"

# Trim window for the audio source. The Clinton/Greenspan recording opens
# with ~3s of room tone, then the President begins speaking. We skip the
# tone and capture 4s of speech.
AUDIO_TRIM_START="00:00:03.0"
AUDIO_TRIM_DURATION="4.0"

require::tool() {
	local tool="$1"
	command -v "$tool" >/dev/null \
		|| log::fatal "required tool not found: $tool"
}

step::00::check_tools() {
	log::info "checking required tools..."
	require::tool curl
	require::tool ffmpeg
	require::tool ps2pdf
	require::tool python3
}

step::10::prepare_workdir() {
	WORKDIR=$(temp::dir)
	log::info "workdir: $WORKDIR"
	mkdir -p "$ASSETSDIR"
}

step::20::fetch_sources() {
	log::info "downloading image source..."
	curl -fsSL -o "$WORKDIR/source.jpg" "$IMAGE_SRC_URL" \
		|| log::fatal "failed to download image source"

	log::info "downloading audio source..."
	curl -fsSL -o "$WORKDIR/source.mp3" "$AUDIO_SRC_URL" \
		|| log::fatal "failed to download audio source"

	log::info "downloading text source..."
	curl -fsSL -o "$WORKDIR/source.txt" "$TEXT_SRC_URL" \
		|| log::fatal "failed to download text source"
}

step::30::build_image() {
	log::info "building sample.jpg..."
	# Resize to 512px wide, re-encode at q=6 (low-ish quality). This both
	# shrinks the file and detaches it from any source-side metadata.
	# -fflags/-flags +bitexact strips encoder version stamps for stable
	# byte output across ffmpeg versions where possible.
	ffmpeg -y -hide_banner -loglevel error \
		-fflags +bitexact -flags +bitexact \
		-i "$WORKDIR/source.jpg" \
		-vf "scale=512:-2" \
		-q:v 6 \
		"$ASSETSDIR/sample.jpg" \
		|| log::fatal "ffmpeg failed to build sample.jpg"
}

step::40::build_audio() {
	log::info "building sample.wav..."
	# Mono, 16 kHz, 16-bit PCM. Trimmed window defined above.
	ffmpeg -y -hide_banner -loglevel error \
		-fflags +bitexact -flags +bitexact \
		-ss "$AUDIO_TRIM_START" -t "$AUDIO_TRIM_DURATION" \
		-i "$WORKDIR/source.mp3" \
		-ac 1 -ar 16000 -sample_fmt s16 \
		-map_metadata -1 \
		"$ASSETSDIR/sample.wav" \
		|| log::fatal "ffmpeg failed to build sample.wav"
}

step::50::build_video() {
	log::info "building sample.mp4..."
	# Combine the still image and trimmed audio into a short H.264/AAC
	# clip. -shortest stops the video when the audio ends.
	ffmpeg -y -hide_banner -loglevel error \
		-fflags +bitexact -flags +bitexact \
		-loop 1 -i "$ASSETSDIR/sample.jpg" \
		-i "$ASSETSDIR/sample.wav" \
		-c:v libx264 -tune stillimage -preset veryslow -crf 32 \
		-pix_fmt yuv420p -r 5 -vf "scale=320:-2" \
		-c:a aac -b:a 24k -ac 1 -ar 16000 \
		-movflags +faststart \
		-shortest \
		"$ASSETSDIR/sample.mp4" \
		|| log::fatal "ffmpeg failed to build sample.mp4"
}

step::60::build_pdf() {
	log::info "building sample.pdf..."
	# Extract the opening paragraph of the Project Gutenberg text and
	# emit a minimal one-page PostScript document, which ps2pdf turns
	# into a PDF. The inline python3 normalizes Gutenberg's smart
	# quotes and em-dashes to ASCII so the document renders in the
	# PostScript standard fonts without requiring a Unicode CMap.
	python3 - "$WORKDIR/source.txt" "$WORKDIR/sample.ps" <<'PYEOF' \
		|| log::fatal "failed to build PostScript"
import re
import sys

src_path, out_path = sys.argv[1], sys.argv[2]
text = open(src_path, encoding="utf-8").read()

# Locate the first chapter heading and take a fixed slice of the body
# that follows it. This keeps the output deterministic across re-runs.
m = re.search(r"\nCHAPTER I\.[^\n]*\n+", text)
if not m:
    raise SystemExit("expected to find a CHAPTER I heading in source text")
body = text[m.end():m.end() + 1200]

# Collapse runs of whitespace, then normalize to ASCII for the PS
# standard Helvetica font.
body = re.sub(r"\s+", " ", body).strip()
table = {
    0x2018: "'", 0x2019: "'",
    0x201C: '"', 0x201D: '"',
    0x2013: "-", 0x2014: "-",
    0x2026: "...",
    0xFEFF: "",
}
body = body.translate(table).encode("ascii", "ignore").decode("ascii")

def ps_escape(s):
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

# Greedy word wrap at ~75 chars; one PostScript `show` per line.
words = body.split(" ")
lines, cur = [], ""
for w in words:
    candidate = (cur + " " + w).strip()
    if len(candidate) > 75 and cur:
        lines.append(cur)
        cur = w
    else:
        cur = candidate
if cur:
    lines.append(cur)
lines = lines[:40]

out = ["%!PS-Adobe-3.0", "%%Pages: 1", "%%EndComments", "%%Page: 1 1"]
out.append("/Helvetica findfont 11 scalefont setfont")
y = 740
out.append("72 760 moveto (The Adventures of Tom Sawyer - Chapter I) show")
for line in lines:
    out.append(f"72 {y} moveto ({ps_escape(line)}) show")
    y -= 14
out += ["showpage", "%%EOF"]

open(out_path, "w", encoding="ascii").write("\n".join(out) + "\n")
PYEOF

	ps2pdf -dPDFSETTINGS=/screen -dCompatibilityLevel=1.4 \
		"$WORKDIR/sample.ps" "$ASSETSDIR/sample.pdf" \
		|| log::fatal "ps2pdf failed to build sample.pdf"
}

step::70::report_sizes() {
	log::info "asset sizes:"
	local total=0
	local bytes
	for f in sample.jpg sample.wav sample.mp4 sample.pdf; do
		bytes=$(wc -c <"$ASSETSDIR/$f")
		total=$((total + bytes))
		log::info "  $f: $bytes bytes"
	done
	log::info "  total: $total bytes"
	if (( total > 500000 )); then
		log::fatal "total asset size $total exceeds 500000 byte budget"
	fi
}

steps::run step
