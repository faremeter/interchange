# gemini-discover test assets

These files are committed test inputs for the Gemini API capability
captures in `bin/gemini-discover/`. They are regenerated from public-
domain or CC0 sources by `bin/gemini-discover/build-assets.sh`.

Every source listed below is either a US federal government work
(public domain in the US, no rights reserved) or a Project Gutenberg
text in the public domain in the US. None require attribution as a
license condition; the provenance is documented here regardless, per
the repository's external-content attribution policy.

## sample.jpg

- **Derived from:** NASA Photojournal image `PIA12235`, "Pulsing Polar
  Auroras", credit NASA/JPL/University of Iowa.
- **Source URL:** <https://images-assets.nasa.gov/image/PIA12235/PIA12235~thumb.jpg>
- **License:** Public domain. NASA still images, audio recordings,
  video, and computer files are generally not subject to copyright in
  the United States (see <https://www.nasa.gov/nasa-brand-center/images-and-media/>).
- **Retrieved:** 2026-05-20.
- **Transformations:** resized to 512px wide via `ffmpeg -vf scale`
  and re-encoded as JPEG at quality 6.

## sample.wav

- **Derived from:** "Audio Recording of President Clinton's Exchange
  with Reporters Prior to a Meeting with Federal Reserve Chairman
  Alan Greenspan", produced by the White House Communications Agency,
  hosted on DPLA and mirrored on Wikimedia Commons.
- **Source URL:** <https://upload.wikimedia.org/wikipedia/commons/f/fa/Audio_Recording_of_President_Clinton%27s_Exhange_with_Reporters_Prior_to_a_Meeting_with_Federal_Reserve_Chairman_Alan_Greenspan_-_DPLA_-_82e3da18fda445589929a687fa90211a.mp3>
- **License:** Public domain. The recording is a work of the United
  States federal government (White House Communications Agency,
  Department of Defense), which under 17 U.S.C. Sec. 105 is not subject
  to copyright protection in the United States. The Wikimedia Commons
  description page confirms the public-domain status.
- **Retrieved:** 2026-05-20.
- **Transformations:** trimmed to 4.0 seconds starting at 3.0 seconds
  into the source, downmixed to mono, resampled to 16 kHz, encoded as
  16-bit signed PCM in a WAV container, with source metadata stripped.

## sample.mp4

- **Derived from:** `sample.jpg` (still frame) and `sample.wav`
  (audio track), both described above. No additional source material.
- **License:** Public domain (composite of two public-domain inputs).
- **Generated:** 2026-05-20.
- **Transformations:** H.264 still-image video at 5 fps, 320 px wide,
  preset `veryslow`, CRF 32, paired with AAC audio at 24 kbps mono
  16 kHz. Faststart enabled. Length matches the audio (4.0 seconds).

## sample.pdf

- **Derived from:** "The Adventures of Tom Sawyer" by Mark Twain
  (Samuel Langhorne Clemens), Project Gutenberg eBook #74.
- **Source URL:** <https://www.gutenberg.org/files/74/74-0.txt>
- **License:** Public domain in the United States. The work entered
  the public domain by virtue of its publication date (1876); Project
  Gutenberg's redistribution adds no copyright. The Project Gutenberg
  trademark and license header are not included in the rendered PDF;
  only the body excerpt is.
- **Retrieved:** 2026-05-20.
- **Transformations:** the first ~1200 characters following the
  "CHAPTER I" heading are extracted, normalized to ASCII (smart
  quotes and em-dashes flattened), word-wrapped, rendered as a
  one-page PostScript document in 11pt Helvetica, and converted to
  PDF via `ps2pdf -dPDFSETTINGS=/screen`.

## Reproducibility

Re-running `bin/gemini-discover/build-assets.sh` against the same
source URLs produces semantically equivalent outputs. Byte-exact
reproducibility is not guaranteed because ffmpeg and Ghostscript
embed encoder version and date strings that vary across releases;
`-fflags +bitexact -flags +bitexact` minimizes but does not eliminate
this. The script strips source-side metadata where ffmpeg allows it.

## Size budget

The combined committed size of these four files is held under
500 KB. The build script fails loudly if the total exceeds that
budget, so you will notice immediately if a future edit blows it.
