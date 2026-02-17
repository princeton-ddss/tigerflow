Build or serve the documentation locally.

## Arguments
- `serve` (default) - Start local dev server with hot reload
- `build` - Build static site to `docs/mkdocs/site/`

## User's request
$ARGUMENTS

## Instructions

Run from the `docs/mkdocs` directory:

- **serve**: `uv run mkdocs serve` (runs at http://127.0.0.1:8000)
- **build**: `uv run mkdocs build`

If the user doesn't specify, default to `serve`.

After starting the server, let the user know they can access it at http://127.0.0.1:8000 and remind them to Ctrl+C to stop.
