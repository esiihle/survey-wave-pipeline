# Publishing survey-wave-pipeline

This is a brand-new repo, so publishing is simple: one clean initial commit,
pushed from the right folder. The only thing to get right is **not** committing
the outer zip folder (the nesting mistake from the first two repos).

## Step 1 — extract and find the real root

Extract the zip and drill in until you reach the folder that **directly
contains `src`, `README.md`, and `pyproject.toml`**. That exact folder is your
repo root — not a parent that contains another `survey-wave-pipeline` folder.

## Step 2 — open it in VSCode

File -> Open Folder -> select the folder from Step 1. Then open a Git Bash
terminal (Terminal -> New Terminal -> dropdown -> Git Bash).

## Step 3 — commit

Put your GitHub-verified email on line 2 so the commit counts as yours:

```bash
git init
git config user.name  "Sicelwesihle Myeza"
git config user.email "your-github-email@example.com"
git add -A
git commit -m "feat: initial release — wave-agnostic PySpark survey pipeline"
```

## Step 4 — create the GitHub repo and push

Create a new **empty** repo on GitHub named `survey-wave-pipeline` (no README,
no license — the repo already has them). Then:

```bash
git branch -M main
git remote add origin https://github.com/esiihle/survey-wave-pipeline.git
git push -u origin main
```

If a GitHub sign-in window pops up, authorise it.

## Step 5 — finish the presentation

- Add a **description** and **topics** on the repo page: e.g. description
  "Wave-agnostic PySpark survey tracker pipeline (synthetic-data showcase)",
  topics `pyspark`, `spark`, `data-engineering`, `etl`, `survey-data`.
- Confirm the README renders at the root and files are at the top level.

## Running it locally (for you, or anyone who clones it)

PySpark needs a JVM, so **Java 17+** must be installed locally (`java -version`
to check). Then:

```bash
pip install -e ".[dev]"
pytest -q
python examples/example_run.py
```

The GitHub Actions CI already sets up Java, so the badge/checks will run on push.

## Checklist before it's public

- [ ] Commit email is verified on your `esiihle` account.
- [ ] `LICENSE` has your full legal name (currently "Sicelwesihle Myeza").
- [ ] Manager sign-off covers generalised, synthetic-data-only publishing.
- [ ] No client data — the only data is synthetic, from the generator.

## When you want to iterate

Keep it honest and real, exactly like the other two tools: build a genuine
improvement (the roadmap in `docs/OVERVIEW.md` has good candidates — Delta Lake
`MERGE` upserts, data-quality assertions, significance testing on deltas), then
commit it as a coherent v0.2.0 change. Don't manufacture versions; let the
history grow as you actually build.
