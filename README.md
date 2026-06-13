# cotflow-data

Public data-sync repo for COT Flow.

This repo is meant to hold:
- GitHub Actions schedules
- data-sync scripts
- generated public data files

## Planned cadence

- COT: weekly on Friday after the report window
- Market data and CBOE: several times per US session on weekdays

## What the sync does

- downloads COT snapshots from CFTC
- downloads delayed CBOE options data
- downloads price history from Yahoo Finance
- builds curve snapshots for supported futures markets
- writes derived feature files into `data/features`

## Next steps

1. Open the GitHub Actions tab and confirm the schedules are enabled.
2. Run `sync-market` manually once to populate the repo.
3. Review the generated files in `data/`.
