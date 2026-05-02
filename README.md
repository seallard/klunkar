# klunkar

Telegram bot that notifies subscribers with the top 10 Vivino-rated wines from Systembolaget's upcoming *tillfälligt sortiment* releases, sent the day before each release.

## Commands

| Command | Description |
|---|---|
| `/start` | Subscribe to release notifications |
| `/stop` | Unsubscribe |
| `/releases` | List upcoming release dates (next 90 days) |
| `/preview` | Show ranked wines for the next upcoming release |
| `/preview 2026-05-08` | Show ranked wines for a specific date |
| `/budget 150` | Only show wines under 150 kr |
| `/budget` | Remove budget filter |
| `/help` | Show this command list |

## Development

Enable git hooks (formats staged Python files on commit, runs tests on push):

```sh
bash scripts/install-hooks.sh
```
