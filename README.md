# 📈 Pricing Fetcher

Stock price monitoring with Telegram alerts.

## Features

- 🔄 Automated stock price fetching via yfinance
- 📊 PostgreSQL storage for historical data
- 🔔 **Telegram alerts** for significant price changes
- ⚡ Runs on GitHub Actions (free!)

## Setup

### 1. Create a Telegram Bot

1. Open Telegram and search for `@BotFather`
2. Send `/newbot` and follow the prompts
3. Copy the **Bot Token** (looks like `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`)
4. Start a chat with your new bot and send any message
5. Get your **Chat ID**:
   - Visit: `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`
   - Look for `"chat":{"id":123456789}` - that number is your Chat ID

### 2. Configure GitHub Secrets

Go to your repo → Settings → Secrets and variables → Actions

Add these **Secrets**:
| Secret Name | Value |
|-------------|-------|
| `DATABASE_URL` | Your PostgreSQL connection string |
| `TELEGRAM_BOT_TOKEN` | Bot token from BotFather |
| `TELEGRAM_CHAT_ID` | Your chat ID |

### 3. (Optional) Configure Alert Thresholds

Go to Settings → Secrets and variables → Actions → **Variables** tab

| Variable Name | Default | Description |
|--------------|---------|-------------|
| `ALERT_THRESHOLD_PERCENT` | `5.0` | Alert when price changes by this % |
| `ALERT_LOOKBACK_HOURS` | `1.0` | Compare current price vs X hours ago |
| `ALERT_COOLDOWN_HOURS` | `4.0` | Don't resend alert for same stock within X hours |

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                     GitHub Actions (every 15 min)               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Fetch current prices (yfinance)                            │
│                    ↓                                            │
│  2. Save to PostgreSQL                                          │
│                    ↓                                            │
│  3. Compare with historical prices                              │
│                    ↓                                            │
│  4. If change > threshold → Send Telegram alert! 📱            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Alert Example

```
🚀📈 ALERT: TSLA 🚀📈

📊 Tesla, Inc.

Cena wzrósł o 7.32%!

💰 Poprzednia cena: $248.50
💵 Aktualna cena: $266.69
📈 Zmiana: +7.32%

🕐 2024-01-15 14:30 UTC

Zobacz na Yahoo Finance →
```

## Local Testing

```bash
# Set environment variables
export DATABASE_URL="postgresql://..."
export TELEGRAM_BOT_TOKEN="your_token"
export TELEGRAM_CHAT_ID="your_chat_id"

# Test Telegram connection
python alerts.py test

# Run price scraping with alerts
python scrape_prices.py

# Manage alerts
python alerts.py list                  # List all alerts
python alerts.py add TSLA 3.0          # Alert when TSLA changes by 3%
python alerts.py remove TSLA           # Remove TSLA alert
```

## Files

| File | Description |
|------|-------------|
| `scrape_prices.py` | Fetches stock prices from Yahoo Finance |
| `alerts.py` | Telegram alert system |
| `update_symbols.py` | Updates list of tracked stock symbols |
| `app.py` | Flask API for accessing stock data |
| `models.py` | SQLAlchemy database models |

## GitHub Actions

| Workflow | Schedule | Description |
|----------|----------|-------------|
| `scrape_symbols.yml` | Every 15 min | Fetch prices & send alerts |
| `update_symbols.yml` | Daily at 21:00 | Update stock symbol list |

## Cost

**$0** 🎉

- GitHub Actions: 2000 free minutes/month (plenty for 15-min intervals)
- Telegram: Free
- PostgreSQL: Use free tier (Neon, Supabase, Railway, etc.)