import os
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, BigInteger, text, func
from sqlalchemy.orm import declarative_base, sessionmaker
import requests
import yfinance as yf
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

Base = declarative_base()


TAKE_PROFIT_PCT = 3.0
STOP_LOSS_PCT = -3.0
TRAILING_STOP_TRIGGER = 3.0
MAX_HOLD_HOURS = 24
SLIPPAGE_PCT = 0.5  # Realistic entry penalty (Spread + Reaction Time) for Trade Republic


class AlertOutcome(Base):
    __tablename__ = 'alert_outcomes'
    id = Column(Integer, primary_key=True)
    alert_id = Column(Integer, nullable=False)
    symbol = Column(String, nullable=False)
    alert_type = Column(String, nullable=False)
    alert_time = Column(DateTime, nullable=False)
    alert_price = Column(Float, nullable=False)
    volume_ratio = Column(Float, nullable=True)
    
    price_1h = Column(Float, nullable=True)
    price_4h = Column(Float, nullable=True)
    price_24h = Column(Float, nullable=True)
    
    profit_1h = Column(Float, nullable=True)
    profit_4h = Column(Float, nullable=True)
    profit_24h = Column(Float, nullable=True)
    
    checked_1h = Column(Boolean, default=False)
    checked_4h = Column(Boolean, default=False)
    checked_24h = Column(Boolean, default=False)
    
    trade_result = Column(Float, nullable=True)
    trade_exit_reason = Column(String, nullable=True)
    trade_max_gain = Column(Float, nullable=True)
    trade_max_drawdown = Column(Float, nullable=True)
    trade_hold_minutes = Column(Integer, nullable=True)
    trade_checked = Column(Boolean, default=False)
    
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("No DATABASE_URL environment variable set")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://")

if "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require" if "?" not in DATABASE_URL else "&sslmode=require"

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')


def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        return response.status_code == 200
    except:
        return False


def get_current_price(symbol: str) -> float:
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info
        if hasattr(info, 'last_price') and info.last_price:
            return float(info.last_price)
        elif hasattr(info, 'previous_close') and info.previous_close:
            return float(info.previous_close)
    except:
        pass
    return None


def create_outcome_from_alert(alert_id: int, symbol: str, alert_type: str, 
                               alert_time: datetime, alert_price: float,
                               volume_ratio: float = None):
    existing = session.query(AlertOutcome).filter(AlertOutcome.alert_id == alert_id).first()
    if existing:
        return
    
    outcome = AlertOutcome(
        alert_id=alert_id,
        symbol=symbol,
        alert_type=alert_type,
        alert_time=alert_time,
        alert_price=alert_price,
        volume_ratio=volume_ratio
    )
    session.add(outcome)
    session.commit()
    print(f"Created outcome tracking for {symbol}")


def simulate_trade(symbol: str, entry_price: float, alert_time: datetime, is_long: bool = True):
    
    try:
        ticker = yf.Ticker(symbol)
        end_time = alert_time + timedelta(hours=MAX_HOLD_HOURS + 1)
        
        hist = ticker.history(start=alert_time, end=end_time, interval="5m")
        
        if hist.empty or len(hist) < 2:
            hist = ticker.history(period="1d", interval="5m")
        
        if hist.empty:
            return None
        
        max_gain = 0.0
        max_drawdown = 0.0
        trailing_stop_active = False
        trailing_stop_level = STOP_LOSS_PCT
        
        for i, (idx, row) in enumerate(hist.iterrows()):
            high = row['High']
            low = row['Low']
            close = row['Close']
            
            if is_long:
                current_gain_high = ((high - entry_price) / entry_price) * 100
                current_gain_low = ((low - entry_price) / entry_price) * 100
                current_gain = ((close - entry_price) / entry_price) * 100
            else:
                current_gain_high = ((entry_price - low) / entry_price) * 100
                current_gain_low = ((entry_price - high) / entry_price) * 100
                current_gain = ((entry_price - close) / entry_price) * 100
            
            max_gain = max(max_gain, current_gain_high)
            max_drawdown = min(max_drawdown, current_gain_low)
            
            if max_gain >= TRAILING_STOP_TRIGGER and not trailing_stop_active:
                trailing_stop_active = True
                trailing_stop_level = 0.0
            
            if trailing_stop_active:
                new_stop = max_gain - 2.0
                trailing_stop_level = max(trailing_stop_level, new_stop)
            
            if current_gain_high >= TAKE_PROFIT_PCT:
                return {
                    'result': float(TAKE_PROFIT_PCT),
                    'exit_reason': 'take_profit',
                    'max_gain': float(max_gain),
                    'max_drawdown': float(max_drawdown),
                    'hold_minutes': (i + 1) * 5
                }
            
            if current_gain_low <= STOP_LOSS_PCT:
                return {
                    'result': float(STOP_LOSS_PCT),
                    'exit_reason': 'stop_loss',
                    'max_gain': float(max_gain),
                    'max_drawdown': float(max_drawdown),
                    'hold_minutes': (i + 1) * 5
                }
            
            if trailing_stop_active and current_gain_low <= trailing_stop_level:
                return {
                    'result': float(trailing_stop_level),
                    'exit_reason': 'trailing_stop',
                    'max_gain': float(max_gain),
                    'max_drawdown': float(max_drawdown),
                    'hold_minutes': (i + 1) * 5
                }
            
            if (i + 1) * 5 >= MAX_HOLD_HOURS * 60:
                return {
                    'result': float(current_gain),
                    'exit_reason': 'timeout',
                    'max_gain': float(max_gain),
                    'max_drawdown': float(max_drawdown),
                    'hold_minutes': (i + 1) * 5
                }
        
        final_gain = ((hist['Close'].iloc[-1] - entry_price) / entry_price) * 100
        if not is_long:
            final_gain = -final_gain
            
        return {
            'result': float(final_gain),
            'exit_reason': 'end_of_data',
            'max_gain': float(max_gain),
            'max_drawdown': float(max_drawdown),
            'hold_minutes': len(hist) * 5
        }
        
    except Exception as e:
        print(f"Trade simulation error for {symbol}: {e}")
        return None


def check_outcomes():
    
    print("Checking alert outcomes...")
    now = datetime.now(timezone.utc)
    print(f"[DEBUG] now = {now}, tzinfo = {now.tzinfo}")
    
    outcomes_to_check = session.query(AlertOutcome).filter(
        (AlertOutcome.checked_24h == False)
    ).all()
    
    print(f"Found {len(outcomes_to_check)} outcomes to check")
    
    if not outcomes_to_check:
        print("No outcomes to check")
        return
    
    # Get unique symbols
    symbols = list(set(o.symbol for o in outcomes_to_check))
    print(f"[DEBUG] Fetching prices for {len(symbols)} unique symbols in batch...")
    
    # Batch download all prices at once (MUCH faster than individual requests)
    current_prices = {}
    try:
        data = yf.download(symbols, period="1d", progress=False, threads=True)
        
        if len(symbols) == 1:
            # Single symbol returns Series, not DataFrame with MultiIndex
            symbol = symbols[0]
            if 'Close' in data and len(data['Close']) > 0:
                current_prices[symbol] = float(data['Close'].iloc[-1])
        else:
            # Multiple symbols returns DataFrame with MultiIndex columns
            if 'Close' in data.columns.get_level_values(0):
                close_prices = data['Close']
                for symbol in symbols:
                    if symbol in close_prices.columns:
                        price = close_prices[symbol].dropna()
                        if len(price) > 0:
                            current_prices[symbol] = float(price.iloc[-1])
        
        print(f"[DEBUG] Got prices for {len(current_prices)} symbols")
    except Exception as e:
        print(f"[ERROR] Batch download failed: {e}")
        return
    
    # Debug: log first 3 outcomes
    for idx, outcome in enumerate(outcomes_to_check[:3]):
        print(f"[DEBUG] outcome.alert_time = {outcome.alert_time}, type = {type(outcome.alert_time)}, tzinfo = {getattr(outcome.alert_time, 'tzinfo', 'N/A')}")
    
    # Process all outcomes
    updated_count = 0
    for outcome in outcomes_to_check:
        # Ensure alert_time is timezone-aware (DB stores as naive UTC)
        alert_time = outcome.alert_time
        if alert_time.tzinfo is None:
            alert_time = alert_time.replace(tzinfo=timezone.utc)
        
        hours_since_alert = (now - alert_time).total_seconds() / 3600
        
        current_price = current_prices.get(outcome.symbol)
        if not current_price:
            continue
        
        # Simulate all as LONG (Buy) to see if 'buying the dip' works on down alerts
        # is_buy_signal = True
        
        # CHANGED: 'Buying the dip' on down alerts is a proven fast way to lose money
        # We will only go LONG on 'up' alerts. For 'down' alerts, we simulate SHORT to see if momentum continues.
        is_buy_signal = 'up' in (outcome.alert_type or '') if outcome.alert_type else True
        
        # Calculate realistic entry price (including slippage/spread)
        if is_buy_signal:
            effective_entry_price = outcome.alert_price * (1 + SLIPPAGE_PCT / 100)
        else:
            effective_entry_price = outcome.alert_price * (1 - SLIPPAGE_PCT / 100)
            
        def get_historical_price_at_offset(hours):
            try:
                target_time = alert_time + timedelta(hours=hours)
                hist = yf.Ticker(outcome.symbol).history(
                    start=target_time - timedelta(minutes=10), 
                    end=target_time + timedelta(hours=2), 
                    interval="5m"
                )
                if not hist.empty:
                    return float(hist['Close'].iloc[0])
            except:
                pass
            return current_price
            
        if hours_since_alert >= 1 and not outcome.checked_1h:
            p_1h = get_historical_price_at_offset(1)
            if p_1h:
                outcome.price_1h = p_1h
                outcome.profit_1h = ((p_1h - effective_entry_price) / effective_entry_price) * 100 if is_buy_signal else ((effective_entry_price - p_1h) / effective_entry_price) * 100
                outcome.checked_1h = True
                print(f"{outcome.symbol} 1h: {outcome.profit_1h:.2f}% (Entry: {effective_entry_price:.2f})")
                updated_count += 1
        
        if hours_since_alert >= 4 and not outcome.checked_4h:
            p_4h = get_historical_price_at_offset(4)
            if p_4h:
                outcome.price_4h = p_4h
                outcome.profit_4h = ((p_4h - effective_entry_price) / effective_entry_price) * 100 if is_buy_signal else ((effective_entry_price - p_4h) / effective_entry_price) * 100
                outcome.checked_4h = True
                print(f"{outcome.symbol} 4h: {outcome.profit_4h:.2f}%")
                updated_count += 1
        
        if hours_since_alert >= 24 and not outcome.checked_24h:
            p_24h = get_historical_price_at_offset(24)
            if p_24h:
                outcome.price_24h = p_24h
                outcome.profit_24h = ((p_24h - effective_entry_price) / effective_entry_price) * 100 if is_buy_signal else ((effective_entry_price - p_24h) / effective_entry_price) * 100
                outcome.checked_24h = True
                print(f"{outcome.symbol} 24h: {outcome.profit_24h:.2f}%")
                updated_count += 1
        
        if hours_since_alert >= MAX_HOLD_HOURS and not outcome.trade_checked:
            trade = simulate_trade(
                symbol=outcome.symbol,
                entry_price=effective_entry_price,
                alert_time=alert_time,
                is_long=is_buy_signal
            )
            if trade:
                outcome.trade_result = trade['result']
                outcome.trade_exit_reason = trade['exit_reason']
                outcome.trade_max_gain = trade['max_gain']
                outcome.trade_max_drawdown = trade['max_drawdown']
                outcome.trade_hold_minutes = trade['hold_minutes']
                outcome.trade_checked = True
                print(f"{outcome.symbol} TRADE: {trade['result']:+.2f}% ({trade['exit_reason']})")
                updated_count += 1
    
    session.commit()
    print(f"Outcome check complete - updated {updated_count} records")


def generate_report(days: int = 7):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    
    outcomes = session.query(AlertOutcome).filter(
        AlertOutcome.alert_time >= cutoff,
        AlertOutcome.checked_24h == True
    ).all()
    
    if not outcomes:
        print("No completed outcomes found")
        return None
    
    total_alerts = len(outcomes)
    
    volume_spike_outcomes = [o for o in outcomes if 'volume_spike' in (o.alert_type or '')]
    extreme_outcomes = [o for o in outcomes if 'extreme' in (o.alert_type or '')]
    regular_outcomes = [o for o in outcomes if o.alert_type in ('spike_up', 'spike_down', None) or 
                        (o.alert_type and 'volume_spike' not in o.alert_type and 'extreme' not in o.alert_type)]
    
    def calc_stats(outcomes_list):
        if not outcomes_list:
            return {"count": 0, "avg_1h": 0, "avg_4h": 0, "avg_24h": 0, 
                    "win_rate_1h": 0, "win_rate_4h": 0, "win_rate_24h": 0,
                    "median_24h": 0, "trade_profit": 0, "trade_win_rate": 0,
                    "take_profits": 0, "stop_losses": 0, "trailing_stops": 0}
        
        profits_1h = [o.profit_1h for o in outcomes_list if o.profit_1h is not None]
        profits_4h = [o.profit_4h for o in outcomes_list if o.profit_4h is not None]
        profits_24h = [o.profit_24h for o in outcomes_list if o.profit_24h is not None]
        trade_results = [o.trade_result for o in outcomes_list if o.trade_result is not None]
        
        avg_1h = sum(profits_1h) / len(profits_1h) if profits_1h else 0
        avg_4h = sum(profits_4h) / len(profits_4h) if profits_4h else 0
        avg_24h = sum(profits_24h) / len(profits_24h) if profits_24h else 0
        
        sorted_24h = sorted(profits_24h)
        median_24h = sorted_24h[len(sorted_24h) // 2] if sorted_24h else 0
        
        win_rate_1h = len([p for p in profits_1h if p > 0]) / len(profits_1h) * 100 if profits_1h else 0
        win_rate_4h = len([p for p in profits_4h if p > 0]) / len(profits_4h) * 100 if profits_4h else 0
        win_rate_24h = len([p for p in profits_24h if p > 0]) / len(profits_24h) * 100 if profits_24h else 0
        
        trade_profit = sum(trade_results) / len(trade_results) if trade_results else 0
        trade_win_rate = len([t for t in trade_results if t > 0]) / len(trade_results) * 100 if trade_results else 0
        
        take_profits = len([o for o in outcomes_list if o.trade_exit_reason == 'take_profit'])
        stop_losses = len([o for o in outcomes_list if o.trade_exit_reason == 'stop_loss'])
        trailing_stops = len([o for o in outcomes_list if o.trade_exit_reason == 'trailing_stop'])
        
        return {
            "count": len(outcomes_list),
            "avg_1h": avg_1h,
            "avg_4h": avg_4h,
            "avg_24h": avg_24h,
            "median_24h": median_24h,
            "win_rate_1h": win_rate_1h,
            "win_rate_4h": win_rate_4h,
            "win_rate_24h": win_rate_24h,
            "trade_profit": trade_profit,
            "trade_win_rate": trade_win_rate,
            "take_profits": take_profits,
            "stop_losses": stop_losses,
            "trailing_stops": trailing_stops
        }
    
    all_stats = calc_stats(outcomes)
    volume_stats = calc_stats(volume_spike_outcomes)
    extreme_stats = calc_stats(extreme_outcomes)
    regular_stats = calc_stats(regular_outcomes)
    
    trades_with_result = [o for o in outcomes if o.trade_result is not None]
    total_trade_pnl = sum(o.trade_result for o in trades_with_result) if trades_with_result else 0
    
    best = sorted([o for o in outcomes if o.trade_result], key=lambda x: x.trade_result, reverse=True)[:5]
    worst = sorted([o for o in outcomes if o.trade_result], key=lambda x: x.trade_result)[:5]
    
    report = f"""
📊 <b>TRADING PERFORMANCE REPORT</b>
📅 Last {days} days | Strategy: TP +{TAKE_PROFIT_PCT}% / SL {STOP_LOSS_PCT}% | Slippage: -{SLIPPAGE_PCT}%

💰 <b>REALISTIC TRADING RESULTS</b>
Total trades: {len(trades_with_result)}
━━━━━━━━━━━━━━━━━━━━━
Total P&L: <b>{total_trade_pnl:+.1f}%</b>
Avg per trade: {all_stats['trade_profit']:+.2f}%
Win rate: {all_stats['trade_win_rate']:.0f}%

✅ Take profits: {all_stats['take_profits']}
🛡️ Trailing stops: {all_stats['trailing_stops']}
❌ Stop losses: {all_stats['stop_losses']}

🔥 <b>VOLUME SPIKE TRADES</b> ({volume_stats['count']})
Avg: {volume_stats['trade_profit']:+.2f}% | Win: {volume_stats['trade_win_rate']:.0f}%
TP: {volume_stats['take_profits']} | TS: {volume_stats['trailing_stops']} | SL: {volume_stats['stop_losses']}

🚨 <b>EXTREME MOVE TRADES</b> ({extreme_stats['count']})
Avg: {extreme_stats['trade_profit']:+.2f}% | Win: {extreme_stats['trade_win_rate']:.0f}%

📈 <b>HOLD COMPARISON</b> (no stops)
1h hold: {all_stats['avg_1h']:+.2f}% | 4h: {all_stats['avg_4h']:+.2f}% | 24h: {all_stats['avg_24h']:+.2f}%

🏆 <b>BEST TRADES</b>
"""
    
    for o in best:
        exit_str = o.trade_exit_reason or 'N/A'
        report += f"{o.symbol}: {o.trade_result:+.2f}% ({exit_str})\n"
    
    report += f"\n💀 <b>WORST TRADES</b>\n"
    for o in worst:
        exit_str = o.trade_exit_reason or 'N/A'
        report += f"{o.symbol}: {o.trade_result:+.2f}% ({exit_str})\n"
    
    if len(trades_with_result) >= 5:
        if total_trade_pnl > 0 and all_stats['trade_win_rate'] >= 50:
            verdict = f"✅ PROFITABLE! {total_trade_pnl:+.1f}% total, {all_stats['trade_win_rate']:.0f}% wins"
        elif total_trade_pnl > 0:
            verdict = f"🟡 Positive P&L ({total_trade_pnl:+.1f}%) but win rate {all_stats['trade_win_rate']:.0f}% is low"
        elif all_stats['trade_win_rate'] >= 50:
            verdict = f"🟡 Good win rate ({all_stats['trade_win_rate']:.0f}%) but negative P&L ({total_trade_pnl:+.1f}%)"
        else:
            verdict = f"❌ NOT PROFITABLE: {total_trade_pnl:+.1f}% P&L, {all_stats['trade_win_rate']:.0f}% wins"
    else:
        verdict = f"⏳ Need more data ({len(trades_with_result)}/5 trades)"
    
    report += f"\n<b>VERDICT:</b> {verdict}"
    
    return report.strip()


def sync_alerts_to_outcomes():
    try:
        from alerts import AlertHistory, Base as AlertsBase
        
        # Ensure alert_history table exists
        AlertsBase.metadata.create_all(engine)
        
        recent_alerts = session.query(AlertHistory).filter(
            AlertHistory.sent_at >= datetime.now(timezone.utc) - timedelta(days=30)
        ).all()
        
        synced = 0
        for alert in recent_alerts:
            create_outcome_from_alert(
                alert_id=alert.id,
                symbol=alert.symbol,
                alert_type=alert.alert_type,
                alert_time=alert.sent_at,
                alert_price=alert.price_after,
                volume_ratio=getattr(alert, 'volume_ratio', None)
            )
            synced += 1
        
        if synced:
            print(f"Synced {synced} alerts to outcomes")
        else:
            print("No new alerts to sync")
            
    except Exception as e:
        print(f"Alert sync skipped: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "check":
            sync_alerts_to_outcomes()
            check_outcomes()
        
        elif command == "report":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
            report = generate_report(days)
            if report:
                print(report)
                send_telegram(report)
        
        elif command == "weekly":
            report = generate_report(7)
            if report:
                print(report)
                send_telegram(report)
        
        elif command == "monthly":
            report = generate_report(30)
            if report:
                print(report)
                send_telegram(report)
        
        else:
            print("""
Usage:
    python backtest.py check     - Check alert outcomes
    python backtest.py report 7  - Generate report (default 7 days)
    python backtest.py weekly    - Weekly report
    python backtest.py monthly   - Monthly report
""")
    else:
        sync_alerts_to_outcomes()
        check_outcomes()
