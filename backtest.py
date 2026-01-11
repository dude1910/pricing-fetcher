import os
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, BigInteger, text, func
from sqlalchemy.orm import declarative_base, sessionmaker
import requests

Base = declarative_base()


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
        import yfinance as yf
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


def check_outcomes():
    print("Checking alert outcomes...")
    now = datetime.now(timezone.utc)
    print(f"[DEBUG] now = {now}, tzinfo = {now.tzinfo}")
    
    outcomes_to_check = session.query(AlertOutcome).filter(
        (AlertOutcome.checked_24h == False)
    ).all()
    
    print(f"Found {len(outcomes_to_check)} outcomes to check")
    
    for idx, outcome in enumerate(outcomes_to_check):
        # Log first 3 outcomes for debugging
        if idx < 3:
            print(f"[DEBUG] outcome.alert_time = {outcome.alert_time}, type = {type(outcome.alert_time)}, tzinfo = {getattr(outcome.alert_time, 'tzinfo', 'N/A')}")
        
        # Ensure alert_time is timezone-aware (DB stores as naive UTC)
        alert_time = outcome.alert_time
        if alert_time.tzinfo is None:
            if idx < 3:
                print(f"[DEBUG] alert_time is naive, adding UTC timezone")
            alert_time = alert_time.replace(tzinfo=timezone.utc)
        
        if idx < 3:
            print(f"[DEBUG] after fix: alert_time = {alert_time}, tzinfo = {alert_time.tzinfo}")
        
        hours_since_alert = (now - alert_time).total_seconds() / 3600
        
        if idx < 3:
            print(f"[DEBUG] hours_since_alert = {hours_since_alert:.2f}h for {outcome.symbol}")
        
        current_price = get_current_price(outcome.symbol)
        if not current_price:
            continue
        
        is_buy_signal = 'up' in outcome.alert_type
        
        if hours_since_alert >= 1 and not outcome.checked_1h:
            outcome.price_1h = current_price
            if is_buy_signal:
                outcome.profit_1h = ((current_price - outcome.alert_price) / outcome.alert_price) * 100
            else:
                outcome.profit_1h = ((outcome.alert_price - current_price) / outcome.alert_price) * 100
            outcome.checked_1h = True
            print(f"{outcome.symbol} 1h: {outcome.profit_1h:.2f}%")
        
        if hours_since_alert >= 4 and not outcome.checked_4h:
            outcome.price_4h = current_price
            if is_buy_signal:
                outcome.profit_4h = ((current_price - outcome.alert_price) / outcome.alert_price) * 100
            else:
                outcome.profit_4h = ((outcome.alert_price - current_price) / outcome.alert_price) * 100
            outcome.checked_4h = True
            print(f"{outcome.symbol} 4h: {outcome.profit_4h:.2f}%")
        
        if hours_since_alert >= 24 and not outcome.checked_24h:
            outcome.price_24h = current_price
            if is_buy_signal:
                outcome.profit_24h = ((current_price - outcome.alert_price) / outcome.alert_price) * 100
            else:
                outcome.profit_24h = ((outcome.alert_price - current_price) / outcome.alert_price) * 100
            outcome.checked_24h = True
            print(f"{outcome.symbol} 24h: {outcome.profit_24h:.2f}%")
        
        session.commit()
    
    print("Outcome check complete")


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
    
    volume_spike_outcomes = [o for o in outcomes if o.volume_ratio and o.volume_ratio >= 2.0]
    regular_outcomes = [o for o in outcomes if not o.volume_ratio or o.volume_ratio < 2.0]
    
    def calc_stats(outcomes_list):
        if not outcomes_list:
            return {"count": 0, "avg_1h": 0, "avg_4h": 0, "avg_24h": 0, 
                    "win_rate_1h": 0, "win_rate_4h": 0, "win_rate_24h": 0}
        
        profits_1h = [o.profit_1h for o in outcomes_list if o.profit_1h is not None]
        profits_4h = [o.profit_4h for o in outcomes_list if o.profit_4h is not None]
        profits_24h = [o.profit_24h for o in outcomes_list if o.profit_24h is not None]
        
        avg_1h = sum(profits_1h) / len(profits_1h) if profits_1h else 0
        avg_4h = sum(profits_4h) / len(profits_4h) if profits_4h else 0
        avg_24h = sum(profits_24h) / len(profits_24h) if profits_24h else 0
        
        win_rate_1h = len([p for p in profits_1h if p > 0]) / len(profits_1h) * 100 if profits_1h else 0
        win_rate_4h = len([p for p in profits_4h if p > 0]) / len(profits_4h) * 100 if profits_4h else 0
        win_rate_24h = len([p for p in profits_24h if p > 0]) / len(profits_24h) * 100 if profits_24h else 0
        
        return {
            "count": len(outcomes_list),
            "avg_1h": avg_1h,
            "avg_4h": avg_4h,
            "avg_24h": avg_24h,
            "win_rate_1h": win_rate_1h,
            "win_rate_4h": win_rate_4h,
            "win_rate_24h": win_rate_24h
        }
    
    all_stats = calc_stats(outcomes)
    volume_stats = calc_stats(volume_spike_outcomes)
    regular_stats = calc_stats(regular_outcomes)
    
    best = sorted([o for o in outcomes if o.profit_24h], key=lambda x: x.profit_24h, reverse=True)[:5]
    worst = sorted([o for o in outcomes if o.profit_24h], key=lambda x: x.profit_24h)[:5]
    
    report = f"""
📊 <b>ALERT PERFORMANCE REPORT</b>
📅 Last {days} days

<b>OVERALL STATS</b>
Total alerts: {total_alerts}
━━━━━━━━━━━━━━━━━━━━━
Timeframe | Avg Profit | Win Rate
1 hour    | {all_stats['avg_1h']:+.2f}%    | {all_stats['win_rate_1h']:.0f}%
4 hours   | {all_stats['avg_4h']:+.2f}%    | {all_stats['win_rate_4h']:.0f}%
24 hours  | {all_stats['avg_24h']:+.2f}%   | {all_stats['win_rate_24h']:.0f}%

🔥 <b>VOLUME SPIKE SIGNALS</b> ({volume_stats['count']} alerts)
1h: {volume_stats['avg_1h']:+.2f}% ({volume_stats['win_rate_1h']:.0f}% win)
4h: {volume_stats['avg_4h']:+.2f}% ({volume_stats['win_rate_4h']:.0f}% win)
24h: {volume_stats['avg_24h']:+.2f}% ({volume_stats['win_rate_24h']:.0f}% win)

📈 <b>REGULAR SIGNALS</b> ({regular_stats['count']} alerts)
1h: {regular_stats['avg_1h']:+.2f}% ({regular_stats['win_rate_1h']:.0f}% win)
4h: {regular_stats['avg_4h']:+.2f}% ({regular_stats['win_rate_4h']:.0f}% win)
24h: {regular_stats['avg_24h']:+.2f}% ({regular_stats['win_rate_24h']:.0f}% win)

🏆 <b>TOP 5 PERFORMERS</b>
"""
    
    for o in best:
        report += f"{o.symbol}: {o.profit_24h:+.2f}% (24h)\n"
    
    report += f"\n💀 <b>WORST 5</b>\n"
    for o in worst:
        report += f"{o.symbol}: {o.profit_24h:+.2f}% (24h)\n"
    
    verdict = "✅ Strategy looks profitable!" if all_stats['avg_24h'] > 0 and all_stats['win_rate_24h'] > 50 else "⚠️ Needs improvement"
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
