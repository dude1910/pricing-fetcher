import os
import requests
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, BigInteger
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class PriceAlert(Base):
    __tablename__ = 'price_alerts'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False, unique=True)
    threshold_percent = Column(Float, default=5.0)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class AlertHistory(Base):
    __tablename__ = 'alert_history'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False)
    alert_type = Column(String, nullable=False)
    price_before = Column(Float, nullable=False)
    price_after = Column(Float, nullable=False)
    percent_change = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=True)
    volume_ratio = Column(Float, nullable=True)
    sent_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

DEFAULT_THRESHOLD_PERCENT = float(os.environ.get('ALERT_THRESHOLD_PERCENT', '3.0'))
VOLUME_MULTIPLIER = float(os.environ.get('VOLUME_MULTIPLIER', '2.0'))
LOOKBACK_HOURS = float(os.environ.get('ALERT_LOOKBACK_HOURS', '1.0'))
ALERT_COOLDOWN_HOURS = float(os.environ.get('ALERT_COOLDOWN_HOURS', '4.0'))


def get_db_session():
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError("No DATABASE_URL environment variable set")
    
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+psycopg2://")
    
    if "sslmode" not in database_url:
        database_url += "?sslmode=require" if "?" not in database_url else "&sslmode=require"
    
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def send_telegram_message(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    try:
        response = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        
        if response.status_code == 200:
            print(f"Telegram message sent")
            return True
        else:
            print(f"Telegram error: {response.status_code}")
            return False
    except Exception as e:
        print(f"Failed to send Telegram: {e}")
        return False


def format_alert_message(symbol: str, name: str, price_before: float, price_after: float, 
                         percent_change: float, alert_type: str, volume: int = None,
                         volume_ratio: float = None) -> str:
    
    if alert_type == 'volume_spike_up':
        emoji = "🚀📈🔥"
        direction = "UP"
        signal_strength = "🔥 VOLUME SPIKE - STRONG SIGNAL"
    elif alert_type == 'volume_spike_down':
        emoji = "🔻📉🔥"
        direction = "DOWN"
        signal_strength = "🔥 VOLUME SPIKE - STRONG SIGNAL"
    elif alert_type == 'extreme_up':
        emoji = "🚨📈💥"
        direction = "UP"
        signal_strength = "⚠️ EXTREME MOVE"
    elif alert_type == 'extreme_down':
        emoji = "🚨📉💥"
        direction = "DOWN"
        signal_strength = "⚠️ EXTREME MOVE"
    else:
        emoji = "📈" if 'up' in alert_type else "📉"
        direction = "UP" if 'up' in alert_type else "DOWN"
        signal_strength = "Signal"
    
    volume_text = ""
    if volume and volume_ratio and volume_ratio > 1.0:
        volume_formatted = f"{volume:,}"
        volume_text = f"\n📊 Volume: <code>{volume_formatted}</code> ({volume_ratio:.1f}x avg)"
    
    message = f"""
{emoji} <b>{symbol}</b> {direction} {abs(percent_change):.1f}% {emoji}

<b>{name or symbol}</b>

💰 ${price_before:.2f} → ${price_after:.2f}
📈 Change: <b>{'+' if percent_change > 0 else ''}{percent_change:.2f}%</b>{volume_text}

{signal_strength}
🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}

<a href="https://finance.yahoo.com/quote/{symbol}">Yahoo Finance</a>
"""
    return message.strip()


def check_price_alerts(session, stock_prices_model):
    import time as time_module
    func_start = time_module.time()
    print(f"\nChecking for alerts...")
    
    # Get engine from session and ensure alert_history table exists
    try:
        engine = session.get_bind()
        Base.metadata.create_all(engine)
        print("[TIMING] Ensured alert_history table exists")
    except Exception as e:
        print(f"Warning: Could not create tables: {e}")
    
    try:
        session.rollback()
    except:
        pass
    
    # Load custom alert thresholds
    alerts_config = {}
    try:
        for alert in session.query(PriceAlert).filter(PriceAlert.enabled == True).all():
            alerts_config[alert.symbol] = alert.threshold_percent
    except Exception as e:
        print(f"Using default alerts")
        session.rollback()
    
    print(f"[TIMING] Load alerts config: {time_module.time() - func_start:.1f}s")
    
    lookback_time = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    cooldown_time = datetime.now(timezone.utc) - timedelta(hours=ALERT_COOLDOWN_HOURS)
    avg_volume_lookback = datetime.now(timezone.utc) - timedelta(hours=8)
    
    try:
        from sqlalchemy import func, and_
        
        # STEP 1: Get latest prices for all symbols (single query)
        step_start = time_module.time()
        latest_subq = session.query(
            stock_prices_model.symbol,
            func.max(stock_prices_model.timestamp).label('max_ts')
        ).group_by(stock_prices_model.symbol).subquery()
        
        latest_prices = session.query(stock_prices_model).join(
            latest_subq,
            and_(
                stock_prices_model.symbol == latest_subq.c.symbol,
                stock_prices_model.timestamp == latest_subq.c.max_ts
            )
        ).all()
        
        # Build lookup dict
        current_data = {p.symbol: {'price': p.price, 'volume': getattr(p, 'volume', None), 'name': p.name} for p in latest_prices}
        symbols = list(current_data.keys())
        
        print(f"[TIMING] Query latest prices ({len(symbols)} symbols): {time_module.time() - step_start:.1f}s")
        
        # STEP 2: Get historical prices for all symbols (single query)
        step_start = time_module.time()
        historical_subq = session.query(
            stock_prices_model.symbol,
            func.max(stock_prices_model.timestamp).label('max_ts')
        ).filter(
            stock_prices_model.timestamp <= lookback_time
        ).group_by(stock_prices_model.symbol).subquery()
        
        historical_prices = session.query(stock_prices_model).join(
            historical_subq,
            and_(
                stock_prices_model.symbol == historical_subq.c.symbol,
                stock_prices_model.timestamp == historical_subq.c.max_ts
            )
        ).all()
        
        historical_data = {p.symbol: p.price for p in historical_prices}
        print(f"[TIMING] Query historical prices ({len(historical_data)} symbols): {time_module.time() - step_start:.1f}s")
        
        # STEP 3: Get average volumes for all symbols (single query)
        step_start = time_module.time()
        avg_volumes_query = session.query(
            stock_prices_model.symbol,
            func.avg(stock_prices_model.volume).label('avg_vol')
        ).filter(
            stock_prices_model.timestamp >= avg_volume_lookback,
            stock_prices_model.volume != None,
            stock_prices_model.volume > 0
        ).group_by(stock_prices_model.symbol).all()
        
        avg_volumes = {row.symbol: float(row.avg_vol) for row in avg_volumes_query if row.avg_vol}
        print(f"[TIMING] Query avg volumes ({len(avg_volumes)} symbols): {time_module.time() - step_start:.1f}s")
        
        # STEP 4: Get recent alerts for cooldown check (single query)
        step_start = time_module.time()
        recent_alerts = session.query(AlertHistory.symbol).filter(
            AlertHistory.sent_at > cooldown_time
        ).distinct().all()
        
        cooldown_symbols = {a.symbol for a in recent_alerts}
        print(f"[TIMING] Query cooldown alerts ({len(cooldown_symbols)} in cooldown): {time_module.time() - step_start:.1f}s")
        
        # STEP 5: Process all symbols in memory (no more DB queries in loop!)
        step_start = time_module.time()
        alerts_sent = 0
        candidates = []
        
        for symbol in symbols:
            current_price = current_data[symbol]['price']
            current_volume = current_data[symbol]['volume']
            
            historical_price = historical_data.get(symbol)
            if not historical_price or historical_price == 0:
                continue
            
            percent_change = ((current_price - historical_price) / historical_price) * 100
            
            # Calculate volume ratio
            volume_ratio = None
            avg_vol = avg_volumes.get(symbol)
            if current_volume and avg_vol and avg_vol > 0:
                volume_ratio = current_volume / avg_vol
            
            threshold = alerts_config.get(symbol, DEFAULT_THRESHOLD_PERCENT)
            
            # NEW SMART ALERT LOGIC:
            # 1. Volume Spike Alert: price change >= threshold AND volume >= 2x (HIGH QUALITY)
            # 2. Extreme Move Alert: price change >= 15% regardless of volume (EMERGENCY)
            # 3. NO MORE noisy "regular spike" alerts without volume confirmation!
            
            is_significant_move = abs(percent_change) >= threshold
            is_volume_spike = volume_ratio and volume_ratio >= VOLUME_MULTIPLIER
            is_extreme_move = abs(percent_change) >= 15.0  # 15% is extreme
            
            should_alert = False
            alert_type = None
            
            # Priority 1: Volume-confirmed signals (best quality)
            if is_significant_move and is_volume_spike:
                should_alert = True
                alert_type = 'volume_spike_up' if percent_change > 0 else 'volume_spike_down'
            
            # Priority 2: Extreme moves (catch black swans even without volume data)
            elif is_extreme_move:
                should_alert = True
                alert_type = 'extreme_up' if percent_change > 0 else 'extreme_down'
            
            if not should_alert:
                continue
            
            # Check cooldown
            if symbol in cooldown_symbols:
                continue
            
            candidates.append({
                'symbol': symbol,
                'name': current_data[symbol]['name'],
                'historical_price': historical_price,
                'current_price': current_price,
                'percent_change': percent_change,
                'alert_type': alert_type,
                'volume': current_volume,
                'volume_ratio': volume_ratio
            })
        
        print(f"[TIMING] Process candidates ({len(candidates)} found): {time_module.time() - step_start:.1f}s")
        
        # STEP 6: Send alerts
        step_start = time_module.time()
        for c in candidates:
            message = format_alert_message(
                symbol=c['symbol'],
                name=c['name'],
                price_before=c['historical_price'],
                price_after=c['current_price'],
                percent_change=c['percent_change'],
                alert_type=c['alert_type'],
                volume=c['volume'],
                volume_ratio=c['volume_ratio']
            )
            
            vol_str = f" (vol: {c['volume_ratio']:.1f}x)" if c['volume_ratio'] else ""
            print(f"ALERT: {c['symbol']} {c['percent_change']:.1f}%{vol_str}")
            
            if send_telegram_message(message):
                alert_record = AlertHistory(
                    symbol=c['symbol'],
                    alert_type=c['alert_type'],
                    price_before=c['historical_price'],
                    price_after=c['current_price'],
                    percent_change=c['percent_change'],
                    volume=c['volume'],
                    volume_ratio=c['volume_ratio']
                )
                session.add(alert_record)
                session.commit()
                alerts_sent += 1
        
        print(f"[TIMING] Send alerts: {time_module.time() - step_start:.1f}s")
        print(f"Sent {alerts_sent} alerts")
        print(f"[TIMING] Total check_price_alerts: {time_module.time() - func_start:.1f}s")
        return alerts_sent
        
    except Exception as e:
        print(f"Error checking alerts: {e}")
        import traceback
        traceback.print_exc()
        return 0


def add_symbol_alert(symbol: str, threshold_percent: float = None):
    session = get_db_session()
    threshold = threshold_percent or DEFAULT_THRESHOLD_PERCENT
    existing = session.query(PriceAlert).filter(PriceAlert.symbol == symbol).first()
    
    if existing:
        existing.threshold_percent = threshold
        existing.enabled = True
        print(f"Updated alert for {symbol}: {threshold}%")
    else:
        alert = PriceAlert(symbol=symbol, threshold_percent=threshold, enabled=True)
        session.add(alert)
        print(f"Added alert for {symbol}: {threshold}%")
    
    session.commit()
    session.close()


def remove_symbol_alert(symbol: str):
    session = get_db_session()
    existing = session.query(PriceAlert).filter(PriceAlert.symbol == symbol).first()
    
    if existing:
        existing.enabled = False
        session.commit()
        print(f"Disabled alert for {symbol}")
    else:
        print(f"No alert found for {symbol}")
    
    session.close()


def list_alerts():
    session = get_db_session()
    alerts = session.query(PriceAlert).all()
    
    if not alerts:
        print(f"Using default: {DEFAULT_THRESHOLD_PERCENT}% + {VOLUME_MULTIPLIER}x volume")
        return
    
    print("\nConfigured Alerts:")
    for alert in alerts:
        status = "ON" if alert.enabled else "OFF"
        print(f"{alert.symbol}: {alert.threshold_percent}% [{status}]")
    
    session.close()


def test_telegram():
    message = "🧪 Test alert!\n\nTelegram integration works! 🎉"
    if send_telegram_message(message):
        print("Test sent!")
    else:
        print("Failed - check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "test":
            test_telegram()
        elif command == "list":
            list_alerts()
        elif command == "add" and len(sys.argv) >= 3:
            symbol = sys.argv[2].upper()
            threshold = float(sys.argv[3]) if len(sys.argv) > 3 else None
            add_symbol_alert(symbol, threshold)
        elif command == "remove" and len(sys.argv) >= 3:
            symbol = sys.argv[2].upper()
            remove_symbol_alert(symbol)
        else:
            print("""
Usage:
    python alerts.py test                    - Test Telegram
    python alerts.py list                    - List alerts
    python alerts.py add SYMBOL [threshold]  - Add alert
    python alerts.py remove SYMBOL           - Remove alert
""")
    else:
        test_telegram()
