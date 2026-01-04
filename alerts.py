"""
Stock Price Alert System with Telegram Integration
"""
import os
import requests
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class PriceAlert(Base):
    """Stores alert configurations for specific stocks"""
    __tablename__ = 'price_alerts'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False, unique=True)
    threshold_percent = Column(Float, default=5.0)  # Alert when price changes by this %
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class AlertHistory(Base):
    """Stores sent alerts to avoid duplicates"""
    __tablename__ = 'alert_history'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False)
    alert_type = Column(String, nullable=False)  # 'spike_up', 'spike_down'
    price_before = Column(Float, nullable=False)
    price_after = Column(Float, nullable=False)
    percent_change = Column(Float, nullable=False)
    sent_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# Telegram configuration
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

# Alert configuration
DEFAULT_THRESHOLD_PERCENT = float(os.environ.get('ALERT_THRESHOLD_PERCENT', '5.0'))
LOOKBACK_HOURS = float(os.environ.get('ALERT_LOOKBACK_HOURS', '1.0'))
ALERT_COOLDOWN_HOURS = float(os.environ.get('ALERT_COOLDOWN_HOURS', '4.0'))


def get_db_session():
    """Create and return a database session"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError("No DATABASE_URL environment variable set")
    
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+psycopg2://")
    
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def send_telegram_message(message: str) -> bool:
    """Send a message via Telegram bot"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    try:
        response = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        
        if response.status_code == 200:
            print(f"Telegram message sent successfully")
            return True
        else:
            print(f"Telegram error: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Failed to send Telegram message: {e}")
        return False


def format_alert_message(symbol: str, name: str, price_before: float, price_after: float, 
                         percent_change: float, alert_type: str) -> str:
    """Format a beautiful alert message for Telegram"""
    
    if alert_type == 'spike_up':
        emoji = "🚀📈"
        color_word = "Increased"
    else:
        emoji = "🔻📉"
        color_word = "Decreased"
    
    message = f"""
{emoji} <b>ALERT: {symbol}</b> {emoji}

📊 <b>{name or symbol}</b>

Price {color_word} o <b>{abs(percent_change):.2f}%</b>!

💰 Last price: <code>${price_before:.2f}</code>
💵 Current price: <code>${price_after:.2f}</code>
📈 Change: <b>{'+' if percent_change > 0 else ''}{percent_change:.2f}%</b>

🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

<a href="https://finance.yahoo.com/quote/{symbol}">View on Yahoo Finance →</a>
"""
    return message.strip()


def check_price_alerts(session, stock_prices_model):
    """
    Check for significant price changes and send alerts
    
    Args:
        session: SQLAlchemy session
        stock_prices_model: The StockPrice model class from scrape_prices.py
    """
    print(f"\nChecking for price alerts...")
    
    # Get alert configurations
    alerts_config = {}
    try:
        for alert in session.query(PriceAlert).filter(PriceAlert.enabled == True).all():
            alerts_config[alert.symbol] = alert.threshold_percent
    except Exception as e:
        print(f"No custom alerts configured, using defaults: {e}")
    
    # Calculate lookback time
    lookback_time = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    cooldown_time = datetime.now(timezone.utc) - timedelta(hours=ALERT_COOLDOWN_HOURS)
    
    # Get unique symbols with recent data
    try:
        from sqlalchemy import func, distinct
        
        # Get the latest price for each symbol
        latest_subq = session.query(
            stock_prices_model.symbol,
            func.max(stock_prices_model.timestamp).label('max_ts')
        ).group_by(stock_prices_model.symbol).subquery()
        
        latest_prices = session.query(stock_prices_model).join(
            latest_subq,
            (stock_prices_model.symbol == latest_subq.c.symbol) & 
            (stock_prices_model.timestamp == latest_subq.c.max_ts)
        ).all()
        
        alerts_sent = 0
        
        for current in latest_prices:
            symbol = current.symbol
            current_price = current.price
            
            # Get historical price (from lookback period)
            historical = session.query(stock_prices_model).filter(
                stock_prices_model.symbol == symbol,
                stock_prices_model.timestamp <= lookback_time
            ).order_by(stock_prices_model.timestamp.desc()).first()
            
            if not historical:
                # No historical data yet, skip
                continue
            
            historical_price = historical.price
            
            # Calculate percent change
            if historical_price == 0:
                continue
                
            percent_change = ((current_price - historical_price) / historical_price) * 100
            
            # Get threshold for this symbol
            threshold = alerts_config.get(symbol, DEFAULT_THRESHOLD_PERCENT)
            
            # Check if change exceeds threshold
            if abs(percent_change) >= threshold:
                alert_type = 'spike_up' if percent_change > 0 else 'spike_down'
                
                # Check cooldown - don't spam alerts
                recent_alert = session.query(AlertHistory).filter(
                    AlertHistory.symbol == symbol,
                    AlertHistory.sent_at > cooldown_time
                ).first()
                
                if recent_alert:
                    print(f"Skipping {symbol} - alert sent recently (cooldown)")
                    continue
                
                # Send alert
                message = format_alert_message(
                    symbol=symbol,
                    name=current.name,
                    price_before=historical_price,
                    price_after=current_price,
                    percent_change=percent_change,
                    alert_type=alert_type
                )
                
                print(f"\nALERT: {symbol} changed by {percent_change:.2f}%!")
                
                if send_telegram_message(message):
                    # Save to history
                    alert_record = AlertHistory(
                        symbol=symbol,
                        alert_type=alert_type,
                        price_before=historical_price,
                        price_after=current_price,
                        percent_change=percent_change
                    )
                    session.add(alert_record)
                    session.commit()
                    alerts_sent += 1
        
        print(f"\nAlert check complete. Sent {alerts_sent} alerts.")
        return alerts_sent
        
    except Exception as e:
        print(f"Error checking alerts: {e}")
        import traceback
        traceback.print_exc()
        return 0


def add_symbol_alert(symbol: str, threshold_percent: float = None):
    """Add or update an alert for a specific symbol"""
    session = get_db_session()
    
    threshold = threshold_percent or DEFAULT_THRESHOLD_PERCENT
    
    existing = session.query(PriceAlert).filter(PriceAlert.symbol == symbol).first()
    
    if existing:
        existing.threshold_percent = threshold
        existing.enabled = True
        print(f"Updated alert for {symbol}: {threshold}% threshold")
    else:
        alert = PriceAlert(symbol=symbol, threshold_percent=threshold, enabled=True)
        session.add(alert)
        print(f"Added alert for {symbol}: {threshold}% threshold")
    
    session.commit()
    session.close()


def remove_symbol_alert(symbol: str):
    """Disable alert for a specific symbol"""
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
    """List all configured alerts"""
    session = get_db_session()
    
    alerts = session.query(PriceAlert).all()
    
    if not alerts:
        print("No custom alerts configured. Using default threshold for all symbols.")
        print(f"Default threshold: {DEFAULT_THRESHOLD_PERCENT}%")
        return
    
    print("\nConfigured Alerts:")
    print("-" * 50)
    for alert in alerts:
        status = "Enabled" if alert.enabled else "Disabled"
        print(f"{alert.symbol}: {alert.threshold_percent}% threshold - {status}")
    
    session.close()


def test_telegram():
    """Test Telegram connection"""
    message = "🧪 Test alert from pricing-fetcher!\n\nIf you see this, Telegram integration works! 🎉"
    
    if send_telegram_message(message):
        print("Test message sent successfully!")
    else:
        print("Failed to send test message. Check your TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")


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
    python alerts.py test                    - Test Telegram connection
    python alerts.py list                    - List all alerts
    python alerts.py add SYMBOL [threshold]  - Add alert for symbol
    python alerts.py remove SYMBOL           - Remove alert for symbol
""")
    else:
        print("Running test...")
        test_telegram()
