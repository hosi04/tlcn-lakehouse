import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
from . import config

PRODUCTS = config.PRODUCTS
CATEGORIES = config.CATEGORIES
SEARCH_KEYWORDS = config.SEARCH_KEYWORDS
SOURCE_CHANNELS = config.SOURCE_CHANNELS
PLATFORMS = config.PLATFORMS
BASE_URL = config.BASE_URL
SITE_NAME = config.SITE_NAME

def _random_user() -> str:
    return str(uuid.uuid4())


def _event_time(base_dt: datetime, offset_seconds: float = 0) -> str:
    dt = base_dt + timedelta(seconds=offset_seconds)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _context(platform: str, channel: str, path: str, title: str) -> dict:
    return {
        "url":            f"{BASE_URL}{path}",
        "path":           path,
        "title":          title,
        "platform":       platform,
        "source_channel": channel,
        "device_type":    "desktop" if platform == "web" else "mobile",
    }


def _event(user_id: str, event_type: str, dt: datetime, properties: dict, context: dict) -> dict:
    return {
        "event_id":   str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": _event_time(dt),
        "user_id":    user_id,
        "properties": properties,
        "context":    context,
    }


def build_search_event(user_id, dt, platform, channel) -> dict:
    keyword = random.choice(SEARCH_KEYWORDS)
    return _event(
        user_id, "search", dt,
        {"keyword": keyword, "result_count": random.randint(5, 120)},
        _context(platform, channel, f"/search?q={keyword.replace(' ', '+')}", f"Search: {keyword} | {SITE_NAME}"),
    )


def build_view_category_event(user_id, dt, platform, channel, category) -> dict:
    return _event(
        user_id, "view_category", dt,
        {"category_id": category["category_id"], "category_name": category["category_name"]},
        _context(platform, channel, f"/category/{category['slug']}", f"{category['category_name']} | {SITE_NAME}"),
    )


def build_view_item_event(user_id, dt, platform, channel, product) -> dict:
    return _event(
        user_id, "view_item", dt,
        {
            "product_id":    product["product_id"],
            "product_name":  product["product_name"],
            "category_id":   product["category_id"],
            "category_name": product["category_name"],
            "price":         product["price"],
            "currency":      "BRL",
        },
        _context(platform, channel, f"/product/{product['slug']}", f"{product['product_name']} | {SITE_NAME}"),
    )


def build_add_to_cart_event(user_id, dt, platform, channel, product, cart_id) -> dict:
    return _event(
        user_id, "add_to_cart", dt,
        {
            "cart_id":       cart_id,
            "product_id":    product["product_id"],
            "product_name":  product["product_name"],
            "category_id":   product["category_id"],
            "category_name": product["category_name"],
            "price":         product["price"],
            "quantity":      random.randint(1, 3),
            "currency":      "BRL",
        },
        _context(platform, channel, f"/product/{product['slug']}", f"{product['product_name']} | {SITE_NAME}"),
    )


def build_remove_from_cart_event(user_id, dt, platform, channel, product, cart_id) -> dict:
    return _event(
        user_id, "remove_from_cart", dt,
        {
            "cart_id":      cart_id,
            "product_id":   product["product_id"],
            "product_name": product["product_name"],
            "price":        product["price"],
            "quantity":     random.randint(1, 2),
            "currency":     "BRL",
        },
        _context(platform, channel, "/cart", f"Cart | {SITE_NAME}"),
    )


def build_begin_checkout_event(user_id, dt, platform, channel, cart_items) -> dict:
    total = sum(p["price"] * p["qty"] for p in cart_items)
    return _event(
        user_id, "begin_checkout", dt,
        {
            "items":        [{"product_id": p["product_id"], "quantity": p["qty"], "price": p["price"]} for p in cart_items],
            "total_amount": total,
            "item_count":   len(cart_items),
            "currency":     "BRL",
        },
        _context(platform, channel, "/checkout", f"Checkout | {SITE_NAME}"),
    )


def build_purchase_event(user_id, dt, platform, channel, cart_items) -> dict:
    order_id = f"ORD{random.randint(100_000, 999_999)}"
    total    = sum(p["price"] * p["qty"] for p in cart_items)
    discount = random.choice([0, 0, 0, 5.0, 10.0, 20.0])
    return _event(
        user_id, "purchase", dt,
        {
            "order_id":       order_id,
            "items":          [{"product_id": p["product_id"], "quantity": p["qty"], "price": p["price"]} for p in cart_items],
            "total_amount":   total,
            "discount":       discount,
            "revenue":        max(total - discount, 0),
            "item_count":     len(cart_items),
            "currency":       "BRL",
            "payment_method": random.choice(["credit_card", "boleto", "voucher", "debit_card"]),
        },
        _context(platform, channel, "/order-success", f"Order Confirmed | {SITE_NAME}"),
    )


def simulate_session(base_dt: datetime) -> list[dict]:
    """
    Simulate one user session with a realistic purchase funnel.
    Returns events ordered by time.
    """
    events      = []
    user_id     = _random_user()
    platform    = random.choices(PLATFORMS, weights=[55, 30, 15])[0]
    channel     = random.choices(SOURCE_CHANNELS, weights=[25, 30, 20, 15, 5, 5])[0]
    cursor_dt   = base_dt
    cart_id     = str(uuid.uuid4())
    cart_items  = []

    # 1. Search or view category (50/50)
    if random.random() < 0.5:
        events.append(build_search_event(user_id, cursor_dt, platform, channel))
        cursor_dt += timedelta(seconds=random.uniform(5, 15))
    else:
        cat = random.choice(CATEGORIES)
        events.append(build_view_category_event(user_id, cursor_dt, platform, channel, cat))
        cursor_dt += timedelta(seconds=random.uniform(5, 20))

    # 2. View 1-4 products
    viewed_products = random.sample(PRODUCTS, k=random.randint(1, 4))
    for product in viewed_products:
        events.append(build_view_item_event(user_id, cursor_dt, platform, channel, product))
        cursor_dt += timedelta(seconds=random.uniform(10, 60))

        # 3. Add to cart (40% chance per product)
        if random.random() < 0.40:
            qty = random.randint(1, 3)
            events.append(build_add_to_cart_event(user_id, cursor_dt, platform, channel, product, cart_id))
            cart_items.append({"product_id": product["product_id"], "price": product["price"], "qty": qty})
            cursor_dt += timedelta(seconds=random.uniform(2, 10))

            # 4. Remove from cart (15% chance after add)
            if random.random() < 0.15 and cart_items:
                events.append(build_remove_from_cart_event(user_id, cursor_dt, platform, channel, product, cart_id))
                cart_items = [c for c in cart_items if c["product_id"] != product["product_id"]]
                cursor_dt += timedelta(seconds=random.uniform(2, 8))

    # 5. Begin checkout when cart is non-empty (70% proceed)
    if cart_items and random.random() < 0.70:
        events.append(build_begin_checkout_event(user_id, cursor_dt, platform, channel, cart_items))
        cursor_dt += timedelta(seconds=random.uniform(30, 120))

        # 6. Purchase (80% complete after begin_checkout)
        if random.random() < 0.80:
            events.append(build_purchase_event(user_id, cursor_dt, platform, channel, cart_items))

    return events


def preview_events(count: int = 2) -> None:
    """Print sample events to stdout (no Kafka)."""
    now = datetime.now(timezone.utc)
    session_events = simulate_session(now)
    samples = session_events[:count]
    if len(samples) < count:
        extra = simulate_session(now - timedelta(hours=1))
        samples.extend(extra[: count - len(samples)])

    print(f"Preview: {len(samples)} event(s)\n")
    for i, event in enumerate(samples, start=1):
        print(f"--- Event {i} ({event['event_type']}) ---")
        print(json.dumps(event, indent=2, ensure_ascii=False))
        print()


def _throttle_delay_seconds() -> float:
    """Pause between sends so we do not flood Kafka (slight jitter)."""
    if config.PRODUCE_DELAY_MS > 0:
        base = config.PRODUCE_DELAY_MS / 1000.0
    else:
        base = 1.0 / max(config.EVENTS_PER_SECOND, 0.1)
    return base * random.uniform(0.85, 1.15)


def produce_events() -> None:
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
        batch_size=16_384,
        compression_type="gzip",
    )

    total_sent = 0
    target = config.TOTAL_EVENTS
    throttle = _throttle_delay_seconds

    if config.PRODUCE_DELAY_MS > 0:
        rate_label = f"{config.PRODUCE_DELAY_MS} ms/event"
    else:
        rate_label = f"~{config.EVENTS_PER_SECOND} events/s"
    eta_sec = target * (config.PRODUCE_DELAY_MS / 1000.0 if config.PRODUCE_DELAY_MS > 0 else 1.0 / max(config.EVENTS_PER_SECOND, 0.1))

    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=30)
    started_at = time.monotonic()

    print(f"Starting produce of {target:,} events -> topic: {config.KAFKA_TOPIC}")
    print(f"   Bootstrap: {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Rate: {rate_label} (est. {eta_sec / 60:.0f} min for {target:,} events)")
    print(f"   Simulated event_time range: {start_dt.date()} -> {now.date()}")

    try:
        while total_sent < target:
            offset_seconds = random.uniform(0, 30 * 24 * 3600)
            base_dt = start_dt + timedelta(seconds=offset_seconds)

            for event in simulate_session(base_dt):
                if total_sent >= target:
                    break

                producer.send(
                    topic=config.KAFKA_TOPIC,
                    value=event,
                    key=event["user_id"].encode("utf-8"),
                )
                total_sent += 1
                time.sleep(throttle())

            if total_sent > 0 and total_sent % 5_000 == 0:
                elapsed = time.monotonic() - started_at
                eps = total_sent / elapsed if elapsed > 0 else 0
                pct = total_sent / target * 100
                print(f"   [{pct:5.1f}%] Sent {total_sent:,} / {target:,} ({eps:.1f} events/s)")

        producer.flush()
        elapsed = time.monotonic() - started_at
        print(f"\nDone. Produced {total_sent:,} events in {elapsed / 60:.1f} min ({total_sent / elapsed:.1f} events/s).")

    except KafkaError as e:
        print(f"Kafka error: {e}")
        raise
    finally:
        producer.close()


def main():
    if config.PREVIEW_EVENTS > 0:
        preview_events(config.PREVIEW_EVENTS)
        return
    produce_events()


if __name__ == "__main__":
    main()