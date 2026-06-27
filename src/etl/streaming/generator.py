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
SOURCE_CHANNELS = config.SOURCE_CHANNELS
PLATFORMS = config.PLATFORMS
BASE_URL = config.BASE_URL
SITE_NAME = config.SITE_NAME

# ── Day classification ────────────────────────────────────────────────────────

# Double days: 1/1, 2/2, ..., 12/12
DOUBLE_DAYS = frozenset({(m, m) for m in range(1, 13)})

def _day_context(dt: datetime) -> dict:
    """Return funnel multipliers and discount probability based on day type."""
    weekday = dt.weekday()  # 0=Mon, 6=Sun
    md = (dt.month, dt.day)

    if md in DOUBLE_DAYS:
        return {
            "type": "double_day",
            "traffic_mult": 4.0,
            "atc_mult": 1.6,
            "checkout_mult": 1.4,
            "purchase_mult": 1.5,
            "discount_prob": 0.75,
            "qty_max": 5,
        }

    # Flash sale: every 4th weekend (ISO week number divisible by 4)
    if weekday >= 5 and dt.isocalendar()[1] % 4 == 0:
        return {
            "type": "flash_sale",
            "traffic_mult": 2.5,
            "atc_mult": 1.3,
            "checkout_mult": 1.2,
            "purchase_mult": 1.3,
            "discount_prob": 0.55,
            "qty_max": 4,
        }

    if weekday >= 5:
        return {
            "type": "weekend",
            "traffic_mult": 1.3,
            "atc_mult": 0.9,
            "checkout_mult": 1.0,
            "purchase_mult": 0.95,
            "discount_prob": 0.25,
            "qty_max": 3,
        }

    return {
        "type": "normal",
        "traffic_mult": 1.0,
        "atc_mult": 1.0,
        "checkout_mult": 1.0,
        "purchase_mult": 1.0,
        "discount_prob": 0.20,
        "qty_max": 3,
    }


# ── Time-of-day traffic weights (index = hour 0–23) ──────────────────────────

_HOUR_WEIGHTS = [
    0.05, 0.03, 0.02, 0.02, 0.02, 0.04,  # 0–5   dead zone
    0.12, 0.30, 0.50, 0.65, 0.80, 0.90,  # 6–11  morning ramp
    1.00, 0.95, 0.82, 0.78, 0.72, 0.82,  # 12–17 lunch peak + afternoon
    0.92, 1.00, 0.96, 0.80, 0.55, 0.25,  # 18–23 evening peak, wind-down
]

def _time_weight(dt: datetime) -> float:
    return _HOUR_WEIGHTS[dt.hour]


# ── User type ─────────────────────────────────────────────────────────────────

_RETURNING_USER_POOL = [f"user-{i:05d}" for i in range(1, 5001)]

def _pick_user() -> tuple[str, str]:
    """Return (user_id, user_type). 30% returning, 70% new."""
    if random.random() < 0.30:
        return random.choice(_RETURNING_USER_POOL), "returning"
    return str(uuid.uuid4()), "new"


# ── Platform conversion multipliers ──────────────────────────────────────────

_PLATFORM_CONV = {"web": 1.00, "ios": 0.85, "android": 0.75}


# ── Session depth (number of products viewed) ─────────────────────────────────

def _session_depth(user_type: str) -> int:
    if user_type == "returning":
        # returning users browse more intentionally
        return random.choices([1, 2, 3, 4, 5], weights=[15, 25, 30, 20, 10])[0]
    # new users bounce more (~35% view only 1 item)
    return random.choices([1, 2, 3, 4, 5], weights=[35, 30, 20, 10, 5])[0]


# ── Event builders ────────────────────────────────────────────────────────────

def _event_time(dt: datetime) -> str:
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

def build_add_to_cart_event(user_id, dt, platform, channel, product, cart_id, qty) -> dict:
    return _event(
        user_id, "add_to_cart", dt,
        {
            "cart_id":       cart_id,
            "product_id":    product["product_id"],
            "product_name":  product["product_name"],
            "category_id":   product["category_id"],
            "category_name": product["category_name"],
            "price":         product["price"],
            "quantity":      qty,
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
            "total_amount": round(total, 2),
            "item_count":   len(cart_items),
            "currency":     "BRL",
        },
        _context(platform, channel, "/checkout", f"Checkout | {SITE_NAME}"),
    )

def build_purchase_event(user_id, dt, platform, channel, cart_items, discount_prob) -> dict:
    order_id = f"ORD{random.randint(100_000, 999_999)}"
    total    = sum(p["price"] * p["qty"] for p in cart_items)

    discount = 0.0
    if random.random() < discount_prob:
        discount = random.choice([5.0, 10.0, 15.0, 20.0, 30.0, 50.0])

    # On sale days, credit card and voucher dominate; on normal days boleto is more common
    if discount_prob > 0.50:
        payment = random.choices(
            ["credit_card", "boleto", "voucher", "debit_card"],
            weights=[55, 10, 25, 10],
        )[0]
    else:
        payment = random.choices(
            ["credit_card", "boleto", "voucher", "debit_card"],
            weights=[40, 30, 15, 15],
        )[0]

    return _event(
        user_id, "purchase", dt,
        {
            "order_id":       order_id,
            "items":          [{"product_id": p["product_id"], "quantity": p["qty"], "price": p["price"]} for p in cart_items],
            "total_amount":   round(total, 2),
            "discount":       discount,
            "revenue":        round(max(total - discount, 0), 2),
            "item_count":     len(cart_items),
            "currency":       "BRL",
            "payment_method": payment,
        },
        _context(platform, channel, "/order-success", f"Order Confirmed | {SITE_NAME}"),
    )


# ── Session simulation ────────────────────────────────────────────────────────

# Baseline funnel rates (normal day, new user, web platform)
_BASE_ATC_RATE      = 0.12   # view_item  → add_to_cart
_BASE_REMOVE_RATE   = 0.18   # add_to_cart → remove_from_cart
_BASE_CHECKOUT_RATE = 0.72   # has cart   → begin_checkout
_BASE_PURCHASE_RATE = 0.68   # checkout   → purchase

def simulate_session(base_dt: datetime) -> list[dict]:
    day_ctx   = _day_context(base_dt)
    user_id, user_type = _pick_user()
    platform  = random.choices(PLATFORMS, weights=[55, 25, 20])[0]
    channel   = random.choices(SOURCE_CHANNELS, weights=[25, 30, 20, 15, 5, 5])[0]
    cursor_dt = base_dt
    cart_id   = str(uuid.uuid4())
    cart_items: list[dict] = []
    events:     list[dict] = []

    user_mult = 1.4 if user_type == "returning" else 1.0
    plat_mult = _PLATFORM_CONV.get(platform, 1.0)

    atc_rate      = min(_BASE_ATC_RATE * day_ctx["atc_mult"] * user_mult * plat_mult, 0.65)
    checkout_rate = min(_BASE_CHECKOUT_RATE * day_ctx["checkout_mult"] * user_mult, 0.95)
    purchase_rate = min(_BASE_PURCHASE_RATE * day_ctx["purchase_mult"] * user_mult, 0.95)

    # Entry: always view_category (no search events)
    cat = random.choice(CATEGORIES)
    events.append(build_view_category_event(user_id, cursor_dt, platform, channel, cat))
    cursor_dt += timedelta(seconds=random.uniform(5, 20))

    # Browse products
    depth = _session_depth(user_type)
    for product in random.sample(PRODUCTS, k=min(depth, len(PRODUCTS))):
        events.append(build_view_item_event(user_id, cursor_dt, platform, channel, product))
        cursor_dt += timedelta(seconds=random.uniform(15, 90))

        if random.random() < atc_rate:
            qty = random.randint(1, day_ctx["qty_max"])
            events.append(build_add_to_cart_event(user_id, cursor_dt, platform, channel, product, cart_id, qty))
            cart_items.append({"product_id": product["product_id"], "price": product["price"], "qty": qty})
            cursor_dt += timedelta(seconds=random.uniform(2, 10))

            # Cart item removal (abandonment of specific item)
            if random.random() < _BASE_REMOVE_RATE and cart_items:
                events.append(build_remove_from_cart_event(user_id, cursor_dt, platform, channel, product, cart_id))
                cart_items = [c for c in cart_items if c["product_id"] != product["product_id"]]
                cursor_dt += timedelta(seconds=random.uniform(2, 8))

    # Checkout funnel
    if cart_items and random.random() < checkout_rate:
        events.append(build_begin_checkout_event(user_id, cursor_dt, platform, channel, cart_items))
        cursor_dt += timedelta(seconds=random.uniform(60, 180))

        if random.random() < purchase_rate:
            events.append(build_purchase_event(
                user_id, cursor_dt, platform, channel, cart_items, day_ctx["discount_prob"]
            ))

    return events


# ── Preview ───────────────────────────────────────────────────────────────────

def preview_events(count: int = 2) -> None:
    now = datetime.now(timezone.utc)
    samples: list[dict] = []
    while len(samples) < count:
        samples.extend(simulate_session(now))
    samples = samples[:count]

    print(f"Preview: {len(samples)} event(s)\n")
    for i, event in enumerate(samples, start=1):
        print(f"--- Event {i} ({event['event_type']}) ---")
        print(json.dumps(event, indent=2, ensure_ascii=False))
        print()


# ── Producer ──────────────────────────────────────────────────────────────────

def _throttle_delay_seconds() -> float:
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
    target     = config.TOTAL_EVENTS
    throttle   = _throttle_delay_seconds

    if config.PRODUCE_DELAY_MS > 0:
        rate_label = f"{config.PRODUCE_DELAY_MS} ms/event"
    else:
        rate_label = f"~{config.EVENTS_PER_SECOND} events/s"
    eta_sec = target * (
        config.PRODUCE_DELAY_MS / 1000.0
        if config.PRODUCE_DELAY_MS > 0
        else 1.0 / max(config.EVENTS_PER_SECOND, 0.1)
    )

    now      = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=30)
    started_at = time.monotonic()

    print(f"Starting produce of {target:,} events -> topic: {config.KAFKA_TOPIC}")
    print(f"   Bootstrap:  {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Rate:       {rate_label} (est. {eta_sec / 60:.0f} min)")
    print(f"   Time range: {start_dt.date()} -> {now.date()}")

    # MAX_TRAFFIC_MULT is the ceiling of traffic_mult across all day types (double_day = 4.0)
    _MAX_TRAFFIC_MULT = 4.0

    try:
        while total_sent < target:
            # Pick a random moment within the 30-day window
            candidate_dt = start_dt + timedelta(seconds=random.uniform(0, 30 * 24 * 3600))

            # Rejection-sample: probability proportional to time-of-day × day-type traffic
            day_ctx = _day_context(candidate_dt)
            p_accept = _time_weight(candidate_dt) * day_ctx["traffic_mult"] / _MAX_TRAFFIC_MULT
            if random.random() > p_accept:
                continue  # low-traffic slot — skip this candidate

            for event in simulate_session(candidate_dt):
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
                print(f"   [{pct:5.1f}%] {total_sent:,} / {target:,}  ({eps:.1f} ev/s)")

        producer.flush()
        elapsed = time.monotonic() - started_at
        print(f"\nDone. {total_sent:,} events in {elapsed / 60:.1f} min ({total_sent / elapsed:.1f} ev/s).")

    except KafkaError as e:
        print(f"Kafka error: {e}")
        raise
    finally:
        producer.close()


def main() -> None:
    if config.PREVIEW_EVENTS > 0:
        preview_events(config.PREVIEW_EVENTS)
        return
    produce_events()


if __name__ == "__main__":
    main()
