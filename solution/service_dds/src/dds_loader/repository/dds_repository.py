from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional
from uuid import UUID

from lib.pg import PgConnect


def _first(d: Dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _as_uuid(val: Any) -> Optional[UUID]:
    if val is None:
        return None
    if isinstance(val, UUID):
        return val
    try:
        return UUID(str(val))
    except Exception:
        return None


def _as_dt(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val)
    # Most common ISO formats. datetime.fromisoformat can parse "YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM]"
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _as_int(val: Any, default: int = 1) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except Exception:
        return default


def _as_decimal(val: Any) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


@dataclass(frozen=True)
class NormalizedItem:
    product_id: UUID
    product_name: Optional[str]
    category_id: Optional[UUID]
    category_name: Optional[str]
    quantity: int
    price: Optional[Decimal]


@dataclass(frozen=True)
class NormalizedOrder:
    order_id: UUID
    order_ts: datetime
    user_id: UUID
    restaurant_id: Optional[UUID]
    restaurant_name: Optional[str]
    user_name: Optional[str]
    status: Optional[str]
    total_sum: Optional[Decimal]
    payment_sum: Optional[Decimal]
    bonus_payment: Optional[Decimal]
    bonus_grant: Optional[Decimal]
    items: List[NormalizedItem]


class DdsRepository:
    """
    Loads detailed (DDS) layer in Postgres.

    Input: enriched order JSON from STG service.
    Output: normalized message for downstream CDM service.
    """

    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self._ddl_applied = False

    def ensure_ddl(self) -> None:
        self._apply_ddl()

    def _apply_ddl(self) -> None:
        if self._ddl_applied:
            return
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS dds;")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dds.dm_users (
                        user_id uuid PRIMARY KEY,
                        user_name text
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
                        restaurant_id uuid PRIMARY KEY,
                        restaurant_name text
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dds.dm_categories (
                        category_id uuid PRIMARY KEY,
                        category_name text
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dds.dm_products (
                        product_id uuid PRIMARY KEY,
                        product_name text,
                        category_id uuid REFERENCES dds.dm_categories(category_id)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dds.fct_orders (
                        order_id uuid PRIMARY KEY,
                        order_ts timestamptz,
                        user_id uuid REFERENCES dds.dm_users(user_id),
                        restaurant_id uuid REFERENCES dds.dm_restaurants(restaurant_id),
                        status text,
                        total_sum numeric(14,2),
                        payment_sum numeric(14,2),
                        bonus_payment numeric(14,2),
                        bonus_grant numeric(14,2),
                        updated_ts timestamptz NOT NULL DEFAULT now()
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dds.fct_order_items (
                        order_id uuid REFERENCES dds.fct_orders(order_id),
                        product_id uuid REFERENCES dds.dm_products(product_id),
                        quantity integer NOT NULL,
                        price numeric(14,2),
                        PRIMARY KEY (order_id, product_id)
                    );
                    """
                )
        self._ddl_applied = True

    def _normalize(self, payload: Dict[str, Any]) -> NormalizedOrder:
        # Many pipelines wrap the actual order under "payload" key.
        root = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload

        order_id = _as_uuid(_first(root, ["order_id", "orderId", "id", "object_id", "order_uuid"]))
        user_id = _as_uuid(_first(root, ["user_id", "userId", "customer_id", "client_id"]))
        order_ts = _as_dt(_first(root, ["order_ts", "orderTs", "created_at", "createdAt", "date", "event_ts", "timestamp"]))

        if order_id is None or user_id is None or order_ts is None:
            raise ValueError(f"Cannot normalize order: missing order_id/user_id/order_ts. keys={list(root.keys())}")

        restaurant_id = _as_uuid(_first(root, ["restaurant_id", "restaurantId"]))
        restaurant_name = _first(root, ["restaurant_name", "restaurantName", "restaurant"])
        if isinstance(restaurant_name, dict):
            restaurant_name = _first(restaurant_name, ["name", "title"])

        user_name = _first(root, ["user_name", "userName", "customer_name", "client_name"])

        status = _first(root, ["status", "order_status", "state"])

        total_sum = _as_decimal(_first(root, ["total_sum", "totalSum", "total", "sum"]))
        payment_sum = _as_decimal(_first(root, ["payment_sum", "paymentSum", "payment"]))
        bonus_payment = _as_decimal(_first(root, ["bonus_payment", "bonusPayment", "bonus_used"]))
        bonus_grant = _as_decimal(_first(root, ["bonus_grant", "bonusGrant", "bonus_earned"]))

        items_raw = _first(root, ["items", "order_items", "orderItems", "products", "positions"]) or []
        if not isinstance(items_raw, list):
            items_raw = []

        items: List[NormalizedItem] = []
        for it in items_raw:
            if not isinstance(it, dict):
                continue

            product = it.get("product") if isinstance(it.get("product"), dict) else it

            product_id = _as_uuid(_first(product, ["product_id", "productId", "id", "product_uuid", "_id"]))
            if product_id is None:
                continue

            product_name = _first(product, ["product_name", "productName", "name", "title"])

            category_id = _as_uuid(_first(product, ["category_id", "categoryId"]))
            category_name = _first(product, ["category_name", "categoryName"])
            cat_obj = product.get("category")
            if isinstance(cat_obj, dict):
                category_id = category_id or _as_uuid(_first(cat_obj, ["id", "category_id", "categoryId"]))
                category_name = category_name or _first(cat_obj, ["name", "title", "category_name"])

            quantity = _as_int(_first(it, ["quantity", "qty", "count"]), default=1)
            price = _as_decimal(_first(it, ["price", "item_price", "cost"]))

            items.append(
                NormalizedItem(
                    product_id=product_id,
                    product_name=str(product_name) if product_name is not None else None,
                    category_id=category_id,
                    category_name=str(category_name) if category_name is not None else None,
                    quantity=quantity,
                    price=price,
                )
            )

        return NormalizedOrder(
            order_id=order_id,
            order_ts=order_ts,
            user_id=user_id,
            restaurant_id=restaurant_id,
            restaurant_name=str(restaurant_name) if restaurant_name is not None else None,
            user_name=str(user_name) if user_name is not None else None,
            status=str(status) if status is not None else None,
            total_sum=total_sum,
            payment_sum=payment_sum,
            bonus_payment=bonus_payment,
            bonus_grant=bonus_grant,
            items=items,
        )

    def process_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Persist order into DDS (idempotent) and return downstream message for CDM.
        """
        self._apply_ddl()
        order = self._normalize(payload)

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                # Dimensions (idempotent upserts).
                cur.execute(
                    """
                    INSERT INTO dds.dm_users(user_id, user_name)
                    VALUES (%(user_id)s, %(user_name)s)
                    ON CONFLICT (user_id) DO UPDATE SET user_name = EXCLUDED.user_name;
                    """,
                    {"user_id": order.user_id, "user_name": order.user_name},
                )

                if order.restaurant_id is not None:
                    cur.execute(
                        """
                        INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name)
                        VALUES (%(restaurant_id)s, %(restaurant_name)s)
                        ON CONFLICT (restaurant_id) DO UPDATE SET restaurant_name = EXCLUDED.restaurant_name;
                        """,
                        {"restaurant_id": order.restaurant_id, "restaurant_name": order.restaurant_name},
                    )

                for it in order.items:
                    if it.category_id is not None:
                        cur.execute(
                            """
                            INSERT INTO dds.dm_categories(category_id, category_name)
                            VALUES (%(category_id)s, %(category_name)s)
                            ON CONFLICT (category_id) DO UPDATE SET category_name = EXCLUDED.category_name;
                            """,
                            {"category_id": it.category_id, "category_name": it.category_name},
                        )
                    cur.execute(
                        """
                        INSERT INTO dds.dm_products(product_id, product_name, category_id)
                        VALUES (%(product_id)s, %(product_name)s, %(category_id)s)
                        ON CONFLICT (product_id) DO UPDATE
                        SET product_name = EXCLUDED.product_name,
                            category_id = EXCLUDED.category_id;
                        """,
                        {
                            "product_id": it.product_id,
                            "product_name": it.product_name,
                            "category_id": it.category_id,
                        },
                    )

                cur.execute(
                    """
                    INSERT INTO dds.fct_orders(
                        order_id, order_ts, user_id, restaurant_id, status,
                        total_sum, payment_sum, bonus_payment, bonus_grant, updated_ts
                    )
                    VALUES (
                        %(order_id)s, %(order_ts)s, %(user_id)s, %(restaurant_id)s, %(status)s,
                        %(total_sum)s, %(payment_sum)s, %(bonus_payment)s, %(bonus_grant)s, now()
                    )
                    ON CONFLICT (order_id) DO UPDATE SET
                        order_ts = EXCLUDED.order_ts,
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        status = EXCLUDED.status,
                        total_sum = EXCLUDED.total_sum,
                        payment_sum = EXCLUDED.payment_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant,
                        updated_ts = now();
                    """,
                    {
                        "order_id": order.order_id,
                        "order_ts": order.order_ts,
                        "user_id": order.user_id,
                        "restaurant_id": order.restaurant_id,
                        "status": order.status,
                        "total_sum": order.total_sum,
                        "payment_sum": order.payment_sum,
                        "bonus_payment": order.bonus_payment,
                        "bonus_grant": order.bonus_grant,
                    },
                )

                for it in order.items:
                    cur.execute(
                        """
                        INSERT INTO dds.fct_order_items(order_id, product_id, quantity, price)
                        VALUES (%(order_id)s, %(product_id)s, %(quantity)s, %(price)s)
                        ON CONFLICT (order_id, product_id) DO UPDATE SET
                            quantity = EXCLUDED.quantity,
                            price = EXCLUDED.price;
                        """,
                        {
                            "order_id": order.order_id,
                            "product_id": it.product_id,
                            "quantity": it.quantity,
                            "price": it.price,
                        },
                    )

        # Downstream message. Keep it compact but sufficient for CDM marts.
        return {
            "order_id": str(order.order_id),
            "order_ts": order.order_ts.isoformat(),
            "user_id": str(order.user_id),
            "items": [
                {
                    "product_id": str(it.product_id),
                    "product_name": it.product_name,
                    "category_id": str(it.category_id) if it.category_id is not None else None,
                    "category_name": it.category_name,
                    "quantity": it.quantity,
                }
                for it in order.items
            ],
        }
