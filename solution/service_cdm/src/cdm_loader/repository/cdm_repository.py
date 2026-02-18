from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
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
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


@dataclass(frozen=True)
class NormalizedItem:
    product_id: UUID
    product_name: Optional[str]
    category_id: Optional[UUID]
    category_name: Optional[str]


@dataclass(frozen=True)
class NormalizedOrder:
    order_id: UUID
    user_id: UUID
    order_ts: Optional[datetime]
    items: List[NormalizedItem]


class CdmRepository:
    """
    Builds CDM marts for popularity dashboards:
    - category popularity by distinct users and by distinct orders
    - product popularity by distinct users and by distinct orders
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
                cur.execute("CREATE SCHEMA IF NOT EXISTS cdm;")

                # Link tables (idempotency for distinct user/order metrics).
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cdm.category_user (
                        category_id uuid NOT NULL,
                        user_id uuid NOT NULL,
                        category_name text,
                        PRIMARY KEY (category_id, user_id)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cdm.category_order (
                        category_id uuid NOT NULL,
                        order_id uuid NOT NULL,
                        category_name text,
                        PRIMARY KEY (category_id, order_id)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cdm.product_user (
                        product_id uuid NOT NULL,
                        user_id uuid NOT NULL,
                        product_name text,
                        category_id uuid,
                        category_name text,
                        PRIMARY KEY (product_id, user_id)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cdm.product_order (
                        product_id uuid NOT NULL,
                        order_id uuid NOT NULL,
                        product_name text,
                        category_id uuid,
                        category_name text,
                        PRIMARY KEY (product_id, order_id)
                    );
                    """
                )

                # Aggregate marts (easy to plug into DataLens).
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cdm.category_stats (
                        category_id uuid PRIMARY KEY,
                        category_name text,
                        user_cnt bigint NOT NULL DEFAULT 0,
                        order_cnt bigint NOT NULL DEFAULT 0,
                        updated_ts timestamptz NOT NULL DEFAULT now()
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cdm.product_stats (
                        product_id uuid PRIMARY KEY,
                        product_name text,
                        category_id uuid,
                        category_name text,
                        user_cnt bigint NOT NULL DEFAULT 0,
                        order_cnt bigint NOT NULL DEFAULT 0,
                        updated_ts timestamptz NOT NULL DEFAULT now()
                    );
                    """
                )

        self._ddl_applied = True

    def _normalize(self, payload: Dict[str, Any]) -> NormalizedOrder:
        root = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload

        order_id = _as_uuid(_first(root, ["order_id", "orderId", "id"]))
        user_id = _as_uuid(_first(root, ["user_id", "userId"]))
        order_ts = _as_dt(_first(root, ["order_ts", "orderTs", "created_at", "createdAt"]))
        if order_id is None or user_id is None:
            raise ValueError(f"Cannot normalize CDM message: missing order_id/user_id. keys={list(root.keys())}")

        items_raw = _first(root, ["items", "order_items", "orderItems", "products", "positions"]) or []
        if not isinstance(items_raw, list):
            items_raw = []

        items: List[NormalizedItem] = []
        for it in items_raw:
            if not isinstance(it, dict):
                continue
            product_id = _as_uuid(_first(it, ["product_id", "productId", "id"]))
            if product_id is None:
                continue
            product_name = _first(it, ["product_name", "productName", "name", "title"])
            category_id = _as_uuid(_first(it, ["category_id", "categoryId"]))
            category_name = _first(it, ["category_name", "categoryName"])
            items.append(
                NormalizedItem(
                    product_id=product_id,
                    product_name=str(product_name) if product_name is not None else None,
                    category_id=category_id,
                    category_name=str(category_name) if category_name is not None else None,
                )
            )

        return NormalizedOrder(order_id=order_id, user_id=user_id, order_ts=order_ts, items=items)

    def process_message(self, payload: Dict[str, Any]) -> None:
        """
        Idempotently updates CDM marts using distinct user/order metrics.
        """
        self._apply_ddl()
        order = self._normalize(payload)

        category_ids: Set[UUID] = set()
        product_ids: Set[UUID] = set()

        cat_user_rows: List[Tuple[UUID, UUID, Optional[str]]] = []
        cat_order_rows: List[Tuple[UUID, UUID, Optional[str]]] = []
        prod_user_rows: List[Tuple[UUID, UUID, Optional[str], Optional[UUID], Optional[str]]] = []
        prod_order_rows: List[Tuple[UUID, UUID, Optional[str], Optional[UUID], Optional[str]]] = []

        # Deduplicate inside message to avoid extra work.
        seen_cat_user: Set[Tuple[UUID, UUID]] = set()
        seen_cat_order: Set[Tuple[UUID, UUID]] = set()
        seen_prod_user: Set[Tuple[UUID, UUID]] = set()
        seen_prod_order: Set[Tuple[UUID, UUID]] = set()

        for it in order.items:
            product_ids.add(it.product_id)

            if (it.product_id, order.user_id) not in seen_prod_user:
                prod_user_rows.append((it.product_id, order.user_id, it.product_name, it.category_id, it.category_name))
                seen_prod_user.add((it.product_id, order.user_id))
            if (it.product_id, order.order_id) not in seen_prod_order:
                prod_order_rows.append((it.product_id, order.order_id, it.product_name, it.category_id, it.category_name))
                seen_prod_order.add((it.product_id, order.order_id))

            if it.category_id is not None:
                category_ids.add(it.category_id)
                if (it.category_id, order.user_id) not in seen_cat_user:
                    cat_user_rows.append((it.category_id, order.user_id, it.category_name))
                    seen_cat_user.add((it.category_id, order.user_id))
                if (it.category_id, order.order_id) not in seen_cat_order:
                    cat_order_rows.append((it.category_id, order.order_id, it.category_name))
                    seen_cat_order.add((it.category_id, order.order_id))

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                if cat_user_rows:
                    cur.executemany(
                        """
                        INSERT INTO cdm.category_user(category_id, user_id, category_name)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (category_id, user_id) DO UPDATE
                        SET category_name = COALESCE(EXCLUDED.category_name, cdm.category_user.category_name);
                        """,
                        cat_user_rows,
                    )
                if cat_order_rows:
                    cur.executemany(
                        """
                        INSERT INTO cdm.category_order(category_id, order_id, category_name)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (category_id, order_id) DO UPDATE
                        SET category_name = COALESCE(EXCLUDED.category_name, cdm.category_order.category_name);
                        """,
                        cat_order_rows,
                    )
                if prod_user_rows:
                    cur.executemany(
                        """
                        INSERT INTO cdm.product_user(product_id, user_id, product_name, category_id, category_name)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (product_id, user_id) DO UPDATE
                        SET product_name = COALESCE(EXCLUDED.product_name, cdm.product_user.product_name),
                            category_id = COALESCE(EXCLUDED.category_id, cdm.product_user.category_id),
                            category_name = COALESCE(EXCLUDED.category_name, cdm.product_user.category_name);
                        """,
                        prod_user_rows,
                    )
                if prod_order_rows:
                    cur.executemany(
                        """
                        INSERT INTO cdm.product_order(product_id, order_id, product_name, category_id, category_name)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (product_id, order_id) DO UPDATE
                        SET product_name = COALESCE(EXCLUDED.product_name, cdm.product_order.product_name),
                            category_id = COALESCE(EXCLUDED.category_id, cdm.product_order.category_id),
                            category_name = COALESCE(EXCLUDED.category_name, cdm.product_order.category_name);
                        """,
                        prod_order_rows,
                    )

                # Recompute only affected aggregates (safe and idempotent).
                for cid in category_ids:
                    cur.execute(
                        """
                        INSERT INTO cdm.category_stats(category_id, category_name, user_cnt, order_cnt, updated_ts)
                        VALUES (%(cid)s, NULL, 0, 0, now())
                        ON CONFLICT (category_id) DO NOTHING;
                        """,
                        {"cid": cid},
                    )
                    cur.execute(
                        """
                        UPDATE cdm.category_stats s
                        SET
                            category_name = COALESCE(
                                (SELECT cu.category_name FROM cdm.category_user cu
                                 WHERE cu.category_id = %(cid)s AND cu.category_name IS NOT NULL
                                 LIMIT 1),
                                s.category_name
                            ),
                            user_cnt = (SELECT COUNT(*) FROM cdm.category_user cu WHERE cu.category_id = %(cid)s),
                            order_cnt = (SELECT COUNT(*) FROM cdm.category_order co WHERE co.category_id = %(cid)s),
                            updated_ts = now()
                        WHERE s.category_id = %(cid)s;
                        """,
                        {"cid": cid},
                    )

                for pid in product_ids:
                    cur.execute(
                        """
                        INSERT INTO cdm.product_stats(product_id, product_name, category_id, category_name, user_cnt, order_cnt, updated_ts)
                        VALUES (%(pid)s, NULL, NULL, NULL, 0, 0, now())
                        ON CONFLICT (product_id) DO NOTHING;
                        """,
                        {"pid": pid},
                    )
                    cur.execute(
                        """
                        UPDATE cdm.product_stats s
                        SET
                            product_name = COALESCE(
                                (SELECT pu.product_name FROM cdm.product_user pu
                                 WHERE pu.product_id = %(pid)s AND pu.product_name IS NOT NULL
                                 LIMIT 1),
                                s.product_name
                            ),
                            category_id = COALESCE(
                                (SELECT pu.category_id FROM cdm.product_user pu
                                 WHERE pu.product_id = %(pid)s AND pu.category_id IS NOT NULL
                                 LIMIT 1),
                                s.category_id
                            ),
                            category_name = COALESCE(
                                (SELECT pu.category_name FROM cdm.product_user pu
                                 WHERE pu.product_id = %(pid)s AND pu.category_name IS NOT NULL
                                 LIMIT 1),
                                s.category_name
                            ),
                            user_cnt = (SELECT COUNT(*) FROM cdm.product_user pu WHERE pu.product_id = %(pid)s),
                            order_cnt = (SELECT COUNT(*) FROM cdm.product_order po WHERE po.product_id = %(pid)s),
                            updated_ts = now()
                        WHERE s.product_id = %(pid)s;
                        """,
                        {"pid": pid},
                    )

