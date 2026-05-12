import os
from datetime import date, datetime

import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, redirect, url_for, jsonify

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")


# =========================
# DB接続
# =========================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL が設定されていません")

    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


# =========================
# DB初期化
# =========================
def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS manufacturers (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    unit_price INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS purchases (
                    id SERIAL PRIMARY KEY,
                    purchase_date DATE NOT NULL,
                    purchase_month TEXT NOT NULL,
                    purchase_count INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'planned',
                    total_amount INTEGER NOT NULL DEFAULT 0,
                    memo TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS purchase_items (
                    id SERIAL PRIMARY KEY,
                    purchase_id INTEGER NOT NULL REFERENCES purchases(id) ON DELETE CASCADE,
                    manufacturer_id INTEGER REFERENCES manufacturers(id) ON DELETE SET NULL,
                    manufacturer_name TEXT,
                    flavor_name TEXT NOT NULL,
                    quantity INTEGER NOT NULL DEFAULT 1,
                    unit_price INTEGER NOT NULL DEFAULT 0,
                    subtotal INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

            conn.commit()


@app.before_request
def before_request():
    init_db()


# =========================
# Jinjaフィルター
# =========================
def yen(value):
    try:
        return f"¥{int(value):,}"
    except Exception:
        return "¥0"


app.jinja_env.filters["yen"] = yen


# =========================
# 共通関数
# =========================
def get_month_string(target_date):
    return target_date.strftime("%Y-%m")


def normalize_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def get_next_purchase_count(purchase_month):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(purchase_count), 0) + 1 AS next_count
                FROM purchases
                WHERE purchase_month = %s
                  AND status = 'purchased';
                """,
                (purchase_month,),
            )
            row = cur.fetchone()
            return row["next_count"]


# =========================
# ホーム
# =========================
@app.route("/")
def index():
    today = date.today()
    current_month = today.strftime("%Y-%m")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS purchased_count,
                    COALESCE(SUM(total_amount), 0) AS purchased_total
                FROM purchases
                WHERE purchase_month = %s
                  AND status = 'purchased';
                """,
                (current_month,),
            )
            month_summary = cur.fetchone()

            cur.execute(
                """
                SELECT p.*,
                       COALESCE(COUNT(i.id), 0) AS item_count
                FROM purchases p
                LEFT JOIN purchase_items i ON p.id = i.purchase_id
                WHERE p.status = 'planned'
                GROUP BY p.id
                ORDER BY p.purchase_date ASC, p.id ASC;
                """
            )
            planned_purchases = cur.fetchall()

            cur.execute(
                """
                SELECT *
                FROM purchases
                WHERE status = 'purchased'
                ORDER BY purchase_date DESC, id DESC
                LIMIT 5;
                """
            )
            recent_purchases = cur.fetchall()

    return render_template(
        "index.html",
        current_month=current_month,
        month_summary=month_summary,
        planned_purchases=planned_purchases,
        recent_purchases=recent_purchases,
    )


# =========================
# 購入登録・編集
# =========================
@app.route("/purchase/new", methods=["GET", "POST"])
def purchase_new():
    if request.method == "POST":
        return save_purchase()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM manufacturers ORDER BY name ASC;")
            manufacturers = cur.fetchall()

    return render_template(
        "purchase_form.html",
        mode="new",
        purchase=None,
        items=[],
        manufacturers=manufacturers,
        today=date.today().isoformat(),
    )


@app.route("/purchase/<int:purchase_id>/edit", methods=["GET", "POST"])
def purchase_edit(purchase_id):
    if request.method == "POST":
        return save_purchase(purchase_id)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM purchases WHERE id = %s;", (purchase_id,))
            purchase = cur.fetchone()

            if not purchase:
                return redirect(url_for("history"))

            cur.execute(
                """
                SELECT *
                FROM purchase_items
                WHERE purchase_id = %s
                ORDER BY id ASC;
                """,
                (purchase_id,),
            )
            items = cur.fetchall()

            cur.execute("SELECT * FROM manufacturers ORDER BY name ASC;")
            manufacturers = cur.fetchall()

    return render_template(
        "purchase_form.html",
        mode="edit",
        purchase=purchase,
        items=items,
        manufacturers=manufacturers,
        today=date.today().isoformat(),
    )


def save_purchase(purchase_id=None):
    purchase_date_str = request.form.get("purchase_date")
    status = request.form.get("status", "planned")
    memo = request.form.get("memo", "")

    if not purchase_date_str:
        purchase_date = date.today()
    else:
        purchase_date = datetime.strptime(purchase_date_str, "%Y-%m-%d").date()

    purchase_month = get_month_string(purchase_date)

    manufacturer_ids = request.form.getlist("manufacturer_id[]")
    flavor_names = request.form.getlist("flavor_name[]")
    quantities = request.form.getlist("quantity[]")
    unit_prices = request.form.getlist("unit_price[]")

    cleaned_items = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            for idx, flavor_name in enumerate(flavor_names):
                flavor_name = flavor_name.strip()

                if not flavor_name:
                    continue

                manufacturer_id_raw = manufacturer_ids[idx] if idx < len(manufacturer_ids) else None
                manufacturer_id = normalize_int(manufacturer_id_raw, None)

                quantity_raw = quantities[idx] if idx < len(quantities) else 1
                unit_price_raw = unit_prices[idx] if idx < len(unit_prices) else 0

                quantity = normalize_int(quantity_raw, 1)
                unit_price = normalize_int(unit_price_raw, 0)

                if quantity <= 0:
                    quantity = 1

                manufacturer_name = None

                if manufacturer_id:
                    cur.execute(
                        """
                        SELECT name
                        FROM manufacturers
                        WHERE id = %s;
                        """,
                        (manufacturer_id,),
                    )
                    maker = cur.fetchone()

                    if maker:
                        manufacturer_name = maker["name"]

                subtotal = quantity * unit_price

                cleaned_items.append(
                    {
                        "manufacturer_id": manufacturer_id,
                        "manufacturer_name": manufacturer_name,
                        "flavor_name": flavor_name,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "subtotal": subtotal,
                    }
                )

            total_amount = sum(item["subtotal"] for item in cleaned_items)

            if purchase_id is None:
                if status == "purchased":
                    purchase_count = get_next_purchase_count(purchase_month)
                else:
                    purchase_count = 0

                cur.execute(
                    """
                    INSERT INTO purchases
                        (
                            purchase_date,
                            purchase_month,
                            purchase_count,
                            status,
                            total_amount,
                            memo
                        )
                    VALUES
                        (%s, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (
                        purchase_date,
                        purchase_month,
                        purchase_count,
                        status,
                        total_amount,
                        memo,
                    ),
                )

                purchase_id = cur.fetchone()["id"]

            else:
                cur.execute(
                    """
                    SELECT status, purchase_count
                    FROM purchases
                    WHERE id = %s;
                    """,
                    (purchase_id,),
                )
                old_purchase = cur.fetchone()

                old_status = old_purchase["status"] if old_purchase else "planned"
                old_count = old_purchase["purchase_count"] if old_purchase else 0

                if status == "purchased" and old_status != "purchased":
                    purchase_count = get_next_purchase_count(purchase_month)
                elif status == "purchased":
                    purchase_count = old_count if old_count and old_count > 0 else get_next_purchase_count(purchase_month)
                else:
                    purchase_count = 0

                cur.execute(
                    """
                    UPDATE purchases
                    SET purchase_date = %s,
                        purchase_month = %s,
                        purchase_count = %s,
                        status = %s,
                        total_amount = %s,
                        memo = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s;
                    """,
                    (
                        purchase_date,
                        purchase_month,
                        purchase_count,
                        status,
                        total_amount,
                        memo,
                        purchase_id,
                    ),
                )

                cur.execute(
                    """
                    DELETE FROM purchase_items
                    WHERE purchase_id = %s;
                    """,
                    (purchase_id,),
                )

            for item in cleaned_items:
                cur.execute(
                    """
                    INSERT INTO purchase_items
                        (
                            purchase_id,
                            manufacturer_id,
                            manufacturer_name,
                            flavor_name,
                            quantity,
                            unit_price,
                            subtotal
                        )
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        purchase_id,
                        item["manufacturer_id"],
                        item["manufacturer_name"],
                        item["flavor_name"],
                        item["quantity"],
                        item["unit_price"],
                        item["subtotal"],
                    ),
                )

            conn.commit()

    return redirect(url_for("history"))


@app.route("/purchase/<int:purchase_id>/delete", methods=["POST"])
def purchase_delete(purchase_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM purchases
                WHERE id = %s;
                """,
                (purchase_id,),
            )
            conn.commit()

    return redirect(url_for("history"))


# =========================
# 履歴
# =========================
@app.route("/history")
def history():
    selected_month = request.args.get("month")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT purchase_month
                FROM purchases
                ORDER BY purchase_month DESC;
                """
            )
            months = cur.fetchall()

            if not selected_month:
                if months:
                    selected_month = months[0]["purchase_month"]
                else:
                    selected_month = date.today().strftime("%Y-%m")

            cur.execute(
                """
                SELECT *
                FROM purchases
                WHERE purchase_month = %s
                ORDER BY purchase_date DESC, id DESC;
                """,
                (selected_month,),
            )
            purchases = cur.fetchall()

            purchase_ids = [p["id"] for p in purchases]
            items_by_purchase = {}

            if purchase_ids:
                cur.execute(
                    """
                    SELECT *
                    FROM purchase_items
                    WHERE purchase_id = ANY(%s)
                    ORDER BY id ASC;
                    """,
                    (purchase_ids,),
                )
                items = cur.fetchall()

                for item in items:
                    items_by_purchase.setdefault(item["purchase_id"], []).append(item)

            cur.execute(
                """
                SELECT COALESCE(SUM(total_amount), 0) AS month_total
                FROM purchases
                WHERE purchase_month = %s
                  AND status = 'purchased';
                """,
                (selected_month,),
            )
            month_total = cur.fetchone()["month_total"]

    return render_template(
        "history.html",
        months=months,
        selected_month=selected_month,
        purchases=purchases,
        items_by_purchase=items_by_purchase,
        month_total=month_total,
    )


# =========================
# ランキング
# =========================
@app.route("/ranking")
def ranking():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    i.flavor_name,
                    SUM(i.quantity) AS total_quantity
                FROM purchase_items i
                JOIN purchases p ON i.purchase_id = p.id
                WHERE p.status = 'purchased'
                GROUP BY i.flavor_name
                ORDER BY total_quantity DESC, i.flavor_name ASC
                LIMIT 50;
                """
            )
            ranking_items = cur.fetchall()

    return render_template(
        "ranking.html",
        ranking_items=ranking_items,
    )


# =========================
# メーカーマスタ
# =========================
@app.route("/master")
def master():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM manufacturers
                ORDER BY name ASC;
                """
            )
            manufacturers = cur.fetchall()

    return render_template(
        "master.html",
        manufacturers=manufacturers,
    )


@app.route("/master/create", methods=["POST"])
def master_create():
    name = request.form.get("name", "").strip()
    unit_price = normalize_int(request.form.get("unit_price"), 0)

    if not name:
        return redirect(url_for("master"))

    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO manufacturers (name, unit_price)
                    VALUES (%s, %s);
                    """,
                    (name, unit_price),
                )
                conn.commit()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()

    return redirect(url_for("master"))


@app.route("/master/<int:manufacturer_id>/update", methods=["POST"])
def master_update(manufacturer_id):
    name = request.form.get("name", "").strip()
    unit_price = normalize_int(request.form.get("unit_price"), 0)

    if not name:
        return redirect(url_for("master"))

    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    UPDATE manufacturers
                    SET name = %s,
                        unit_price = %s
                    WHERE id = %s;
                    """,
                    (name, unit_price, manufacturer_id),
                )
                conn.commit()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()

    return redirect(url_for("master"))


@app.route("/master/<int:manufacturer_id>/delete", methods=["POST"])
def master_delete(manufacturer_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE purchase_items
                SET manufacturer_id = NULL
                WHERE manufacturer_id = %s;
                """,
                (manufacturer_id,),
            )

            cur.execute(
                """
                DELETE FROM manufacturers
                WHERE id = %s;
                """,
                (manufacturer_id,),
            )

            conn.commit()

    return redirect(url_for("master"))


# =========================
# API
# =========================
@app.route("/api/manufacturers")
def api_manufacturers():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM manufacturers
                ORDER BY name ASC;
                """
            )
            manufacturers = cur.fetchall()

    return jsonify(manufacturers)


@app.route("/health")
def health():
    return jsonify({"ok": True})


# =========================
# 起動
# =========================
if __name__ == "__main__":
    app.run(debug=True)