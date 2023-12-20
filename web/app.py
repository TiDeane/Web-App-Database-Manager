#!/usr/bin/python3
from logging.config import dictConfig

import psycopg
from flask import flash
from flask import Flask
from flask import jsonify
from flask import redirect
from flask import render_template
from flask import request
from flask import url_for
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool

from datetime import datetime

# postgres://{user}:{password}@{hostname}:{port}/{database-name}
DATABASE_URL = "postgres://db:db@postgres/db"

pool = ConnectionPool(conninfo=DATABASE_URL)
# the pool starts connecting immediately.

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'
log = app.logger


@app.route("/", methods=("GET",))
@app.route("/index", methods=("GET",))
def product_index():
    """Show all the products, ordered by SKU."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            products = cur.execute(
                """
                SELECT name, SKU, description, price
                FROM product
                ORDER BY SKU;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(products)

    return render_template("product/index.html", products=products)


@app.route("/list_products", methods=("GET",))
def product_list():
    """Lists all products."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            products = cur.execute(
                """
                SELECT *
                FROM product
                ORDER BY sku;
                """,
                {},
            ).fetchall()

        conn.commit()

    return render_template("product/list_products.html", products=products)


@app.route("/products/<sku>/delete", methods=("POST",))
def product_delete(sku):
    """Delete a product and it's corresponding supplier and 'contains' entries."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            supplier = cur.execute(
                """
                SELECT tin FROM supplier
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            ).fetchone()
            
            if supplier is not None:
                cur.execute(
                    """
                    DELETE FROM delivery
                    WHERE tin = %(tin)s;
                    """,
                    {"tin": supplier[0]},
                )
            cur.execute(
                """
                DELETE FROM supplier
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )
            cur.execute(
                """
                DELETE FROM contains
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )
            cur.execute(
                """
                DELETE FROM product
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )

        conn.commit()
    return redirect(url_for("product_index"))


@app.route("/products/<sku>/update", methods=["GET", "POST"])
def product_update(sku):
    """Update the product description and price."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            product = cur.execute(
                """
                SELECT name, description, price, SKU
                FROM product
                WHERE SKU = %(sku)s;
                """,
                {"sku": sku},
            ).fetchone()
            log.debug(f"Found {cur.rowcount} rows.")

    if request.method == "POST":
        description = request.form["description"]
        price = request.form["price"]

        error = None

        if not description:
            error = "Description is required."
        elif not price:
            error = "Price is required."
        elif not price.replace('.', '', 1).isdigit():
            error = "Price must be numeric."

        if error is not None:
            flash(error)
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        UPDATE product
                        SET description = %(description)s, price = %(price)s
                        WHERE SKU = %(sku)s;
                        """,
                        {"sku": sku, "description": description, "price": price},
                    )
                conn.commit()
            return redirect(url_for("product_list"))

    return render_template("product/update.html", product=product)


@app.route("/products/register_prod", methods=["GET", "POST"])
def product_register():
    """Register a new product."""

    if request.method == "POST":
        sku = request.form["sku"]
        name = request.form["name"]
        description = request.form["description"]
        price = request.form["price"]
        ean = request.form["ean"]

        error = None

        if not sku:
            error = "SKU is required."
        elif not name:
            error = "Name is required."
        elif not description:
            error = "Description is required."
        elif not price:
            error = "Price is required."
        elif not price.replace('.', '', 1).isdigit():
            error = "Price must be numeric."
        elif not ean:
            error = "EAN is required."
        elif not ean.isnumeric():
            error = "EAN must be numeric."

        if error is not None:
            flash(error)
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        INSERT INTO product
                        VALUES (%(sku)s, %(name)s, %(description)s, %(price)s, %(ean)s);
                        """,
                        {"sku": sku, "name": name, "description": description, "price": price, "price": price, "ean": ean},
                    )
                conn.commit()
            return redirect(url_for("product_index"))

    return render_template("product/register_prod.html")


@app.route("/list_suppliers", methods=("GET",))
def supplier_list():
    """Lists all suppliers."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            suppliers = cur.execute(
                """
                SELECT TIN, name, address, SKU, date
                FROM supplier
                ORDER BY SKU;
                """,
                {},
            ).fetchall()

        conn.commit()

    return render_template("product/list_suppliers.html", suppliers=suppliers)


@app.route("/list_suppliers/<tin>/<sku>/delete", methods=("POST",))
def supplier_delete(tin, sku):
    """Delete the supplier and associated product."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                DELETE FROM delivery
                WHERE tin = %(tin)s;
                """,
                {"tin": tin},
            )
            cur.execute(
                """
                DELETE FROM supplier
                WHERE tin = %(tin)s;
                """,
                {"tin": tin},
            )

            cur.execute(
                """
                DELETE FROM contains
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )

            cur.execute(
                """
                DELETE FROM product
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )

        conn.commit()
    return redirect(url_for("supplier_list"))


@app.route("/list_customers", methods=("GET",))
def customer_list():
    """Lists all suppliers."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            customers = cur.execute(
                """
                SELECT *
                FROM customer
                ORDER BY cust_no;
                """,
                {},
            ).fetchall()

        conn.commit()

    return render_template("product/list_customers.html", customers=customers)


@app.route("/list_customers/<cust_no>/delete", methods=("POST",))
def customer_delete(cust_no):
    """Delete the customer."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                SELECT order_no
                FROM orders
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )
            order_numbers = [row[0] for row in cur.fetchall()]

            for order_no in order_numbers:
                cur.execute(
                    """
                    DELETE FROM process
                    WHERE order_no = %(order_no)s;
                    """,
                    {"order_no": order_no},
                )
                cur.execute(
                    """
                    DELETE FROM contains
                    WHERE order_no = %(order_no)s;
                    """,
                    {"order_no": order_no},
                )

            cur.execute(
                """
                DELETE FROM pay
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )

            cur.execute(
                """
                DELETE FROM orders
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )

            cur.execute(
                """
                DELETE FROM customer
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )

        conn.commit()
    return redirect(url_for("customer_list"))


@app.route("/list_products/<sku>/order_make", methods=("GET", "POST"))
def order_make(sku):
    """Buy a product."""

    if request.method == "POST":
        cust_no = request.form["cust_no"]
        date_str = request.form["date"]
        qty = request.form["qty"]

        error = None

        if not cust_no:
            error = "Customer number is required."
        elif not date_str:
            error = "Date is required."
        elif not qty:
            error = "Quantity is required."
        elif not qty.isnumeric():
            error = "Quantity must be numeric."

        if error is not None:
            flash(error)
        else:
            date = datetime.strptime(date_str, "%Y-%m-%d").date()

            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    query = cur.execute(
                        """
                        SELECT order_no
                        FROM orders
                        WHERE order_no >= ALL(
                            SELECT order_no
                            FROM orders o
                        )
                        """,
                        {},
                    ).fetchone()

                    if query is not None:
                        max_order_no = query[0]
                        order_no = max_order_no + 1
                    else:
                        order_no = 1

                    cur.execute(
                        """
                        INSERT INTO orders
                        VALUES (%(order_no)s, %(cust_no)s, %(date)s);
                        """,
                        {"order_no": order_no, "cust_no": cust_no, "date": date},
                    )

                    cur.execute(
                        """
                        INSERT INTO contains
                        VALUES (%(order_no)s, %(sku)s, %(qty)s);
                        """,
                        {"order_no": order_no, "sku": sku, "qty": qty},
                    )
                conn.commit()

            return redirect(url_for("product_index"))

    return render_template("product/order_make.html", sku=sku)


@app.route("/products/register_sup", methods=["GET", "POST"])
def supplier_register():
    """Register a new supplier."""

    if request.method == "POST":
        tin = request.form["tin"]
        name = request.form["name"]
        address = request.form["address"]
        sku = request.form["sku"]
        date_str = request.form["date"]

        error = None

        if not tin:
            error = "TIN is required."
        elif not name:
            error = "Name is required."
        elif not address:
            error = "Address is required."
        elif not sku:
            error = "SKU is required."
        elif not date_str:
            error = "Date is required."

        if error is not None:
            flash(error)
        else:
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        INSERT INTO supplier
                        VALUES (%(tin)s, %(name)s, %(address)s, %(sku)s, %(date)s);
                        """,
                        {"tin": tin, "name": name, "address": address, "sku": sku, "date": date},
                    )
                conn.commit()
            return redirect(url_for("product_index"))

    return render_template("product/register_sup.html")


@app.route("/products/register_cus", methods=["GET", "POST"])
def customer_register():
    """Register a new Customer."""

    if request.method == "POST":
        name = request.form["name"]
        email = request.form["email"]
        phone = request.form["phone"]
        address = request.form["address"]

        error = None

        if not name:
            error = "Name is required."
        elif not email:
            error = "Email is required."
        elif not phone:
            error = "Phone is required."
        elif not address:
            error = "Address is required."

        if error is not None:
            flash(error)
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    query = cur.execute(
                        """
                        SELECT MAX(cust_no) FROM customer
                        """
                    ).fetchone()

                    if query is not None:
                        max_cust_no = query[0]
                        cust_no = max_cust_no + 1

                    cur.execute(
                        """
                        INSERT INTO customer
                        VALUES (%(cust_no)s, %(name)s, %(email)s, %(phone)s, %(address)s);
                        """,
                        {"cust_no": cust_no, "name": name, "email": email, "phone": phone, "address": address},
                    )
                conn.commit()
            return redirect(url_for("product_index"))

    return render_template("product/register_cus.html")


@app.route("/list_customers/<cust_no>/orders", methods=("GET",))
def order_cus_list(cust_no):
    """Lists all of the customer's orders."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            o = cur.execute(
                """
                SELECT *
                FROM orders o
                WHERE cust_no = (%(cust_no)s)
                AND o.order_no NOT IN (SELECT order_no FROM pay)
                ORDER BY o.order_no;
                """,
                {"cust_no": cust_no},
            ).fetchall()

        conn.commit()

    return render_template("product/list_orders.html", cust_no=cust_no, orders=o)


@app.route("/products/pay_order/<cust_no>/<order_no>", methods=["GET", "POST"])
def order_pay(order_no, cust_no):
    """Pay for an order."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute(
                """
                INSERT INTO pay
                VALUES (%(order_no)s, %(cust_no)s);
                """,
                {"order_no": order_no, "cust_no": cust_no}
            )

        conn.commit()

    return redirect(url_for("product_index"))


@app.route("/ping", methods=("GET",))
def ping():
    log.debug("ping!")
    return jsonify({"message": "pong!", "status": "success"})


if __name__ == "__main__":
    app.run()
