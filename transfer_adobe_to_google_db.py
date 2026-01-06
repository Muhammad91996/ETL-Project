#!/usr/bin/env python3
"""
Automated data transfer from Magento to Google Cloud SQL with denormalized tables.
"""

import sys
import subprocess
import time
import signal
import logging
from datetime import datetime
from pathlib import Path
import mysql.connector
from mysql.connector import Error
import os
import socket
import shutil
import pandas as pd
import zipfile
import requests
import atexit
from dotenv import load_dotenv



# Load environment variables
load_dotenv()


class DatabaseTransfer:
    def __init__(self):
        # ===== Paths =====
        self.script_dir = Path(__file__).parent.absolute()
        os.chdir(self.script_dir)

        # ===== Project / Environment =====
        self.project = os.getenv("PROJECT")
        self.environment = os.getenv("ENVIRONMENT")
        self.instance_connection = os.getenv("INSTANCE_CONNECTION")

        # ===== GCP Database =====
        self.gcp_db_ip = os.getenv("GCP_DB_IP")
        self.gcp_db_name = os.getenv("GCP_DB_NAME")
        self.gcp_db_user = os.getenv("GCP_DB_USER")
        self.gcp_db_pass = os.getenv("GCP_DB_PASSWORD")

        # ===== Time =====
        self.now = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.year = datetime.now().strftime("%Y")
        self.month = datetime.now().strftime("%m")
        self.day = datetime.now().strftime("%d")

        # ===== Directories =====
        self.dump_dir = self.script_dir / "dump"
        self.log_dir = self.script_dir / "logs" / self.year / self.month / self.day
        self.dump_dir.mkdir(exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.dump_file = self.dump_dir / f"magento_dump_{self.now}.sql"
        self.logfile = self.log_dir / "automation.log"

        # ===== Telegram =====
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN1")
        self.telegram_chat_id = int(os.getenv("TELEGRAM_CHAT_ID1"))

        # ===== SSH =====
        self.ssh_host = os.getenv("SSH_HOST")
        self.local_port = int(os.getenv("LOCAL_PORT", 30000))
        self.ssh_process = None

        # ===== Remote Magento DB =====
        self.remote_db_user = os.getenv("REMOTE_DB_USER")
        self.remote_db_pass = os.getenv("REMOTE_DB_PASSWORD")
        self.remote_db_name = os.getenv("REMOTE_DB_NAME")

        # ===== Tables =====
        self.tables = os.getenv("TABLES1").split(",")

        # ===== Validation =====
        required_vars = [
            "PROJECT", "ENVIRONMENT", "INSTANCE_CONNECTION",
            "GCP_DB_IP", "GCP_DB_NAME", "GCP_DB_USER", "GCP_DB_PASSWORD",
            "TELEGRAM_BOT_TOKEN1", "TELEGRAM_CHAT_ID1",
            "SSH_HOST",
            "REMOTE_DB_USER", "REMOTE_DB_PASSWORD", "REMOTE_DB_NAME",
            "TABLES1"
        ]
        missing = [v for v in required_vars if not os.getenv(v)]
        if missing:
            raise EnvironmentError(f"❌ Missing environment variables: {', '.join(missing)}")

        # ===== Setup =====
        self.setup_logging()
        atexit.register(self.cleanup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    # ================= Logging =================
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S',
            handlers=[
                logging.FileHandler(self.logfile),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}, cleaning up...")
        self.cleanup()
        sys.exit(1)

    # ================= Utilities =================
    def cleanup(self):
        if self.ssh_process and self.ssh_process.poll() is None:
            self.logger.info("🧹 Cleaning up SSH tunnel...")
            try:
                self.ssh_process.terminate()
                self.ssh_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.ssh_process.kill()

        try:
            subprocess.run(
                ["pkill", "-f", f"ssh.*{self.local_port}:localhost:3306"],
                capture_output=True,
                timeout=5
            )
        except Exception:
            pass

    def check_dependencies(self):
        for cmd in ["ssh", "nc", "mysqldump", "mysql"]:
            if subprocess.run(["which", cmd], capture_output=True).returncode != 0:
                self.logger.error(f"❌ Missing dependency: {cmd}")
                sys.exit(1)

    def has_enough_space(self, path, required_gb=2):
        _, _, free = shutil.disk_usage(path)
        return free >= required_gb * 1024 ** 3

    def wait_for_port(self, host, port, timeout=60):
        for _ in range(timeout):
            try:
                with socket.create_connection((host, port), timeout=1):
                    return True
            except Exception:
                time.sleep(1)
        return False

    # ================= SSH =================
    def setup_ssh_tunnel(self):
        self.logger.info("🔐 Opening SSH tunnel...")
        subprocess.run(
            ["pkill", "-f", f"ssh.*{self.local_port}:localhost:3306"],
            capture_output=True
        )
        time.sleep(2)

        ssh_cmd = [
            "ssh", "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=no",
            "-N",
            "-L", f"{self.local_port}:localhost:3306",
            self.ssh_host
        ]

        self.ssh_process = subprocess.Popen(ssh_cmd)

        if not self.wait_for_port("127.0.0.1", self.local_port):
            self.logger.error("❌ SSH tunnel failed.")
            sys.exit(1)

        self.logger.info("✅ SSH tunnel established.")

    # ================= Dump =================
    def dump_database(self):
        if not self.has_enough_space(self.dump_dir):
            self.logger.error("❌ Not enough disk space.")
            sys.exit(1)

        self.logger.info("📦 Starting mysqldump...")

        dump_cmd = [
            "mysqldump",
            "--single-transaction",
            "--quick",
            "--no-tablespaces",
            "--skip-lock-tables",
            "--set-gtid-purged=OFF",
            "-h", "127.0.0.1",
            "-P", str(self.local_port),
            "-u", self.remote_db_user,
            f"-p{self.remote_db_pass}",
            self.remote_db_name
        ] + self.tables

        with open(self.dump_file, "w") as f:
            result = subprocess.run(dump_cmd, stdout=f, stderr=subprocess.PIPE, text=True)

        if result.returncode != 0:
            self.logger.error(result.stderr)
            sys.exit(1)

        self.logger.info("✅ Dump completed.")

    # ================= Import =================
    def import_to_gcp(self):
        self.logger.info("📥 Importing to GCP...")
        mysql_cmd = [
            "mysql",
            "-h", self.gcp_db_ip,
            "-u", self.gcp_db_user,
            f"-p{self.gcp_db_pass}",
            self.gcp_db_name
        ]
        with open(self.dump_file, "r") as f:
            result = subprocess.run(mysql_cmd, stdin=f, stderr=subprocess.PIPE, text=True)

        if result.returncode != 0:
            self.logger.error(result.stderr)
            sys.exit(1)

        self.logger.info("✅ Import completed.")

    # ================= SQL Executor =================
    def execute_sql_query(self, query, description):
        self.logger.info(f"🛠 {description}")
        try:
            conn = mysql.connector.connect(
                host=self.gcp_db_ip,
                user=self.gcp_db_user,
                password=self.gcp_db_pass,
                database=self.gcp_db_name
            )
            cursor = conn.cursor()
            for stmt in [s for s in query.split(";") if s.strip()]:
                cursor.execute(stmt)
            conn.commit()
            self.logger.info(f"✅ {description} done.")
        except Error as e:
            self.logger.error(e)
            sys.exit(1)
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()


    def create_product_categories_table(self):
        query = """
            DROP TABLE IF EXISTS product_categories;
            CREATE TABLE product_categories AS
            SELECT
                ccp.product_id,
                GROUP_CONCAT(
                    (
                        SELECT 
                            GROUP_CONCAT(
                                cat_name.value 
                                ORDER BY FIND_IN_SET(parent_cat.entity_id, REPLACE(child_cat.path, '/', ',')) 
                                SEPARATOR ' > '
                            )
                        FROM catalog_category_entity AS parent_cat
                        JOIN catalog_category_entity_varchar AS cat_name 
                            ON parent_cat.row_id = cat_name.row_id
                        WHERE FIND_IN_SET(parent_cat.entity_id, REPLACE(child_cat.path, '/', ','))
                        AND parent_cat.entity_id NOT IN (1, 2)
                        AND cat_name.attribute_id = (
                                SELECT attribute_id 
                                FROM eav_attribute
                                WHERE attribute_code = 'name'
                                AND entity_type_id = 3
                                LIMIT 1
                            )
                        AND cat_name.store_id IN (0, 1)
                    )
                    SEPARATOR ' | '
                ) AS category_path
            FROM catalog_category_product ccp
            JOIN catalog_category_entity child_cat 
                ON child_cat.entity_id = ccp.category_id
            WHERE child_cat.entity_id NOT IN (
                SELECT entity_id 
                FROM catalog_category_entity 
                WHERE path LIKE '1/2/1078/%' 
                    OR path LIKE '1/2/11466/%' 
                    OR path LIKE '1/2/45779/%' 
                    OR entity_id IN (1078,11466,45779) 
                    OR parent_id IN (1078,11466,45779)

                UNION

                /*  offline offers / Haram Store / Installment */
                SELECT DISTINCT c.entity_id
                FROM catalog_category_entity AS c
                JOIN catalog_category_entity AS p
                    ON FIND_IN_SET(p.entity_id, REPLACE(c.path, '/', ','))
                JOIN catalog_category_entity_varchar AS v
                    ON p.row_id = v.row_id
                JOIN eav_attribute AS a
                    ON a.attribute_id = v.attribute_id
                WHERE a.attribute_code = 'name'
                    AND a.entity_type_id = 3
                    AND v.value IN ('offline offers','Haram Store','Installment','Order Sku')
            )
            GROUP BY ccp.product_id;
        """
        self.execute_sql_query(query, "Creating product_categories table")

    def create_shipments_agg_table(self):
        query = """
          DROP TABLE IF EXISTS shipments_agg;
        CREATE TABLE shipments_agg AS
            WITH ranked_shipments AS (
              SELECT
                ssi.order_item_id,
                s.increment_id,
                st.title AS courier_names,
                s.shipment_status,
                ROW_NUMBER() OVER (
                  PARTITION BY ssi.order_item_id
                  ORDER BY s.created_at DESC  
                ) AS rn
              FROM sales_shipment_item ssi
              LEFT JOIN sales_shipment s ON s.entity_id = ssi.parent_id
              LEFT JOIN sales_shipment_track st ON st.parent_id = s.entity_id
            )

            SELECT
              order_item_id,
              increment_id AS shipment_ids,
              courier_names,
              CASE shipment_status
                WHEN '1' THEN 'Packed'
                WHEN '2' THEN 'Ready_to_ship'
                WHEN '3' THEN 'Canceled'
                WHEN '4' THEN 'Shipped'
                WHEN '5' THEN 'Delivered'
                WHEN '6' THEN 'Failed_Delivery'
                WHEN '7' THEN 'Returned'
                WHEN '8' THEN 'Lost_Delivery'
                WHEN '9' THEN 'Complete'
                WHEN '10' THEN 'Under_Investigation'
                WHEN '11' THEN 'Invoiced'
                WHEN '12' THEN 'Collected'
                WHEN '13' THEN 'Lost_Collected'
                WHEN '14' THEN 'Picked'
                WHEN '15' THEN 'Dispatched'
                WHEN '16' THEN 'Received_at_WH'
                WHEN '17' THEN 'In_Process'
                WHEN '18' THEN 'RECEIVED_AT_SORTING_CENTER'
                WHEN '19' THEN 'FAILED_TO_PICK'
                WHEN '20' THEN 'RETURN_TO_SELLER'
                WHEN '21' THEN 'RETURNED_TO_SELLER'
                WHEN '22' THEN 'FAILED_TO_RETURN'
                WHEN '23' THEN 'DRAWING REQUEST'
                WHEN '24' THEN 'DRAWING RECEIVED'
                WHEN '25' THEN 'BUY MATERIAL'
                WHEN '26' THEN 'DESIGN IMPLEMENTING'
                WHEN '27' THEN 'DONE IMPLEMENTATION'
                WHEN '28' THEN 'PACKING READY FOR SHIP'
                WHEN '404' THEN 'DUPLICATE_ERROR'
                ELSE 'pending'
              END AS shipment_statuses
            FROM ranked_shipments
            WHERE rn = 1;
        """
        self.execute_sql_query(query, "Creating shipments_agg table")

    def create_sales_order_item_cost_table(self):
        query = """
          DROP TABLE IF EXISTS sales_order_item_cost;
        CREATE TABLE sales_order_item_cost AS
        SELECT 
          main.item_id As item_id,
            CASE  
            WHEN main.product_type = 'configurable' THEN (main.base_price - sec.base_cost) * main.qty_ordered
              WHEN main.base_price = 0 AND sec.base_price > main.base_price THEN (sec.base_price - main.base_cost) * sec.qty_ordered 
              ELSE  (main.base_price - main.base_cost) * main.qty_ordered 
              END AS margin
          FROM  
              sales_order_item AS main
              LEFT JOIN sales_order_item AS sec 
              ON sec.sku = main.sku
              AND sec.order_id = main.order_id 
              AND sec.product_id <> main.product_id
            AND sec.product_type <> main.product_type
          WHERE 
              main.marketplace_seller IS NULL
        """
        self.execute_sql_query(query, "Creating sales_order_item_cost table")
    def create_Sales_Refunded_table(self):
        query = """
          DROP TABLE IF EXISTS Sales_Refunded;
        CREATE TABLE Sales_Refunded AS
        SELECT
        i.item_id AS order_item_id,
           CASE
				    WHEN i.qty_refunded > 0 AND (p.method <> 'cashondelivery' OR sa.shipment_statuses = 'Complete') THEN 'True'
				    ELSE 'False'
				END AS is_refunded
        FROM sales_order_item AS i
        LEFT JOIN shipments_agg AS sa ON sa.order_item_id = i.item_id
        LEFT JOIN sales_order_payment AS p ON p.parent_id = i.order_id
        WHERE i.parent_item_id IS NULL
        """
        self.execute_sql_query(query, "Creating Sales_Refunded table")

    def create_shipping_fee_table(self):
        query = """
        DROP TABLE IF EXISTS shipping_fee;
        CREATE TABLE shipping_fee AS
        SELECT 
            soi.item_id,
            soi.order_id,
            soi.created_at,
            r.rule_id AS applied_rule_id,
            sr.name AS rule_name,
            sr.rule_cart_type,
            sr.discount_amount,
            soi.original_shipping_amount,
            soi.paid_shipping_amount,
            CASE 
                WHEN sr.rule_cart_type = 'commercial' 
                    AND (soi.paid_shipping_amount = 0 OR soi.paid_shipping_amount IS NULL) 
                    THEN soi.original_shipping_amount
                ELSE 0
            END AS commercial_shipping,
            CASE 
                WHEN sr.rule_cart_type = 'marketing' 
                    AND (soi.paid_shipping_amount = 0 OR soi.paid_shipping_amount IS NULL) 
                    THEN soi.original_shipping_amount
                ELSE 0
            END AS marketing_shipping
        FROM sales_order_item soi
        JOIN JSON_TABLE(
                CONCAT('[', soi.applied_rule_ids, ']'),
                '$[*]' COLUMNS(rule_id INT PATH '$')
            ) AS r ON TRUE
        JOIN salesrule sr ON sr.rule_id = r.rule_id
        LEFT JOIN salesrule_coupon sc ON sr.rule_id = sc.rule_id
        WHERE soi.applied_rule_ids IS NOT NULL
            AND soi.applied_rule_ids <> ''
            AND soi.parent_item_id IS NULL
            AND sr.discount_amount = 0
            AND (soi.paid_shipping_amount = 0 OR soi.paid_shipping_amount IS NULL)
            AND soi.created_at >= '2025-04-30 23:59:59'
        ORDER BY soi.item_id DESC;
        """
        self.execute_sql_query(query, "Creating shipping_fee table")

    def create_orders_adminfees_table(self):
        query = """
          DROP TABLE IF EXISTS orders_adminfees;
          CREATE TABLE orders_adminfees AS
                SELECT 
            so.entity_id,
            so.created_at ,
            so.increment_id ,
            sop.method,
            CASE 
                WHEN sop.method = 'cashondelivery' THEN so.paymentfees
                WHEN sop.method = 'ocean_NBEGate_installments' THEN so.nbeinstallmentfees
                WHEN sop.method = 'ocean_MisrGate_installments' THEN so.misrinstallmentfees
                WHEN sop.method = 'ocean_QNBGate_installments' THEN so.qnbinstallmentfees
                WHEN sop.method in ( 'aman', 'souhoola', 'premium', 'contact' ,'forsa','valu')THEN COALESCE(so.amount_to_collect, 0)
                ELSE 0
                END AS Paymentfees
            FROM 
                sales_order so
            JOIN 
                sales_order_payment sop ON so.entity_id = sop.parent_id;
            """
        self.execute_sql_query(query, "Creating orders_adminfees table")

    def create_order_total_table(self):
        query = """
          DROP TABLE IF EXISTS order_total;
          CREATE TABLE order_total AS
                SELECT 
                  o.entity_id,
                  CASE 
                      WHEN p.method <> 'Free' 
                          THEN (COALESCE(o.base_grand_total, 0) + COALESCE(o.base_customer_balance_amount, 0))
                      WHEN p.method = 'Free' 
                          THEN COALESCE(o.base_customer_balance_amount, 0)
                      ELSE 0
                  END AS order_total
              FROM sales_order AS o
              LEFT JOIN sales_order_payment AS p 
                ON o.entity_id = p.parent_id;
            """
        self.execute_sql_query(query, "Creating order_total table")

    def create_customer_balance_table(self):
        query = """
          DROP TABLE IF EXISTS customer_balance;
          CREATE TABLE customer_balance AS
              SELECT 
              mc.customer_id AS user_id,
              mc.amount AS balance_amount,
              CONCAT(ce.firstname," ",ce.lastname) AS customer_name,
              ce.email AS user_email, 
              ce.created_in AS Website
              FROM magento_customerbalance mc
              LEFT JOIN customer_entity ce ON mc.customer_id = ce.entity_id
              WHERE mc.amount > 0
            """
        self.execute_sql_query(query, "Creating customer_balance table")
    def create_RMA_table(self):
        query = """
          DROP TABLE IF EXISTS RMA;
          CREATE TABLE RMA AS
            SELECT 
                ssi.order_item_id AS 'Order_Item_ID',
                rma.qty_approved AS 'QTY_Returned',
                mr.date_requested AS 'RMA_Created_At',
                rma.status AS 'Item_Status',
                mr.status AS 'RMA_Status',
                COALESCE(option_value.value, '') AS 'Return_Reason'
            FROM 
                sales_shipment_item AS ssi
            LEFT JOIN 
                sales_shipment ss ON ssi.parent_id = ss.entity_id
            LEFT JOIN 
                magento_rma_item_entity rma ON ssi.order_item_id = rma.order_item_id
            LEFT JOIN 
                magento_rma mr ON rma.rma_entity_id = mr.entity_id
            LEFT JOIN 
                magento_rma_item_entity_int mrei_reason ON rma.entity_id = mrei_reason.entity_id 
                AND mrei_reason.attribute_id = 1470
            LEFT JOIN 
                eav_attribute_option_value AS option_value ON mrei_reason.value = option_value.option_id 
                AND option_value.store_id = 0
            WHERE
                rma.status = "approved"
            ORDER BY 
                ss.created_at DESC;
            """
        self.execute_sql_query(query, "Creating RMA table")

    def create_orders_items_denorm_table(self):
        query = """
          DROP TABLE IF EXISTS orders_items_denorm;
          CREATE TABLE orders_items_denorm AS
          WITH joined_data AS (
              SELECT
                o.increment_id AS order_number,
                DATE_ADD(o.created_at, INTERVAL 2 HOUR) AS transaction_date,
                o.customer_id AS user_id,
                CONCAT(o.customer_firstname, ' ', o.customer_lastname) AS customer_name,
                o.customer_email AS user_email,

                sh.order_status,
                sh.status_changed_at,
                o.status AS current_status,
                DATE_ADD(o.updated_at, INTERVAL 2 HOUR) AS updated_at,
                o.store_id AS site_language,
                o.coupon_code AS promo_code,
                salesrule.rule_cart_type AS coupon_type,
                o.base_currency_code AS currency,
                ot.order_total AS order_total,
                o.base_discount_amount AS discount_amount,
                o.base_shipping_amount AS shipping_fee,
                o.base_tax_amount AS tax_amount,
                oaf.Paymentfees AS order_fees,
                o.checkout_from AS checkout_from,

                i.item_id,
                i.sku,
                i.name AS product_name,
                i.product_id,
                i.qty_ordered AS quantity,
                i.qty_refunded AS quantity_refunded,
                i.qty_shipped AS quantity_shipped,
                i.base_price AS unit_price,
                i.base_row_total AS total_price,
                i.base_discount_amount AS item_discount,
                i.marketing_discount AS Marketing_Discount, 
                i.commercial_discount AS Commercial_Discount,
                i.marketplace_commission AS marketplace_commission_percentage,
                ROUND(i.base_row_total * i.marketplace_commission / 100, 2) AS marketplace_commission_amount,
                soic.margin AS retail_margin,
                (soic.margin - i.commercial_discount) AS retail_margin_after_discount,
                i.base_tax_amount AS item_tax,
                i.original_shipping_amount AS original_shipping_fee,
                i.paid_shipping_amount,
                s.is_refunded AS is_refunded,
                 osfo.commercial_shipping AS commercial_shipping_fee,
                 osfo.marketing_shipping AS marketing_shipping_fee,

                brand.value AS brand_name,
                color.value AS variant_id,
                CASE WHEN i.marketplace_seller IS NULL THEN 'raneen' ELSE i.marketplace_seller END AS seller_name,

                pc.category_path AS product_category_name,
                i.main_category,
                i.sub_category,

                p.method AS payment_method,

                addr.city AS area,
                addr.region AS city,

                sa.shipment_ids,
                sa.courier_names,
                sa.shipment_statuses,
                rma.QTY_Returned AS quantity_returned,
                rma.RMA_Created_At AS rma_created_at,
                rma.`Item_Status` AS rma_item_status,
                rma.`RMA_Status` AS rma_status,
                rma.Return_Reason AS rma_return_reason,
                ROW_NUMBER() OVER (PARTITION BY i.item_id ORDER BY sh.status_changed_at DESC) AS rn
              FROM sales_order o
              LEFT JOIN salesrule_coupon ON o.coupon_code = salesrule_coupon.code
              LEFT JOIN salesrule ON salesrule_coupon.rule_id = salesrule.rule_id
              JOIN order_total ot ON o.entity_id = ot.entity_id
              LEFT JOIN orders_adminfees oaf ON o.entity_id = oaf.entity_id
              INNER JOIN sales_order_item i ON o.entity_id = i.order_id
              LEFT JOIN RMA rma ON i.item_id = rma.Order_Item_ID
              LEFT JOIN shipping_fee osfo ON i.item_id = osfo.item_id
              LEFT JOIN sales_order_item_cost soic ON i.item_id = soic.item_id
              LEFT JOIN (
                SELECT parent_id,
                      GROUP_CONCAT(DISTINCT status ORDER BY created_at SEPARATOR ' | ') AS order_status,
                      MAX(DATE_ADD(created_at, INTERVAL 3 HOUR)) AS status_changed_at
                FROM sales_order_status_history
                GROUP BY parent_id
              ) sh ON sh.parent_id = o.entity_id
              LEFT JOIN sales_order_payment p ON o.entity_id = p.parent_id
              LEFT JOIN sales_order_address addr
                ON o.entity_id = addr.parent_id AND addr.address_type = 'shipping'
              LEFT JOIN catalog_product_entity cpe ON cpe.entity_id = i.product_id
              LEFT JOIN eav_attribute brand_attr_def
                ON brand_attr_def.attribute_code = 'brand' AND brand_attr_def.entity_type_id = 4
              LEFT JOIN catalog_product_entity_int brand_attr
                ON brand_attr.row_id = cpe.row_id AND brand_attr.attribute_id = brand_attr_def.attribute_id
              LEFT JOIN eav_attribute_option_value brand
                ON brand.option_id = brand_attr.value AND brand.store_id = 0
              LEFT JOIN eav_attribute color_attr_def
                ON color_attr_def.attribute_code = 'color' AND color_attr_def.entity_type_id = 4
              LEFT JOIN catalog_product_entity_int color_attr
                ON color_attr.row_id = cpe.row_id AND color_attr.attribute_id = color_attr_def.attribute_id
              LEFT JOIN eav_attribute_option_value color
                ON color.option_id = color_attr.value AND color.store_id = 0
              LEFT JOIN product_categories pc ON pc.product_id = i.product_id
              LEFT JOIN shipments_agg sa ON sa.order_item_id = i.item_id
              LEFT JOIN Sales_Refunded s ON i.item_id = s.order_item_id
              WHERE i.parent_item_id IS NULL
          )
          SELECT *
          FROM joined_data
          WHERE rn = 1
          ORDER BY item_id;
        """
        self.execute_sql_query(query, "Creating orders_items_denorm table")
        
    def send_telegram_message(self, message):
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.telegram_chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        try:
            import requests
            response = requests.post(url, data=payload)
            if response.status_code == 200:
                self.logger.info("📨 Telegram message sent.")
            else:
                self.logger.error(f"❌ Telegram send failed: {response.text}")
        except Exception as e:
            self.logger.error(f"❌ Telegram error: {e}")

    def run(self):
        try:
            self.logger.info("🚀 Starting automation...")
            self.check_dependencies()
            self.setup_ssh_tunnel()
            self.dump_database()
            self.import_to_gcp()
            self.create_product_categories_table()
            self.create_shipments_agg_table()
            self.create_sales_order_item_cost_table()
            self.create_Sales_Refunded_table()
            self.create_shipping_fee_table()
            self.create_orders_adminfees_table()
            self.create_order_total_table()
            self.create_customer_balance_table()
            self.create_RMA_table()
            self.create_orders_items_denorm_table()
            message = f"✅ Sales transaction data transfer completed successfully at {self.now}.Dump file: {self.dump_file}"
            self.send_telegram_message(message)
            self.logger.info(f"🎉 Done. Dump at: {self.dump_file}")
        except Exception as e:
            self.logger.error(f"❌ Script failed: {e}")
            self.send_telegram_message(f"❌ Sales transaction data Transfer Failed: {e}")
            sys.exit(1)
        finally:
            self.cleanup()

def main():
    DatabaseTransfer().run()

if __name__ == "__main__":
    main()