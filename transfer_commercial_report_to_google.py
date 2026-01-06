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
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN2")
        self.telegram_chat_id = int(os.getenv("TELEGRAM_CHAT_ID2"))

        # ===== SSH =====
        self.ssh_host = os.getenv("SSH_HOST")
        self.local_port = int(os.getenv("LOCAL_PORT", 30000))
        self.ssh_process = None

        # ===== Remote Magento DB =====
        self.remote_db_user = os.getenv("REMOTE_DB_USER")
        self.remote_db_pass = os.getenv("REMOTE_DB_PASSWORD")
        self.remote_db_name = os.getenv("REMOTE_DB_NAME")

        # ===== Tables =====
        self.tables = os.getenv("TABLES2").split(",")

        # ===== Validation =====
        required_vars = [
            "PROJECT", "ENVIRONMENT", "INSTANCE_CONNECTION",
            "GCP_DB_IP", "GCP_DB_NAME", "GCP_DB_USER", "GCP_DB_PASSWORD",
            "TELEGRAM_BOT_TOKEN2", "TELEGRAM_CHAT_ID2",
            "SSH_HOST",
            "REMOTE_DB_USER", "REMOTE_DB_PASSWORD", "REMOTE_DB_NAME",
            "TABLES2"
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

    def create_vw_product_base_table(self):
        query = """
          DROP TABLE IF EXISTS vw_product_base;
        CREATE TABLE vw_product_base AS
            SELECT 
                entity_id,
                sku,
                type_id,
                attribute_set_id,
                created_at,
                updated_at,
                row_id
            FROM catalog_product_entity;
        """
        self.execute_sql_query(query, "Creating vw_product_base table")

    def create_vw_product_attributes_varchar_table(self):
        query = """
          DROP TABLE IF EXISTS vw_product_attributes_varchar;
        CREATE TABLE vw_product_attributes_varchar AS
        SELECT 
            cpv.row_id,
            MAX(CASE WHEN attribute_id = 73 THEN value END) AS product_name,
            MAX(CASE WHEN attribute_id = 87 THEN value END) AS image,
            MAX(CASE WHEN attribute_id = 864 THEN value END) AS barcode,
            MAX(CASE WHEN attribute_id = 959 THEN value END) AS other_language_name
        FROM catalog_product_entity_varchar cpv
        WHERE cpv.store_id = 0
        GROUP BY cpv.row_id;
        """
        self.execute_sql_query(query, "Creating vw_product_attributes_varchar table")

    def create_vw_product_attributes_decimal_table(self):
        query = """
         DROP TABLE IF EXISTS vw_product_attributes_decimal;
         CREATE TABLE vw_product_attributes_decimal AS
         SELECT 
            cpd.row_id,
            MAX(CASE WHEN attribute_id = 77 THEN value END) AS price,
            MAX(CASE WHEN attribute_id = 81 THEN value END) AS cost,
            MAX(CASE WHEN attribute_id = 82 THEN value END) AS weight
         FROM catalog_product_entity_decimal cpd
        WHERE cpd.store_id = 0
         GROUP BY cpd.row_id;
        """
        self.execute_sql_query(query, "Creating vw_product_attributes_decimal table")

    def create_vw_product_attributes_int_options_table(self):
        query = """
          DROP TABLE IF EXISTS vw_product_attributes_int_options;
        CREATE TABLE vw_product_attributes_int_options AS
        SELECT 
            cpi.row_id,
            MAX(CASE WHEN cpi.attribute_id = 97 THEN 
                CASE cpi.value WHEN 1 THEN 'Enabled' WHEN 2 THEN 'Disabled' ELSE 'Unknown' END END) AS status,
            MAX(CASE WHEN cpi.attribute_id = 99 THEN 
                CASE cpi.value 
                    WHEN 1 THEN 'Not Visible Individually'
                    WHEN 2 THEN 'Catalog'
                    WHEN 3 THEN 'Search'
                    WHEN 4 THEN 'Catalog, Search'
                    ELSE 'Unknown'
                END END) AS visibility,
            MAX(CASE WHEN cpi.attribute_id = 139 THEN o1.value END) AS brand,
            MAX(CASE WHEN cpi.attribute_id = 900 THEN o2.value END) AS warranty,
            MAX(CASE WHEN cpi.attribute_id = 1009 THEN o3.value END) AS sold_by
        FROM catalog_product_entity_int cpi
        LEFT JOIN eav_attribute_option_value o1 ON o1.option_id = cpi.value AND cpi.attribute_id = 139 AND o1.store_id = 0
        LEFT JOIN eav_attribute_option_value o2 ON o2.option_id = cpi.value AND cpi.attribute_id = 900 AND o2.store_id = 0
        LEFT JOIN eav_attribute_option_value o3 ON o3.option_id = cpi.value AND cpi.attribute_id = 1009 AND o3.store_id = 0
        WHERE cpi.store_id = 0
        GROUP BY cpi.row_id;
        """
        self.execute_sql_query(query, "Creating vw_product_attributes_int_options table") 

    def create_vw_product_special_prices_table(self):
        query = """
      DROP TABLE IF EXISTS vw_product_special_prices;
      CREATE TABLE vw_product_special_prices AS
      SELECT 
          base.row_id,
          MAX(CASE WHEN base.attribute_id = 78 THEN base.value END) AS special_price,
          MAX(CASE WHEN base.attribute_id = 79 THEN base.value END) AS special_from,
          MAX(CASE WHEN base.attribute_id = 80 THEN base.value END) AS special_to
      FROM (
          SELECT row_id, attribute_id, value, store_id FROM catalog_product_entity_decimal WHERE attribute_id = 78
          UNION ALL
          SELECT row_id, attribute_id, value, store_id FROM catalog_product_entity_datetime WHERE attribute_id IN (79, 80)
      ) AS base
      WHERE base.store_id = 0
      GROUP BY base.row_id;
        """
        self.execute_sql_query(query, "Creating vw_product_special_prices table")

    def create_vw_attribute_sets_table(self):
        query = """
          DROP TABLE IF EXISTS vw_attribute_sets;
        CREATE TABLE vw_attribute_sets AS
        SELECT attribute_set_id, attribute_set_name
        FROM eav_attribute_set;
        """
        self.execute_sql_query(query, "Creating vw_attribute_sets table")

    def create_vw_inventory_stock_table(self):
        query = """
          DROP TABLE IF EXISTS vw_inventory_stock;
        CREATE TABLE vw_inventory_stock AS
        SELECT sku,
       MAX(CASE WHEN stock_id = 2 THEN quantity END) AS online_qty,
       MAX(CASE WHEN stock_id = 2 THEN is_salable END) AS is_salable_online,
       MAX(CASE WHEN stock_id = 6 THEN quantity END) AS smart_qty,
       MAX(CASE WHEN stock_id = 6 THEN is_salable END) AS is_salable_smart
        FROM (
            SELECT sku, quantity, is_salable, 2 AS stock_id FROM inventory_stock_2
            UNION ALL
            SELECT sku, quantity, is_salable, 6 AS stock_id FROM inventory_stock_6
        ) AS all_stock
        GROUP BY sku;
        """
        self.execute_sql_query(query, "Creating vw_inventory_stock table")

    def create_vw_product_urls_table(self):
        query = """
          DROP TABLE IF EXISTS vw_product_urls;
        CREATE TABLE vw_product_urls AS
        SELECT entity_id, request_path
        FROM url_rewrite
        WHERE entity_type = 'product';
                """
        self.execute_sql_query(query, "Creating vw_product_urls table")

    def create_vw_product_table(self):
        query = """
      DROP TABLE IF EXISTS vw_product;
      
      CREATE TABLE vw_product (
          Id INT,
          SKU VARCHAR(255),
          Type VARCHAR(50),
          Barcode VARCHAR(255),
          `Attribute Set` VARCHAR(255),
          Brand VARCHAR(255),
          Warranty VARCHAR(100),
          `Product Name` TEXT,
          `Other Language Name` TEXT,
          `Sold By` VARCHAR(255),
          Weight VARCHAR(100),
          Status VARCHAR(100),
          Visibility VARCHAR(100),
          Price DECIMAL(25,2),
          special_price DECIMAL(25,2),
          special_from DATETIME,
          special_to DATETIME,
          cost DECIMAL(15,2),
          `Qty` DECIMAL(10,2),
          `Online Qty` DECIMAL(10,2),
          `is salable online` BOOLEAN,
          `Smart Store Qty` DECIMAL(10,2),
          `is salable smart` BOOLEAN,
          created_at DATETIME,
          updated_at DATETIME,
          Image TEXT,
          `Online Url Key` TEXT,
          `rstore Url key` TEXT
      );

      INSERT INTO vw_product (
          Id, SKU, Type, Barcode, `Attribute Set`, Brand, Warranty,
          `Product Name`, `Other Language Name`, `Sold By`, Weight,
          Status, Visibility, Price, special_price, special_from,
          special_to, cost, `Qty`,`Online Qty`, `is salable online`,
          `Smart Store Qty`, `is salable smart`, created_at, updated_at,
          Image, `Online Url Key`, `rstore Url key`
      )
      WITH ranked_products AS (
          SELECT 
              pb.entity_id AS Id,
              pb.sku AS SKU,
              pb.type_id AS Type,
              COALESCE(pav.barcode, '') AS Barcode,
              eas.attribute_set_name AS `Attribute Set`,
              COALESCE(pai.brand, '') AS Brand,
              COALESCE(pai.warranty, '') AS Warranty,
              pav.product_name AS `Product Name`,
              COALESCE(pav.other_language_name, '') AS `Other Language Name`,
              COALESCE(pai.sold_by, '') AS `Sold By`,
              COALESCE(pad.weight, '') AS `Weight`,
              pai.status AS `Status`,
              pai.visibility AS `Visibility`,
              pad.price AS `Price`,
              psp.special_price,
              psp.special_from,
              psp.special_to,
              pad.cost,
              ist.online_qty AS `Qty`,
              ist.online_qty AS `Online Qty`,
              ist.is_salable_online AS `is salable online`,
              ist.smart_qty AS `Smart Store Qty`,
              ist.is_salable_smart AS `is salable smart`,
              pb.created_at,
              pb.updated_at,
              CONCAT('https://www.raneen.com/media/catalog/product', pav.image) AS `Image`,
              CONCAT('https://www.raneen.com/ar/', pu.request_path) AS `Online Url Key`,
              CONCAT('https://rstore.raneen.com/ss_zayed/', pu.request_path) AS `rstore Url key`,
              ROW_NUMBER() OVER (PARTITION BY pb.entity_id ORDER BY pb.updated_at DESC) AS rn
          FROM vw_product_base pb
          LEFT JOIN vw_product_attributes_varchar pav ON pb.row_id = pav.row_id
          LEFT JOIN vw_product_attributes_decimal pad ON pb.row_id = pad.row_id
          LEFT JOIN vw_product_attributes_int_options pai ON pb.row_id = pai.row_id
          LEFT JOIN vw_product_special_prices psp ON pb.row_id = psp.row_id
          LEFT JOIN vw_inventory_stock ist ON pb.sku = ist.sku
          LEFT JOIN vw_attribute_sets eas ON pb.attribute_set_id = eas.attribute_set_id
          LEFT JOIN vw_product_urls pu ON pb.entity_id = pu.entity_id
      )
      SELECT 
          Id, SKU, Type, Barcode, `Attribute Set`, Brand, Warranty,
          `Product Name`, `Other Language Name`, `Sold By`, Weight,
          Status, Visibility, Price, special_price, special_from,
          special_to, cost, `Qty`, `Online Qty`, `is salable online`,
          `Smart Store Qty`, `is salable smart`, created_at, updated_at,
          Image, `Online Url Key`, `rstore Url key`
      FROM ranked_products
      WHERE rn = 1
      ORDER BY Id;
            """
        self.execute_sql_query(query, "Creating vw_product table")
    def export_vw_product_to_csv(self, chunk_size=50000):
        self.logger.info("📤 Exporting vw_product to CSV (chunked)...")

        csv_file = self.dump_dir / f"Commercial_Report_{self.now}.csv"

        conn = mysql.connector.connect(
            host=self.gcp_db_ip,
            user=self.gcp_db_user,
            password=self.gcp_db_pass,
            database=self.gcp_db_name
        )
        cursor = conn.cursor(dictionary=True)

        query = "SELECT * FROM vw_product ORDER BY Id"
        cursor.execute(query)

        first_chunk = True
        rows_written = 0

        with open(csv_file, "w", encoding="utf-8", newline="") as f:
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break

                df = pd.DataFrame(rows)
                df.to_csv(
                    f,
                    index=False,
                    header=first_chunk,
                    mode="a"
                )
                rows_written += len(df)
                first_chunk = False

                self.logger.info(f"🧱 Exported {rows_written:,} rows...")

        cursor.close()
        conn.close()

        self.logger.info(f"✅ CSV saved safely at {csv_file}")
        return csv_file


    def send_telegram_file(self, file_path):
        self.logger.info("📦 Compressing CSV for Telegram...")

        zip_path = str(file_path) + ".zip"
        try:
            # ضغط الملف
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(file_path, arcname=file_path.name)

            # إرسال الملف المضغوط
            self.logger.info("📦 Sending ZIP to Telegram...")
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendDocument"
            with open(zip_path, 'rb') as file:
                data = {
                    "chat_id": self.telegram_chat_id
                }
                files = {
                    "document": file
                }
                response = requests.post(url, data=data, files=files, timeout=120)
                if response.status_code == 200:
                    self.logger.info("✅ ZIP file sent to Telegram.")
                else:
                    self.logger.error(f"❌ Telegram ZIP send failed: {response.text}")
        except Exception as e:
            self.logger.error(f"❌ Telegram ZIP error: {e}")

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
            self.create_vw_product_base_table()
            self.create_vw_product_attributes_varchar_table()
            self.create_vw_product_attributes_decimal_table()
            self.create_vw_product_attributes_int_options_table()
            self.create_vw_product_special_prices_table()
            self.create_vw_attribute_sets_table()
            self.create_vw_inventory_stock_table()
            self.create_vw_product_urls_table()
            self.create_vw_product_table()
            message = f"✅ Product table data transfer completed successfully at {self.now}.Dump file: {self.dump_file}"
            self.send_telegram_message(message)
            self.logger.info(f"🎉 Done. Dump at: {self.dump_file}")
            csv_file = self.export_vw_product_to_csv()
            self.send_telegram_file(csv_file)
        except Exception as e:
            self.logger.error(f"❌ Script failed: {e}")
            self.send_telegram_message(f"❌ Product table  data Transfer Failed: {e}")
            sys.exit(1)
        finally:
            self.cleanup()

def main():
    DatabaseTransfer().run()

if __name__ == "__main__":
    main()