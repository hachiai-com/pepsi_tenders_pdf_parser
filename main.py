#!/usr/bin/env python3

import os
import re
import json
import sys
from concurrent.futures import ThreadPoolExecutor
import pdfplumber

try:
    import mysql.connector
except ImportError:
    mysql = None


CAPABILITY_NAME = "la_pepsi_tenders_pdf_parser"
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
TABLE_NAME = "la_enhanced_shipment_creation_raw"
DB_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MYSQL_PORT", "3307")),
    "user": os.environ.get("MYSQL_USER", "neehar"),
    "password": os.environ.get("MYSQL_PASSWORD", "X2fjRge7VtjDzETc"),
    "database": os.environ.get("MYSQL_DATABASE", "logistics_alliance"),
}
TABLE_COLUMNS_WHITELIST = {
    "vendorname", "po", "vendorRef", "vendorno", "shipto", "consignee", "customer",
    "vendorLoadAt", "delDate", "lifts", "pickApptNo", "weight", "pallets", "cases",
    "cubes", "invoiceRef", "temp", "pickupDate", "template_flag", "description",
    "ship_from", "shipment_type", "item_id", "monday_group_name", "email_from",
    "filename", "audit_runs", "audit_status",
}


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def insert_results_to_db(results: list) -> tuple:
    """
    Insert parsed results into la_enhanced_shipment_creation_raw.
    Returns (rows_inserted, error_message). error_message is None on success.
    Skips any item that has an "error" key (e.g. PDF not found).
    """
    if mysql is None:
        return 0, "mysql-connector-python not installed; run: pip install mysql-connector-python"
    rows_inserted = 0
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        for row in results:
            if not isinstance(row, dict) or "error" in row:
                continue
            cols = [k for k in row.keys() if k in TABLE_COLUMNS_WHITELIST]
            if not cols:
                continue
            placeholders = ", ".join(["%s"] * len(cols))
            columns = ", ".join(f"`{c}`" for c in cols)
            sql = f"INSERT INTO `{TABLE_NAME}` ({columns}) VALUES ({placeholders})"
            values = [row.get(c) if row.get(c) != "" else None for c in cols]
            cursor.execute(sql, values)
            rows_inserted += 1
        conn.commit()
        cursor.close()
        conn.close()
        return rows_inserted, None
    except Exception as e:
        return rows_inserted, str(e)


def extract_pdf_text(pdf_path: str) -> str:
    """
    Extract text using pdfplumber (layout-aware, line-preserving)
    """
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text(x_tolerance=2, y_tolerance=2)
            if page_text:
                text += page_text + "\n"
    return text


# -------------------------------------------------
# Pepsi-specific regex
# -------------------------------------------------

LOCATION_ID_REGEX = r"Location ID:\s*(\d+)"
DELIVERY_DATE_REGEX = r"DELIVERY\n(\d{1,2}\/\d{1,2}\/\d{2})"
DELIVERY_DATE_ALT_REGEX = r"DELIVERY\n(\d{1,2}\/\d{1,2}\/\d{2})"
PICKUP_DATE_REGEX = r"PICKUP\n(\d{1,2}/\d{1,2}/\d{2})"
PICKUP_DATE_ALT_REGEX = r"PICKUP\n(\d{1,}/\d{1,}/\d{2})"
LOAD_NUMBER_REGEX = r"Load Number:(.*)"
TEMP_REGEX = r"Item Desc\.PU Number D Number Apt IDSAP Order#(.*)Pallets"
TEMP_CLEANUP_REGEX = r"(?:(?:[0-9A-Z-]{5,})\s+(?:[0-9A-Z-]{3,})\s+(?:[0-9A-Z\-]+)\s+([0-9A-Z,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)|([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+))"
ALT_TEMP_CLEANUP_REGEX = r"(?:(?:[0-9A-Z-]{5,})\s+(?:[0-9A-Z-]{3,})\s+(?:[0-9A-Z\-]+)\s+([0-9A-Z,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+))"
ALTERNATE_TEMP_REGEX = r"(OMS.*|OTHERS.*)\s+.*Page\s\d+ of \d+"
SHIP_TO_REGEX = r"Location Name:\s+(.*)ARRIVE"
# REGEX = r"^([0-9A-Z-]{5,})\s+([0-9A-Z-]{5,})\s+([0-9]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)$"
REGEX = r"^([0-9A-Z-]{5,})\s+([0-9A-Z-]{3,})\s+([0-9A-Z\-]+)\s+(?:([A-Z]{3})\s+)?([0-9A-Z,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)$"

STOP_ONE_ADDRESS_REGEX = (
    r"DC Milton ON DWD\s+Address:\s+1890 READING COURT\s+Appointment Info\s+MILTON, ON L9T2X8"
)

HEADER_SPLIT = "PU Number Item Desc. PepsiCo Order# SAP Order# D Number PO Number Apt ID Pieces Pallets Weight Volume"


def get_location_id(text: str) -> str:
    matches = re.findall(LOCATION_ID_REGEX, text)
    if len(matches) >= 2:
        return matches[1].lstrip("0")
    return ""


def get_cases_and_order_no(value: str) -> dict:
    cases = ""
    order_no = ""

    m = re.search(r"([0-9,]+)[A-Z]", value)
    if m:
        cases = m.group(1)
        order_no = value.replace(cases, "")
    elif len(value) > 9:
        order_no = value[-9:]
        cases = value[:-9]

    return {
        "cases": cases.strip(),
        "orderNo": order_no.strip()
    }


# -------------------------------------------------
# Core Parser
# -------------------------------------------------

class LAShipmentCreationPdfParser:

    def __init__(self, pdf_path: str):
        self.config = load_json(CONFIG_PATH)
        self.pdf_path = pdf_path
        self.format = self.config["format"]
        self.column_count = self.config["columnCount"]

    def process(self):
        if os.path.isdir(self.pdf_path):
            return self._process_directory()
        return [self._process_pdf(self.pdf_path)]

    def _process_directory(self):
        pdfs = [
            os.path.join(self.pdf_path, f)
            for f in os.listdir(self.pdf_path)
            if f.lower().endswith(".pdf")
        ]

        results = []
        with ThreadPoolExecutor(max_workers=1) as executor:
            for result in executor.map(self._process_pdf, pdfs):
                # Flatten: result is a list, extend instead of append
                results.extend(result)

        return results

    def _process_pdf(self, pdf_path: str) -> list:
        if not os.path.exists(pdf_path):
            return [{"error": "PDF not found"}]

        full_text = extract_pdf_text(pdf_path)

        # Print full PDF text for debugging
        # print(f"\n{'='*80}", file=sys.stderr)
        # print(f"Full PDF text from: {os.path.basename(pdf_path)}", file=sys.stderr)
        # print(f"{'='*80}", file=sys.stderr)
        # print(full_text, file=sys.stderr)
        # print(f"{'='*80}\n", file=sys.stderr)

        # --------------------------------
        # Header-level extraction (common to all records)
        # --------------------------------
        header_data = {
            "vendorname": "Pepsi Co Tender",
            "pickupDate": self._extract_pickup_date(full_text),
            "deliveryDate": self._extract_delivery_date(full_text),
            "invoiceRef": self._extract_invoice_ref(full_text),
            "shipTo": self._extract_ship_to(full_text),
            "ship_from": self._extract_ship_from(full_text),
            "temp": self._extract_temp(full_text),
            "locationId": get_location_id(full_text),
            "stopId": "21200Y" if re.search(STOP_ONE_ADDRESS_REGEX, full_text, re.I) else "",
            "filename": os.path.basename(pdf_path)
        }

        # --------------------------------
        # Line items - collect all matches
        # --------------------------------
        records = []
        parts = full_text.split(HEADER_SPLIT)

        # Debug: print parts length
        # print(f"full_text - parts length: {len(parts)}", file=sys.stderr)

        if len(parts) > 1:
            for line in parts[1].splitlines():
                # Debug: print line
                # print(f"line : {line}", file=sys.stderr)
                m = re.match(REGEX, line.strip())
                if not m:
                    continue
                # print(f"line matched: {line}", file=sys.stderr)
                # Create a new record for each line item
                record = [""] * self.column_count

                # Set header data for all records
                record[0] = header_data["vendorname"]
                record[1] = header_data["pickupDate"]
                record[2] = header_data["deliveryDate"]
                record[8] = header_data["shipTo"]
                record[9] = header_data["temp"]
                record[10] = header_data["invoiceRef"]
                record[12] = header_data["locationId"]
                record[13] = header_data["stopId"]
                record[14] = header_data["filename"]
                record[15] = header_data["ship_from"]

                # Set line item data
                record[3] = m.group(3)              # po
                record[4] = m.group(5)              # cases
                record[5] = m.group(6)              # pallets
                record[6] = m.group(7)              # weight
                record[7] = m.group(8)              # cubes
                record[11] = m.group(1)             # orderNo

                # Convert to SQL column mapping
                sql_mapping = {}
                for field in self.format:
                    idx = field.get("id")
                    if idx:
                        sql_mapping[field["sql_column_name"]] = record[idx - 1]

                records.append(sql_mapping)

        # Debug: total items
        # print(f"total items : {len(records)}", file=sys.stderr)

        # if len(records) == 0:
            # Print full PDF text for debugging
            # print(f"\n{'='*80}", file=sys.stderr)
            # print(f"Full PDF text from: {os.path.basename(pdf_path)}", file=sys.stderr)
            # print(f"{'='*80}", file=sys.stderr)
            # print(full_text, file=sys.stderr)
            # print(f"{'='*80}\n", file=sys.stderr)

        return records

    # --------------------------------
    # Extraction helpers
    # --------------------------------

    def _extract_pickup_date(self, text: str) -> str:
        m = re.search(PICKUP_DATE_REGEX, text, re.M)
        if m:
            return m.group(1)
        parts = text.split("Item Desc.PU Number D Number Apt IDSAP Order#")
        if len(parts) > 1:
            m = re.search(PICKUP_DATE_ALT_REGEX, parts[1], re.M)
            if m:
                return m.group(1)
        return ""

    def _extract_delivery_date(self, text: str) -> str:
        m = re.search(DELIVERY_DATE_REGEX, text, re.M)
        if m:
            return m.group(1)
        parts = text.split("Item Desc.PU Number D Number Apt IDSAP Order#")
        if len(parts) > 1:
            m = re.search(DELIVERY_DATE_ALT_REGEX, parts[1], re.M)
            if m:
                return m.group(1)
        return ""

    def _extract_invoice_ref(self, text: str) -> str:
        m = re.search(LOAD_NUMBER_REGEX, text)
        return m.group(1).strip() if m else ""

    def _extract_temp(self, text: str) -> str:
        temp = ""
        m = re.search(TEMP_REGEX, text, re.S)
        if m:
            temp = m.group(1).strip()
        
        m = re.search(ALTERNATE_TEMP_REGEX, text, re.S)
        if m:
            temp = m.group(1).strip()

        m = re.search(ALTERNATE_TEMP_REGEX, temp, re.S)
        if m:
            temp = m.group(1).strip()
        
        # Debug: print parts length
        # print(f"temp before cleanup: {temp}", file=sys.stderr)

        # pattern = re.compile(TEMP_CLEANUP_REGEX)
        # temp = re.sub(pattern, '', temp).rstrip()


        TRUNCATE_REGEX = re.compile(
            rf'^{TEMP_CLEANUP_REGEX}[\s\S]*$',
            re.MULTILINE
            )
        temp = re.sub(TRUNCATE_REGEX, '', temp).rstrip()
        # pattern = re.compile(ALT_TEMP_CLEANUP_REGEX)
        # temp = re.sub(pattern, '', temp).rstrip()

        # Debug: print parts length
        # print(f"{temp}", file=sys.stderr)

        return temp

    def _extract_ship_to(self, text: str) -> str:
        parts = text.split(HEADER_SPLIT)

        # Debug: print parts length
        # print(f"DEBUG: SHIP_TO extraction - parts length: {len(parts)}", file=sys.stderr)

        if len(parts) > 2:
            m = re.search(SHIP_TO_REGEX, parts[1], re.S | re.M)
            if m:
                return m.group(1).replace("Address: ", "").replace("Appointment Info\n", "").strip()
            # if m:
            #     return re.sub(
            #         r"(?m)^.*(Address:|Stop Type:|Stop Number:).*\n?",
            #         "",
            #         m.group(1)
            #     ).strip()
        return ""
    
    def _extract_ship_from(self, text: str) -> str:
        
        parts = text.split(HEADER_SPLIT)
        # Debug: print parts length
        # print(f"DEBUG: SHIP_FROM extraction - parts length: {len(parts)}", file=sys.stderr)
        if len(parts) > 1:
            m = re.search(SHIP_TO_REGEX, parts[0], re.S | re.M)
            if m:
                return m.group(1).replace("Address: ", "").replace("Appointment Info\n", "").strip()
        return ""


# -------------------------------------------------
# Capability Function
# -------------------------------------------------

def la_pepsi_tenders_pdf_parser(pdf_path: str) -> dict:
    try:
        if not pdf_path:
            raise ValueError("pdf_path is required")

        parser = LAShipmentCreationPdfParser(pdf_path)
        result = parser.process()

        return {
            "result": result,
            "capability": CAPABILITY_NAME
        }

    except Exception as e:
        return {
            "error": str(e),
            "capability": CAPABILITY_NAME
        }


# -------------------------------------------------
# Main (stdin / stdout)
# -------------------------------------------------

def main():
    try:
        input_data = json.load(sys.stdin)

        capability = input_data.get("capability")
        args = input_data.get("args", {})

        if capability == CAPABILITY_NAME:
            response = la_pepsi_tenders_pdf_parser(
                pdf_path=args.get("pdf_path")
            )
            if "result" in response and response.get("result"):
                rows_inserted, db_error = insert_results_to_db(response["result"])
                out = {
                    "capability": CAPABILITY_NAME,
                    "rows_inserted": rows_inserted,
                    "database": DB_CONFIG["database"],
                    "table": TABLE_NAME,
                }
                if response.get("error"):
                    out["error"] = response["error"]
                if db_error:
                    out["error"] = db_error
            else:
                out = {
                    "capability": CAPABILITY_NAME,
                    "error": response.get("error", "No result from parser"),
                    "rows_inserted": 0,
                    "database": DB_CONFIG["database"],
                    "table": TABLE_NAME,
                }
            print(json.dumps(out, indent=2))
        else:
            print(json.dumps({
                "error": f"Unknown capability: {capability}",
                "capability": capability
            }, indent=2))

    except Exception as e:
        print(json.dumps({
            "capability": "unknown",
            "error": str(e),
            "rows_inserted": 0,
            "database": DB_CONFIG["database"],
            "table": TABLE_NAME,
        }, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()