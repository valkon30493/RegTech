import pyodbc
from lxml import etree
import re
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(filename='trade_activity.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Paths to your files
xml_file = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/IG/Trade Activity Report/TRUVT_DATTAR_21380017XKSVQ3LC3V75_SE-250130_001001.xml'
xsd_file = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/IG/Trade Activity Report/head.003.001.01.xsd'

# SQL Server connection details
connection_string = (
    'DRIVER={SQL Server};'
    'SERVER=AZR-WE-BI-02;'
    'DATABASE=RTS;'
    'Trusted_Connection=yes;'
)

BATCH_SIZE = 100  # Batch size for commits

# Extract Report Date from filename
match = re.search(r'SE-(\d{6})', xml_file)
report_date = datetime.strptime(match.group(1), "%y%m%d").date().isoformat() if match else None

# Load and parse the XSD file
with open(xsd_file, 'rb') as f:
    schema_root = etree.XML(f.read())
schema = etree.XMLSchema(schema_root)
parser = etree.XMLParser(schema=schema)

# Parse the XML file
try:
    tree = etree.parse(xml_file, parser)
    logging.info("XML is valid.")
except etree.XMLSchemaError as e:
    logging.error(f"XML schema validation error: {e}")
    exit(1)

root = tree.getroot()

# Define namespaces
namespaces = {
    'auth': 'urn:iso:std:iso:20022:tech:xsd:auth.030.001.03'
}


def get_text(element):
    return element.text.strip() if element is not None and element.text else None


def get_attr(element, attr_name):
    return element.get(attr_name).strip() if element is not None and element.get(attr_name) else None


def validate_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").isoformat() if date_str else None
    except ValueError:
        return None


# Connect to SQL Server
tx_details_elements = root.findall('.//auth:DerivsTradRpt/auth:TradData/auth:Rpt', namespaces)

try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    batch = []

    for tx_detail in tx_details_elements:
        action_type = next((key for key in ['New', 'Mod', 'Crrctn', 'Termntn', 'PosCmpnt', 'ValtnUpd', 'Err', 'Rvv']
                            if tx_detail.find(f'.//auth:{key}', namespaces) is not None), None)

        counterparty1 = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Id/auth:Lgl/auth:Id/auth:LEI',
                           namespaces))
        counterparty1_sector = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:FI/auth:Sctr/auth:Cd',
                           namespaces))
        counterparty1_side = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:DrctnOrSd/auth:CtrPtySd',
                           namespaces))
        counterparty1_clearing_threshold = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:FI/auth:ClrThrshld',
                           namespaces))

        counterparty2 = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:IdTp/auth:Lgl/auth:Id/auth:LEI',
                           namespaces))
        counterparty2_sector = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:FI/auth:Sctr/auth:Cd',
                           namespaces))

        broker_id = get_text(tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:Brkr/auth:LEI', namespaces))
        submitting_agent = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:SubmitgAgt/auth:LEI', namespaces))
        entity_responsible = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:NttyRspnsblForRpt/auth:LEI', namespaces))

        valuation_element = tx_detail.find('.//auth:Valtn/auth:CtrctVal/auth:Amt', namespaces)
        valuation_amount = get_text(valuation_element)
        valuation_currency = get_attr(valuation_element, 'Ccy')

        transaction_price = get_text(
            tx_detail.find('.//auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt', namespaces))
        transaction_currency = get_attr(
            tx_detail.find('.//auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt', namespaces), 'Ccy')

        reporting_timestamp = validate_date(get_text(tx_detail.find('.//auth:RptgTmStmp', namespaces)))
        execution_timestamp = validate_date(get_text(tx_detail.find('.//auth:TxData/auth:ExctnTmStmp', namespaces)))
        contract_type = get_text(tx_detail.find('.//auth:CtrctData/auth:CtrctTp', namespaces))
        asset_class = get_text(tx_detail.find('.//auth:CtrctData/auth:AsstClss', namespaces))
        product_id = get_text(tx_detail.find('.//auth:CtrctData/auth:PdctId/auth:UnqPdctIdr/auth:Id', namespaces))
        isin = get_text(tx_detail.find('.//auth:CtrctData/auth:UndrlygInstrm/auth:ISIN', namespaces))
        settlement_currency = get_text(tx_detail.find('.//auth:CtrctData/auth:SttlmCcy/auth:Ccy', namespaces))

        batch.append((report_date, action_type, counterparty1, counterparty1_sector, counterparty1_side,
                      counterparty1_clearing_threshold, counterparty2, counterparty2_sector, broker_id,
                      submitting_agent, entity_responsible, valuation_amount, valuation_currency,
                      transaction_price, transaction_currency, reporting_timestamp, execution_timestamp,
                      contract_type, asset_class, product_id, isin, settlement_currency))

        if len(batch) >= BATCH_SIZE:
            cursor.executemany('''
                    INSERT INTO UnavistaIG_Trade_Activity_Report (
                        ReportDate, ActionType, Counterparty1_LEI, Counterparty1_Sector, Counterparty1_Side,
                        Counterparty1_ClearingThreshold, Counterparty2_LEI, Counterparty2_Sector, Counterparty2_ClearingThreshold,
                        BrokerID, SubmittingAgent_LEI, EntityResponsible_LEI, ValuationAmount, ValuationCurrency,
                        TransactionPrice, TransactionCurrency, ReportingTimestamp, ExecutionTimestamp,
                        ContractType, AssetClass, ProductID, ISIN, SettlementCurrency
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', batch)
            conn.commit()
            batch = []  # Clear batch

    # Insert any remaining records in the batch
    if batch:
        cursor.executemany('''
                INSERT INTO UnavistaIG_Trade_Activity_Report (
                    ReportDate, ActionType, Counterparty1_LEI, Counterparty1_Sector, Counterparty1_Side,
                    Counterparty1_ClearingThreshold, Counterparty2_LEI, Counterparty2_Sector, Counterparty2_ClearingThreshold,
                    BrokerID, SubmittingAgent_LEI, EntityResponsible_LEI, ValuationAmount, ValuationCurrency,
                    TransactionPrice, TransactionCurrency, ReportingTimestamp, ExecutionTimestamp,
                    ContractType, AssetClass, ProductID, ISIN, SettlementCurrency
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', batch)
        conn.commit()

    logging.info("✅ Data successfully inserted into the database.")

except Exception as e:
    logging.error(f"❌ Error inserting data: {e}")
    conn.rollback()
finally:
    conn.close()
    logging.info("✅ Database connection closed.")