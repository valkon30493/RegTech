import pyodbc
from lxml import etree

# Paths to your files
XML_FILE = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Warnings Report/TRUVT_DATWRN_5493003EETVWYSIJ5A20_SE-250130_001001.xml'
XSD_FILE = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Warnings Report/head.003.001.01.xsd'

# SQL Server connection details
CONNECTION_STRING = (
    'DRIVER={SQL Server};'
    'SERVER=AZR-WE-BI-02;'
    'DATABASE=RTS;'
    'Trusted_Connection=yes;'
)

# Define namespaces
NAMESPACES = {
    'auth': 'urn:iso:std:iso:20022:tech:xsd:auth.106.001.01',
    'head': 'urn:iso:std:iso:20022:tech:xsd:head.003.001.01'
}

def validate_xml(xml_file, xsd_file):
    """Validate XML file against the XSD schema."""
    try:
        with open(xsd_file, 'rb') as f:
            schema_root = etree.XML(f.read())
        schema = etree.XMLSchema(schema_root)
        parser = etree.XMLParser(schema=schema)
        etree.parse(xml_file, parser)
        print("✅ XML is valid.")
    except etree.XMLSchemaError as e:
        raise ValueError(f"❌ XML schema validation error: {e}")

def get_text(element):
    """Safely extract text from an XML element."""
    return element.text.strip() if element is not None else None

def extract_data(xml_file):
    """Parse XML and extract relevant data."""
    tree = etree.parse(xml_file)
    root = tree.getroot()

    records = []

    # Extract `RefDt`
    ref_date = get_text(root.find('.//auth:WrnngsSttstcs/auth:Rpt/auth:RefDt', NAMESPACES))
    print(f"📌 RefDate found: {ref_date}")

    # Locate abnormal valuation reports
    abnormal_values_reports = root.findall('.//auth:WrnngsSttstcs/auth:Rpt/auth:AbnrmlVals/auth:Rpt', NAMESPACES)
    print(f"🔍 Found {len(abnormal_values_reports)} abnormal valuation reports.")

    for abnrml_vals in abnormal_values_reports:
        warnings_elements = abnrml_vals.findall('.//auth:Wrnngs', NAMESPACES)
        print(f"🔍 Found {len(warnings_elements)} <Wrnngs> elements.")

        for warnings in warnings_elements:
            reporting_counterparty_lei = get_text(warnings.find('.//auth:CtrPtyId/auth:RptgCtrPty/auth:LEI', NAMESPACES))
            submitting_entity_lei = get_text(warnings.find('.//auth:CtrPtyId/auth:RptSubmitgNtty/auth:LEI', NAMESPACES))

            # Debugging extracted LEIs
            print(f"✔ Reporting LEI: {reporting_counterparty_lei}, Submitting LEI: {submitting_entity_lei}")

            # Extract transaction details
            tx_details_elements = warnings.findall('.//auth:TxDtls', NAMESPACES)
            print(f"🔍 Found {len(tx_details_elements)} <TxDtls> elements.")

            for tx_details in tx_details_elements:
                other_counterparty_id = get_text(tx_details.find('.//auth:TxId/auth:OthrCtrPty/auth:Lgl/auth:Id/auth:LEI', NAMESPACES))
                transaction_id_unique = get_text(tx_details.find('.//auth:TxId/auth:UnqIdr/auth:UnqTxIdr', NAMESPACES))
                transaction_id_proprietary = get_text(tx_details.find('.//auth:TxId/auth:UnqIdr/auth:Prtry/auth:Id', NAMESPACES))
                action_type = get_text(tx_details.find('.//auth:TxId/auth:ActnTp', NAMESPACES))
                reporting_timestamp = get_text(tx_details.find('.//auth:TxId/auth:RptgTmStmp', NAMESPACES))
                derivative_event_type = get_text(tx_details.find('.//auth:TxId/auth:DerivEvtTp', NAMESPACES))
                derivative_event_timestamp = get_text(tx_details.find('.//auth:TxId/auth:DerivEvtTmStmp/auth:Dt', NAMESPACES))

                # Extract notional amount and currency
                notional_amount_element = tx_details.find('.//auth:NtnlAmt/auth:FrstLeg/auth:Amt/auth:Amt', NAMESPACES)
                notional_amount = get_text(notional_amount_element)
                notional_currency = notional_amount_element.get('Ccy') if notional_amount_element is not None else None

                record = {
                    'RefDate': ref_date,
                    'ReportingCounterparty_LEI': reporting_counterparty_lei,
                    'SubmittingEntity_LEI': submitting_entity_lei,
                    'OtherCounterparty_ID': other_counterparty_id,
                    'TransactionID_UniqueTransaction': transaction_id_unique,
                    'TransactionID_Proprietary': transaction_id_proprietary,
                    'ActionType': action_type,
                    'ReportingTimestamp': reporting_timestamp,
                    'DerivativeEventType': derivative_event_type,
                    'DerivativeEventTimestamp': derivative_event_timestamp,
                    'NotionalAmount': float(notional_amount) if notional_amount else None,
                    'NotionalAmountCurrency': notional_currency
                }
                records.append(record)

    print(f"✅ Total records extracted: {len(records)}")
    return records

def insert_data_to_db(data, connection_string, batch_size=1000):
    """Insert extracted data into the SQL Server database."""
    query = '''
    INSERT INTO UnavistaMarex_Warnings_Report (
        ReferenceDate, ReportingCounterparty_LEI, SubmittingEntity_LEI, OtherCounterparty_ID,
        TransactionID_UniqueTransaction, TransactionID_Proprietary, ActionType,
        ReportingTimestamp, DerivativeEventType, DerivativeEventTimestamp,
        NotionalAmount, NotionalAmountCurrency
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            count = 0
            batch = []

            for row in data:
                batch.append((
                    row['RefDate'],
                    row['ReportingCounterparty_LEI'],
                    row['SubmittingEntity_LEI'],
                    row['OtherCounterparty_ID'],
                    row['TransactionID_UniqueTransaction'],
                    row['TransactionID_Proprietary'],
                    row['ActionType'],
                    row['ReportingTimestamp'],
                    row['DerivativeEventType'],
                    row['DerivativeEventTimestamp'],
                    row['NotionalAmount'],
                    row['NotionalAmountCurrency']
                ))

                count += 1

                if count % batch_size == 0:
                    cursor.executemany(query, batch)
                    conn.commit()
                    print(f"📌 Committed {count} rows so far...")

                    batch = []

            if batch:
                cursor.executemany(query, batch)
                conn.commit()
                print(f"📌 Final commit for remaining {len(batch)} rows.")

            print(f"✅ Data successfully inserted into the database. Total rows: {count}")

    except pyodbc.Error as e:
        print(f"❌ Database error: {e}")

if __name__ == "__main__":
    try:
        validate_xml(XML_FILE, XSD_FILE)
        data = extract_data(XML_FILE)
        insert_data_to_db(data, CONNECTION_STRING)
    except Exception as e:
        print(f"❌ Error: {e}")
