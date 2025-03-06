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
        print("XML is valid.")
    except etree.XMLSchemaError as e:
        raise ValueError(f"XML schema validation error: {e}")

def get_text(element):
    """Safely extract text from an XML element."""
    return element.text if element is not None else None

def extract_data(xml_file):
    """Parse XML and extract relevant data."""
    tree = etree.parse(xml_file)
    root = tree.getroot()

    records = []

    # Extract `RefDt` once
    ref_date = get_text(root.find('.//auth:WrnngsSttstcs/auth:Rpt/auth:RefDt', NAMESPACES))
    print(f"RefDate found: {ref_date}")

    # Debug: Check <MssngValtn>
    missing_valuations = root.findall('.//auth:WrnngsSttstcs/auth:Rpt/auth:MssngValtn', NAMESPACES)
    print(f"Found {len(missing_valuations)} <MssngValtn> elements")

    for mv in missing_valuations:
        warnings_elements = mv.findall('.//auth:Rpt/auth:Wrnngs', NAMESPACES)
        print(f"Found {len(warnings_elements)} <Wrnngs> elements in current <MssngValtn>")

        for warnings in warnings_elements:
            reporting_counterparty_lei = get_text(warnings.find('.//auth:RptgCtrPty/auth:LEI', NAMESPACES))
            submitting_entity_lei = get_text(warnings.find('.//auth:RptSubmitgNtty/auth:LEI', NAMESPACES))
            responsible_entity_lei = get_text(warnings.find('.//auth:NttyRspnsblForRpt/auth:LEI', NAMESPACES))

            # Debug: Check extracted shared fields
            print(f"ReportingCounterparty_LEI: {reporting_counterparty_lei}, SubmittingEntity_LEI: {submitting_entity_lei}, EntityResponsibleForReporting_LEI: {responsible_entity_lei}")

            tx_details_elements = warnings.findall('.//auth:TxDtls', NAMESPACES)
            print(f"Found {len(tx_details_elements)} <TxDtls> elements in current <Wrnngs>")

            for tx_details in tx_details_elements:
                record = {
                    'RefDate': ref_date,  # Reference Date (common for all rows)
                    'ReportingCounterparty_LEI': reporting_counterparty_lei,
                    'SubmittingEntity_LEI': submitting_entity_lei,
                    'EntityResponsibleForReporting_LEI': responsible_entity_lei,
                    'TransactionID_UniqueTransactionId': get_text(
                        tx_details.find('.//auth:TxId/auth:OthrCtrPty/auth:Ntrl/auth:Id/auth:Id', NAMESPACES)) or None,
                    'TransactionID_ProprietaryId': get_text(
                        tx_details.find('.//auth:TxId/auth:UnqIdr/auth:Prtry/auth:Id', NAMESPACES)) or None,
                    'OtherCounterparty_ID': get_text(
                        tx_details.find('.//auth:TxId/auth:OthrCtrPty/auth:Ntrl/auth:Id/auth:Id/auth:Id', NAMESPACES)),
                    'OtherCounterparty_Country': get_text(
                        tx_details.find('.//auth:TxId/auth:OthrCtrPty/auth:Ntrl/auth:Ctry', NAMESPACES)) or None,
                    'CollateralTimestamp': get_text(
                        tx_details.find('.//auth:ValtnTmStmp/auth:DtTm', NAMESPACES)) or None,
                    'ValuationAmount': get_text(tx_details.find('.//auth:ValtnAmt/auth:Amt', NAMESPACES)) or None,
                    'ValuationAmountCurrency': (tx_details.find('.//auth:ValtnAmt/auth:Amt', NAMESPACES).get('Ccy')
                                                if tx_details.find('.//auth:ValtnAmt/auth:Amt', NAMESPACES) is not None
                                                else None
                                                ),
                    'ValuationTimestamp': get_text(
                        tx_details.find('.//auth:ValtnTmStmp/auth:DtTm', NAMESPACES)) or None,
                }
                records.append(record)

    print(f"Total records extracted: {len(records)}")
    return records





def insert_data_to_db(data, connection_string, batch_size=1000):
    query = '''
    INSERT INTO UnavistaMarex_Warnings_Report (
        ReferenceDate, ReportingCounterparty_LEI, SubmittingEntity_LEI, EntityResponsibleForReporting_LEI,
        TransactionID_UniqueTransactionId, TransactionID_ProprietaryId, OtherCounterparty_ID, OtherCounterparty_Country,
        CollateralTimestamp, ValuationAmount, ValuationAmountCurrency, ValuationTimestamp
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
                    row['EntityResponsibleForReporting_LEI'],
                    row['TransactionID_UniqueTransactionId'],
                    row['TransactionID_ProprietaryId'],
                    row['OtherCounterparty_ID'],
                    row['OtherCounterparty_Country'],
                    row['CollateralTimestamp'],
                    float(row['ValuationAmount']) if row['ValuationAmount'] else None,
                    row['ValuationAmountCurrency'],
                    row['ValuationTimestamp']
                ))

                count += 1

                if count % batch_size == 0:
                    cursor.executemany(query, batch)
                    conn.commit()
                    print(f"Committed {count} rows so far...")
                    batch = []

            # Commit remaining rows
            if batch:
                cursor.executemany(query, batch)
                conn.commit()
                print(f"Final commit for remaining {len(batch)} rows.")

            print(f"Data successfully inserted into the database. Total rows: {count}")

    except pyodbc.Error as e:
        print(f"Database error: {e}")


if __name__ == "__main__":
    try:
        validate_xml(XML_FILE, XSD_FILE)
        data = extract_data(XML_FILE)
        insert_data_to_db(data, CONNECTION_STRING)
    except Exception as e:
        print(f"Error: {e}")
