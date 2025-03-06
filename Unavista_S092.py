import pyodbc
from lxml import etree

# File paths
XML_FILE = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Rejection Statistics Report/TRUVT_DATREJ_5493003EETVWYSIJ5A20_SE-250130_001001.xml'
XSD_FILE = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Rejection Statistics Report/head.003.001.01.xsd'

# SQL Server connection details
CONNECTION_STRING = (
    'DRIVER={SQL Server};'
    'SERVER=AZR-WE-BI-02;'
    'DATABASE=RTS;'
    'Trusted_Connection=yes;'
)

# Namespaces
NAMESPACES = {
    'auth': 'urn:iso:std:iso:20022:tech:xsd:auth.092.001.04',
    'head': 'urn:iso:std:iso:20022:tech:xsd:head.003.001.01'
}


def validate_xml(xml_file, xsd_file):
    """Validate XML file against XSD schema."""
    try:
        with open(xsd_file, 'rb') as f:
            schema_root = etree.XML(f.read())
        schema = etree.XMLSchema(schema_root)
        parser = etree.XMLParser(schema=schema)
        etree.parse(xml_file, parser)
        print("XML is valid.")
    except etree.XMLSchemaError as e:
        raise ValueError(f"XML validation error: {e}")


def get_text(element):
    """Safely extract text from an XML element."""
    return element.text if element is not None else None


def extract_data(xml_file):
    """Parse XML and extract data."""
    tree = etree.parse(xml_file)
    root = tree.getroot()

    records = []
    ref_date = get_text(root.find('.//auth:RefDt', NAMESPACES))

    for rejection_stats in root.findall('.//auth:DerivsTradRjctnSttstclRpt/auth:RjctnSttstcs', NAMESPACES):
        reporting_counterparty_lei = get_text(rejection_stats.find('.//auth:RptgCtrPty/auth:LEI', NAMESPACES))
        submitting_entity_lei = get_text(rejection_stats.find('.//auth:RptSubmitgNtty/auth:LEI', NAMESPACES))

        for rejection_reason in rejection_stats.findall('.//auth:TxsRjctnsRsn', NAMESPACES):
            record = {
                'RefDate': ref_date,
                'ReportingCounterparty_LEI': reporting_counterparty_lei,
                'SubmittingEntity_LEI': submitting_entity_lei,
                'ActionType': get_text(rejection_reason.find('.//auth:TxId/auth:ActnTp', NAMESPACES)),
                'ReportingTimestamp': get_text(rejection_reason.find('.//auth:TxId/auth:RptgTmStmp', NAMESPACES)),
                'DerivativeEventType': get_text(rejection_reason.find('.//auth:TxId/auth:DerivEvtTp', NAMESPACES)),
                'DerivativeEventTimestamp': get_text(
                    rejection_reason.find('.//auth:TxId/auth:DerivEvtTmStmp/auth:Dt', NAMESPACES)),
                'OtherCounterparty_LEI': get_text(
                    rejection_reason.find('.//auth:TxId/auth:OthrCtrPty/auth:Lgl/auth:Id/auth:LEI',
                                          NAMESPACES)),
                'UniqueTransactionID': get_text(rejection_reason.find('.//auth:TxId/auth:UnqIdr/auth:UnqTxIdr', NAMESPACES)),
                'CollateralPortfolioCode': get_text(
                    rejection_reason.find('.//auth:TxId/auth:CollPrtflCd/auth:Prtfl/auth:Cd', NAMESPACES)),
                'RejectionStatus': get_text(rejection_reason.find('.//auth:Sts', NAMESPACES)),
                'ValidationRuleId': get_text(rejection_reason.find('.//auth:DtldVldtnRule/auth:Id', NAMESPACES)),
                'ValidationRuleDesc': get_text(rejection_reason.find('.//auth:DtldVldtnRule/auth:Desc', NAMESPACES))

            }
            records.append(record)

    return records


def insert_data_to_db(data, connection_string, batch_size=1000):
    """Insert data into the SQL Server database."""
    query = '''
    INSERT INTO UnavistaMarex_Rejection_Statistics_Report (
        RefDate, ReportingCounterparty_LEI, SubmittingEntity_LEI, ActionType, ReportingTimestamp, DerivativeEventType, 
        DerivativeEventTimestamp, OtherCounterparty_LEI, UniqueTransactionID, 
        CollateralPortfolioCode, RejectionStatus, ValidationRuleId, ValidationRuleDesc
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            batch = []
            count = 0

            for row in data:
                batch.append((
                    row['RefDate'], row['ReportingCounterparty_LEI'], row['SubmittingEntity_LEI'],  row['ActionType'], row['ReportingTimestamp'], row['DerivativeEventType'],
                    row['DerivativeEventTimestamp'], row['OtherCounterparty_LEI'], row['UniqueTransactionID'],
                    row['CollateralPortfolioCode'], row['RejectionStatus'], row['ValidationRuleId'],
                    row['ValidationRuleDesc']
                ))
                count += 1

                if count % batch_size == 0:
                    cursor.executemany(query, batch)
                    conn.commit()
                    print(f"Committed {count} rows so far.")
                    batch = []

            if batch:
                cursor.executemany(query, batch)
                conn.commit()
                print(f"Committed remaining {len(batch)} rows.")

            print(f"Data successfully inserted. Total rows: {count}")
    except pyodbc.Error as e:
        print(f"Database error: {e}")


if __name__ == "__main__":
    try:
        validate_xml(XML_FILE, XSD_FILE)
        data = extract_data(XML_FILE)
        print(f"Total records extracted: {len(data)}")
        insert_data_to_db(data, CONNECTION_STRING)
    except Exception as e:
        print(f"Error: {e}")
