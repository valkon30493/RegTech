import pyodbc
from lxml import etree

# Paths to your files
XML_FILE = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Reconciliations Report/TRUVT_DATREC_5493003EETVWYSIJ5A20_SE-250130_001001.xml'
XSD_FILE = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Reconciliations Report/head.003.001.01.xsd'

# SQL Server connection details
CONNECTION_STRING = (
    'DRIVER={SQL Server};'
    'SERVER=AZR-WE-BI-02;'
    'DATABASE=RTS;'
    'Trusted_Connection=yes;'
)

# Define namespaces
NAMESPACES = {
    'auth': 'urn:iso:std:iso:20022:tech:xsd:auth.091.001.02',
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

    # Extract common headers
    header = root.find('.//head:AppHdr', NAMESPACES)
    report_date = get_text(root.find('.//auth:RefDt', NAMESPACES))

    tx_details_elements = root.findall('.//auth:TxDtls', NAMESPACES)

    for tx_detail in tx_details_elements:
        base_data = {
            'ReportDate': report_date,
            'ReportingType': get_text(root.find('.//auth:RptgRqrmnt/auth:RptgTp', NAMESPACES)),
            'Pairing': get_text(root.find('.//auth:RptgRqrmnt/auth:Pairg', NAMESPACES)),
            'Reconciliation': get_text(root.find('.//auth:RptgRqrmnt/auth:Rcncltn', NAMESPACES)),
            'ValuationReconciliation': get_text(root.find('.//auth:RptgRqrmnt/auth:ValtnRcncltn', NAMESPACES)),
            'Revised': get_text(root.find('.//auth:RptgRqrmnt/auth:Rvvd', NAMESPACES)),
            'FurtherModification': get_text(root.find('.//auth:RptgRqrmnt/auth:FrthrMod', NAMESPACES)),
            'ReportingCounterparty_LEI': get_text(
                tx_detail.find('.//auth:CtrPtyId/auth:RptgCtrPty/auth:LEI', NAMESPACES)),
            'OtherCounterparty_Legal_LEI': get_text(
                tx_detail.find('.//auth:CtrPtyId/auth:OthrCtrPty/auth:Lgl/auth:LEI', NAMESPACES)),
            'OtherCounterparty_Natural_Id': get_text(
                tx_detail.find('.//auth:CtrPtyId/auth:OthrCtrPty/auth:Ntrl/auth:Id/auth:Id', NAMESPACES)),
            'ReportSubmittingEntityID_LEI': get_text(
                tx_detail.find('.//auth:CtrPtyId/auth:RptSubmitgNtty/auth:LEI', NAMESPACES)),
            'EntityResponsibleForReporting_LEI': get_text(
                tx_detail.find('.//auth:CtrPtyId/auth:NttyRspnsblForRpt/auth:LEI', NAMESPACES)),
            'CounterpartyPair_TotalNumberOfTransactions': int(
                get_text(tx_detail.find('.//auth:TtlNbOfTxs', NAMESPACES)) or 0)
        }

        for rcncltn_rpt in tx_detail.findall('.//auth:RcncltnRpt', NAMESPACES):
            yield {
                **base_data,
                'TransactionID_UniqueTransactionId': get_text(rcncltn_rpt.find('.//auth:TxId/auth:UnqIdr/auth:UnqTxIdr', NAMESPACES)),
                'TransactionID_ProprietaryId': get_text(rcncltn_rpt.find('.//auth:TxId/auth:UnqIdr/auth:Prtry/auth:Id', NAMESPACES)),
            }

def insert_data_to_db(data, connection_string, batch_size=1000):
    """Insert extracted data into the SQL Server database."""
    query = '''
    INSERT INTO UnavistaMarex_Reconciliations_Report (
        ReportDate, ReportingType, Pairing, Reconciliation, ValuationReconciliation, Revised, FurtherModification, ReportingCounterparty_LEI, OtherCounterparty_Legal_LEI, OtherCounterparty_Natural_Id, ReportSubmittingEntityID_LEI, 
        EntityResponsibleForReporting_LEI, CounterpartyPair_TotalNumberOfTransactions, 
        TransactionID_UniqueTransactionId, TransactionID_ProprietaryId
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            count = 0

            for row in data:
                cursor.execute(query, (
                    row['ReportDate'],
                    row['ReportingType'],
                    row['Pairing'],
                    row['Reconciliation'],
                    row['ValuationReconciliation'],
                    row['Revised'],
                    row['FurtherModification'],
                    row['ReportingCounterparty_LEI'],
                    row['OtherCounterparty_Legal_LEI'],
                    row['OtherCounterparty_Natural_Id'],
                    row['ReportSubmittingEntityID_LEI'],
                    row['EntityResponsibleForReporting_LEI'],
                    row['CounterpartyPair_TotalNumberOfTransactions'],
                    row['TransactionID_UniqueTransactionId'],
                    row['TransactionID_ProprietaryId']
                ))
            count += 1

            # Commit the batch
            if count % batch_size == 0:
                conn.commit()
                print(f"Committed {count} rows so far...")

            # Commit any remaining rows
        if count % batch_size != 0:
            conn.commit()
            print(f"Final commit for remaining {count % batch_size} rows.")

    except pyodbc.Error as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    try:
        validate_xml(XML_FILE, XSD_FILE)
        data = extract_data(XML_FILE)
        insert_data_to_db(data, CONNECTION_STRING)
    except Exception as e:
        print(f"Error: {e}")
