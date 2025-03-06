import pyodbc
from lxml import etree

# Paths to your files
XML_FILE = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Margin Activity Report/TRUVT_DATMDA_5493003EETVWYSIJ5A20_SE-250130_001001.xml'
XSD_FILE = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Margin Activity Report/head.003.001.01.xsd'

# SQL Server connection details
CONNECTION_STRING = (
    'DRIVER={SQL Server};'
    'SERVER=AZR-WE-BI-02;'
    'DATABASE=RTS;'
    'Trusted_Connection=yes;'
)

# Define namespaces
NAMESPACES = {
    'auth': 'urn:iso:std:iso:20022:tech:xsd:auth.108.001.01',
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

    for report in root.findall('.//auth:Rpt', NAMESPACES):
        for margin_update in report.findall('.//auth:MrgnUpd', NAMESPACES):
            record = {
                'ReportingTimestamp': get_text(margin_update.find('.//auth:RptgTmStmp', NAMESPACES)),
                'ReportingCounterparty_LEI': get_text(margin_update.find('.//auth:CtrPtyId/auth:RptgCtrPty/auth:Id/auth:Lgl/auth:Id/auth:LEI', NAMESPACES)),
                'OtherCounterparty_Id': get_text(margin_update.find('.//auth:CtrPtyId/auth:OthrCtrPty/auth:IdTp/auth:Ntrl/auth:Id/auth:Id/auth:Id', NAMESPACES)),
                'SubmittingAgent_LEI': get_text(margin_update.find('.//auth:CtrPtyId/auth:SubmitgAgt/auth:LEI', NAMESPACES)),
                'EntityResponsibleForReporting_LEI': get_text(margin_update.find('.//auth:CtrPtyId/auth:NttyRspnsblForRpt/auth:LEI', NAMESPACES)),
                'EventDate': get_text(margin_update.find('.//auth:EvtDt', NAMESPACES)),
                'PortfolioCode': get_text(margin_update.find('.//auth:Coll/auth:CollPrtflCd/auth:Prtfl/auth:Cd', NAMESPACES)),
                'CollateralCategory': get_text(margin_update.find('.//auth:Coll/auth:CollstnCtgy', NAMESPACES)),
                'CollateralTimestamp': get_text(margin_update.find('.//auth:Coll/auth:TmStmp', NAMESPACES)),
                'VariationMarginReceived_PreHaircut': get_text(margin_update.find('.//auth:RcvdMrgnOrColl/auth:VartnMrgnRcvdPreHrcut', NAMESPACES)),
                'VariationMarginReceived_PostHaircut': get_text(margin_update.find('.//auth:RcvdMrgnOrColl/auth:VartnMrgnRcvdPstHrcut', NAMESPACES)),
                'ExcessCollateralReceived': get_text(margin_update.find('.//auth:RcvdMrgnOrColl/auth:XcssCollRcvd', NAMESPACES))
            }
            records.append(record)

    return records

def insert_data_to_db(data, connection_string, batch_size=1000):
    """Insert extracted data into the SQL Server database."""
    query = '''
    INSERT INTO UnavistaMarex_Margin_Activity_Report (
        ReportingTimestamp, ReportingCounterparty_LEI, OtherCounterparty_Id, SubmittingAgent_LEI, EntityResponsibleForReporting_LEI,
        EventDate, PortfolioCode, CollateralCategory, CollateralTimestamp,
        VariationMarginReceived_PreHaircut, VariationMarginReceived_PostHaircut, ExcessCollateralReceived
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            count = 0

            for row in data:
                cursor.execute(query, (
                    row['ReportingTimestamp'],
                    row['ReportingCounterparty_LEI'],
                    row['OtherCounterparty_Id'],
                    row['SubmittingAgent_LEI'],
                    row['EntityResponsibleForReporting_LEI'],
                    row['EventDate'],
                    row['PortfolioCode'],
                    row['CollateralCategory'],
                    row['CollateralTimestamp'],
                    float(row['VariationMarginReceived_PreHaircut']) if row['VariationMarginReceived_PreHaircut'] else None,
                    float(row['VariationMarginReceived_PostHaircut']) if row['VariationMarginReceived_PostHaircut'] else None,
                    float(row['ExcessCollateralReceived']) if row['ExcessCollateralReceived'] else None
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
