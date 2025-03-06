import pyodbc
from lxml import etree
import re
from datetime import datetime, timedelta



# Paths to your files
XML_FILE = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Trade State Report/TRUVT_DATTSR_5493003EETVWYSIJ5A20_SE-250130_001001.xml'
XSD_FILE = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Trade State Report/head.003.001.01.xsd'

# Extract the date from the filename
file_name = XML_FILE.split('/')[-1]  # Extract just the filename
match = re.search(r'_SE-(\d{6})_', file_name)  # Match 'SE-250130'
report_date = datetime.strptime(match.group(1), "%y%m%d").date() if match else None
report_date_str = report_date.isoformat() if report_date else ""



# SQL Server connection details
CONNECTION_STRING = (
    'DRIVER={SQL Server};'
    'SERVER=AZR-WE-BI-02;'
    'DATABASE=RTS;'
    'Trusted_Connection=yes;'
)

# Define namespaces
NAMESPACES = {
    'auth': 'urn:iso:std:iso:20022:tech:xsd:auth.107.001.01',
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

    stat_elements = root.findall('.//auth:Stat', NAMESPACES)
    for stat in stat_elements:
        data = {
            'ReportDate': report_date_str,
            'ReportingCounterparty_LEI': get_text(
                stat.find('.//auth:RptgCtrPty/auth:Id/auth:Lgl/auth:Id/auth:LEI', NAMESPACES)),
            'ReportingCounterparty_Sector': get_text(
                stat.find('.//auth:RptgCtrPty/auth:Ntr/auth:FI/auth:Sctr/auth:Cd', NAMESPACES)),
            'ReportingCounterparty_Threshold': get_text(
                stat.find('.//auth:RptgCtrPty/auth:Ntr/auth:FI/auth:ClrThrshld', NAMESPACES)),
            'ReportingCounterparty_Side': get_text(
                stat.find('.//auth:RptgCtrPty/auth:DrctnOrSd/auth:CtrPtySd', NAMESPACES)),

            'OtherCounterparty_ID': get_text(
                stat.find('.//auth:OthrCtrPty/auth:IdTp/auth:Lgl/auth:Id/auth:LEI', NAMESPACES)),
            'OtherCounterparty_Sector': get_text(
                stat.find('.//auth:OthrCtrPty/auth:Ntr/auth:FI/auth:Sctr/auth:Cd', NAMESPACES)),
            'OtherCounterparty_ReportingObligation': get_text(
                stat.find('.//auth:OthrCtrPty/auth:RptgOblgtn', NAMESPACES)),

            'SubmittingAgent_LEI': get_text(stat.find('.//auth:SubmitgAgt/auth:LEI', NAMESPACES)),
            'ResponsibleEntity_LEI': get_text(stat.find('.//auth:NttyRspnsblForRpt/auth:LEI', NAMESPACES)),

            'Valuation_Amount': get_text(stat.find('.//auth:Valtn/auth:CtrctVal/auth:Amt', NAMESPACES)),
            'Valuation_Currency': stat.find('.//auth:Valtn/auth:CtrctVal/auth:Amt', NAMESPACES).get('Ccy') if stat.find(
                './/auth:Valtn/auth:CtrctVal/auth:Amt', NAMESPACES) is not None else None,
            'Valuation_Timestamp': get_text(stat.find('.//auth:Valtn/auth:TmStmp', NAMESPACES)),
            'Valuation_Type': get_text(stat.find('.//auth:Valtn/auth:Tp', NAMESPACES)),

            'Reporting_Timestamp': get_text(stat.find('.//auth:RptgTmStmp', NAMESPACES)),

            'Contract_Type': get_text(stat.find('.//auth:CtrctData/auth:CtrctTp', NAMESPACES)),
            'Asset_Class': get_text(stat.find('.//auth:CtrctData/auth:AsstClss', NAMESPACES)),
            'Product_Classification': get_text(stat.find('.//auth:CtrctData/auth:PdctClssfctn', NAMESPACES)),

            'TransactionID_UniqueTransactionId': get_text(
                stat.find('.//auth:TxData/auth:TxId/auth:UnqTxIdr', NAMESPACES)),
            'TransactionID_ProprietaryId': get_text(
                stat.find('.//auth:TxData/auth:TxId/auth:Prtry/auth:Id', NAMESPACES)),

            'UnderlyingInstrumentIndex_ISIN': get_text(
                stat.find('.//auth:CtrctData/auth:UndrlygInstrm/auth:Indx/auth:ISIN', NAMESPACES)),
            'UnderlyingInstrumentIndex_Name': get_text(
                stat.find('.//auth:CtrctData/auth:UndrlygInstrm/auth:Indx/auth:Nm', NAMESPACES)),
            'Settlement_Currency': get_text(stat.find('.//auth:CtrctData/auth:SttlmCcy/auth:Ccy', NAMESPACES)),
            'Derivative_Based_On_Crypto':get_text(stat.find('.//auth:CmonTradData/auth:CtrctData/auth:DerivBasedOnCrptAsst', NAMESPACES)),

            'Collateral_Portfolio_Code': get_text(
                stat.find('.//auth:TxData/auth:CollPrtflCd/auth:Prtfl/auth:Cd', NAMESPACES)),
            'Platform_ID': get_text(stat.find('.//auth:TxData/auth:PltfmIdr', NAMESPACES)),
            'Transaction_Price': get_text(
                stat.find('.//auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt', NAMESPACES)),
            'Transaction_Price_Currency': stat.find('.//auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt',
                                                    NAMESPACES).get('Ccy') if stat.find(
                './/auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt', NAMESPACES) is not None else None,

            'Notional_Amount': get_text(
                stat.find('.//auth:TxData/auth:NtnlAmt/auth:FrstLeg/auth:Amt/auth:Amt', NAMESPACES)),
            'Notional_Quantity': get_text(
                stat.find('.//auth:TxData/auth:NtnlQty/auth:FrstLeg/auth:TtlQty', NAMESPACES)),
            'Delivery_Type': get_text(stat.find('.//auth:TxData/auth:DlvryTp', NAMESPACES)),

            'Execution_Timestamp': get_text(stat.find('.//auth:TxData/auth:ExctnTmStmp', NAMESPACES)),
            'Effective_Date': get_text(stat.find('.//auth:TxData/auth:FctvDt', NAMESPACES)),

            'Master_Agreement_Type': get_text(stat.find('.//auth:TxData/auth:MstrAgrmt/auth:Tp/auth:Tp', NAMESPACES)),
            'Master_Agreement_Details': get_text(
                stat.find('.//auth:TxData/auth:MstrAgrmt/auth:OthrMstrAgrmtDtls', NAMESPACES)),

            'Derivative_Event_Type': get_text(stat.find('.//auth:TxData/auth:DerivEvt/auth:Tp', NAMESPACES)),
            'Derivative_Event_Timestamp': get_text(
                stat.find('.//auth:TxData/auth:DerivEvt/auth:TmStmp/auth:Dt', NAMESPACES)),

            'Trade_Confirmation_Type': get_text(
                stat.find('.//auth:TxData/auth:TradConf/auth:Confd/auth:Tp', NAMESPACES)),
            'Trade_Confirmation_Timestamp': get_text(
                stat.find('.//auth:TxData/auth:TradConf/auth:Confd/auth:TmStmp', NAMESPACES)),

            'Clearing_Obligation': get_text(stat.find('.//auth:TxData/auth:TradClr/auth:ClrOblgtn', NAMESPACES)),
            'Clearing_Status_Reason': get_text(
                stat.find('.//auth:TxData/auth:TradClr/auth:ClrSts/auth:NonClrd/auth:Rsn', NAMESPACES)),
            'Intragroup_Trade': get_text(stat.find('.//auth:CmonTradData/auth:TxData/auth:TradClr/auth:IntraGrp', NAMESPACES)),

            'Contract_Action_Type': get_text(stat.find('.//auth:CtrctMod/auth:ActnTp', NAMESPACES)),
            'Contract_Level': get_text(stat.find('.//auth:CtrctMod/auth:Lvl', NAMESPACES)),
            'Reconciliation_Flag': get_text(stat.find('.//auth:TechAttrbts/auth:RcncltnFlg', NAMESPACES))
        }
        yield data

def insert_data_to_db(data, connection_string):
    """Insert extracted data into the SQL Server database."""
    query = '''
    INSERT INTO UnavistaMarex_Trade_State_Report (
    ReportDate, 
    ReportingCounterparty_LEI, 
    ReportingCounterparty_Sector, 
    ReportingCounterparty_Threshold, 
    ReportingCounterparty_Side,
    OtherCounterparty_ID, 
    OtherCounterparty_Sector, 
    OtherCounterparty_ReportingObligation,
    SubmittingAgent_LEI, 
    ResponsibleEntity_LEI, 
    Valuation_Amount, 
    Valuation_Currency, 
    Valuation_Timestamp,
    Valuation_Type, 
    Reporting_Timestamp, 
    Contract_Type, 
    Asset_Class, 
    Product_Classification, 
    TransactionID_UniqueTransactionId,
    TransactionID_ProprietaryId, 
    UnderlyingInstrumentIndex_ISIN, 
    UnderlyingInstrumentIndex_Name, 
    Settlement_Currency,
    Derivative_Based_On_Crypto, 
    Collateral_Portfolio_Code,
    Platform_ID, 
    Transaction_Price, 
    Transaction_Price_Currency, 
    Notional_Amount, 
    Notional_Quantity, 
    Delivery_Type,
    Execution_Timestamp, 
    Effective_Date, 
    Master_Agreement_Type, 
    Master_Agreement_Details, 
    Derivative_Event_Type,
    Derivative_Event_Timestamp, 
    Trade_Confirmation_Type, 
    Trade_Confirmation_Timestamp, 
    Clearing_Obligation,
    Clearing_Status_Reason, 
    Intragroup_Trade, 
    Contract_Action_Type, 
    Contract_Level, 
    Reconciliation_Flag
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
);
    '''

    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            batch_size = 500  # Example batch size
            batch = []

            for row in data:
                batch.append((
                    row['ReportDate'],
                    row['ReportingCounterparty_LEI'],
                    row['ReportingCounterparty_Sector'],
                    row['ReportingCounterparty_Threshold'],
                    row['ReportingCounterparty_Side'],
                    row['OtherCounterparty_ID'],
                    row['OtherCounterparty_Sector'],
                    row['OtherCounterparty_ReportingObligation'],
                    row['SubmittingAgent_LEI'],
                    row['ResponsibleEntity_LEI'],
                    row['Valuation_Amount'],
                    row['Valuation_Currency'],
                    row['Valuation_Timestamp'],
                    row['Valuation_Type'],
                    row['Reporting_Timestamp'],
                    row['Contract_Type'],
                    row['Asset_Class'],
                    row['Product_Classification'],
                    row['TransactionID_UniqueTransactionId'],
                    row['TransactionID_ProprietaryId'],
                    row['UnderlyingInstrumentIndex_ISIN'],
                    row['UnderlyingInstrumentIndex_Name'],
                    row['Settlement_Currency'],
                    row['Derivative_Based_On_Crypto'],
                    row['Collateral_Portfolio_Code'],
                    row['Platform_ID'],
                    row['Transaction_Price'],
                    row['Transaction_Price_Currency'],
                    row['Notional_Amount'],
                    row['Notional_Quantity'],
                    row['Delivery_Type'],
                    row['Execution_Timestamp'],
                    row['Effective_Date'],
                    row['Master_Agreement_Type'],
                    row['Master_Agreement_Details'],
                    row['Derivative_Event_Type'],
                    row['Derivative_Event_Timestamp'],
                    row['Trade_Confirmation_Type'],
                    row['Trade_Confirmation_Timestamp'],
                    row['Clearing_Obligation'],
                    row['Clearing_Status_Reason'],
                    row['Intragroup_Trade'],
                    row['Contract_Action_Type'],
                    row['Contract_Level'],
                    row['Reconciliation_Flag']
                ))

                # Commit in batches
                if len(batch) == batch_size:
                    cursor.executemany(query, batch)
                    conn.commit()
                    print(f"Committed {len(batch)} rows.")
                    batch = []

            # Commit any remaining rows
            if batch:
                cursor.executemany(query, batch)
                conn.commit()
                print(f"Committed {len(batch)} remaining rows.")

            print("Data successfully inserted into the database.")
    except pyodbc.Error as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    try:
        validate_xml(XML_FILE, XSD_FILE)
        data = extract_data(XML_FILE)
        insert_data_to_db(data, CONNECTION_STRING)
    except Exception as e:
        print(f"Error: {e}")
