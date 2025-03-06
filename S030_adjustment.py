import pyodbc
from lxml import etree

# Paths to your files
xml_file = 'C:/Users/valentinosko/Desktop/REGIS_EOD_Reports/Trade_Activity_Report/eudbp6tyb000_S030_20250120000000_TAR_0014.xml'
xsd_file = 'C:/Users/valentinosko/Desktop/REGIS_EOD_Reports/Trade_Activity_Report/head.003.001.01.xsd'

# SQL Server connection details
connection_string = (
    'DRIVER={SQL Server};'
    'SERVER=AZR-WE-BI-02;'
    'DATABASE=RTS;'
    'Trusted_Connection=yes;'
)

# Load and parse the XSD file
with open(xsd_file, 'rb') as f:
    schema_root = etree.XML(f.read())

schema = etree.XMLSchema(schema_root)
parser = etree.XMLParser(schema=schema)

# Parse the XML file
try:
    tree = etree.parse(xml_file, parser)
    print("XML is valid.")
except etree.XMLSchemaError as e:
    print("XML schema validation error:", e)

# Define your namespaces (modify as needed)
namespaces = {
    'auth': 'urn:iso:std:iso:20022:tech:xsd:auth.030.001.03'
}

# Extract relevant data based on the structure defined by the schema
root = tree.getroot()


# Define a function to safely extract text from an element
def get_text(element):
    return element.text if element is not None else None


# Connect to SQL Server database
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Example extraction based on your structure (customize as needed)
tx_details_elements = root.findall('.//auth:DerivsTradRpt/auth:TradData/auth:Rpt', namespaces)
for tx_detail in tx_details_elements:
    action_type = None
    if tx_detail.find('.//auth:New', namespaces) is not None:
        action_type = 'NEWT'
    elif tx_detail.find('.//auth:Mod', namespaces) is not None:
        action_type = 'MODI'
    elif tx_detail.find('.//auth:Crrctn', namespaces) is not None:
        action_type = 'CORR'
    elif tx_detail.find('.//auth:Termntn', namespaces) is not None:
        action_type = 'TERM'
    elif tx_detail.find('.//auth:PosCmpnt', namespaces) is not None:
        action_type = 'POSC'
    elif tx_detail.find('.//auth:ValtnUpd', namespaces) is not None:
        action_type = 'VALU'
    elif tx_detail.find('.//auth:Err', namespaces) is not None:
        action_type = 'EROR'
    elif tx_detail.find('.//auth:Rvv', namespaces) is not None:
        action_type = 'REVI'


    counterparty1 = get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Id/auth:Lgl/auth:Id/auth:LEI',
                       namespaces))

    counterparty1_nature = None
    if tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:FI', namespaces) is not None:
        counterparty1_nature = 'F'
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:NFI',
                        namespaces) is not None:
        counterparty1_nature = 'N'
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:CntrlCntrPty',
                        namespaces) is not None:
        counterparty1_nature = 'C'
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:Othr',
                        namespaces) is not None:
        counterparty1_nature = 'O'

    counterparty1_sector = None
    if tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:FI', namespaces) is not None:
        counterparty1_sector = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:FI/auth:Sctr/auth:Cd',
                           namespaces))
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:NFI',
                        namespaces) is not None:
        counterparty1_sector = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:NFI/auth:Sctr/auth:Id',
                           namespaces))
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:CntrlCntrPty',
                        namespaces) is not None:
        counterparty1_sector = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:CntrlCntrPty',
                           namespaces))
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:Othr',
                        namespaces) is not None:
        counterparty1_sector = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:Othr', namespaces))

    counterparty1_clearing_threshold = None
    if tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:FI', namespaces) is not None:
        counterparty1_clearing_threshold = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:ClrThrshld', namespaces))
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:NFI',
                        namespaces) is not None:
        counterparty1_clearing_threshold = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:ClrThrshld', namespaces))

    directly_linked_activity = get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:Ntr/auth:NFI/auth:DrctlyLkdActvty',
                       namespaces))
    direction_of_leg1 = get_text(tx_detail.find(
        './/auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:DrctnOrSd/auth:Drctn/auth:DrctnOfTheFrstLeg',
        namespaces))
    direction_of_leg2 = get_text(tx_detail.find(
        './/auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:DrctnOrSd/auth:Drctn/auth:DrctnOfTheScndLeg',
        namespaces))
    direction = get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:RptgCtrPty/auth:DrctnOrSd/auth:CtrPtySd', namespaces))

    counterparty2_idtype = None
    if tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:IdTp/auth:Lgl', namespaces) is not None:
        counterparty2_idtype = 'True'
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:IdTp/auth:Ntrl',
                        namespaces) is not None:
        counterparty2_idtype = 'False'

    # counterparty2 = None
    # if tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:IdTp/auth:Lgl', namespaces) is not None:
    #     counterparty2 = get_text(
    #         tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Id/auth:LEI', namespaces))
    # elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:IdTp/auth:Ntrl',
    #                     namespaces) is not None:
    counterparty2 = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty//auth:Ntrl/auth:Id/auth:Id/auth:Id', namespaces))

    counterparty2_country = get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:IdTp/auth:Ntrl/auth:Ctry', namespaces))

    counterparty2_nature = None
    if tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:FI', namespaces) is not None:
        counterparty2_nature = 'F'
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:NFI',
                        namespaces) is not None:
        counterparty2_nature = 'N'
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:CntrlCntrPty',
                        namespaces) is not None:
        counterparty2_nature = 'C'
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:Othr',
                        namespaces) is not None:
        counterparty2_nature = 'O'

    counterparty2_sector = None
    if tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:FI', namespaces) is not None:
        counterparty2_sector = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:FI/auth:Sctr/auth:Cd',
                           namespaces))
    elif tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:NFI',
                        namespaces) is not None:
        counterparty2_sector = get_text(
            tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:NFI/auth:Sctr/auth:Id',
                           namespaces))

    counterparty2_clearing_threshold = get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:FI/auth:ClrThrshld',
                       namespaces)) or get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:Ntr/auth:NFI/auth:ClrThrshld',
                       namespaces))
    counterparty2_reporting_obligation = get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:OthrCtrPty/auth:RptgOblgtn', namespaces))
    broker_id = get_text(tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:Brkr/auth:LEI', namespaces))
    report_submitting_entity_id = get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:SubmitgAgt/auth:LEI', namespaces))
    clearing_member = get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:ClrMmb/auth:Lgl/auth:Id/auth:LEI', namespaces))
    entity_responsible_for_reporting = get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:CtrPty/auth:NttyRspnsblForRpt/auth:LEI', namespaces))

    valuation_amount_element = tx_detail.find('.//auth:CtrPtySpcfcData/auth:Valtn/auth:CtrctVal/auth:Amt', namespaces)
    valuation_amount = None
    if valuation_amount_element is not None:
        valuation_amount = -1 * float(valuation_amount_element.text) if valuation_amount_element.get(
            'Sgn') == 'False' else float(valuation_amount_element.text)
    valuation_currency = get_text(
        tx_detail.find('.//auth:CtrPtySpcfcData/auth:Valtn/auth:CtrctVal/auth:Amt', namespaces))
    valuation_timestamp = get_text(tx_detail.find('.//auth:CtrPtySpcfcData/auth:Valtn/auth:TmStmp', namespaces))
    valuation_method = get_text(tx_detail.find('.//auth:CtrPtySpcfcData/auth:Valtn/auth:Tp', namespaces))
    delta = get_text(tx_detail.find('.//auth:CtrPtySpcfcData/auth:Valtn/auth:Dlta', namespaces))
    reporting_timestamp = get_text(tx_detail.find('.//auth:CtrPtySpcfcData/auth:RptgTmStmp', namespaces))
    contract_type = get_text(tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:CtrctTp', namespaces))
    asset_class = get_text(tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:AsstClss', namespaces))
    product_classification = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:PdctClssfctn', namespaces))
    isin = get_text(tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:PdctId/auth:ISIN', namespaces))
    upi = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:PdctId/auth:UnqPdctIdr/auth:Id', namespaces))

    underlying_id_type = None
    if tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:UndrlygInstrm/auth:ISIN', namespaces) is not None:
        underlying_id_type = 'I'
    elif tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:UndrlygInstrm/auth:Bskt', namespaces) is not None:
        underlying_id_type = 'B'
    elif tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:UndrlygInstrm/auth:Indx', namespaces) is not None:
        underlying_id_type = 'X'

    underlying_id_isin = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:UndrlygInstrm/auth:ISIN', namespaces)) or get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:UndrlygInstrm/auth:Indx/auth:ISIN', namespaces))
    custom_basket_code_structurer = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:UndrlygInstrm/auth:Bskt/auth:Strr', namespaces))
    custom_basket_code_id = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:UndrlygInstrm/auth:Bskt/auth:Id', namespaces))
    identifier_of_the_baskets_constituents = get_text(tx_detail.find(
        './/auth:CmonTradData/auth:CtrctData/auth:UndrlygInstrm/auth:Bskt/auth:Cnsttnts/auth:InstrmId/auth:ISIN',
        namespaces))
    underlying_id_index_name = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:UndrlygInstrm/auth:Indx/auth:Nm', namespaces))
    underlying_id_index_indicator = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:UndrlygInstrm/auth:Indx/auth:Indx', namespaces))
    settlement_currency1 = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:SttlmCcy/auth:Ccy', namespaces))
    settlement_currency2 = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:SttlmCcyScndLeg/auth:Ccy', namespaces))
    derivative_based_on_crypto_assets = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:CtrctData/auth:DerivBasedOnCrptAsst', namespaces))
    TransactionID_UniqueTransactionId = get_text(tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:TxId/auth:UnqTxIdr', namespaces))
    TransactionID_ProprietaryId = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:TxId/auth:UnqTxIdr/auth:Prtry/auth:Id', namespaces))
    prior_uti = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:PrrTxId/auth:UnqTxIdr', namespaces)) or get_text(
        tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:PrrTxId/auth:Prtry/auth:Id', namespaces)) or get_text(
        tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:PrrTxId/auth:NotAvlbl', namespaces))
    subsequent_position_uti = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:SbsqntTxId/auth:UnqTxIdr', namespaces)) or get_text(
        tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:SbsqntTxId/auth:Prtry/auth:Id', namespaces)) or get_text(
        tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:SbsqntTxId/auth:NotAvlbl', namespaces))
    collateral_portfolio_indicator = 'True' if tx_detail.find(
        './/auth:CmonTradData/auth:TxData/auth:CollPrtflCd/auth:Prtfl/auth:Cd',
        namespaces) is not None else 'False' if tx_detail.find(
        './/auth:CmonTradData/auth:TxData/auth:CollPrtflCd/auth:Prtfl/auth:NoPrtfl', namespaces) is not None else None
    collateral_portfolio_code = get_text(
        tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:CollPrtflCd/auth:Prtfl/auth:Cd', namespaces)) or get_text(
        tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:CollPrtflCd/auth:Prtfl/auth:NoPrtfl', namespaces))
    report_tracking_number = get_text(tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:RptTrckgNb', namespaces))
    venue_of_execution = get_text(tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:PltfmIdr', namespaces))

    transaction_price_type = None
    if tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal', namespaces) is not None:
        transaction_price_type = 'Monetary Value'
    elif tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:TxPric/auth:Pric/auth:Pctg', namespaces) is not None:
        transaction_price_type = 'Percentage'

    transaction_price = None
    if tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal', namespaces) is not None:
        transaction_price = -1 * float(
            tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt',
                           namespaces).text) if tx_detail.find(
            './/auth:CmonTradData/auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal/auth:Sgn',
            namespaces).text == 'False' else float(
            tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt',
                           namespaces).text)
    elif tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:TxPric/auth:Pric/auth:Pctg', namespaces) is not None:
        transaction_price = float(
            tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:TxPric/auth:Pric/auth:Pctg', namespaces).text)


    # Continue extracting other fields as needed...

    # Insert data into the table, handle None values appropriately
    cursor.execute('''
    INSERT INTO trade_activity_report (
        ActionType, Counterparty1, Counterparty1_Nature, Counterparty1_Sector, Counterparty1_ClearingThreshold, 
        DirectlyLinkedActivity, DirectionOfLeg1, DirectionOfLeg2, Direction, Counterparty2_IdType, 
        Counterparty2, Counterparty2_Country, Counterparty2_Nature, Counterparty2_Sector, Counterparty2_ClearingThreshold,
        Counterparty2_ReportingObligation, BrokerID, ReportSubmittingEntityID, ClearingMember, EntityResponsibleForReporting, 
        ValuationAmount, ValuationCurrency, ValuationTimestamp, ValuationMethod, Delta, 
        ReportingTimestamp, ContractType, AssetClass, ProductClassification, ISIN, 
        UPI, UnderlyingId_Type, UnderlyingId_ISIN, CustomBasketCode_Structurer, CustomBasketCode_Id, 
        IdentifierOfTheBasketsConstituents, UnderlyingId_Index_Name, UnderlyingId_Index_Indicator, SettlementCurrency1, SettlementCurrency2, 
        DerivativeBasedOnCryptoAssets, TransactionID_UniqueTransactionId, TransactionID_ProprietaryId, PriorUTI, SubsequentPositionUTI, CollateralPortfolioIndicator, 
        CollateralPortfolioCode, ReportTrackingNumber, VenueOfExecution, TransactionPriceType, TransactionPrice
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        action_type, counterparty1, counterparty1_nature, counterparty1_sector, counterparty1_clearing_threshold,
        directly_linked_activity, direction_of_leg1, direction_of_leg2, direction, counterparty2_idtype,
        counterparty2, counterparty2_country, counterparty2_nature, counterparty2_sector,
        counterparty2_clearing_threshold,
        counterparty2_reporting_obligation, broker_id, report_submitting_entity_id, clearing_member,
        entity_responsible_for_reporting,
        valuation_amount, valuation_currency, valuation_timestamp, valuation_method, delta,
        reporting_timestamp, contract_type, asset_class, product_classification, isin,
        upi, underlying_id_type, underlying_id_isin, custom_basket_code_structurer, custom_basket_code_id,
        identifier_of_the_baskets_constituents, underlying_id_index_name, underlying_id_index_indicator,
        settlement_currency1, settlement_currency2,
        derivative_based_on_crypto_assets, TransactionID_UniqueTransactionId, TransactionID_ProprietaryId, prior_uti, subsequent_position_uti, collateral_portfolio_indicator,
        collateral_portfolio_code, report_tracking_number, venue_of_execution, transaction_price_type,
        transaction_price
    ))

# Commit the transaction
conn.commit()

# Close the connection
conn.close()