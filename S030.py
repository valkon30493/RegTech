import pyodbc
from lxml import etree

# Paths to your files
xml_file = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Trade Activity Report/TRUVT_DATTAR_5493003EETVWYSIJ5A20_SE-250130_001001.xml'
xsd_file = 'C:/Users/valentinosko/Desktop/Unavista_EOD_Reports/Marex/Trade Activity Report/head.003.001.01.xsd'

# SQL Server connection details
connection_string = (
    'DRIVER={SQL Server};'
    'SERVER=AZR-WE-BI-02;'
    'DATABASE=RTS;'
    'Trusted_Connection=yes;'
)

# Batch size for commits
BATCH_SIZE = 100

# Load and parse the XSD file
try:
    with open(xsd_file, 'rb') as f:
        schema_root = etree.XML(f.read())
    schema = etree.XMLSchema(schema_root)
    parser = etree.XMLParser(schema=schema)
except Exception as e:
    print(f"Error loading XSD schema: {e}")
    exit(1)  # Exit if schema loading fails

# Parse the XML file
try:
    tree = etree.parse(xml_file, parser)
    print("XML is valid.")
except etree.XMLSchemaError as e:
    print(f"XML schema validation error: {e}")
    exit(1)  # Exit if XML schema validation fails

# Define your namespaces
namespaces = {'auth': 'urn:iso:std:iso:20022:tech:xsd:auth.030.001.03'}
root = tree.getroot()

# Utility Functions
def get_text(element):
    """Safely extract text from an XML element."""
    return element.text.strip() if element is not None and element.text else None

def get_attr(element, attr_name):
    """Safely extract attribute from an XML element."""
    return element.get(attr_name) if element is not None else None

# Connect to SQL Server
try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.fast_executemany = True  # Optimize batch execution
except Exception as e:
    print(f"Database connection error: {e}")
    exit(1)

# Example extraction based on your structure (customize as needed)
tx_details_elements = root.findall('.//auth:DerivsTradRpt/auth:TradData/auth:Rpt', namespaces)
batch = []  # Store rows in the current batch

try:
    for tx_detail in tx_details_elements:
        # Determine ActionType
        action_type = next(
            (t for t in ['NEWT', 'MODI', 'CORR', 'TERM', 'POSC', 'VALU', 'EROR', 'REVI']
             if tx_detail.find(f".//auth:{t.capitalize()}", namespaces) is not None),
            None
        )


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

        # Valuation Details (Handling float conversion safely)
        valuation_amount_element = tx_detail.find('.//auth:CtrPtySpcfcData/auth:Valtn/auth:CtrctVal/auth:Amt',
                                                  namespaces)
        try:
            valuation_amount = -1 * float(valuation_amount_element.text) if valuation_amount_element.get(
                'Sgn') == 'False' else float(valuation_amount_element.text)
        except (TypeError, ValueError, AttributeError):
            valuation_amount = None

        valuation_currency = get_attr(valuation_amount_element, 'Ccy')


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
            tx_detail.find('.//auth:CmonTradData/auth:TxData/auth:TxId/auth:Prtry/auth:Id', namespaces))
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

        # Transaction Price Handling
        transaction_price_element = tx_detail.find(
            './/auth:CmonTradData/auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt', namespaces)
        try:
            transaction_price = float(
                transaction_price_element.text) if transaction_price_element is not None else None
            if transaction_price_element is not None and tx_detail.find(
                    './/auth:CmonTradData/auth:TxData/auth:TxPric/auth:Pric/auth:MntryVal/auth:Sgn',
                    namespaces).text == 'False':
                transaction_price *= -1
        except (TypeError, ValueError, AttributeError):
            transaction_price = None


        # Add the row to the current batch
        batch.append((
            action_type,
            counterparty1,
            counterparty1_nature,
            counterparty1_sector,
            counterparty1_clearing_threshold,
            directly_linked_activity,
            direction_of_leg1,
            direction_of_leg2,
            direction,
            counterparty2_idtype,
            counterparty2,
            counterparty2_country,
            counterparty2_nature,
            counterparty2_sector,
            counterparty2_clearing_threshold,
            counterparty2_reporting_obligation,
            broker_id,
            report_submitting_entity_id,
            clearing_member,
            entity_responsible_for_reporting,
            valuation_amount,
            valuation_currency,
            valuation_timestamp,
            valuation_method,
            delta,
            reporting_timestamp,
            contract_type,
            asset_class,
            product_classification,
            isin,
            upi,
            underlying_id_type,
            underlying_id_isin,
            custom_basket_code_structurer,
            custom_basket_code_id,
            identifier_of_the_baskets_constituents,
            underlying_id_index_name,
            underlying_id_index_indicator,
            settlement_currency1,
            settlement_currency2,
            derivative_based_on_crypto_assets,
            TransactionID_UniqueTransactionId,
            TransactionID_ProprietaryId,
            prior_uti,
            subsequent_position_uti,
            collateral_portfolio_indicator,
            collateral_portfolio_code,
            report_tracking_number,
            venue_of_execution,
            transaction_price_type,
            transaction_price
        ))

        # Insert batch if batch size is reached
        if len(batch) >= BATCH_SIZE:
            cursor.executemany('''
                        INSERT INTO UnavistaMarex_Trade_Activity_Report (
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
                    ''', batch)
            conn.commit()  # Commit the batch
            print(f"Committed {len(batch)} records.")
            batch = []  # Clear the batch

     # Commit any remaining records in the last batch
    if batch:
        cursor.executemany('''
                    INSERT INTO UnavistaMarex_Trade_Activity_Report (
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
                ''', batch)
        conn.commit()
        print(f"Committed {len(batch)} remaining records.")


except Exception as e:
    print(f"Error during data insertion: {e}")
    conn.rollback()

finally:
    conn.close()
    print("Database connection closed.")

