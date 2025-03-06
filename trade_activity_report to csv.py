import xml.etree.ElementTree as ET
import csv

def extract_all_data_with_action_type(input_file, output_file):
    # Define namespaces
    namespaces = {
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "head": "urn:iso:std:iso:20022:tech:xsd:head.001.001.01",
        "auth": "urn:iso:std:iso:20022:tech:xsd:auth.030.001.03"
    }

    # Parse the XML file
    tree = ET.parse(input_file)
    root = tree.getroot()

    # Open CSV file for writing
    with open(output_file, mode="w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write the header row
        csv_writer.writerow([
            "ActionType", "ReportingCounterparty", "ReportingCounterpartySector",
            "ReportingCounterpartySide", "OtherCounterpartyLEI", "OtherCounterpartySector",
            "ReportingObligation", "TransactionID", "ProductClassification", "AssetClass",
            "NotionalAmountFirstLeg", "NotionalAmountSecondLeg", "SettlementCurrency",
            "ExecutionTimestamp", "ValuationAmount", "ValuationCurrency", "UnderlyingAsset",
            "MaturityDate", "ClearingObligation", "Level"
        ])

        # Extract transaction data
        for trad_data in root.findall('.//auth:TradData', namespaces):
            # Determine the ActionType
            action_type = None
            if trad_data.find('.//auth:Rpt/auth:New', namespaces) is not None:
                action_type = 'NEWT'
            elif trad_data.find('.//auth:Rpt/auth:Mod', namespaces) is not None:
                action_type = 'MODI'
            elif trad_data.find('.//auth:Rpt/auth:Crrctn', namespaces) is not None:
                action_type = 'CORR'
            elif trad_data.find('.//auth:Rpt/auth:Termntn', namespaces) is not None:
                action_type = 'TERM'
            elif trad_data.find('.//auth:Rpt/auth:PosCmpnt', namespaces) is not None:
                action_type = 'POSC'
            elif trad_data.find('.//auth:Rpt/auth:ValtnUpd', namespaces) is not None:
                action_type = 'VALU'
            elif trad_data.find('.//auth:Rpt/auth:Err', namespaces) is not None:
                action_type = 'EROR'
            elif trad_data.find('.//auth:Rpt/auth:Rvv', namespaces) is not None:
                action_type = 'REVI'

            # Base data extraction
            base_data = {
                "ActionType": action_type,
                "ReportingCounterparty": trad_data.find('.//auth:RptgCtrPty/auth:Id/auth:Lgl/auth:Id/auth:LEI', namespaces).text if trad_data.find('.//auth:RptgCtrPty/auth:Id/auth:Lgl/auth:Id/auth:LEI', namespaces) else None,
                "ReportingCounterpartySector": trad_data.find('.//auth:RptgCtrPty/auth:Ntr/auth:FI/auth:Sctr/auth:Cd', namespaces).text if trad_data.find('.//auth:RptgCtrPty/auth:Ntr/auth:FI/auth:Sctr/auth:Cd', namespaces) else None,
                "ReportingCounterpartySide": trad_data.find('.//auth:RptgCtrPty/auth:DrctnOrSd/auth:CtrPtySd', namespaces).text if trad_data.find('.//auth:RptgCtrPty/auth:DrctnOrSd/auth:CtrPtySd', namespaces) else None,
                "OtherCounterpartyLEI": trad_data.find('.//auth:OthrCtrPty/auth:Id/auth:Lgl/auth:Id/auth:LEI', namespaces).text if trad_data.find('.//auth:OthrCtrPty/auth:Id/auth:Lgl/auth:Id/auth:LEI', namespaces) else None,
                "OtherCounterpartySector": trad_data.find('.//auth:OthrCtrPty/auth:Ntr/auth:FI/auth:Sctr/auth:Cd', namespaces).text if trad_data.find('.//auth:OthrCtrPty/auth:Ntr/auth:FI/auth:Sctr/auth:Cd', namespaces) else None,
                "ReportingObligation": trad_data.find('.//auth:OthrCtrPty/auth:RptgOblgtn', namespaces).text if trad_data.find('.//auth:OthrCtrPty/auth:RptgOblgtn', namespaces) else None,
            }

            # Nested loop for each transaction
            for tx_detail in trad_data.findall('.//auth:TxData', namespaces):
                # Extract transaction-specific data
                transaction_data = {
                    "TransactionID": tx_detail.find('.//auth:TxId/auth:UnqTxIdr', namespaces).text if tx_detail.find('.//auth:TxId/auth:UnqTxIdr', namespaces) else None,
                    "ProductClassification": trad_data.find('.//auth:CtrctData/auth:PdctClssfctn', namespaces).text if trad_data.find('.//auth:CtrctData/auth:PdctClssfctn', namespaces) else None,
                    "AssetClass": trad_data.find('.//auth:CtrctData/auth:AsstClss', namespaces).text if trad_data.find('.//auth:CtrctData/auth:AsstClss', namespaces) else None,
                    "NotionalAmountFirstLeg": trad_data.find('.//auth:NtnlAmt/auth:FrstLeg/auth:Amt/auth:Amt', namespaces).text if trad_data.find('.//auth:NtnlAmt/auth:FrstLeg/auth:Amt/auth:Amt', namespaces) else None,
                    "NotionalAmountSecondLeg": trad_data.find('.//auth:NtnlAmt/auth:ScndLeg/auth:Amt/auth:Amt', namespaces).text if trad_data.find('.//auth:NtnlAmt/auth:ScndLeg/auth:Amt/auth:Amt', namespaces) else None,
                    "SettlementCurrency": trad_data.find('.//auth:CtrctData/auth:SttlmCcy/auth:Ccy', namespaces).text if trad_data.find('.//auth:CtrctData/auth:SttlmCcy/auth:Ccy', namespaces) else None,
                    "ExecutionTimestamp": tx_detail.find('.//auth:ExctnTmStmp', namespaces).text if tx_detail.find('.//auth:ExctnTmStmp', namespaces) else None,
                    "ValuationAmount": tx_detail.find('.//auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt', namespaces).text if tx_detail.find('.//auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt', namespaces) else None,
                    "ValuationCurrency": tx_detail.find('.//auth:TxPric/auth:Pric/auth:MntryVal', namespaces).attrib.get('Ccy') if tx_detail.find('.//auth:TxPric/auth:Pric/auth:MntryVal', namespaces) else None,
                    "UnderlyingAsset": trad_data.find('.//auth:CtrctData/auth:UndrlygInstrm/auth:ISIN', namespaces).text if trad_data.find('.//auth:CtrctData/auth:UndrlygInstrm/auth:ISIN', namespaces) else None,
                    "MaturityDate": trad_data.find('.//auth:CtrctData/auth:MtrtyDt', namespaces).text if trad_data.find('.//auth:CtrctData/auth:MtrtyDt', namespaces) else None,
                    "ClearingObligation": tx_detail.find('.//auth:ClrOblgtn', namespaces).text if tx_detail.find('.//auth:ClrOblgtn', namespaces) else None,
                    "Level": trad_data.find('.//auth:Lvl', namespaces).text if trad_data.find('.//auth:Lvl', namespaces) else None,
                }

                # Combine and write to CSV
                row = {**base_data, **transaction_data}
                csv_writer.writerow(row.values())

    print(f"Data successfully written to {output_file}")


# Input XML file and output CSV file paths
input_file = r'C:\Users\valentinosko\Desktop\REGIS_EOD_Reports\Trade_Activity_Report\eudbp6tyb000_S030_20250120000000_TAR_0014.xml'
output_file = r'C:\Users\valentinosko\Desktop\REGIS_EOD_Reports\Trade_Activity_Report\eudbp6tyb000_S030_20250120000000_TAR_0014.csv'

# Call the function
extract_all_data_with_action_type(input_file, output_file)
