import xml.etree.ElementTree as ET
import csv

def extract_all_data_with_clearing(input_file, output_file):
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
            "FromID", "ToID", "BizMsgIdr", "MsgDefIdr", "CreDt", "NbRcrds",
            "TxId", "TxPrice", "TxPriceCcy", "NtnlAmt", "NtnlAmtCcy", "NtnlQty",
            "DlvryTp", "ExctnTmStmp", "FctvDt", "XprtnDt", "SttlmDt",
            "AsstClss", "PdctClssfctn", "CtrctTp",
            "RptgCtrPty.LEI", "RptgCtrPty.Sector", "RptgCtrPty.Side",
            "OthrCtrPty.LEI", "OthrCtrPty.Sector", "SubmitgAgt",
            "ClrMbr.LEI", "ClrAcctTp", "ClrSttlmDt", "CCP.LEI"
        ])

        # Extract header data
        hdr = root.find(".//head:AppHdr", namespaces)
        from_id = hdr.find(".//head:Fr/head:OrgId/head:Id/head:OrgId/head:Othr/head:Id", namespaces).text if hdr.find(".//head:Fr/head:OrgId/head:Id/head:OrgId/head:Othr/head:Id", namespaces) else ""
        to_id = hdr.find(".//head:To/head:OrgId/head:Id/head:OrgId/head:Othr/head:Id", namespaces).text if hdr.find(".//head:To/head:OrgId/head:Id/head:OrgId/head:Othr/head:Id", namespaces) else ""
        biz_msg_idr = hdr.find(".//head:BizMsgIdr", namespaces).text if hdr.find(".//head:BizMsgIdr", namespaces) else ""
        msg_def_idr = hdr.find(".//head:MsgDefIdr", namespaces).text if hdr.find(".//head:MsgDefIdr", namespaces) else ""
        cre_dt = hdr.find(".//head:CreDt", namespaces).text if hdr.find(".//head:CreDt", namespaces) else ""

        # Extract report data
        reports = root.findall(".//auth:DerivsTradRpt", namespaces)
        for report in reports:
            nb_rcrds = report.find(".//auth:RptHdr/auth:NbRcrds", namespaces).text if report.find(".//auth:RptHdr/auth:NbRcrds", namespaces) else ""

            # Extract transactions
            tx_data_list = report.findall(".//auth:TxData", namespaces)
            for tx_data in tx_data_list:
                # Transaction-level details
                tx_id = tx_data.find(".//auth:TxId/auth:UnqTxIdr", namespaces).text if tx_data.find(".//auth:TxId/auth:UnqTxIdr", namespaces) else ""
                tx_price_element = tx_data.find(".//auth:TxPric/auth:Pric/auth:MntryVal/auth:Amt", namespaces)
                tx_price = tx_price_element.text if tx_price_element is not None else ""
                tx_price_ccy = tx_price_element.attrib.get("Ccy") if tx_price_element is not None else ""
                ntnl_amt_element = tx_data.find(".//auth:NtnlAmt/auth:FrstLeg/auth:Amt/auth:Amt", namespaces)
                ntnl_amt = ntnl_amt_element.text if ntnl_amt_element is not None else ""
                ntnl_amt_ccy = ntnl_amt_element.attrib.get("Ccy") if ntnl_amt_element is not None else ""
                ntnl_qty = tx_data.find(".//auth:NtnlQty/auth:FrstLeg/auth:TtlQty", namespaces).text if tx_data.find(".//auth:NtnlQty/auth:FrstLeg/auth:TtlQty", namespaces) else ""
                dlvry_tp = tx_data.find(".//auth:DlvryTp", namespaces).text if tx_data.find(".//auth:DlvryTp", namespaces) else ""
                exctn_tmstmp = tx_data.find(".//auth:ExctnTmStmp", namespaces).text if tx_data.find(".//auth:ExctnTmStmp", namespaces) else ""
                fctv_dt = tx_data.find(".//auth:FctvDt", namespaces).text if tx_data.find(".//auth:FctvDt", namespaces) else ""
                xprtn_dt = tx_data.find(".//auth:XprtnDt", namespaces).text if tx_data.find(".//auth:XprtnDt", namespaces) else ""
                sttlm_dt = tx_data.find(".//auth:SttlmDt", namespaces).text if tx_data.find(".//auth:SttlmDt", namespaces) else ""
                asst_clss = tx_data.find(".//auth:CtrctData/auth:AsstClss", namespaces).text if tx_data.find(".//auth:CtrctData/auth:AsstClss", namespaces) else ""
                pdct_clssfctn = tx_data.find(".//auth:CtrctData/auth:PdctClssfctn", namespaces).text if tx_data.find(".//auth:CtrctData/auth:PdctClssfctn", namespaces) else ""
                ctrct_tp = tx_data.find(".//auth:CtrctData/auth:CtrctTp", namespaces).text if tx_data.find(".//auth:CtrctData/auth:CtrctTp", namespaces) else ""

                # Clearing details
                clr_mbr = tx_data.find(".//auth:ClrMbr/auth:Id/auth:LEI", namespaces)
                clr_mbr_lei = clr_mbr.text if clr_mbr is not None else ""
                clr_acct_tp = tx_data.find(".//auth:ClrAcctTp", namespaces).text if tx_data.find(".//auth:ClrAcctTp", namespaces) else ""
                clr_sttlm_dt = tx_data.find(".//auth:ClrSttlmDt", namespaces).text if tx_data.find(".//auth:ClrSttlmDt", namespaces) else ""
                ccp = tx_data.find(".//auth:CCP/auth:Id/auth:LEI", namespaces)
                ccp_lei = ccp.text if ccp is not None else ""

                # Write row to CSV
                csv_writer.writerow([
                    from_id, to_id, biz_msg_idr, msg_def_idr, cre_dt, nb_rcrds,
                    tx_id, tx_price, tx_price_ccy, ntnl_amt, ntnl_amt_ccy, ntnl_qty,
                    dlvry_tp, exctn_tmstmp, fctv_dt, xprtn_dt, sttlm_dt,
                    asst_clss, pdct_clssfctn, ctrct_tp,
                    clr_mbr_lei, clr_acct_tp, clr_sttlm_dt, ccp_lei
                ])

    print(f"Data successfully written to {output_file}")

# Input XML file and output CSV file paths
input_file = r"C:\Users\valentinosko\Downloads\CLIENT_TRUVT_DATTAR_5493003EETVWYSIJ5A20_SE-250107.xml"
output_file = r"C:\Users\valentinosko\Downloads\output_with_clearing.csv"

# Call the function
extract_all_data_with_clearing(input_file, output_file)
