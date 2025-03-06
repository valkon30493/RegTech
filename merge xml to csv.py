import xml.etree.ElementTree as ET
import os
import csv


def xml_to_csv(input_folder, output_csv):
    # Define namespaces
    namespaces = {
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "head": "urn:iso:std:iso:20022:tech:xsd:head.001.001.01",
        "auth": "urn:iso:std:iso:20022:tech:xsd:auth.030.001.03"
    }

    # Open the CSV file for writing
    with open(output_csv, mode="w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write the header row for the CSV
        csv_writer.writerow(["FromID", "ToID", "BizMsgIdr", "MsgDefIdr", "CreDt", "NbRcrds", "DataSetActn"])

        # Iterate through all XML files in the input folder
        for filename in os.listdir(input_folder):
            if filename.endswith(".xml"):
                file_path = os.path.join(input_folder, filename)
                tree = ET.parse(file_path)
                root = tree.getroot()

                # Extract relevant fields
                hdr = root.find(".//head:AppHdr", namespaces)
                pyld = root.find(".//auth:DerivsTradRpt", namespaces)

                if hdr is not None and pyld is not None:
                    # Extract values for the CSV
                    from_id = hdr.find(".//head:Fr/head:OrgId/head:Id/head:OrgId/head:Othr/head:Id", namespaces)
                    to_id = hdr.find(".//head:To/head:OrgId/head:Id/head:OrgId/head:Othr/head:Id", namespaces)
                    biz_msg_idr = hdr.find(".//head:BizMsgIdr", namespaces)
                    msg_def_idr = hdr.find(".//head:MsgDefIdr", namespaces)
                    cre_dt = hdr.find(".//head:CreDt", namespaces)
                    nb_rcrds = pyld.find(".//auth:RptHdr/auth:NbRcrds", namespaces)
                    data_set_actn = pyld.find(".//auth:TradData/auth:DataSetActn", namespaces)

                    # Write the row to the CSV file
                    csv_writer.writerow([
                        from_id.text if from_id is not None else "",
                        to_id.text if to_id is not None else "",
                        biz_msg_idr.text if biz_msg_idr is not None else "",
                        msg_def_idr.text if msg_def_idr is not None else "",
                        cre_dt.text if cre_dt is not None else "",
                        nb_rcrds.text if nb_rcrds is not None else "",
                        data_set_actn.text if data_set_actn is not None else ""
                    ])

    print(f"CSV file created: {output_csv}")


# Input folder containing the XML files and the output CSV file name
input_folder = r"C:\Users\valentinosko\Downloads\xml"
output_csv = r"C:\Users\valentinosko\Downloads\xml\merged.csv"

# Call the function to convert XML to CSV
xml_to_csv(input_folder, output_csv)
