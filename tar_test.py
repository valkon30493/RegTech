import os
from lxml import etree

# Path to the large XML file
xml_file_path = "C:/Users/valentinosko/Desktop/REGIS_EOD_Reports/Trade_Activity_Report/eudbp6tyb000_S030_20250120000000_TAR_0014.xml"

# Define namespaces for the XML (update these as necessary for your file)
NAMESPACES = {
    "auth": "urn:iso:std:iso:20022:tech:xsd:auth.030.001.03"
}

def parse_large_xml(file_path):
    """Parse a large XML file."""
    try:
        # Parse the XML file
        tree = etree.parse(file_path)
        root = tree.getroot()

        # Example: Extract some data
        for trad_data in root.findall('.//auth:TradData', namespaces=NAMESPACES):
            tx_id = trad_data.find('.//auth:TxId/auth:UnqTxIdr', namespaces=NAMESPACES)
            print(f"Transaction ID: {tx_id.text if tx_id is not None else 'N/A'}")

        print("Parsing complete.")
    except Exception as e:
        print(f"Error parsing XML: {e}")

# Run the parser
if os.path.isfile(xml_file_path):
    parse_large_xml(xml_file_path)
else:
    print(f"File not found: {xml_file_path}")
