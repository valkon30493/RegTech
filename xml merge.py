import xml.etree.ElementTree as ET
import os

def merge_xml_files(input_folder, output_file):
    # Define namespaces
    namespaces = {
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "head": "urn:iso:std:iso:20022:tech:xsd:head.003.001.01",
    }
    # Register namespaces to avoid prefix issues
    for prefix, uri in namespaces.items():
        ET.register_namespace(prefix, uri)

    # Root element for the merged XML
    merged_root = ET.Element("BizData", attrib={
        f"{{{namespaces['xsi']}}}schemaLocation": "urn:iso:std:iso:20022:tech:xsd:head.003.001.01 head.003.001.01.xsd"
    })

    # Iterate through all XML files in the input folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".xml"):
            file_path = os.path.join(input_folder, filename)
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Copy the <Hdr> and <Pyld> elements (or others as needed) into the merged XML
            hdr = root.find("head:Hdr", namespaces)
            pyld = root.find("head:Pyld", namespaces)
            if hdr is not None:
                merged_root.append(hdr)
            if pyld is not None:
                merged_root.append(pyld)

    # Write the merged XML to the output file
    merged_tree = ET.ElementTree(merged_root)
    merged_tree.write(output_file, encoding="utf-8", xml_declaration=True)
    print(f"Merged XML written to: {output_file}")

# Input folder containing the XML files and the output file name
input_folder = r"C:\Users\valentinosko\Downloads\xml"
output_file = r"C:\Users\valentinosko\Downloads\xml\merged.xml"

# Call the function to merge files
merge_xml_files(input_folder, output_file)
