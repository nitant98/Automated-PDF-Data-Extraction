from bs4 import BeautifulSoup
import lxml.etree as ET  # Use lxml's etree for full XPath support
from xml.etree import ElementTree as BT
import pandas as pd
import json
import os
import re
import csv
import sys
from pathlib import Path

# Add the root directory to sys.path
root_dir = str(Path(__file__).resolve().parent.parent)  # Adjust the number of parent calls as necessary
sys.path.append(root_dir)
from Scripts.Validation import ContentValidator, MetadataValidator

FILENAME_MAPPING = {
    'Grobid_2024-l1-topics-combined-2_combined.xml': {
        'content_csv': 'grobid_content_2024_l1_topics_combined_2.csv',
        'metadata_csv': 'grobid_metadata_2024_l1_topics_combined_2.csv'
    },
    'Grobid_2024-l2-topics-combined-2_combined.xml': {
        'content_csv': 'grobid_content_2024_l2_topics_combined_2.csv',
        'metadata_csv': 'grobid_metadata_2024_l2_topics_combined_2.csv'
    },
    'Grobid_2024-l3-topics-combined-2_combined.xml': {
        'content_csv': 'grobid_content_2024_l3_topics_combined_2.csv',
        'metadata_csv': 'grobid_metadata_2024_l3_topics_combined_2.csv'
    }
}

def remove_special_characters(s):
    return re.sub(r'[^A-Za-z0-9 ]+', '', s)

def print_validation(validation_errors):
    if validation_errors:
        print("Validation Errors Encountered:")
    for error in validation_errors:
        print(f"Row: {error['row']}")
        print(f"Error: {error['error']}\n")
    else:
        print("No validation errors encountered.")


class ContentPDFClass:
    def __init__(self, file_path):
        self.file_path = file_path
        self.namespaces = {
            'tei': 'http://www.tei-c.org/ns/1.0'
        }
        self.data = []
    
    def replace_symbols_with_numbers(self, text, symbol):
        parts = text.split(symbol)
        new_text = parts[0]
        for i, part in enumerate(parts[1:], 1):
            new_text += f"{i}. {part}"
        return new_text

    def parse_xml_and_replace_symbols(self):
        tree = BT.parse(self.file_path)
        root = tree.getroot()
        
        current_title = ''
        for div in root.findall('.//tei:div', self.namespaces):
            head = div.find('.//tei:head', self.namespaces)
            paragraphs = div.findall('.//tei:p', self.namespaces)
            head_text = head.text if head is not None else 'No Title'

            # If 'head' exists and there are no paragraphs, adjust 'current_title' based on 'head'
            if head is not None and not paragraphs:
                current_title = head_text if head_text != 'LEARNING OUTCOMES' else current_title

            # If both 'head' and 'paragraphs' exist, process as subtitle and content
            elif paragraphs:
                subtitle = head_text if head_text != 'No Title' else 'No Subtitle'
                content = " ".join(p.text for p in paragraphs if p.text)
                content = self.replace_symbols_with_numbers(content, '□')

                current_title = current_title or 'Not Available'

                # Remove Special Characters
                current_title = remove_special_characters(current_title)
        
                self.data.append([current_title, subtitle, content])

    
    def save_to_csv(self, csv_file_path):
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Title', 'Subtitle', 'Content'])
            writer.writerows(self.data)

class MetadataPDFClass:
    def __init__(self, xml_file_path):
        self.tree = ET.parse(xml_file_path)
        self.root = self.tree.getroot()
        self.namespaces = {
            'tei': 'http://www.tei-c.org/ns/1.0',
            'xlink': 'http://www.w3.org/1999/xlink'
        }
    def replace_symbols_with_numbers(self, text, symbol):
        parts = text.split(symbol)
        new_text = parts[0]
        for i, part in enumerate(parts[1:], 1):
            new_text += f"{i}. {part}"
        return new_text
    
    def extract_first_item(self, xpath_result):
        if xpath_result:
            # Clean and return the first item
            return xpath_result[0].replace('\n', '').replace('\t', '').strip()
        else:
            return "No Data"

    def extract_abstract(self):
        # Extracting all <div> elements within <abstract>
        abstract_divs = self.root.xpath('//tei:profileDesc/tei:abstract/tei:div', namespaces=self.namespaces)
        abstract_texts = []
        for div in abstract_divs:
            # For each <div>, extract <head> text (if present) and all <p> text, and combine them
            head_text = " ".join(div.xpath('./tei:head/text()', namespaces=self.namespaces)).strip()
            p_texts = " ".join(div.xpath('./tei:p/text()', namespaces=self.namespaces)).strip()
            div_text = (head_text + " " + p_texts).strip()
            abstract_texts.append(div_text)
        
        combined_abstract = " ".join(abstract_texts)
        # Apply replace_symbols_with_numbers to the combined abstract text
        final_abstract = self.replace_symbols_with_numbers(combined_abstract, '□')
        
        return final_abstract

    def extract_metadata(self):
        metadata_dict = {
            "Title": self.extract_first_item(self.root.xpath('//tei:titleStmt/tei:title[@level="a" and @type="main"]/text()', namespaces=self.namespaces)),
            "Publisher": self.extract_first_item(self.root.xpath('//tei:publicationStmt/tei:publisher/text()', namespaces=self.namespaces)),
            "AvailabilityStatus": self.extract_first_item(self.root.xpath('//tei:availability/@status', namespaces=self.namespaces)),
            "BiblicalReference": self.extract_first_item(self.root.xpath('//tei:back//tei:listBibl/text()', namespaces=self.namespaces)),
            "AppInfoDescription": self.extract_first_item(self.root.xpath('//tei:application/tei:desc/text()', namespaces=self.namespaces)),
            "Abstract": self.extract_abstract(),
        }
        return metadata_dict

def process_files(input_dir, output_dir):
    for xml_filename, paths in FILENAME_MAPPING.items():
        xml_file_path = os.path.join(input_dir, xml_filename).replace('\\','/')
        content_pdf = ContentPDFClass(xml_file_path)
        content_pdf.parse_xml_and_replace_symbols()
        metadata_data = MetadataPDFClass(xml_file_path).extract_metadata()

        content_csv_dir = os.path.join(output_dir, 'content', 'csv')
        metadata_csv_dir = os.path.join(output_dir, 'metadata', 'csv')
        os.makedirs(content_csv_dir, exist_ok=True)
        os.makedirs(metadata_csv_dir, exist_ok=True)

        # Save content and metadata to CSV
        # content_csv_path = os.path.join(output_dir, 'content', 'csv', paths['content_csv'])
        # metadata_csv_path = os.path.join(output_dir, 'metadata', 'csv', paths['metadata_csv'])

        content_csv_path = os.path.join(content_csv_dir, paths['content_csv'])
        metadata_csv_path = os.path.join(metadata_csv_dir, paths['metadata_csv'])

        # Writing Content & Metadata
        content_pdf.save_to_csv(content_csv_path)
        pd.DataFrame([metadata_data]).to_csv(metadata_csv_path, index=False)

        # Validating all Content rows and saving clean files
        content_class_instance = ContentValidator(str(content_csv_path), str(content_csv_path))
        valid_topics, validation_errors = content_class_instance.clean_and_validate_content_csv()
        
        print('*************** Content Validation ***************')
        print(f"Valid rows: {len(valid_topics)}, Validation errors: {len(validation_errors)}")
        print_validation(validation_errors)
        print()

        # Validating all Metadata rows and saving clean files
        metadata_class_instance = MetadataValidator(str(metadata_csv_path), str(metadata_csv_path))
        valid_topics, validation_errors = metadata_class_instance.clean_and_validate_metadata_csv()
        
        print('*************** Metadata Validation ***************')
        print(f"Valid rows: {len(valid_topics)}, Validation errors: {len(validation_errors)}")
        print_validation(validation_errors)
        print()



if __name__ == "__main__":
    input_dir = 'Grobid/xml'
    output_dir = 'parsed_into_schema'
    process_files(input_dir, output_dir)


