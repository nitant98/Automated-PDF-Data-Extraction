import os
import boto3
import requests
from dotenv import load_dotenv
from io import BytesIO
from xml.etree import ElementTree

# Load environment variables
load_dotenv()

# AWS credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# S3 Bucket name
S3_BUCKET_NAME = "bigdatas3team4"

# Initialize a boto3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def list_s3_objects(bucket_name):
    """List PDF objects in an S3 bucket."""
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].lower().endswith('.pdf')]

def process_files_with_grobid(bucket_name):
    """Process PDF files from S3 bucket with Grobid and save XML outputs locally."""
    pdf_files = list_s3_objects(bucket_name)
    
    # Ensure the output directories exist
    xml_output_dir = "xml"
    txt_output_dir = "txt"
    os.makedirs(xml_output_dir, exist_ok=True)
    os.makedirs(txt_output_dir, exist_ok=True)
    
    for pdf_file in pdf_files:
        # Download PDF file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=pdf_file)
        pdf_content = response['Body'].read()
        
        # Prepare the request to Grobid
        files = {'input': (pdf_file, BytesIO(pdf_content), 'application/pdf')}
        response = requests.post("http://host.docker.internal:8070/api/processFulltextDocument", files=files)
        
        if response.status_code == 200:
            # Save the Grobid output to an XML file
            xml_filename = f"Grobid_{os.path.basename(pdf_file).replace('.pdf', '')}_combined.xml"
            xml_filepath = os.path.join(xml_output_dir, xml_filename)
            with open(xml_filepath, 'wb') as f:
                f.write(response.content)
            print(f"Processed {pdf_file} and saved XML output to {xml_filepath}")
            
            # Convert XML to TXT and save
            convert_xml_to_txt(xml_filepath, txt_output_dir)
        else:
            print(f"Failed to process {pdf_file} with Grobid. Status code: {response.status_code}")

def convert_xml_to_txt(xml_file_path, txt_output_dir):
    """Converts an XML file to a TXT file and saves it in the specified directory."""
    try:
        tree = ElementTree.parse(xml_file_path)
        root = tree.getroot()
        # Extract text from XML. This is a basic example and might need to be adjusted based on your XML structure.
        text_content = '\n'.join(elem.text for elem in root.iter() if elem.text)
        
        txt_filename = os.path.basename(xml_file_path).replace('.xml', '.txt')
        txt_filepath = os.path.join(txt_output_dir, txt_filename)
        
        with open(txt_filepath, 'w', encoding='utf-8') as txt_file:
            txt_file.write(text_content)
        print(f"Converted {xml_file_path} to TXT and saved to {txt_filepath}")
    except Exception as e:
        print(f"Error converting XML to TXT for {xml_file_path}: {e}")

process_files_with_grobid(S3_BUCKET_NAME)
