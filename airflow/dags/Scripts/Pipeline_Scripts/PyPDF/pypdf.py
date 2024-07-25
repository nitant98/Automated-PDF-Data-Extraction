import os
import tempfile
import PyPDF2
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Hardcoded S3 bucket name
S3_BUCKET_NAME = "bigdatas3team4"

# Initialize a boto3 client without specifying a region
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def list_s3_objects(bucket_name, prefix=''):
    """List objects in an S3 bucket."""
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                yield obj["Key"]

def download_file_from_s3(bucket_name, object_name):
    """Download a file from S3 to a temporary file."""
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        s3_client.download_file(bucket_name, object_name, temp_file.name)
        return temp_file.name

def extract_text_pypdf(s3_object_name, output_folder="PyPDF_Extracted"):
    """Extract text from a PDF file in S3 and save it to a local text file."""
    # Ensure the output_folder exists
    os.makedirs(output_folder, exist_ok=True)
    text = ""
    
    # Download PDF from S3
    pdf_path = download_file_from_s3(S3_BUCKET_NAME, s3_object_name)
    
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page_num in range(len(reader.pages)):
                text += reader.pages[page_num].extract_text()
    except Exception as e:
        print(f"Error reading PDF file {s3_object_name}: {e}")
    finally:
        os.remove(pdf_path)  # Clean up the temporary PDF file
    
    output_filename = os.path.join(output_folder, f'{os.path.basename(s3_object_name)[:-4]}.txt')
    try:
        with open(output_filename, 'w') as file:
            file.write(text)
        print(f"Text extracted and saved to {output_filename}")
    except Exception as e:
        print(f"Error writing output file {output_filename}: {e}")

def process_all_pdfs(bucket_name, output_folder="PyPDF"):
    """Process all PDF files in the specified S3 bucket."""
    for s3_object_name in list_s3_objects(bucket_name):
        if s3_object_name.lower().endswith('.pdf'):
            print(f"Processing {s3_object_name}...")
            extract_text_pypdf(s3_object_name, output_folder)

# Example usage
process_all_pdfs(S3_BUCKET_NAME)
