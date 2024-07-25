from pydantic import BaseModel, HttpUrl, Field, ValidationError, validator, ValidationInfo
from pydantic.functional_validators import field_validator
from pathlib import Path
import re
import csv
from typing import List, Optional
import os  # For directory operations


class Content(BaseModel):
    Title: str 
    Subtitle: str
    Content: str
    
    @field_validator('Content') 
    def content_must_not_be_blank(cls, value):
        if not value.strip():
            raise ValueError("Content must not be blank")
        return value
    
    @field_validator('Title', 'Content')  
    def must_not_be_blank(cls, value):
        if not value.strip():
            raise ValueError("Must not be blank")
        return value

    @field_validator('Title') 
    def title_only_letters_and_numbers(cls, value):
        if not value.replace(" ", "").isalnum():
            raise ValueError("Title must consist of only letters and numbers, without any special characters")
        return value.strip()

    @field_validator('Content')  
    def check_content_special_characters(cls, value):
        if any(char in set('□') for char in value):
            raise ValueError("Content must not contain special characters")
        return value

    
class ContentValidator:
    def __init__(self, csv_file_path: str, output_file_path:str):
        self.csv_file_path = csv_file_path
        self.output_file_name = output_file_path  
    
    def clean_and_validate_content_csv(self) -> List[Content]:
        valid_rows = []
        errors = []
        with open(self.csv_file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    content = Content.parse_obj(row)
                    valid_rows.append(content)
                except ValidationError as e:
                    errors.append({'row': row, 'error': str(e)})
        
        # Ensure the output directory exists
        #os.makedirs(self.output_file_name, exist_ok=True)
        
        # Define the full path for the cleaned CSV file
        cleaned_csv_path = self.output_file_name
        
        # Write the valid rows to the cleaned CSV, overwriting any existing file
        with open(cleaned_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=Content.__fields__.keys())
            writer.writeheader()
            for content in valid_rows:
                writer.writerow(content.dict())
        
        return valid_rows, errors

class Metadata(BaseModel):
    Title: str
    Publisher: str
    AvailabilityStatus: str
    BiblicalReference: Optional[str] = None
    AppInfoDescription: str
    Abstract: str

    @field_validator('Title', 'Publisher', 'AppInfoDescription')
    def alphanumeric_and_no_trailing_spaces(cls, v):
        if not re.match(r'^[\w\d\s\-\:\,]+$', v):
            raise ValueError("Only alphanumeric characters and spaces allowed")
        return v.strip()

    @field_validator('AvailabilityStatus')
    def availability_status_values(cls, v):
        valid_statuses = ["available", "unavailable", "unknown"]
        if v not in valid_statuses:
            raise ValueError(f"Status must be one of {valid_statuses}")
        return v

    @field_validator('BiblicalReference')
    def biblical_reference_format(cls, v):
        # Example pattern validation, adjust according to your specific needs
        if v and not re.match(r'^[A-Za-z\s\d:]+$', v):
            raise ValueError("Invalid format for Biblical Reference")
        return v

    @field_validator('Abstract')  
    def check_content_special_characters(cls, value):
        if any(char in set('□') for char in value):
            raise ValueError("Abstract must not contain special characters")
        return value 

class MetadataValidator:
    def __init__(self, csv_file_path: str, output_file_path:str):
        self.csv_file_path = csv_file_path
        self.output_file_name = output_file_path  
    
    def clean_and_validate_metadata_csv(self) -> List[Metadata]:
        valid_rows = []
        errors = []
        with open(self.csv_file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    metadata = Metadata.parse_obj(row)
                    valid_rows.append(metadata)
                except ValidationError as e:
                    errors.append({'row': row, 'error': str(e)})
        
        # Ensure the output directory exists
        #os.makedirs(self.output_file_name, exist_ok=True)
        
        # Define the full path for the cleaned CSV file
        cleaned_csv_path = self.output_file_name
        
        # Write the valid rows to the cleaned CSV, overwriting any existing file
        with open(cleaned_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=Metadata.__fields__.keys())
            writer.writeheader()
            for metadata in valid_rows:
                writer.writerow(metadata.dict())
        
        return valid_rows, errors
















# Usage
'''def main():
    csv_file_path = Path(__file__).parents[0] / 'CSV/extracted_updated.csv'
    url_class_instance = URLClass(str(csv_file_path))
    valid_topics, validation_errors = url_class_instance.clean_and_validate_csv()
    print(f"Valid rows: {len(valid_topics)}, Validation errors: {len(validation_errors)}")
    if validation_errors:
        print("Validation Errors Encountered:")
        for error in validation_errors:
            print(f"Row: {error['row']}")
            print(f"Error: {error['error']}\n")
    else:
        print("No validation errors encountered.")

if __name__ == "__main__":
    main()
'''