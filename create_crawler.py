import boto3
from config import *

def setup_crawler():
    glue_client = boto3.client('glue', region_name=AWS_REGION)
    
    # Check if crawler exists
    try:
        crawler_info = glue_client.get_crawler(Name='retail_data_crawler')
        print("Crawler already exists, checking if it needs to run...")
        
        # Check crawler status
        if crawler_info['Crawler']['State'] == 'READY':  # Only start if it's not already running
            print("Starting existing crawler...")
            glue_client.start_crawler(Name='retail_data_crawler')
        else:
            print(f"Crawler is in {crawler_info['Crawler']['State']} state, waiting for completion...")
            
    except glue_client.exceptions.EntityNotFoundException:
        print("Crawler doesn't exist, creating new crawler...")
        # Create new crawler
        glue_client.create_crawler(
            Name='retail_data_crawler',
            Role=CRAWLER_ROLE,
            DatabaseName=DATABASE_NAME,
            Targets={
                'S3Targets': [
                    {'Path': f's3://{S3_BUCKET}/raw/customers/'},
                    {'Path': f's3://{S3_BUCKET}/raw/products/'},
                    {'Path': f's3://{S3_BUCKET}/raw/orders/'}
                ]
            }
        )
        print("Created and starting new crawler...")
        glue_client.start_crawler(Name='retail_data_crawler')