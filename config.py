# AWS configs
AWS_REGION = 'eu-west-3'  # Paris region based on your cluster
S3_BUCKET = 'retail-analytics-project-2025'  # Your S3 bucket name
DATABASE_NAME = 'retail_raw_db'  # Glue Catalog database name
CRAWLER_ROLE = 'role_glue'

# Redshift configs
REDSHIFT_HOST = 'retail-analytics-dw.cg8wdztehtjo.eu-west-3.redshift.amazonaws.com'
REDSHIFT_PORT = '5439'
REDSHIFT_DB = 'retail_analytics'  # Your existing database name
REDSHIFT_USER = 'admin'
REDSHIFT_PASSWORD = 'Strongpassword098!'  # You'll need to put your actual password here
REDSHIFT_ROLE = 'arn:aws:iam::010438482524:role/RedshiftS3AccessRole'