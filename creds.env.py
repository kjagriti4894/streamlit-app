from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access credentials using os.getenv
conn_params = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': 'ASCEND_SM_WH',
    'database': 'EDL_SAP_PROD',
    'schema': 'PS_SAP',
}
