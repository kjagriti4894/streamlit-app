import streamlit as st
import pandas as pd
import snowflake.connector
import re
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Snowflake connection parameters
conn_params = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'role': 'ASCEND_ANALYST_PROD',
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
}

# Query function with optimization (LIMIT the number of results)
def query_snowflake(query):
    try:
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        st.write("Running query...")
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            st.write("No rows found.")
            return None
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
        return df
    except Exception as e:
        st.error(f"Error during query execution: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

# Streamlit chatbot interface
def chatbot():
    st.title("Dispatch and Return Tracker")

    # Capture query from user input
    user_query = st.text_input("Ask a question (e.g., 'Show orders', 'Search for customer'):")

    if st.button("Submit"):
        if user_query:
            # Simplified query logic based on keywords
            pattern = r'PD\d+'
            match = re.search(pattern, user_query)

            if match:
                query = (f"SELECT header.cust_reference as 'Dispatch Order', "
                         "item.material AS 'Part Number', "
                         "item.ITEM_DESCR AS 'Description', "
                         "FROM EDL_SAP_PROD.PS_SAP.V_SAP_ORDER_HEADER_SET AS header "
                         "INNER JOIN EDL_SAP_PROD.PS_SAP.V_SAP_ORDER_item_SET AS item "
                         f"WHERE header.cust_reference = '{match.group()}' LIMIT 100")  # Optimized

                df = query_snowflake(query)
                if df is not None:
                    st.write(df)

# Run the chatbot
if __name__ == "__main__":
    chatbot()
