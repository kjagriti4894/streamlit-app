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


# Query function for Snowflake
def query_snowflake(query):
    conn = None
    cursor = None
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
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Streamlit chatbot interface
def chatbot():
    st.title("Dispatch and Return Tracker")

    # Capture query parameters from the URL
    query_params = st.query_params  # Access the query parameters

    # Add a debug statement to check if the URL parameter is captured correctly
    st.write(f"Query parameters: {query_params}")

    # Get 'user_query' parameter from URL if present, else use an empty string
    user_query = query_params.get('user_query', [''])[0]  # Get 'user_query' parameter

    # Ensure any trailing/leading spaces are removed
    user_query = user_query.strip()

    # Display the pre-filled query from the URL or allow user to input a new one
    user_query = st.text_input("Ask a question (e.g., 'Show orders', 'Search for customer'):", value=user_query)

    # Process user query
    if st.button("Submit"):
        if user_query:
            # Simplified query logic based on keywords
            pattern = r'PD\d+'
            match = re.search(pattern, user_query)

            if match:
                query = (f"SELECT header.cust_reference as \"Dispatch Order\", "
                         "item.material AS \"Part Number\", "
                         "item.ITEM_DESCR AS \"Description\", "
                         "item.user_status_desc as \"User Status\", "
                         "item.fe_status as \"FE User Status\", "
                         "item.ups_order_number AS \"UPS Order Number\", "
                         "header.order_reason AS \"Rejection Reason\", "
                         "header.order_status AS \"Overall Header User Status\", "
                         "item.requested_fe_arrival_zreqfearrdatecust AS \"Requested FE Date\", "
                         "item.requested_fe_arrival_zreqfearrtimecust AS \"Requested FE Time\", "
                         "header.lifsk AS \"Delivery Block\", "
                         "item.quantity AS \"Quantity\", "
                         "item.item_category, "
                         "item.ARRAY_NAME, item.return_uii, item.dispatch_uii "
                         "FROM EDL_SAP_PROD.PS_SAP.V_SAP_ORDER_HEADER_SET AS header "
                         "INNER JOIN EDL_SAP_PROD.PS_SAP.V_SAP_ORDER_item_SET AS item "
                         "ON header.sales_document = item.sales_document "
                         f"WHERE item.item_category IN ('ZRLB', 'ZSPR', 'ZSRV', 'ZPRE', 'ZREN') "
                         f"AND header.cust_reference = '{match.group()}'")

                df = query_snowflake(query)
                if df is not None:
                    st.write(df)
                else:
                    st.write("No data found.")
            else:
                st.write("No valid order ID found in the query.")
        else:
            st.write("Please enter a query.")


# Run the chatbot
if __name__ == "__main__":
    chatbot()
