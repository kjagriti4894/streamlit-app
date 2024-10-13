
import streamlit as st
import pandas as pd
import snowflake.connector
import re


# Snowflake connection parameters
conn_params = {
    'account': 'PURESTORAGEIT',
    'user': 'SVC_SAP_PROD',
    'password': '2BEv/BxHiHCCAe&ixTy2',
    'role': 'ASCEND_ANALYST_PROD',
    'warehouse': 'ASCEND_SM_WH',
    'database': 'EDL_SAP_PROD',
    'schema': 'PS_SAP',
}

# Query function for Snowflake
def query_snowflake(query):
    conn = None
    cursor = None
    try:
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
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
    statuses = ["Open", "Blocked", "Submitted to Vendors", "Cancelled", "Delivered", "Ready to Dispatch", "Confirmed"]

    # Get user input
    user_query = st.text_input("Ask a question (e.g., 'Show orders', 'Search for customer'):")

    # Process user query
    if st.button("Submit"):
        if user_query:
            # Simplified query logic based on keywords
            pattern = r'PD\d+'
            match = re.search(pattern, user_query)

            if match:
                query = ("SELECT header.cust_reference as \"Dispatch Order\" , "
                         "header.order_status AS \"Overall Header User Status\", "
                         "item.requested_fe_arrival_zreqfearrdatecust AS \"Requested FE Date\", "
                         "item.requested_fe_arrival_zreqfearrtimecust AS \"Requested FE Time\", "
                         "header.lifsk AS \"Delivery Block\", "
                         "item.material AS \"Part Number\", "
                         "item.ITEM_DESCR AS \"Description\", "
                         "item.quantity AS \"Quantity\", "
                         "item.ups_order_number AS \"UPS Order Number\", "
                         "item.rejectionreason AS \"Rejection Reason\", "
                         "item.item_category, "
                         "item.ARRAY_NAME, item.return_uii, item.dispatch_uii "
                         "FROM EDL_SAP_PROD.PS_SAP.V_SAP_ORDER_HEADER_SET AS header "
                         "INNER JOIN EDL_SAP_PROD.PS_SAP.V_SAP_ORDER_item_SET AS item "
                         "ON header.sales_document = item.sales_document "
                         "WHERE item.item_category IN ('ZRLB', 'ZSPR', 'ZSRV', 'ZPRE', 'ZREN') "
                         f"AND header.cust_reference = '{match.group()}'")

                df = query_snowflake(query)
                if df is not None:
                    st.write(df)
                else:
                    st.write("No data found.")

            elif "order" in user_query.lower():
                query = "SELECT sales_document, order_status, cust_reference FROM V_SAP_ORDER_HEADER_SET"
                order_status = None

                for status in statuses:
                    if status.lower() in user_query:
                        order_status = status  # Capitalize for SQL consistency
                        break

                if order_status:
                    query += f" where  order_status = '{order_status}'"
                df = query_snowflake(query)
                if df is not None:
                    st.write(df)
                else:
                    st.write("No data found.")
            else:
                st.write("Sorry, I didn't understand your query.")
        else:
            st.write("Please enter a query.")

# Run the chatbot
if __name__ == "__main__":
    chatbot()
