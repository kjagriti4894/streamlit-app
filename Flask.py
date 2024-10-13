from flask import Flask, request, jsonify
import pandas as pd
import snowflake.connector
import re
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize Flask app for the API
api_app = Flask(__name__)

# Snowflake connection parameters using environment variables
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
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
        return df
    except Exception as e:
        return {"Error": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Define a route to receive queries from Glean or any source
@api_app.route('/api/query', methods=['GET'])
def handle_query():
    # data = request.get_json()
    # user_query = data.get('query')
    user_query = request.args.get('query')
    if not user_query:
        return jsonify({"error": "No query provided."}), 400

    # Process user query to decide which query to send to Snowflake
    result = chatbot_logic(user_query)

    if isinstance(result, pd.DataFrame):
        return result.to_json(orient="records")
    elif isinstance(result, dict):
        return jsonify(result)
    else:
        return jsonify({"error": "Something went wrong."})


# Chatbot logic without Streamlit interaction
def chatbot_logic(user_query):
    statuses = ["Open", "Blocked", "Submitted to Vendors", "Cancelled", "Delivered", "Ready to Dispatch", "Confirmed"]
    pattern = r'PD\d+'
    match = re.search(pattern, user_query)

    if match:
        query = (f"SELECT header.cust_reference as \"Dispatch Order\" ,"
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
                 " item.item_category, "
                 "item.ARRAY_NAME, item.return_uii, item.dispatch_uii "
                 "FROM EDL_SAP_PROD.PS_SAP.V_SAP_ORDER_HEADER_SET AS header "
                 "INNER JOIN EDL_SAP_PROD.PS_SAP.V_SAP_ORDER_item_SET AS item "
                 "ON header.sales_document = item.sales_document "
                 f"WHERE item.item_category IN ('ZRLB', 'ZSPR', 'ZSRV', 'ZPRE', 'ZREN') "
                 f"AND header.cust_reference = '{match.group()}'")
        return query_snowflake(query)

    elif "order" in user_query.lower():
        query = "SELECT sales_document, order_status, cust_reference FROM V_SAP_ORDER_HEADER_SET"
        order_status = None
        for status in statuses:
            if status.lower() in user_query:
                order_status = status
                break

        if order_status:
            query += f" WHERE order_status = '{order_status}'"
        return query_snowflake(query)

    return {"message": "Sorry, I didn't understand your query."}


# Run the Flask app for the API
if __name__ == "__main__":
    api_app.run(port=5001)
