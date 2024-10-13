import snowflake.connector
import pandas as pd

# Snowflake connection parameters
conn_params = {
    'account': 'PURESTORAGEIT',
    'user': 'SVC_SAP_PROD',
    'password': '2BEv/BxHiHCCAe&ixTy2',
    'role': 'ASCEND_ANALYST_PROD',  # Optional, specify if needed
    'warehouse': 'ASCEND_SM_WH',
    'database': 'EDL_SAP_PROD',
    'schema': 'PS_SAP',
   # 'authenticator': 'externalbrowser'  # This enables SSO via external browser
    }
def query_snowflake(query):
    conn = None
    cursor = None
    try:
        # Establish connection to Snowflake
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Fetch the data
        rows = cursor.fetchall()
        if not rows:
            print("No data found.")
            return None

        # Convert the result to a pandas DataFrame
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
        return df

    except Exception as e:
        print(f"Error during query execution: {e}")
        return None

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Example usage
if __name__ == "__main__":
    df = query_snowflake("SELECT cust_reference, order_status FROM V_SAP_ORDER_HEADER_SET ")
    # Show all rows and columns
    pd.set_option('display.max_rows', None)  # None means unlimited rows
    pd.set_option('display.max_columns', None)  # None means unlimited columns

    for index, row in df.iterrows():
     if df is not None:
        print(row)
    else:
        print("Query returned no data.")