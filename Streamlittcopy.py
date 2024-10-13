import streamlit as st
import requests

# Streamlit app interface
st.title("Dispatch and Return Tracker")
# Input field for user query
user_query = st.text_input("Ask a question (e.g., 'Show orders', 'Search for customer'):")

# Send query to Flask API and display the response
def send_query_to_api(user_query):
    # api_url = "http://localhost:5001/api/query"
    # headers = {"Content-Type": "application/json"}
    api_url = f"http://localhost:5001/api/query?query={user_query}"
    response = requests.get(api_url)

    # data = {"query": user_query}
    # response = requests.post(api_url, json=data, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Error {response.status_code}: {response.text}")
        return None

# Handling 'Submit' button click
if st.button("Submit"):
    if user_query:
        result = send_query_to_api(user_query)
        if result:
            # st.write("API Response:")
            st.dataframe(result)
    else:
        st.error("Please enter a query.")
