import requests
import json
import pandas as pd
from datetime import datetime, timedelta

# Define the URL and headers
url = "https://microservices.socket.tech/loki/tx-history?sort=desc&fromChainId=&toChainId=&bridgeName="
headers = {
    "Content-Type": "application/json"
}

# Define the date range
start_date = datetime(2024, 1, 1)
end_date = datetime.now()

# Convert dates to ISO format for the API request
start_date_str = start_date.isoformat()
end_date_str = end_date.isoformat()

# Function to fetch and filter data
def fetch_and_filter_data(start_date_str, end_date_str):
    # Initialize an empty DataFrame to store the flattened data
    all_data = pd.DataFrame()
    
    # Initialize the page number
    page = 0
    
    while True:
        # Fetch data from the URL with pagination and date filtering
        response = requests.get(f"{url}&startDate={start_date_str}&endDate={end_date_str}&page={page}", headers=headers)
        # Parse the JSON response
        data = json.loads(response.text)
        
        # Check if 'result' key exists in the response
        if 'result' in data:
            # Normalize the data and append it to the DataFrame
            page_data = pd.json_normalize(data['result'])
            all_data = pd.concat([all_data, page_data], ignore_index=True)
            
            # Print feedback on what is being fetched
            print(f"Fetched page {page + 1} with {len(page_data)} transactions.")
            
            # Check if there's a next page
            if 'paginationData' in data and 'totalCount' in data['paginationData']:
                total_count = data['paginationData']['totalCount']
                page_size = data['paginationData'].get('pageSize', 10) # Default to 10 if pageSize is not provided
                total_pages = (total_count + page_size - 1) // page_size
                
                if page + 1 < total_pages:
                    page += 1
                else:
                    break # No more pages to fetch
        else:
            print("No 'result' key found in the response.")
            break
    
    return all_data

# Fetch and filter data
df = fetch_and_filter_data(start_date_str, end_date_str)

# Print feedback on the total number of transactions fetched
print(f"Total transactions fetched: {len(df)}")

# Debugging: Print the first few rows of the DataFrame to verify the data
print(df.head())

# Exclude specified columns
columns_to_exclude = [
    'destTransactionHash', 'srcTransactionHash', 'destBlockHash', 'destTokenAddress', 'destTokenLogoURI',
    'metadata', 'socketContractVersion', 'srcBlockHash', 'srcTokenAddress', 'srcTokenLogoURI', 'to',
    'isMultiBridge', 'amountOutMin'
]

# Drop the specified columns from the DataFrame
df = df.drop(columns=columns_to_exclude)

# Save the DataFrame to a CSV file
df.to_csv('soketscan.csv', index=False)
print("Data saved to soketscan.csv")
