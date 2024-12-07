import pandas as pd
import requests

USERS_API_URL = "http://127.0.0.1:8000/users"
WASTES_API_URL = "http://127.0.0.1:8000/wastes"
def fetch_data_from_api(api_url):
    """Fetch data from the provided API URL and return the response in JSON format."""
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching data from {api_url}. Status code: {response.status_code}")

def load_datasets():
    """Fetch and load both users and products data into separate DataFrames."""
    print("Loading users data...")
    users_data = fetch_data_from_api(USERS_API_URL)
    print("Loading products data...")
    wastes_data = fetch_data_from_api(WASTES_API_URL)
    
    # Convert data into DataFrames
    users_df = pd.DataFrame(users_data)
    wastes_df = pd.DataFrame(wastes_data)
    
    return users_df, wastes_df

def summarize_dataset(dataframe, name):
    """Generate summary statistics for the dataset."""
    print(f"\n--- Dataset Summary for {name} ---")
    print(dataframe.describe(include="all"))
    print("\nColumns with missing values:")
    print(dataframe.isnull().sum())

def handle_missing_values(df):
    """Handle missing values by filling nulls with appropriate defaults."""
    print("\nHandling missing data...")
    
    # Replace missing text data with "Not Available", numeric with zero
    for column in df.columns:
        if df[column].dtype == "object":
            df[column] = df[column].fillna("Not Available")
        else:
            df[column] = df[column].fillna(0)
    
    print("Missing values have been handled.")
    return df

def clean_and_prepare_data(df):
    """Remove duplicates and clean column names."""
    print("\nCleaning data...")
    
    # Remove duplicates
    df.drop_duplicates(inplace=True)
    
    # Standardize column names (lowercase and replace spaces with underscores)
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    
    print("Data cleaning complete.")
    return df

def add_custom_features(users_df, wastes_df):
    """Add new features to the user dataset by aggregating product data."""
    print("\nAdding new features...")

    # Count the number of products owned by each user
    waste_counts = wastes_df.groupby("owner_id").size().reset_index(name="num_products")
    users_df = users_df.merge(waste_counts, left_on="id", right_on="owner_id", how="left")
    users_df["num_products"] = users_df["num_products"].fillna(0)

    # Calculate the average price of products owned by each user
    avg_prices = wastes_df.groupby("owner_id")["price"].mean().reset_index(name="avg_product_price")
    users_df = users_df.merge(avg_prices, left_on="id", right_on="owner_id", how="left")
    users_df["avg_product_price"] = users_df["avg_product_price"].fillna(0)

    print("Custom features added.")
    return users_df

if __name__ == "__main__":
    # Load datasets
    users_df, wastes_df = load_datasets()

    # Summarize datasets
    summarize_dataset(users_df, "Users")
    summarize_dataset(wastes_df, "Wastes")

    # Handle missing values
    users_df = handle_missing_values(users_df)
    wastes_df = handle_missing_values(wastes_df)

    # Clean and prepare data
    users_df = clean_and_prepare_data(users_df)
    wastes_df = clean_and_prepare_data(wastes_df)

    # Add custom features to users dataset
    enriched_users_df = add_custom_features(users_df, wastes_df)

    # Save processed data to CSV
    users_df.to_csv("cleaned_users_data.csv", index=False)
    wastes_df.to_csv("cleaned_wastes_data.csv", index=False)
    enriched_users_df.to_csv("enriched_users_data.csv", index=False)

    print("\nData processing complete. Files saved as 'cleaned_users_data.csv', 'cleaned_wastes_data.csv', and 'enriched_users_data.csv'.")
