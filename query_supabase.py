import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def get_table_as_csv(table_name: str) -> None:
    """
    Fetches all records from the specified Supabase table and saves them as a CSV file.

    Args:
        table_name (str): The name of the Supabase table to query.
        csv_file_path (str): The file path where the CSV will be saved.
    """
    response = supabase.table(table_name).select("*").csv().execute()
    data = response.data

    return response.data

def save_csv_locally(csv_data, filename):
    with open(filename, 'w', encoding='utf-8') as csv_file:
        for row in csv_data:
            csv_file.write(row)
my_tables = ["users", "opportunities", "opportunity_participants"]
for table in my_tables:
    table_csv = get_table_as_csv(table)
    save_as = os.path.join("data", table.lower().replace(" ", "_") + ".csv")
    print(save_as)
    save_csv_locally(table_csv, save_as)