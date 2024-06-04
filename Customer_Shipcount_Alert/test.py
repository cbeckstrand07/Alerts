import pandas as pd
from dataclasses import dataclass
from datetime import date

# Step 1: Load your DataFrame from the CSV file
def load_data(csv_file):
    return pd.read_csv(csv_file)

# Step 2: Define a data class to represent each 'Bill of Lading'
@dataclass
class BillOfLading:
    bol: str
    company: str
    mode: str
    cleaned_carrier: str
    charge: float
    total_weight: float

# Step 3: Create a list to store instances of BillOfLading
def create_bills_of_lading(data_frame):
    bills_of_lading = []
    for index, row in data_frame.iterrows():
        bill = BillOfLading(
            bol=row['Bill_of_Lading'],
            company=row['Company'],
            mode=row['Mode'],
            cleaned_carrier=row['Cleaned_Carrier'],
            charge=row['Charge'],
            total_weight=row['Total_Weight']
        )
        bills_of_lading.append(bill)
    return bills_of_lading

# Step 4: Main function
def main():
    # Replace 'your_data.csv' with the actual path or URL of your CSV file
    csv_file = '/Users/carlybackstrand/Desktop/HomeBase/Dashboards/Customer/Fireclay/fireclay_data.csv'

    # Load data
    df = load_data(csv_file)
    
    # Create a dictionary with current column names as keys and new column names as values
    column_mapping = {
        'Bill of Lading': 'Bill_of_Lading',
        'Date Generated': 'Date_Generated',
        'Total Weight': 'Total_Weight',
        'Cleaned Carrier': 'Cleaned_Carrier'
    }
    
    # Use the rename method to rename the columns
    df.rename(columns=column_mapping, inplace=True)

    # Create instances of BillOfLading
    bills_of_lading = create_bills_of_lading(df)

    # Example: Accessing attributes of each instance
    for bill in bills_of_lading:
        print(bill.bol, bill.company, bill.mode, bill.cleaned_carrier, bill.charge, bill.total_weight)

if __name__ == '__main__':
    main()
