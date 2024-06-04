import pandas as pd
from scipy.stats import ttest_ind
from scipy.stats import ttest_rel
from datetime import datetime, timedelta
import math
from scipy.stats import wilcoxon
import statsmodels.stats.api as sms
import numpy as np
from sklearn.linear_model import LinearRegression # To detect trends
import os
from lookups import COMPANY_ALTERNATE_NAMES

def load_data():
    df = pd.read_csv(r"/Users/carlybackstrand/Desktop/Flat World Dashboard Database.csv")
    
    df = df[['Company', 'Bill of Lading', 'Mode', 'Charge', 'Date Generated', 'Week Num']]

    # Convert 'Date Generated' column to datetime if it's not already
    df['Date Generated'] = pd.to_datetime(df['Date Generated'])
    
    df = df.sort_values(by='Date Generated', ascending=True)
    
    return df

def company_start(df):
    # Make sure date generated is a date
    #df['Date Generated'] = pd.to_datetime(['Date Generated'])
    
    # Group by Company and find the min date
    min_dates = df.groupby('Company')['Date Generated'].transform(min)
    max_dates = df.groupby('Company')['Date Generated'].transform(max)
    
    # Assign the min dates to the start date column
    df['First Date'] = min_dates
    df['Last Date'] = max_dates
    
    return df

def company_ranking(df_group):
    # Convert 'Bill of Lading' to numeric values
    df_group['Bill of Lading'] = pd.to_numeric(df_group['Bill of Lading'], errors='coerce')
    
    # Convert 'Bill of Lading' to numeric values and replace NaN with 0
    df_group['Bill of Lading'] = pd.to_numeric(df_group['Bill of Lading'], errors='coerce').fillna(0)
    
    # Calculate and print the percentiles
    percentiles = df_group['Bill of Lading'].quantile([0, 1 / 3, 2 / 3, 1])
    print(percentiles)
    
    # Define a function to assign ranks based on percentiles
    def assign_rank(shipment_count):
        if shipment_count >= percentiles.iloc[2]:
            return 1
        elif shipment_count >= percentiles.iloc[1]:
            return 2
        else:
            return 3
        
    # Apply the function to create a new 'Rank' column
    df_group['Rank'] = df_group['Bill of Lading'].apply(assign_rank)
    
    # Display the resulting DataFrame
    return df_group

def yoy_monthly(df_ranked, df_date, file_path):
    # Get unique customers
    customers = df_ranked['Company'].unique()
    
    window_size = 30
    
    # Find yesterday's date for the end of the last 30 calendar days
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=window_size - 1)
    
    # Calculate the start and end date for the previous year
    prev_start_date = start_date - timedelta(days=365)
    prev_end_date = end_date - timedelta(days=365)
    
    # Display the results
    print("Recent Start Date: ", start_date)
    print("Recent End Date: ", end_date)
    print("Previous Year Start Date: ", prev_start_date)
    print("Previous Year End Date: ", prev_end_date)

    # Convert 'Date Generated' column to datetime if it's not already
    df_ranked['Date Generated'] = pd.to_datetime(df_ranked['Date Generated'])
    
    # Filter the DataFrame based on the date range
    prev_yoy_monthly_data = df_ranked[
        ((df_ranked['Date Generated'] >= prev_start_date) & (df_ranked['Date Generated'] <= prev_end_date))
        ]
    
    recent_yoy_monthly_data = df_ranked[
        ((df_ranked['Date Generated'] >= start_date) & (df_ranked['Date Generated'] <= end_date))
        ]
    
    # Display the new DataFrame
    print("Previous YOY monthly data: ", prev_yoy_monthly_data)
    print("Recent YOY monthly data: ", recent_yoy_monthly_data)
    
    # Save data
    prev_yoy_monthly_data.to_csv(f'{file_path}prev_yoy_monthly_data.csv', index=False)
    recent_yoy_monthly_data.to_csv(f'{file_path}recent_yoy_monthly_data.csv', index=False)
    
    # Add a new row to the DataFrame using loc
    new_index = len(df_date)
    df_date.loc[new_index] = [start_date, end_date, prev_start_date, prev_end_date, 'Monthly YOY Alert']
    
    for customer in customers:
        # Filter data for the current customer
        customer_data = df_ranked[df_ranked['Company'] == customer]
        print("\n")
        print(customer)
        first_date = customer_data['First Date'].min()
        print("First Date: ", first_date)
        last_date = customer_data['Last Date'].max()
        print("Last Date: ", last_date)
        
        # Filter data based on the time frame
        recent_filtered_data = customer_data[(customer_data['Date Generated'] >= start_date) & (customer_data['Date Generated'] <= end_date)]
        prev_filtered_data = customer_data[(customer_data['Date Generated'] >= prev_start_date) & (customer_data['Date Generated'] <= prev_end_date)]
        
        # Group Recent Data
        grouped_recent_data = recent_filtered_data.groupby('Date Generated').agg({'Charge': 'sum', 'Bill of Lading': 'count'})
        grouped_prev_data = prev_filtered_data.groupby('Date Generated').agg({'Charge': 'sum', 'Bill of Lading': 'count'})
        
        # Calculate the sum of 'Charge' within the specified time frame
        recent_charge = grouped_recent_data['Charge'].sum()
        prev_charge = grouped_prev_data['Charge'].sum()
        shipcount_recent = grouped_recent_data['Bill of Lading'].sum()
        shipcount_prev = grouped_prev_data['Bill of Lading'].sum()
        
        print("Grouped Recent Shipcount: ", shipcount_recent)
        print("Grouped Previous Shipcount: ", shipcount_prev)
        print("Grouped Recent Charge: ", recent_charge)
        print("Grouped Previous Charge: ", prev_charge)
        
        if first_date <= prev_start_date and last_date >= end_date - timedelta(days=30):
            # Reset Index
            grouped_recent_data = grouped_recent_data.reset_index()
            grouped_prev_data = grouped_prev_data.reset_index()
            
            # Create Date Range and fill in missing dates with 0's
            recent_date_range = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
            prev_date_range = pd.date_range(start=prev_start_date.date(), end=prev_end_date.date(), freq='D')
            grouped_recent_data['Date Generated'] = pd.to_datetime(grouped_recent_data['Date Generated']).dt.date
            grouped_prev_data['Date Generated'] = pd.to_datetime(grouped_prev_data['Date Generated']).dt.date
            grouped_recent_data = grouped_recent_data.set_index('Date Generated').reindex(recent_date_range).fillna(0).reset_index()
            grouped_prev_data = grouped_prev_data.set_index('Date Generated').reindex(prev_date_range).fillna(0).reset_index()
            
            # Print new data frames
            print("Grouped Recent Data: \n", grouped_recent_data)
            print("Grouped Previous Data: \n", grouped_prev_data)
            
            # Perform paired t-test
            t_stat, p_value = ttest_rel(grouped_prev_data['Bill of Lading'], grouped_recent_data['Bill of Lading'])
            
            if p_value < 0.01:
                if shipcount_recent > shipcount_prev:
                    status = 'Significantly More'
                elif shipcount_recent < shipcount_prev:
                    status = 'Significantly Less'
                else:
                    status = 'Error'
            elif p_value >= 0.01:
                status = 'No Significant Difference'
            else:
                status = 'No p-value'
            
            print(f'Status: {status}')
            print(f"T-statistic: {t_stat:.10f}")
            print(f"P-value: {p_value:.10f}\n")
            
            print("-----------------------------------------------------\n")
            
            # Update the 'Status' column
            df_ranked.loc[df_ranked['Company'] == customer, 'Monthly YOY Alert'] = status
        else:
            status = 'Not Enough Data'
        
        df_ranked.loc[df_ranked['Company'] == customer, 'Monthly YOY Alert'] = status
    
    return df_ranked, df_date
    
def yoy(df_yoy_monthly, df_date, file_path):
    # Get unique customers
    customers = df_yoy_monthly['Company'].unique()
    
    window_size = 365
    
    # Find yesterday's date for the end of the last 30 calendar days
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=window_size - 1)
    
    # Calculate the start and end date for the previous year
    prev_start_date = start_date - timedelta(days=365)
    prev_end_date = end_date - timedelta(days=365)
    
    # Display the results
    print("\n*------------------------------------------------------*")
    print("Recent Start Date: ", start_date)
    print("Recent End Date: ", end_date)
    print("Previous Year Start Date: ", prev_start_date)
    print("Previous Year End Date: ", prev_end_date)
    
    # Convert 'Date Generated' column to datetime if it's not already
    df_yoy_monthly['Date Generated'] = pd.to_datetime(df_yoy_monthly['Date Generated'])
    
    # Filter the DataFrame based on the date range
    prev_yoy_data= df_yoy_monthly[
        ((df_yoy_monthly['Date Generated'] >= prev_start_date) & (df_yoy_monthly['Date Generated'] <= prev_end_date))
        ]
    
    recent_yoy_data= df_yoy_monthly[
        ((df_yoy_monthly['Date Generated'] >= start_date) & (df_yoy_monthly['Date Generated'] <= end_date))
        ]
    
    # Display the new DataFrame
    print("Previous YOY Data:", prev_yoy_data)
    print("Recent YOY Data:", recent_yoy_data)
    
    # Save data
    prev_yoy_data.to_csv(f'{file_path}prev_yoy_data.csv', index=False)
    recent_yoy_data.to_csv(f'{file_path}recent_yoy_data.csv', index=False)
    
    # Add a new row to the DataFrame using loc
    new_index = len(df_date)
    df_date.loc[new_index] = [start_date, end_date, prev_start_date, prev_end_date, 'YOY Alert']
    
    for customer in customers:
        # Filter data for the current customer
        customer_data = df_yoy_monthly[df_yoy_monthly['Company'] == customer]
        print("\n")
        print(customer)
        first_date = customer_data['First Date'].min()
        print("First Date: ", first_date)
        last_date = customer_data['Last Date'].max()
        print("Last Date: ", last_date)
        
        # Filter data based on the time frame
        recent_filtered_data = customer_data[(customer_data['Date Generated'] >= start_date) & (customer_data['Date Generated'] <= end_date)]
        prev_filtered_data = customer_data[(customer_data['Date Generated'] >= prev_start_date) & (customer_data['Date Generated'] <= prev_end_date)]
        
        # Group Recent Data
        grouped_recent_data = recent_filtered_data.groupby('Date Generated').agg({'Charge': 'sum', 'Bill of Lading': 'count'})
        grouped_prev_data = prev_filtered_data.groupby('Date Generated').agg({'Charge': 'sum', 'Bill of Lading': 'count'})
        
        # Calculate the sum of 'Charge' within the specified time frame
        recent_charge = grouped_recent_data['Charge'].sum()
        prev_charge = grouped_prev_data['Charge'].sum()
        shipcount_recent = grouped_recent_data['Bill of Lading'].sum()
        shipcount_prev = grouped_prev_data['Bill of Lading'].sum()
        
        print("Grouped Recent Shipcount: ", shipcount_recent)
        print("Grouped Previous Shipcount: ", shipcount_prev)
        print("Grouped Recent Charge: ", recent_charge)
        print("Grouped Previous Charge: ", prev_charge)
        
        if first_date <= prev_start_date and last_date >= end_date - timedelta(days=30):
            # Reset Index
            grouped_recent_data = grouped_recent_data.reset_index()
            grouped_prev_data = grouped_prev_data.reset_index()
            
            # Create Date Range and fill in missing dates with 0's
            recent_date_range = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
            prev_date_range = pd.date_range(start=prev_start_date.date(), end=prev_end_date.date(), freq='D')
            grouped_recent_data['Date Generated'] = pd.to_datetime(grouped_recent_data['Date Generated']).dt.date
            grouped_prev_data['Date Generated'] = pd.to_datetime(grouped_prev_data['Date Generated']).dt.date
            grouped_recent_data = grouped_recent_data.set_index('Date Generated').reindex(recent_date_range).fillna(0).reset_index()
            grouped_prev_data = grouped_prev_data.set_index('Date Generated').reindex(prev_date_range).fillna(0).reset_index()
            
            # Print new data frames
            print("Grouped Recent Data:\n", grouped_recent_data)
            print("Grouped Previous Data:\n", grouped_prev_data)
            
            # Perform paired t-test
            t_stat, p_value = ttest_rel(grouped_prev_data['Bill of Lading'], grouped_recent_data['Bill of Lading'])
            
            if p_value < 0.01:
                status = 'Significantly More' if shipcount_recent > shipcount_prev else 'Significantly Less' if shipcount_recent < shipcount_prev else 'Error'
            elif p_value >= 0.01:
                status = 'No Significant Difference'
            else:
                status = 'No p-value'
            
            print(f'Status: {status}')
            print(f"T-statistic: {t_stat:.10f}")
            print(f"P-value: {p_value:.10f}\n")
            
            print("-----------------------------------------------------\n")
            
            # Update the 'Status' column
            df_yoy_monthly.loc[df_yoy_monthly['Company'] == customer, 'YOY Alert'] = status
        else:
            status = 'Not Enough Data'
            df_yoy_monthly.loc[df_yoy_monthly['Company'] == customer, 'YOY Alert'] = status
    
    return df_yoy_monthly, df_date

def six_monthly(df_yoy, df_date, file_path):
    # Get unique customers
    customers = df_yoy['Company'].unique()
    
    window_size = 182
    
    # Find yesterday's date for the end of the last 30 calendar days
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=window_size - 1)
    
    prev_end_date = start_date - timedelta(days=1)
    prev_start_date = prev_end_date - timedelta(days=window_size - 1)
    
    # Display the results
    print("\n*------------------------------------------------------*")
    print("Recent Start Date: ", start_date)
    print("Recent End Date: ", end_date)
    print("Previous Year Start Date: ", prev_start_date)
    print("Previous Year End Date: ", prev_end_date)
    
    # Convert 'Date Generated' column to datetime if it's not already
    df_yoy['Date Generated'] = pd.to_datetime(df_yoy['Date Generated'])
    
    # Filter the DataFrame based on the date range
    prev_six_monthly_data = df_yoy[
        ((df_yoy['Date Generated'] >= prev_start_date) & (df_yoy['Date Generated'] <= prev_end_date))
        ]
    
    recent_six_monthly_data = df_yoy[
        ((df_yoy['Date Generated'] >= start_date) & (df_yoy['Date Generated'] <= end_date))
        ]
    
    # Display the new DataFrame
    print("Previous Six Month Data: ", prev_six_monthly_data)
    print("Recent Six Month Data: ", recent_six_monthly_data)
    
    # Save data
    prev_six_monthly_data.to_csv(f'{file_path}prev_six_monthly_data.csv', index=False)
    recent_six_monthly_data.to_csv(f'{file_path}recent_six_monthly_data.csv', index=False)
    
    # Add a new row to the DataFrame using loc
    new_index = len(df_date)
    df_date.loc[new_index] = [start_date, end_date, prev_start_date, prev_end_date, 'Six-Monthly Alert']
    
    for customer in customers:
        # Filter data for the current customer
        customer_data = df_yoy[df_yoy['Company'] == customer]
        print("\n")
        print(customer)
        first_date = customer_data['First Date'].min()
        print("First Date: ", first_date)
        last_date = customer_data['Last Date'].max()
        print("Last Date: ", last_date)
        
        # Filter data based on the time frame
        recent_filtered_data = customer_data[(customer_data['Date Generated'] >= start_date) & (customer_data['Date Generated'] <= end_date)]
        prev_filtered_data = customer_data[(customer_data['Date Generated'] >= prev_start_date) & (customer_data['Date Generated'] <= prev_end_date)]
        
        # Group Recent Data
        grouped_recent_data = recent_filtered_data.groupby('Date Generated').agg({'Charge': 'sum', 'Bill of Lading': 'count'})
        grouped_prev_data = prev_filtered_data.groupby('Date Generated').agg({'Charge': 'sum', 'Bill of Lading': 'count'})
        
        # Calculate the sum of 'Charge' within the specified time frame
        recent_charge = grouped_recent_data['Charge'].sum()
        prev_charge = grouped_prev_data['Charge'].sum()
        shipcount_recent = grouped_recent_data['Bill of Lading'].sum()
        shipcount_prev = grouped_prev_data['Bill of Lading'].sum()
        
        print("Grouped Recent Shipcount: ", shipcount_recent)
        print("Grouped Previous Shipcount: ", shipcount_prev)
        print("Grouped Recent Charge: ", recent_charge)
        print("Grouped Previous Charge: ", prev_charge)
        
        if first_date <= prev_start_date and last_date >= end_date - timedelta(days=30):
            # Reset Index
            grouped_recent_data = grouped_recent_data.reset_index()
            grouped_prev_data = grouped_prev_data.reset_index()
            
            # Create Date Range and fill in missing dates with 0's
            recent_date_range = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
            prev_date_range = pd.date_range(start=prev_start_date.date(), end=prev_end_date.date(), freq='D')
            grouped_recent_data['Date Generated'] = pd.to_datetime(grouped_recent_data['Date Generated']).dt.date
            grouped_prev_data['Date Generated'] = pd.to_datetime(grouped_prev_data['Date Generated']).dt.date
            grouped_recent_data = grouped_recent_data.set_index('Date Generated').reindex(recent_date_range).fillna(0).reset_index()
            grouped_prev_data = grouped_prev_data.set_index('Date Generated').reindex(prev_date_range).fillna(0).reset_index()
            
            # Print new data frames
            print("Grouped Recent Data:\n", grouped_recent_data)
            print("Grouped Previous Data:\n", grouped_prev_data)
            
            # Perform paired t-test
            t_stat, p_value = ttest_rel(grouped_prev_data['Bill of Lading'], grouped_recent_data['Bill of Lading'])
            
            if p_value < 0.01:
                status = 'Significantly More' if shipcount_recent > shipcount_prev else 'Significantly Less' if shipcount_recent < shipcount_prev else 'Error'
            elif p_value >= 0.01:
                status = 'No Significant Difference'
            else:
                status = 'No p-value'
            
            print(f'Status: {status}')
            print(f"T-statistic: {t_stat:.10f}")
            print(f"P-value: {p_value:.10f}\n")
            
            print("-----------------------------------------------------\n")
            
            # Update the 'Status' column
            df_yoy.loc[df_yoy['Company'] == customer, 'Six-Monthly Alert'] = status
        else:
            status = 'Not Enough Data'
            df_yoy.loc[df_yoy['Company'] == customer, 'Six-Monthly Alert'] = status
    
    return df_yoy, df_date

def three_monthly(df_six_monthly, df_date, file_path):
    # Get unique customers
    customers = df_six_monthly['Company'].unique()
    
    window_size = 90
    
    # Find yesterday's date for the end of the last 30 calendar days
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=window_size - 1)
    
    prev_end_date = start_date - timedelta(days=1)
    prev_start_date = prev_end_date - timedelta(days=window_size - 1)
    
    # Display the results
    print("\n*------------------------------------------------------*")
    print("Recent Start Date: ", start_date)
    print("Recent End Date: ", end_date)
    print("Previous Year Start Date: ", prev_start_date)
    print("Previous Year End Date: ", prev_end_date)
    
    # Convert 'Date Generated' column to datetime if it's not already
    df_six_monthly['Date Generated'] = pd.to_datetime(df_six_monthly['Date Generated'])
    
    # Filter the DataFrame based on the date range
    prev_three_monthly_data = df_six_monthly[
        ((df_six_monthly['Date Generated'] >= prev_start_date) & (df_six_monthly['Date Generated'] <= prev_end_date))
        ]
    
    recent_three_monthly_data = df_six_monthly[
        ((df_six_monthly['Date Generated'] >= start_date) & (df_six_monthly['Date Generated'] <= end_date))
        ]
    
    # Display the new DataFrame
    print("Prev 3 months data: ", prev_three_monthly_data)
    print("Recent 3 months data", recent_three_monthly_data)
    
    # Save data
    prev_three_monthly_data.to_csv(f'{file_path}prev_three_monthly_data.csv')
    recent_three_monthly_data.to_csv(f'{file_path}recent_three_monthly_data.csv', index=False)
    
    # Add a new row to the DataFrame using loc
    new_index = len(df_date)
    df_date.loc[new_index] = [start_date, end_date, prev_start_date, prev_end_date, 'Three-Monthly Alert']
    
    for customer in customers:
        # Filter data for the current customer
        customer_data = df_six_monthly[df_six_monthly['Company'] == customer]
        print("\n")
        print(customer)
        first_date = customer_data['First Date'].min()
        print("First Date: ", first_date)
        last_date = customer_data['Last Date'].max()
        print("Last Date: ", last_date)
        
        # Filter data based on the time frame
        recent_filtered_data = customer_data[(customer_data['Date Generated'] >= start_date) & (customer_data['Date Generated'] <= end_date)]
        prev_filtered_data = customer_data[(customer_data['Date Generated'] >= prev_start_date) & (customer_data['Date Generated'] <= prev_end_date)]
        
        # Group Recent Data
        grouped_recent_data = recent_filtered_data.groupby('Date Generated').agg({'Charge': 'sum', 'Bill of Lading': 'count'})
        grouped_prev_data = prev_filtered_data.groupby('Date Generated').agg({'Charge': 'sum', 'Bill of Lading': 'count'})
        
        # Calculate the sum of 'Charge' within the specified time frame
        recent_charge = grouped_recent_data['Charge'].sum()
        prev_charge = grouped_prev_data['Charge'].sum()
        shipcount_recent = grouped_recent_data['Bill of Lading'].sum()
        shipcount_prev = grouped_prev_data['Bill of Lading'].sum()
        
        print("Grouped Recent Shipcount: ", shipcount_recent)
        print("Grouped Previous Shipcount: ", shipcount_prev)
        print("Grouped Recent Charge: ", recent_charge)
        print("Grouped Previous Charge: ", prev_charge)
        
        if first_date <= prev_start_date and last_date >= end_date - timedelta(days=30):
            # Reset Index
            grouped_recent_data = grouped_recent_data.reset_index()
            grouped_prev_data = grouped_prev_data.reset_index()
            
            # Create Date Range and fill in missing dates with 0's
            recent_date_range = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
            prev_date_range = pd.date_range(start=prev_start_date.date(), end=prev_end_date.date(), freq='D')
            grouped_recent_data['Date Generated'] = pd.to_datetime(grouped_recent_data['Date Generated']).dt.date
            grouped_prev_data['Date Generated'] = pd.to_datetime(grouped_prev_data['Date Generated']).dt.date
            grouped_recent_data = grouped_recent_data.set_index('Date Generated').reindex(recent_date_range).fillna(0).reset_index()
            grouped_prev_data = grouped_prev_data.set_index('Date Generated').reindex(prev_date_range).fillna(0).reset_index()
            
            # Print new data frames
            print("Grouped Recent Data:\n", grouped_recent_data)
            print("Grouped Previous Data:\n", grouped_prev_data)
            
            # Perform paired t-test
            t_stat, p_value = ttest_rel(grouped_prev_data['Bill of Lading'], grouped_recent_data['Bill of Lading'])
            
            if p_value < 0.01:
                status = 'Significantly More' if shipcount_recent > shipcount_prev else 'Significantly Less' if shipcount_recent < shipcount_prev else 'Error'
            elif p_value >= 0.01:
                status = 'No Significant Difference'
            else:
                status = 'No p-value'
            
            print(f'Status: {status}')
            print(f"T-statistic: {t_stat:.10f}")
            print(f"P-value: {p_value:.10f}\n")
            
            print("-----------------------------------------------------\n")
            
            # Update the 'Status' column
            df_six_monthly.loc[df_six_monthly['Company'] == customer, 'Three-Monthly Alert'] = status
        else:
            status = 'Not Enough Data'
            df_six_monthly.loc[df_six_monthly['Company'] == customer, 'Three-Monthly Alert'] = status
    
    return df_six_monthly, df_date

def weekly(df):
    # Filter data for the current year
    df = df[df['Date Generated'].dt.year == 2023]
    
    # Group df by Company and Week Num
    df_group = df.groupby(['Company', 'Week Num']).size().reset_index(name="Shipment Count")
    
    # Sort the grouped dataframe by Company and Week Num in descending order
    df_group = df_group.sort_values(['Company', 'Week Num'], ascending=[True, False])
    
    # Keep only the max-1 and max-2 Week Num values for each specific Company
    result_df = df_group.groupby('Company').head(2)
    
    return result_df

def monthly(df_three_monthly, df_date, file_path):
    # Get unique customers
    customers = df_three_monthly['Company'].unique()
    
    window_size = 30
    
    # Find yesterday's date for the end of the last 30 calendar days
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=window_size - 1)
    
    print("Recent Start Date: ", start_date)
    print("Recent End Date: ", end_date)
    
    prev_end_date = start_date - timedelta(days=1)
    prev_start_date = prev_end_date - timedelta(days=window_size - 1)
    
    print("Previous Start Date: ", prev_start_date)
    print("Previous End Date: ", prev_end_date)
    
    # Convert 'Date Generated' column to datetime if it's not already
    df_three_monthly['Date Generated'] = pd.to_datetime(df_three_monthly['Date Generated'])
    
    # Filter the DataFrame based on the date range
    prev_monthly_data = df_three_monthly[
        ((df_three_monthly['Date Generated'] >= prev_start_date) & (df_three_monthly['Date Generated'] <= prev_end_date))
        ]
    
    recent_monthly_data = df_three_monthly[
        ((df_three_monthly['Date Generated'] >= start_date) & (df_three_monthly['Date Generated'] <= end_date))
        ]
    
    # Display the new DataFrame
    print("Previous Monthly Data:", prev_monthly_data)
    print("Recent Monthly Data: ", recent_monthly_data)
    
    # Save data
    prev_monthly_data.to_csv(f'{file_path}prev_monthly_data.csv', index=False)
    recent_monthly_data.to_csv(f'{file_path}recent_monthly_data.csv', index=False)
    
    # Add a new row to the DataFrame using loc
    new_index = len(df_date)
    df_date.loc[new_index] = [start_date, end_date, prev_start_date, prev_end_date, 'Monthly Alert']
    
    for customer in customers:
        # Filter data for the current customer
        customer_data = df_three_monthly[df_three_monthly['Company'] == customer]
        print("\n")
        print(customer)

        # Filter data based on the time frame
        recent_filtered_data = customer_data[(customer_data['Date Generated'] >= start_date) & (customer_data['Date Generated'] <= end_date)]
        prev_filtered_data = customer_data[(customer_data['Date Generated'] >= prev_start_date) & (customer_data['Date Generated'] <= prev_end_date)]
        
        # Group Recent Data
        grouped_recent_data = recent_filtered_data.groupby('Date Generated').agg({'Charge': 'sum', 'Bill of Lading': 'count'})
        grouped_prev_data = prev_filtered_data.groupby('Date Generated').agg({'Charge': 'sum', 'Bill of Lading': 'count'})
        
        # Calculate the sum of 'Charge' within the specified time frame
        recent_charge = grouped_recent_data['Charge'].sum()
        prev_charge = grouped_prev_data['Charge'].sum()
        shipcount_recent = grouped_recent_data['Bill of Lading'].sum()
        shipcount_prev = grouped_prev_data['Bill of Lading'].sum()
        
        print("Grouped Recent Shipcount: ", shipcount_recent)
        print("Grouped Previous Shipcount: ", shipcount_prev)
        print("Grouped Recent Charge: ", recent_charge)
        print("Grouped Previous Charge: ", prev_charge)
        
        # Reset Index
        grouped_recent_data = grouped_recent_data.reset_index()
        grouped_prev_data = grouped_prev_data.reset_index()
        
        # Create Date Range and fill in missing dates with 0's
        recent_date_range = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
        prev_date_range = pd.date_range(start=prev_start_date.date(), end=prev_end_date.date(), freq='D')
        grouped_recent_data['Date Generated'] = pd.to_datetime(grouped_recent_data['Date Generated']).dt.date
        grouped_prev_data['Date Generated'] = pd.to_datetime(grouped_prev_data['Date Generated']).dt.date
        grouped_recent_data = grouped_recent_data.set_index('Date Generated').reindex(recent_date_range).fillna(0).reset_index()
        grouped_prev_data = grouped_prev_data.set_index('Date Generated').reindex(prev_date_range).fillna(0).reset_index()
        
        # Print new data frames
        print("Grouped Recent Data: \n", grouped_recent_data)
        print("Grouped Previous Data: \n", grouped_prev_data)
        
        
        # Perform paired t-test
        t_stat, p_value = ttest_rel(grouped_prev_data['Bill of Lading'], grouped_recent_data['Bill of Lading'])
        
        
        if p_value < 0.01:
            if shipcount_recent > shipcount_prev:
                status = 'Significantly More'
            elif shipcount_recent < shipcount_prev:
                status = 'Significantly Less'
            else:
                status = 'Error'
        elif p_value >= 0.01:
            status = 'No Significant Difference'
        else:
            status = 'Not Enough Data'
            
        print(f'Status: {status}')
        print(f"T-statistic: {t_stat:.10f}")
        print(f"P-value: {p_value:.10f}\n")
        
        print("-----------------------------------------------------\n")

        # Update the 'Status' column
        df_three_monthly.loc[df_three_monthly['Company'] == customer, 'Monthly Alert'] = status

    return df_three_monthly, df_date

def concatenate_csv_files(folder_path):
    # Get the list of file names in the folder
    file_names = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

    # List to store DataFrames
    dfs = []

    # Loop through the file names, read CSV files, and append them to the list
    for file_name in file_names:
        file_path = os.path.join(folder_path, file_name)
        df = pd.read_csv(file_path)
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    df_sales = pd.concat(dfs, ignore_index=True)

    df_sales['Sales Rep'] = df_sales['Sales Rep'].str.replace('Dave Helm', 'David Helm')

    new_rows = [
        {'Sales Rep': 'David Helm', 'Company': 'Herff Jones Arcola'},
        {'Sales Rep': 'David Helm', 'Company': 'Herff Jones Champaign'},
        {'Sales Rep': 'Charlie Conners', 'Company': 'BGR'},
        {'Sales Rep': 'Charlie Conners', 'Company': 'BGR Label Printing'},
        {'Sales Rep': 'House', 'Company': 'Clover Imaging Group OTR'}
    ]

    # Append the new rows to the df_sales DataFrame
    df_sales = df_sales.append(new_rows, ignore_index=True)

    # Iterate through the 'Company' column in df_sales
    for main_name, alternate_names in COMPANY_ALTERNATE_NAMES.items():
        for alternate_name in alternate_names:
            df_sales['Company'] = df_sales['Company'].replace(alternate_name, main_name)

    # Remove duplicates from the concatenated DataFrame
    df_sales.drop_duplicates(inplace=True)

    df_sales = df_sales[['Sales Rep', 'Company']]

    return df_sales

def main():
    file_path = '/Users/carlybackstrand/Desktop/HomeBase/Projects/Alerts/Customer Shipcount Alert/'
    folder_path = '/Users/carlybackstrand/Desktop/GeneralTools/Sales_Reps_Dictionaries/'
    
    # Load Data
    print("Loading Data...\n")
    df = load_data()
    print(df.columns)
    
    # Add Company Start Date
    df['First Date'] = ''
    df['Last Date'] = ''
    df_ranked = company_start(df)
    
    # Add alert columns to df
    df_ranked['Overall Trend'] = ''
    #df_ranked['2023 Trend'] = ''
    df_ranked['YOY Alert'] = ''
    df_ranked['Monthly YOY Alert'] = ''
    df_ranked['Six-Monthly Alert'] = ''
    df_ranked['Three-Monthly Alert'] = ''
    df_ranked['Monthly Alert'] = ''

    print(df_ranked.columns)
    
    # Add a new df to capture the time indervals
    df_date = pd.DataFrame(columns=['Recent Start Date', 'Recent End Date', 'Previous Start Date', 'Previous End Date', 'Alert'])
    
    ###### ALERT CREATION SECTION ######
    # YOY recent month Comparison
    df_yoy_monthly, df_date = yoy_monthly(df_ranked, df_date, file_path)
    
    # Compare YOY
    df_yoy, df_date = yoy(df_yoy_monthly, df_date, file_path)
    
    # Compare the recent 6 months with the previous 6 months
    df_six_monthly, df_date = six_monthly(df_yoy, df_date, file_path)
    
    # Compare the recent 3 months with the previous 3 months
    df_three_monthly, df_date = three_monthly(df_six_monthly, df_date, file_path)
    
    # Compare the recent 2 months
    df_monthly, df_date = monthly(df_three_monthly, df_date, file_path)

    # Assuming 'Date Generated' is the column containing your dates
    df_sorted = df_monthly.sort_values(by='Date Generated', ascending=True)
    
    df_group = df_sorted.groupby('Company').agg({
        'Bill of Lading': 'count',
        'Date Generated': 'last',
        'First Date': 'last',
        'Last Date': 'last',
        'Charge': 'sum',
        'Mode': 'first',
        'Week Num': 'last',
        'Monthly Alert': 'first',
        'Monthly YOY Alert': 'first',
        'Three-Monthly Alert': 'first',
        'Six-Monthly Alert': 'first',
        'YOY Alert': 'first',
    }).reset_index()
    
    # Add Company Ranking
    df_rank = company_ranking(df_group)

    # Add in Sales Rep
    df_sales = concatenate_csv_files(folder_path)

    # merge the sales reps with the companies to create the df_merged df
    df_merged = df_rank.merge(df_sales, on='Company', how='left')
    
    # Fill missing values in 'Sales Rep' column with 'Unassigned'
    df_merged['Sales Rep'].fillna('Unassigned', inplace=True)

    # Drop duplicates based on 'Company' column
    df_merged.drop_duplicates(subset=['Company'], inplace=True)

    print("Saving...\n")
    df_merged.to_csv(f'{file_path}Company_alerts.csv', index=False)
    
    # Make sure the dates columns display correctly
    df_date['Recent Start Date'] = pd.to_datetime(df_date['Recent Start Date'])
    df_date['Recent End Date'] = pd.to_datetime(df_date['Recent End Date'])
    df_date['Previous Start Date'] = pd.to_datetime(df_date['Previous Start Date'])
    df_date['Previous End Date'] = pd.to_datetime(df_date['Previous End Date'])
    
    print("Saving Time Intervals...\n")
    df_date.to_csv(f'{file_path}date_intervals.csv', index=False)
    print('Complete')
    

if __name__ == '__main__':
    main()