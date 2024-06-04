# imports
import pandas as pd
import os
from IPython.display import display
import glob


def main():
    # Read CSV files from List
    print("Loading combined Data....")
    path = f"/Users/carlybackstrand/Desktop/HomeBase/Projects/FlagProject/"
    all_files = glob.glob(path + "/*.csv")

    df = pd.concat((pd.read_csv(f) for f in all_files)).set_index('Company')

    print(df)

    # create a function that assigns 'Active Month' values to Tiers
    def tier_status(value):
        if value <= 12:
            return "Tier 1"
        elif 13 <= value <= 24:
            return "Tier 2"
        elif value >= 25:
            return "Tier 3"
        else:
            return "No Tier Assigned"

    df['Tier_status'] = df['Active Months'].map(tier_status)

    # create a function that assigns the flagged Tier status
    def alert_status(value):
        if value == 13:
            return "New Tier 2 Status"
        elif value == 25:
            return "New Tier 3 Status"
        else:
            return "No Status Change"

    df['Flagged_Tiers'] = df['Active Months'].map(alert_status)

    # Calculating outliers
    def find_outliers_IQR(df):
        q1 = df.quantile(0.25)
        q3 = df.quantile(0.75)
        IQR = q3-q1
        outliers = df[(df < q1-1.5*IQR) | (df > (q3+1.5*IQR))]
        return outliers

    outliers = find_outliers_IQR(df['Margin'])
    print("number of outliers:" + str(len(outliers)))
    print("max outlier value:" + str(outliers.max()))
    print("min outlier value:" + str(outliers.min()))
    print("All outliers:\n", outliers)

    df['Margin Outliers'] = find_outliers_IQR(df[['Margin']])

    # Final output
    print("final dataset...")
    df = df.query("Flagged_Tiers == 'New Tier 3 Status' or Flagged_Tiers == 'New Tier 2 Status'")
    print("Queried Df:\n", df)

    # Saving Data
    print("Saving...")
    df.to_csv(f'{file_path}DomesticFlagFinal.csv', index=True)
    print('Complete')


if __name__ == '__main__':
    file_path = '/Users/carlybackstrand/Desktop/HomeBase/Projects/'
    main()


