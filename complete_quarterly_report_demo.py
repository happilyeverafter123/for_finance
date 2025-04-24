# Description: This script is used to align the data for the 10-K and 10-Q reports.
from datetime import datetime
from gather_and_extract_10K import gather_and_extract_10K
from gather_and_extract_10Q import gather_and_extract_10Q
import argparse
import calendar
import pandas as pd

# Set up command line arguments (to specify the number of 10-Q downloads)
parser = argparse.ArgumentParser(description="Process SEC data and limit the number of results.")
parser.add_argument(
    "-n", "--number", type=int, default=1,
    help="Number of data entries to process (default is 1)."
)

# Analyze arguments
args = parser.parse_args()
data_limit = args.number

# --------------------------------------------------------------------------------------------------

# Create the list of quarters
def create_quarters(n_periods=int(data_limit)):
    
    today = datetime.today()
    year = today.year
    quarters = []

    quarter_starts = [(1, 1), (4, 1), (7, 1), (10, 1)]

    # Estimate current quarter
    for i, (start_month, _) in enumerate(quarter_starts):
        if today.month >= start_month and (i == 3 or today.month < quarter_starts[i + 1][0]):
            current_quarter = i
            break

    # List up the quarters
    for _ in range(n_periods):
        start_month, start_day = quarter_starts[current_quarter]
        end_month = start_month + 2  # End month of quarter
        _, end_day = calendar.monthrange(year, end_month)
        start_date = datetime(year, start_month, start_day)
        end_date = datetime(year, end_month, end_day)

        quarters.append((start_date, end_date))

        # Move to the previous period
        current_quarter -= 1
        if current_quarter < 0:
            current_quarter = 3
            year -= 1

    return quarters[::-1]  # Reverse to have past-to-recent order

# Verify conformed_date and check periods where 10-Q doesn't exist
def check_missing_quarters(original_10q, quarter_list):

    existing_10q_dates = []
    for _, entry in original_10q.iterrows():
        existing_10q_dates.append(datetime.strptime(str(entry["conformed_date"]), "%Y%m%d").date())

    print(existing_10q_dates)

    missing_periods = []
    
    for start_date, end_date in quarter_list:
        if not any(start_date.date() <= date <= end_date.date() for date in existing_10q_dates):
            missing_periods.append((start_date, end_date))
        
    print(missing_periods)
    return missing_periods

# Download 10-K for missing periods and update results
def download_additional_10k(missing_periods):
    print(f"Downloading 10-K for {ticker_symbol}...")
    results_10k = gather_and_extract_10K(ticker_symbol, limit=len(missing_periods))
    return results_10k

# Download additional 10-Q if needed and adjust 10-K based on them
def download_additional_10q(original_10q, downloaded_10k):

    total_results = downloaded_10k
    print(total_results)
    for annual_entry in downloaded_10k:
                
        print(f"Here is annual entry: {annual_entry}")
        entry_year = str(annual_entry["conformed_date"])[:4]

        # Get 10q for the year
        existing_q_reports = original_10q[
            (original_10q["conformed_date"].astype(str).str[:4] == entry_year) & 
            (original_10q["result_type"] == "10-Q")
        ]

        # If reports needed do not exist, add
        if len(existing_q_reports) < 3:
            missing_q_count = 3 - len(existing_q_reports)
            print(f"Missing {missing_q_count} quarters for {ticker_symbol} in {entry_year}, downloading additional 10-Q results...")
            total_10q_count = int(data_limit) + missing_q_count
            updated_10q = gather_and_extract_10Q(ticker_symbol=ticker_symbol, limit=total_10q_count)
            total_results = pd.concat([total_results, updated_10q])
            total_results.extend(updated_10q)
            total_results = pd.concat([total_results, original_10q])
            total_results.extend(original_10q)

    return total_results

# Display the results
if __name__ == "__main__":
    for ticker_symbol in ["AAPL"]:
        print(f"Processing {ticker_symbol}...")
    # Execute the process
        original_10q = gather_and_extract_10Q(ticker_symbol="AAPL", limit=(int(data_limit)))
        quarter_list = create_quarters(n_periods=int(data_limit))
        missing_periods = check_missing_quarters(original_10q, quarter_list)
        if missing_periods:
            downloaded_10k = download_additional_10k(missing_periods)
            final_result= download_additional_10q(original_10q, downloaded_10k)
        else:
            final_result = original_10q
        df = pd.DataFrame(final_result)
        print(df.head())
    
        # Save the DataFrame to a CSV file
        df.to_csv(f"{ticker_symbol}_{data_limit}_final_result.csv", index=False)
        
        #報告書の作成当時のドル相場を反映させる
