#Created by happilyeverafter123

#import the packages ------------------------------------------------------------------------------
import logging
import os
import pandas as pd
import re
import sys
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pathlib import Path
from sec_edgar_downloader import Downloader

#--------------------------------------------------------------------------------------------------

#for debug
#logging.basicConfig(level=logging.DEBUG)

load_dotenv()

#set up the EDGAR downloader
company_name = os.environ.get("COMPANY_NAME")
email = os.environ.get("EMAIL")
dl = Downloader(company_name, email) # local directory for saving data

#download the data and extract the information ----------------------------------------------------
def gather_filings(ticker_symbol: str, limit: int = 1) -> Path:

    print(f"Downloading 10-K for {ticker_symbol}...")

    try:
        #get the 10-Q of the company
        base_path = Path(os.environ.get("BASE_PATH"))
        dl.get("10-K", ticker_symbol, limit=limit)
        time.sleep(1)
        filings_path = base_path / ticker_symbol / "10-K"
        return filings_path
    
    except Exception as e:
        print(f" I could not download filings for {ticker_symbol}: {e}")
    
#classes for components to extract.  You can append any if you want--------------------------------

#for conformed period of report
def parse_conformed_period_of_time(soup: BeautifulSoup) -> str:
    conformed_period_of_time = soup.find('acceptance-datetime')

    if conformed_period_of_time:
        #get the text
        text_content = conformed_period_of_time.get_text()
        
        #using re to extract the number next to: CONFORMED PERIOD OF REPORT:
        match = re.search(r"CONFORMED PERIOD OF REPORT:\s*(\d+)", text_content)
        
        if match:
            conformed_period = match.group(1)
            return conformed_period
        else:
            print("No matching text found.")
            return None
    else:
        print(f"Conformed period of time not found...")
        return None
        
#for net income (in millions)
def parse_net_income(soup: BeautifulSoup) -> str:
    net_income = soup.find("ix:nonfraction", {"name": "us-gaap:NetIncomeLoss"})

    if net_income:
        return net_income.text
    else:
        print(f"Net Income not found...")
        return None
        
#for shares outstanding (in thousands)
def parse_shares_outstanding(soup: BeautifulSoup) -> str:
    shares_outstanding = soup.find("ix:nonfraction", {"name": "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding"})

    if shares_outstanding:
        return shares_outstanding.text
    else:
        print(f"Shares Outstanding not found...")
        return None

#for total revenue (in millions)
def parse_revenue(soup: BeautifulSoup) -> str:
    revenue = soup.find("ix:nonfraction", {"name": "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"})

    if revenue:
        return revenue.text
    else:
        print(f"Revenue not found...")
        return None
    
#for equity (in millions)
def parse_equity(soup: BeautifulSoup) -> str:
    equity = soup.find("ix:nonfraction", {"name": "us-gaap:StockholdersEquity"})

    if equity:
        return equity.text
    else:
        print(f"Equity not found...")
        return None
    
#convert the datatype (str) to int
def convert_str_to_int_in_dict_list(df, keys_to_convert):
    for key in keys_to_convert:
        if key in df.columns:
            df[key] = df[key].replace(",","", regex=True).astype(float).astype("Int64")
    return df
#--------------------------------------------------------------------------------------------------

#analyze the file----------------------------------------------------------------------------------
def analyze_filings(filings_path: Path, ticker_symbol: str) :

    results = pd.DataFrame()
    
    for filing in filings_path.rglob("*.txt") :
        results_temp = pd.DataFrame()
        print(f"Analizing file: {filing}...")
            
        try:
            #analyze the file
            with open(filing, "r", encoding="utf-8") as f:
                soup = BeautifulSoup(f, "lxml")

            #extract 発行日程(conformed time of report)、純利益(Net Income)、発行済み株式数(Shares Outstanding)、売り上げ(Total revenue)、株式資本(Equity)、
            conformed_date_data = parse_conformed_period_of_time(soup)
            net_income_data = parse_net_income(soup)
            shares_outstanding_data = parse_shares_outstanding(soup)
            revenue_data = parse_revenue(soup)
            equity_data = parse_equity(soup)
            
            print(conformed_date_data)
            
            #save the results
            # results_temp["file"] = filing.name
            # results_temp["ticker_symbol"] = ticker_symbol
            # results_temp["result_type"] = "10-Q"
            # results_temp["conformed_date"] = conformed_date_data
            # results_temp["net_income"] = net_income_data
            # results_temp["shares_outstanding"] = shares_outstanding_data
            # results_temp["revenue"] = revenue_data
            # results_temp["equity"] = equity_data
            
            results_temp = pd.DataFrame([{
                "file": filing.name,
                "ticker_symbol": ticker_symbol,
                "result_type": "10-K",
                "conformed_date": conformed_date_data,
                "net_income": net_income_data,
                "shares_outstanding": shares_outstanding_data,
                "revenue": revenue_data,
                "equity": equity_data,
            }])
            
            results = pd.concat([results, results_temp], ignore_index=True)
            # results.append({
            #     "file": filing.name,
            #     "ticker_symbol": ticker_symbol,
            #     "result_type": "10-Q",
            #     "conformed_date": conformed_date_data,
            #     "net_income": net_income_data,
            #     "shares_outstanding": shares_outstanding_data,
            #     "revenue": revenue_data,
            #     "equity": equity_data,
            # })
        except Exception as e:
            print(f"I could not extract file {filing}...")
            print(e)
    return results

#--------------------------------------------------------------------------------------------------

#main function-------------------------------------------------------------------------------------
def gather_and_extract_10K(ticker_symbol: str, limit: int):
    
    print(f"Processing {ticker_symbol}...")

    #gather the data
    filings_path = gather_filings(ticker_symbol, limit=limit)
    
    #analyze the data
    analysis_results_str = analyze_filings(filings_path, ticker_symbol)

    #keys for changing the results (str) to int
    keys_to_convert = [
            "conformed_date",
            "net_income",
            "shares_outstanding",
            "revenue",
            "equity"]
        
    #change the results (str) to int
    analysis_results = convert_str_to_int_in_dict_list(analysis_results_str, keys_to_convert)
    
    # print(analysis_results.head())
    
    return analysis_results

    # return analysis_results_str
#--------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    # change the limit of the filings you get if needed
    results = gather_and_extract_10K(ticker_symbol="AAPL", limit=3)
    print(results)
