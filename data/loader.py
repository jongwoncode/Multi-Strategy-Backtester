"""
Handles loading historical price data
If data is exist load the data else crawling data and load.
"""
import os
import pandas as pd
from loguru import logger


def load_price_data(market:str, symbol:str, timeframe:str, start_date:str, end_date:str, save_name:str) -> pd.DataFrame:
    """Functions that load data. Load data if it already exists in the folder. Crawling and loading non-existent data

    TODO
        1. 데이터 없을 시 crwaling해서 로드하는 부분 추가.
        2. 데이터 일부분만 없을 시 crwaling하고 기존 파일하고 병합하는 부분 추가.
        2. 데이터의 범위에 end time도 포함 필요.

    Args:
        market (str): market type - "stock" or "crypto"
        symbol (str): symbol
        timeframe (str): timeframe - "1m", "5m", "15m", "1h", "4h", "1d", "1w", "1m" ...
        start_date (str): start_date - YYYY-MM-DD
        end_date (str): end_date - YYYY-MM-DD
        save_name (str): btc.csv, eth.csv

    Returns:
        pd.DataFrmae: stock data. open, high, low, cloase, volumne
    """

    assert market in ['crypto'], ValueError("Currently, the marker only supports crypto.") 

    # file check
    file_path = os.path.join(os.getcwd(), 'data', market, timeframe, save_name)
    logger.info(file_path)

    if os.path.isfile(file_path):
        logger.info(f"Data already exist!!, Load Data from : {file_path}")
        
        if market == 'crypto':
            # 데이터 로드
            df = pd.read_csv(file_path)
            # 데이터 전처리
            df.drop('Open time', axis=1, inplace=True)
            df['Close time'] = pd.to_datetime(df['Close time']+1,unit='ms')
            df.rename({'Close time' : 'Date'}, axis=1, inplace=True)
            df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)].reset_index(drop=True)
            return df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    
    else:  
        pass
        # 크롤링 데이터
        # 로드 데이터


if __name__ == "__main__":
    test_df = load_price_data(market = 'crypto',
                              symbol = 'BTC', 
                              timeframe = '3m', 
                              start_date= '2020-01-01', 
                              end_date = '2024-02-24',
                              save_name = 'btc.csv')
    
    print(test_df.head())
    print(test_df.tail())