import pandas as pd
import ta
import numpy as np

class Preprocessor:
    """기술적 지표를 생성하기 위한 클래스"""
    def possible_columns(self):
        return ['EMA/{number/}', 'MA/{number/}', "RSI", 'BBUpper', 'BBLower', 'PANGLE', 'ANGLE']
    
    def make_column(self, column, df):
        df_copy = df.copy()
        
        if 'EMA' in column and '_' not in column:
            period = int(column[3:])
            return self.exponential_moving_average(df_copy, period)
        
        if 'MA' in column and '_' not in column:
            period = int(column[2:])
            return self.moving_average(df_copy, period)
        
        if column == 'RSI':
            return self.rsi(df_copy)
        
        if column == 'BBUpper':
            return self.bollinger_bands(df_copy, 'upper')
        
        if column == 'BBLower':
            return self.bollinger_bands(df_copy, 'lower')
        
        if 'PANGLE' in column:
            '''PANGLE_MA1920_100'''
            base, diff = column.split('_')[1], int(column.split('_')[2])
            return self.calculate_percent_angle(df_copy, column, base, diff)        
        
        if 'ANGLE' in column:
            '''PANGLE_MA1920_100'''
            base, diff = column.split('_')[1], int(column.split('_')[2])
            return self.calculate_angle(df_copy, column, base, diff)
        
        
    def moving_average(self, df_copy, n):
        df_copy[f'MA{n}'] = ta.trend.sma_indicator(df_copy['Close'], window=n)
        return df_copy[f'MA{n}']
    
    def exponential_moving_average(self, df_copy, n):
        df_copy[f'EMA{n}'] = ta.trend.ema_indicator(df_copy['Close'], window=n)
        return df_copy[f'EMA{n}']
    
    def rsi(self, df_copy, window=14):
        df_copy['RSI'] = ta.momentum.rsi(df_copy['Close'], window=window)
        return df_copy['RSI']
    
    def bollinger_bands(self, df_copy, band_type='upper', window=20, window_dev=2):
        indicator_bb = ta.volatility.BollingerBands(close=df_copy['Close'], window=window, window_dev=window_dev)
        
        if band_type == 'upper':
            df_copy['BBUpper'] = indicator_bb.bollinger_hband()
            return df_copy['BBUpper']
        
        if band_type == 'lower':
            df_copy['BBLower'] = indicator_bb.bollinger_lband()
            return df_copy['BBLower']
    

    def calculate_angle(self, df_copy, column, base, diff):
        df_copy[column] = np.degrees(np.arctan((df_copy['Close'] - df_copy[base].shift(diff)) / 100))
        return df_copy[column]
    
    def calculate_percent_angle(self, df_copy, column, base, diff):
        # 100기간의 base에 대한 현재수익률 계산
        df_copy['Return'] = (df_copy['Close'] / df_copy[base].shift(diff)) - 1
        # 각도 계산
        df_copy[column] = np.degrees(np.arctan(df_copy['Return']))    
        return df_copy[column]
    