# 진입 자금, 진입 
from typing import Tuple, List
# size: 진입 사이즈, notional_size, entry_price, margin, unrealized_pnl, realized_pnl

class Side:
    NONE = 'NONE'
    BUY = 'BUY'
    SELL = 'SELL'
    

class StrategyManager:
    def __init__(self
                 , asset
                 , strategy_name
                 , trading_fee=0.045
                 , leverage=1):
        
        self.ASSET = asset
        self.STRATEGY_NAME = strategy_name
        self.TRADING_FEE = trading_fee
        self.LEVERAGE = leverage
        self.SIDE = None # BUY, SELL
        self.INITIAL_BALANCE = 0 # 초기 진입 자금
        self.ENTER_PRICE = 0  
        
        self.position_size = 0           # 진입 수량(with 레베리지)
        self.notional_position_size = 0  # 진입 포지션 사이즈(with 레버리지)  
        self.balance = 0        # 밸런스 (빌린 금액을 차감한 현재 자산)
        self.realized_amount = 0   # 실현 수익
        self.close_count = 0    # 청산 횟수
    
    
    def open(self, side, initial_balance, open_price):
        """포지션 오픈 및 정보 업데이트"""
        # 변하지 않는 부분
        self.SIDE = side
        self.ENTER_PRICE = open_price
        self.INITIAL_BALANCE = initial_balance
        
        self.balance = self.INITIAL_BALANCE * (1- (self.TRADING_FEE * self.LEVERAGE)/100) 
        self.notional_position_size = self.balance * self.LEVERAGE
        self.position_size = self.notional_position_size / open_price
        
        # 변하는 부분
        self.realized_amount = self.balance - self.INITIAL_BALANCE   # 수수료 부분 제외
    
    
    def close(self, close_size, close_price):
        close_size = min(self.position_size, close_size)
        # 실현 수익 계산
        realized_amount = self.calculate_realized_amount(close_size, close_price)
        self.realized_amount += realized_amount
        
        # position, balance 업데이트
        self.position_size -= close_size
        self.notional_position_size = close_price * self.position_size
        self.balance = self.update_balance(self.position_size, close_price)
        self.close_count += 1
        
        return { 'strategy_name': self.STRATEGY_NAME
                , 'close_count': self.close_count # 1 or 2
                , 'clear': True if self.position_size == 0 else False
                , 'close_size': close_size
                , 'close_price': close_price
                , 'enter_price': self.ENTER_PRICE
                , 'realized_now_amount': self.realized_amount if (self.close_count == 1) else realized_amount
                , 'realized_total_amount': self.realized_amount
                , 'leverage': self.LEVERAGE
                , 'pnl(%)': ((close_price/self.ENTER_PRICE) - 1) * self.LEVERAGE if self.SIDE == Side.BUY \
                                else ((self.ENTER_PRICE/close_price) - 1) * self.LEVERAGE 
                }
        
    
    def update(self, close):
        self.notional_position_size = close * self.position_size
        self.balance = self.update_balance(self.position_size, close)
        

    def calculate_realized_amount(self, close_size, close):
        """포지션 청산시 실현 수익 계산"""
        if self.SIDE == 'BUY':
            return ( (self.ENTER_PRICE * close_size) + self.LEVERAGE * (close - self.ENTER_PRICE) * close_size ) * (1- (self.TRADING_FEE))            
            
        if self.SIDE == 'SELL':
            return ( (self.ENTER_PRICE * close_size) + self.LEVERAGE * (self.ENTER_PRICE - close) * close_size ) * (1 - (self.TRADING_FEE))
            
        
    def update_balance(self, size, close):
        return self.calculate_realized_amount(size, close)
    
    
    def open_condition(self, check_data) -> Tuple[bool, Side, float]:
        """포지션 진입 여부 조건
            input: check_data (pd.DataFrame)
            output: Tuple[is_open, side, enter_price]: (bool, Side, float)  
        """
        pass
    
    def close_condition(self, check_data) -> Tuple[bool, float, float]:
        """포지션 청산 여부 조건
            input: check_data (pd.DataFrame)
            output: Tuple[is_close, close_size, close_price]: (bool, float, float)
        """
        pass