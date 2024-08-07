from typing import Tuple
from utils import Side, Status
import pandas as pd
from strategy.base import StrategyManager
    
class SimpleMovingAverageStrategy(StrategyManager):
    def __init__(self, asset, strategy_name, trading_fee=0.045):
        super().__init__(asset, strategy_name, trading_fee)
        
    def need_columns(self):
        return ['Close', 'MA5', 'MA20']    

    def open_condition(self, check_data: pd.Series) -> Tuple[bool, str, float]:
        """포지션 진입 여부 조건
            - 단기 이동 평균이 장기 이동 평균을 상향 돌파할 때 매수 신호
            - 장기 이동 평균이 단기 이동 평균을 상향 돌파할 때 매도 신호
        """
        ma5, ma20 = check_data['MA5'], check_data['MA20']
        
        if ma5 > ma20:
            is_open, side, enter_price = True, Side.BUY, check_data['Close']
            return is_open, side, enter_price
        elif ma5 < ma20:
            is_open, side, enter_price = True, Side.SELL, check_data['Close']
            return is_open, side, enter_price
        
        is_open, side, enter_price = False, Side.NONE, 0.0
        return is_open, side, enter_price
    
    def close_condition(self, check_data: pd.Series) -> Tuple[bool, float, float]:
        """포지션 청산 여부 조건
            - 매수 포지션일 때, 단기 이동 평균이 장기 이동 평균을 하향 돌파하면 청산
            - 매도 포지션일 때, 단기 이동 평균이 장기 이동 평균을 상향 돌파하면 청산
        """
        ma5, ma20 = check_data['MA5'], check_data['MA20']
        if self.SIDE == Side.BUY and ma5 < ma20:
            is_close, close_size, close_price = True, self.position_size, check_data['Close']
            return is_close, close_size, close_price
        
        elif self.SIDE == Side.SELL and ma5 > ma20:
            is_close, close_size, close_price = True, self.position_size, check_data['Close']
            return is_close, close_size, close_price
        
        is_close, close_size, close_price = False, 0.0, 0.0
        return is_close, close_size, close_price

    
    
class PartialCloseMovingAverageStrategy(StrategyManager):
    def __init__(self, asset, strategy_name, trading_fee=0.045):
        super().__init__(asset, strategy_name, trading_fee)
        self.partial_close_done = False  # 절반 청산 여부를 확인하는 플래그
    
    def need_columns(self):
        return ['Close', 'MA5', 'MA20']    

    def open_condition(self, check_data: pd.Series) -> Tuple[bool, str, float]:
        """포지션 진입 여부 조건
            - 단기 이동 평균이 장기 이동 평균을 상향 돌파할 때 매수 신호
            - 장기 이동 평균이 단기 이동 평균을 상향 돌파할 때 매도 신호
        """
        ma5, ma20 = check_data['MA5'], check_data['MA20']
        
        if ma5 > ma20:
            is_open, side, enter_price = True, Side.BUY, check_data['Close']
            return is_open, side, enter_price
        elif ma5 < ma20:
            is_open, side, enter_price = True, Side.SELL, check_data['Close']
            return is_open, side, enter_price
        
        is_open, side, enter_price = False, Side.NONE, 0.0        
        return is_open, side, enter_price
    
    def close_condition(self, check_data: pd.Series) -> Tuple[bool, float, float]:
        """포지션 청산 여부 조건
            - 첫 번째 조건: 포지션의 절반을 청산
            - 두 번째 조건: 나머지 포지션을 청산
        """
        ma5, ma20 = check_data['MA5'], check_data['MA20']
        if self.SIDE == Side.BUY and ma5 < ma20:
            if not self.partial_close_done:
                # 첫 번째 청산 조건
                is_close, close_size, close_price = True, self.position_size / 2, check_data['Close']
                self.partial_close_done = True
                return is_close, close_size, close_price
            else:
                # 두 번째 청산 조건
                is_close, close_size, close_price = True, self.position_size, check_data['Close']
                return is_close, close_size, close_price
        
        elif self.SIDE == Side.SELL and ma5 > ma20:
            if not self.partial_close_done:
                # 첫 번째 청산 조건
                is_close, close_size, close_price = True, self.position_size / 2, check_data['Close']
                self.partial_close_done = True
                return is_close, close_size, close_price
            else:
                # 두 번째 청산 조건
                is_close, close_size, close_price = True, self.position_size, check_data['Close']
                return is_close, close_size, close_price
        
        is_close, close_size, close_price = False, 0.0, 0.0
        return is_close, close_size, close_price
    
    def update(self, close):
        super().update(close)
        # 포지션 청산 후 포지션이 없으면 플래그 초기화
        if self.position_size == 0:
            self.partial_close_done = False