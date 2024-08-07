import time
from tqdm import tqdm
import numpy as np
import pandas as pd
from collections import defaultdict
from utils import Status, Side
from typing import Tuple, List, Dict

from preprocessor import Preprocessor   
    
class Backtesting:
    def __init__(self
                 , strategy_list
                 , total_balance=10000
                 , max_strategy_cnt=5
                 , max_strategy_simultaneously_cnt=3
                 , min_trading_amount=100):
        """Backtesting Infra for multi-asset, multi-strategy trading.

        Args:
            strategy_list (list): 전략 객체 및 파라미터 리스트
                [
                    {'object': StrategyManager, 'parameter': {'asset': 'ETHUSDT', 'strategy_name': 'longSt1', 'trading_fee':0.045}},
                    {'object': StrategyManager, 'parameter': {'asset': 'BTCUSDT', 'strategy_name': 'longSt2', 'trading_fee':0.045}}
                ]
                
            total_balance (int, optional): 초기 자산(USDT). Defaults to 10000.
            max_strategy_cnt (int, optional): 최대 전략 개수. Defaults to 5.
            max_strategy_simultaneously_cnt (int, optional): 동일 전략의 최대 동시 진입 개수. Defaults to 3.
            min_trading_amount (int, optional): 최소 거래 금액. Defaults to 100.
        """
        self.total_balance = total_balance # 전체 자산(USDT)
        self.remain_balance = self.total_balance # 진입 가능한 자산(USDT)
        self.enter_balance = 0
        self.total_notional_position_size = 0
        self.MAX_STRATEGY_CNT = max_strategy_cnt    # 동시에 진입 가능한 포지션 개수
        self.MAX_STRATEGY_SIMULTANEOUSLY_CNT = max_strategy_simultaneously_cnt  # 동시에 진입 가능한 동일 전략 최대 개수  
        self.MIN_TRADING_AMOUNT = min_trading_amount
        
        #### 전략 관리 ####
        self.strategy_list = strategy_list # 전략 객체와 파라미터 (인스턴스 생성 전)를 담아두는 리스트
        self.strategy_queue = []    # 선언된 전략 인스턴스를 담아두는 List --> 진입 가능한 전략을 체크할 때 활용
        self.enter_strategy_list = []   # 진입한 전략 인스턴스를 담아두는 List --> Close 여부 확인
        self.strategy_in_mangement = {'total': 0}   # 현재 진입 중인 전략 개수 저장 객체 -->전략이 들어갈 자리가 있는지 
        
        
        #### 정보 저장 ####
        self.strategy_clear_info = []
        self.backtesting_info = []
        
        ### 데이터 전처리기 ###
        self.preprocessor = Preprocessor()
        
        # 진입 가능한 전략 queue에 담아두기.
        self.fill_strategy_queue()
        # 전략 별 진입 개수 관리 객체 초기화
        self.initialize_strategy_in_mangement()
        
    def ready_data(self, datalist):
        """BackTesting을 위해서 필요한 컬럼을 생성하는 부분."""
        for strategy_instance in self.strategy_queue:
            need_columns = strategy_instance.need_columns()
            strategy_asset = strategy_instance.ASSET

            """datalist formay example
                datalist = [
                            { 'BTCUSDT': pd.DataFrame },
                            { 'ETHUSDT': pd.DataFrame }
                           ]             
            """
            for data_info in datalist:
                data_asset = list(data_info.keys())[0]
                # data의 asset과 전략에서 다루고 있는 asset이 동일할 때 필요한 컬럼 생성.
                if data_asset == strategy_asset:
                    df_asset = data_info[data_asset]
                    
                    # Preprocessor
                    for column in need_columns:
                        if column not in df_asset.columns:
                            df_asset[column] = self.preprocessor.make_column(column, df_asset) 
            
        # NaN 제거        
        for data_info in datalist:
            data_asset = list(data_info.keys())[0]
            data_info[data_asset] = data_info[data_asset].dropna()
            
        return datalist
    
    
    def fill_strategy_queue(self):
        """전략을 담아 두는 list - 진입 가능한 전략 체크할 때 활용"""
        for strategy in self.strategy_list:
            # 전략 인스턴스 생성
            strategy_instance = strategy['object'](strategy_name=strategy['parameter']['strategy_name']
                                                   , asset=strategy['parameter']['asset']
                                                   , trading_fee=strategy['parameter']['trading_fee'])
            
            self.strategy_queue.append(strategy_instance)
    
    def initialize_strategy_in_mangement(self):
        """전략마다 진입 개수를 관리하는 strategy_in_cnt를 업데이트"""
        for strategy_instance in self.strategy_queue:
            self.strategy_in_mangement[strategy_instance.STRATEGY_NAME] = 0
    
    def update_strategy_in_management(self, status, strategy_instance):
        """현재 진입 중인 전략 개수 정보 업데이트"""
        add = 1 if status == Status.IN else -1    
        self.strategy_in_mangement['total'] += add
        self.strategy_in_mangement[strategy_instance.STRATEGY_NAME] += add
        
        # 전략 관리 디버깅
        if (0 > self.strategy_in_mangement['total']) \
            or (self.strategy_in_mangement['total'] > self.MAX_STRATEGY_CNT) \
            or (0 > self.strategy_in_mangement[strategy_instance.STRATEGY_NAME]) \
            or (self.strategy_in_mangement[strategy_instance.STRATEGY_NAME] > self.MAX_STRATEGY_SIMULTANEOUSLY_CNT):
            raise Exception('전략 진입에 허용범위를 넘어섰습니다. 전략 관리 로직을 다시 살펴보세요 ')
    
    def update_strategy_in_list(self, i):
        """strategy_queue에서 빼서 enter_strategy_list에 넣어주기
        
        self.enter_strategy_list = [     S2_Instance  ]
                                        (3)push |
                                                |
        self.strategy_queue= [S1_Instance,    (1)pop, (2)push   S3_Instance,     S4_Instance,   ]
                                                           |
                                                           | 
        self.strategy_list = [S1_Object&Param, S2_Object&Param, S3_Object&Param, S4_Object&Param]
        """
        pop_instance = self.strategy_queue.pop(i)
        strategy = self.strategy_list[i]

        push_instance = strategy['object'](strategy_name=strategy['parameter']['strategy_name']
                                            , asset=strategy['parameter']['asset']
                                            , trading_fee=strategy['parameter']['trading_fee'])
        
        self.strategy_queue.insert(i, push_instance)
        self.enter_strategy_list.append(pop_instance)
        
    def update_strategy_out_list(self, idx_list):
        """청산된 전략 enter_strategy_list에서 제거"""
        self.enter_strategy_list = [self.enter_strategy_list[i] for i in range(len(self.enter_strategy_list)) if i not in idx_list]
        
        
    def check_slot_for_open(self, strategy_instance) -> bool:
        """현재 전략이 들어갈 수 있는 자리가 있는지 확인, 이를 만족해야 전략 진입 조건을 고려"""
        condition_total = self.strategy_in_mangement['total'] < self.MAX_STRATEGY_CNT
        condition_strategy = self.strategy_in_mangement[strategy_instance.STRATEGY_NAME] < self.MAX_STRATEGY_SIMULTANEOUSLY_CNT
        return True if (condition_total and condition_strategy) else False 
    
    def decision_enter_balance(self) -> float:
        """포지션 진입시 진입 금액 계산 - 잔여 자산 / 진입 가능한 포지션 수"""
        return self.remain_balance / (self.MAX_STRATEGY_CNT - self.strategy_in_mangement['total'])
        
    def asset_checker(self, data, strategy_instance) -> bool:
        """진입 전략의 asset과 data의 asset의 일치 여부 확인"""
        return True if (list(data.keys())[0] == strategy_instance.ASSET) else False

    
    def update_backtesting_info(self, data):
        self.backtesting_info.append({
            'Date': data['Date'],
            'total_balance': self.total_balance,
            'enter_balacne': self.enter_balance,
            'remain_balance': self.remain_balance,
            'entered_strategy_cnt': len(self.enter_strategy_list)
        })
    
    
    def data_checker(self, datalist) -> bool:
        """
            BackTesting 객체에서 허용하는 데이터 입력 규칙을 만족하는지 체크
            (1) Date는 datetime 형식이다.
            (2) 여러 데이터가 있다면 시작시점과 끝 시점이 동일해야 한다.
            (3) 동일한 timeframe이어야 한다.
            (4) 입력 형태는 {'Asset' : pd.DataFrame}으로 구성된 List이다.
            (4) 전략들이 condition에서 사용하는 모든 정보를 포함하고 있어야 한다. (이 부분은 추가로 업데이트)
        """
        return True
    
    def run(self, datalist: List[Dict[str, pd.DataFrame]]):
        """ datalist = [ 
                        { 'ETHUSDT' : pd.DataFrame },
                        { 'BTCUSDT' : pd.DataFrame },
                       ]
            각 데이터들은 시간 시간이 일치해야 함. 
        """
        assert self.data_checker(datalist), 'Check Data Condition Rules!!'
        
        # [HERE] 여기 부분에서 Data 준비.
        datalist = self.ready_data(datalist)
        
        for didx in tqdm(range(len(list(datalist[0].values())[0]))):
            # (1) 진입된 전략 청산 조건 파악.
            for data in datalist:
                clear_strategy_idx = []
                for sidx, strategy_instance in enumerate(self.enter_strategy_list):
                    # Asset 일치 여부 확인
                    if self.asset_checker(data, strategy_instance):
                        # [TODO] Strategy Manager 하나 만들어서 해보자. 청산 조건 확인
                        # [TODO] 모든 asset을 close했을 때 어디서 전략을 pop할지 고민하자, update or 
                        check_data = data[strategy_instance.ASSET].iloc[didx, :]
                        
                        is_close, close_size, close_price = strategy_instance.close_condition(check_data)
                        if is_close:
                            # 포지션 종료(청산 or 익절(or 손절))
                            close_info = strategy_instance.close(close_size=close_size
                                                                , close_price=close_price)
                            # 청산 정보, balance 정보 업데이트
                            self.remain_balance += close_info['realized_now_amount']
                            if close_info['clear']:
                                self.update_strategy_in_management(status=Status.OUT, strategy_instance=strategy_instance)
                                clear_strategy_idx.append(sidx)
                                
                            # 청산 정보 저장
                            self.strategy_clear_info.append(close_info)
            
            # (1-1) 청산된 전략들 제거
            self.update_strategy_out_list(clear_strategy_idx)
            
            # (2) 전략 리스트 진입 조건 파악
            for data in datalist:
                for i, strategy_instance in enumerate(self.strategy_queue):
                    # 자산 매칭 확인
                    if self.asset_checker(data, strategy_instance):
                        # 1.전체 전략 동시 개수 & 2.전략당 동시에 들어갈 수 있는 최대 개수 & 3.잔여 자금 확인 
                        if self.strategy_in_mangement['total'] < self.MAX_STRATEGY_CNT \
                            and  self.strategy_in_mangement[strategy_instance.STRATEGY_NAME] < self.MAX_STRATEGY_SIMULTANEOUSLY_CNT \
                            and  self.remain_balance > self.MIN_TRADING_AMOUNT:
                            
                            # 진입 여부 계산
                            check_data = data[strategy_instance.ASSET].iloc[didx, :]
                            is_open, side, enter_price = strategy_instance.open_condition(check_data)
                            if is_open:
                                # 포지션 진입 금액(USDT) 계산
                                enter_balance = self.decision_enter_balance()
                                # 포지션 진입
                                strategy_instance.open(side=side
                                                       , initial_balance=enter_balance
                                                       , open_price=enter_price)
                                # 진입 정보, balance 정보 업데이트
                                self.update_strategy_in_management(status=Status.IN, strategy_instance=strategy_instance)
                                self.remain_balance -= enter_balance
                                
                                # strategy_queue에서 빼서 enter_strategy_list에 넣어주기
                                self.update_strategy_in_list(i)
                                
                                           
            # (3) 진입 중인 전략들 정보 업데이트
            self.enter_balance, self.notional_position_size = 0, 0
            for strategy_instance in self.enter_strategy_list:
                for data in datalist:
                    if self.asset_checker(data, strategy_instance):
                        close = data[strategy_instance.ASSET].iloc[didx, :]['Close']
                        strategy_instance.update(close)
                        self.enter_balance += strategy_instance.balance
                        self.total_notional_position_size += strategy_instance.notional_position_size
            
            self.total_balance = self.enter_balance + self.remain_balance
            self.update_backtesting_info(list(datalist[0].values())[0].iloc[didx, :])
        