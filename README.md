# Multi Strategy Backtesting Infra

- 여러 전략을 동시에 진입, 진출 관리할 수 있는 백테스팅 인프라입니다.
- 전략 자체를 factor로 삼아 강건한 앙상블 전략을 만드는 것을 목표로 하고 있는 프로젝트 중 일부 입니다.
- 시간날 때 틈틈히 개발하고 있습니다.

<br>

## data 수집
- Binance API를 이용하여 수집합니다.
- argparser는 추후 추가 예정입니다.
```
cd data
python crawler.py
```