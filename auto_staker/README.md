```sh
python3 -m venv .venv
pip install -r requirements.txt
python -m src/main.py
```

## TODO
- add checks for subnet for example if slippage to high or tao in pool
- make sync lopp for every block 
- on start add_stake all free balnce to netuid 0, keep only minial required tao x times bigger than fees 
- after every swap_stake check if you have enough for fees, if not ustake from netuid 0 for x amount of fees
- 