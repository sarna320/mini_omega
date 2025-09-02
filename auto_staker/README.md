```sh
python3 -m venv .venv
pip install -r requirements.txt
python -m src/main.py
```

```sh
btcli w regen-coldkey --mnemonic "catalog dial crazy print bracket race text work review sight horror cabbage" --no-use-password --overwrite --wallet_name trader_test
```


## TODO
- add redis
- check for last event, if in window of 2 days skip event else stake, if staked add to redis
- make check for every type for example swap coldkey alwas but skip new subnet or changes subnet only for name
- add checks for subnet for example if slippage to high or blaclisted subnet etc. 