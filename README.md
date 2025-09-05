1. Regen wallet
```sh
btcli w regen-coldkey --mnemonic "xxxxxxx" --wallet-name trader --wallet-path ~/.bittensor/wallets --no-use-password --overwrite
```
2. Rename .env.example to .env
```sh
mv .env.example .env
```
3. Adjust .env 
4. Run compose
```sh
docker compose -f 'docker-compose.yml' up -d --build 
```