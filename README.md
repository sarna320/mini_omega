1. Regen wallet
```sh
btcli w regen-coldkey --mnemonic "xxxxxxx" --wallet-name trader --wallet-path ~/.bittensor/wallets --no-use-password --overwrite
```
2. Rename .env.example to .env
```sh
mv .env.example .env
```
3. Adjust .env 
4. Install docker - copy paste all
```sh
if ! command -v docker &> /dev/null; then
    echo "Installing Docker…"
    apt-get update
    apt-get -y install ca-certificates curl
    install -m0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    echo "deb [arch=$(dpkg --print-architecture) \
        signed-by=/etc/apt/keyrings/docker.asc] \
        https://download.docker.com/linux/ubuntu \
        $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        | tee /etc/apt/sources.list.d/docker.list > /dev/null
    apt-get update
    apt-get -y install docker-ce docker-ce-cli containerd.io \
                    docker-buildx-plugin docker-compose-plugin
else
    echo "Docker already installed."
fi
if ! docker compose version &> /dev/null; then
    apt-get update && apt-get install -y docker-compose-plugin
fi
if ! docker-compose -version &> /dev/null; then
    apt-get update && apt-get install -y docker-compose
fi
```
5. Run compose
```sh
docker compose -f 'docker-compose.yml' up -d --build 
```