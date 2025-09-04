import json
import asyncio
from typing import Optional, Any, Dict, Tuple
from bittensor.core.async_subtensor import AsyncSubtensor
import bittensor as bt
import redis.asyncio as aioredis


class AutoStaker:
    """
    Staking logic isolated from transport:
    - Manages wallet and balance.
    - Validates incoming staking signals (payloads).
    - Executes on-chain 'add_stake' extrinsic using AsyncSubtensor.
    - Caches last successful trade per netuid in Redis (per wallet).
    - Skips signals for a netuid if last success happened within a given
      block spacing window (min_spacing_in_blocks).
    - Provides a background periodic refresh loop.
    """

    def __init__(
        self,
        *,
        subtensor: AsyncSubtensor,
        wallet_name: str,
        test_mode: bool = True,
        hotkey_to_stake: str,
        balance_to_stake_tao: float = 0.01,
        balance_always_to_keep_tao: float = 0.05,
        max_delay_in_blocks: int = 10,
        redis_url: Optional[str] = None,
        min_spacing_in_blocks: int = 50,
        ignore_netuids: Optional[set[int]] = None,
    ) -> None:
        self.subtensor = subtensor
        self.test_mode = test_mode
        self.wallet = bt.wallet(wallet_name)
        if not self.wallet.coldkey_file.exists_on_device():
            raise RuntimeError(
                f"Coldkey file not found for wallet '{wallet_name}'. "
                "Please make sure the wallet is initialized on this machine."
            )
        self.hotkey_to_stake = hotkey_to_stake
        if (
            not isinstance(self.hotkey_to_stake, str)
            or not self.hotkey_to_stake.strip()
        ):
            raise ValueError("hotkey_to_stake must be a non-empty SS58 string")

        self.balance_to_stake = bt.Balance(balance_to_stake_tao)
        self.balance_always_to_keep = bt.Balance(balance_always_to_keep_tao)
        self.min_required_tao = bt.Balance(
            self.balance_to_stake.tao + self.balance_always_to_keep.tao
        )
        self.balance: bt.Balance = bt.Balance(0)
        self.current_block: Optional[int] = None
        self.max_delay_in_blocks = max_delay_in_blocks

        self.redis: Optional[aioredis.Redis] = (
            aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)
            if redis_url
            else None
        )
        self.min_spacing_in_blocks = min_spacing_in_blocks

        bt.logging.debug(
            f"Using wallet: {self.wallet.name} | ss58: {self.wallet.coldkey.ss58_address}"
        )
        if self.redis:
            bt.logging.debug("Redis caching enabled for successful trades.")
        else:
            bt.logging.debug("Redis not configured; caching disabled.")

        self.ignore_netuids: set[int] = ignore_netuids or set()

        bt.logging.info(
            "AutoStaker config: "
            f"wallet={self.wallet.name} ({self.wallet.coldkey.ss58_address}), "
            f"test_mode={self.test_mode}, "
            f"stake={self.balance_to_stake.tao} TAO, "
            f"keep={self.balance_always_to_keep.tao} TAO, "
            f"max_delay={self.max_delay_in_blocks} blocks, "
            f"min_spacing={self.min_spacing_in_blocks} blocks, "
            f"redis={'on' if self.redis else 'off'}, "
            f"ignore_netuids={sorted(self.ignore_netuids) if self.ignore_netuids else []}"
        )

    def _redis_key(self) -> str:
        """Redis key scoped by wallet coldkey ss58 address."""
        return f"autostaker:{self.wallet.coldkey.ss58_address}"

    async def _redis_load_map(self) -> Dict[str, Any]:
        """
        Load the per-wallet map { "<netuid>": <payload>, ... }.
        Returns empty dict if missing or Redis disabled.
        """
        if not self.redis:
            return {}
        s = await self.redis.get(self._redis_key())
        if not s:
            return {}
        try:
            return json.loads(s)
        except Exception as e:
            bt.logging.warning(f"Failed to parse Redis value: {e!r}")
            return {}

    async def _redis_save_payload(self, netuid: int, payload: Dict[str, Any]) -> None:
        """Update the per-wallet map, storing the full payload under this netuid."""
        if not self.redis:
            return
        key = self._redis_key()
        cache = await self._redis_load_map()
        cache[str(int(netuid))] = payload
        await self.redis.set(key, json.dumps(cache, ensure_ascii=False))

    async def _last_success_block_for_netuid(self, netuid: int) -> Optional[int]:
        """
        Return the 'block' field from last successful payload for a netuid, if any.
        """
        cache = await self._redis_load_map()
        item = cache.get(str(int(netuid)))
        if not item:
            return None
        try:
            return int(item.get("block"))
        except Exception:
            return None

    async def refresh_balance(self) -> None:
        """Refresh cached balance from chain."""
        self.balance = await self.subtensor.get_balance(
            address=self.wallet.coldkey.ss58_address
        )

    async def refresh_current_block(self) -> Optional[int]:
        """Refresh current block once and cache it. Returns the block or None on failure."""
        try:
            blk = await self.subtensor.get_current_block()
            self.current_block = int(blk)
            bt.logging.debug(f"📦 Current block: {self.current_block}")
            return self.current_block
        except Exception as e:
            bt.logging.warning(f"current block fetch failed: {e!r}")
            return None

    async def ensure_min_balance(self) -> None:
        """Ensure we have enough balance to run the staker at all."""
        await self.refresh_balance()
        bt.logging.info(f"💰 Current balance: {self.balance.tao}")
        if self.balance.tao < self.min_required_tao.tao:
            raise RuntimeError(
                f"Too low balance to start autostaker. At least {self.min_required_tao} TAO is required "
                f"(have {self.balance.tao:.6f})."
            )

    async def _run_refreshers_once(self) -> None:
        """Run the built-in balance refresh and current block fetch concurrently."""
        try:
            balance_coro = asyncio.wait_for(self.refresh_balance(), timeout=10.0)
            block_coro = asyncio.wait_for(self.refresh_current_block(), timeout=10.0)
            await asyncio.gather(balance_coro, block_coro, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            bt.logging.warning(f"refresh tick failed: {e!r}")

    async def periodic_refresh_loop(
        self,
        stop_event: asyncio.Event,
        interval_s: float = 30.0,
        initial_delay_s: float = 0.0,
    ) -> None:
        """
        Background loop that periodically refreshes balance and runs extra refreshers.
        It exits when 'stop_event' is set.

        - Does NOT block Kafka consumer (runs as a separate asyncio Task).
        - Uses 'stop_event' to coordinate shutdown with the rest of the app.
        """
        if initial_delay_s > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=initial_delay_s)
                return  # stop requested during initial delay
            except asyncio.TimeoutError:
                pass

        while not stop_event.is_set():
            await self._run_refreshers_once()

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_s)
                break
            except asyncio.TimeoutError:
                continue

    def _parse_block_from_payload(self, payload: Dict[str, Any]) -> Optional[int]:
        """Extract 'block' as int from payload; return None if missing/invalid."""
        raw = payload.get("block")
        if raw is None:
            return None
        try:
            return int(raw)
        except Exception:
            return None

    def _parse_netuid_from_payload(self, payload: Dict[str, Any]) -> Optional[int]:
        """Extract 'netuid' as int; if missing and test_mode=True, default to 0."""
        netuid = payload.get("netuid")
        if netuid is None and self.test_mode:
            return 0
        try:
            return int(netuid) if netuid is not None else None
        except Exception:
            return None

    async def _ensure_current_block(self) -> Optional[int]:
        """Return a known current block, refreshing if needed."""
        if self.current_block is not None:
            return self.current_block
        return await self.refresh_current_block()

    def _is_event_too_old(self, event_block: int, current_block: int) -> bool:
        """
        Return True iff the event is older than the allowed delay:
        (current_block - event_block) > max_delay_in_blocks
        """
        return (current_block - event_block) > self.max_delay_in_blocks

    def _should_ignore(self, netuid: int) -> bool:
        """Return True if the netuid is explicitly ignored."""
        return netuid in self.ignore_netuids

    async def execute_trade(
        self,
        payload: Dict[str, Any],
        *,
        max_retries: int = 5,
        balance_settle_sleep_s: float = 0.2,
    ) -> Tuple[bool, Optional[str]]:
        """
        Retry policy per your constraints:
        - Do not fetch balance before the first attempt: use cached self.balance as baseline.
        - For each attempt:
            send extrinsic ->
                if RPC True: success
                else: short sleep, refresh_balance(), check RAO delta:
                    if delta >= amount_rao -> assumed_ok success
                    else -> next attempt (update baseline to the refreshed balance)
        RAO for comparisons; TAO for logs.
        """
        netuid = self._parse_netuid_from_payload(payload)
        if netuid is None or netuid < 0:
            return False, f"Missing/invalid 'netuid': {netuid}"

        # Amount to stake
        amount_rao = int(self.balance_to_stake.rao)
        amount_tao = float(self.balance_to_stake.tao)

        # Compose extrinsic once
        try:
            call = await self.subtensor.substrate.compose_call(
                call_module="SubtensorModule",
                call_function="add_stake",
                call_params={
                    "hotkey": self.hotkey_to_stake,
                    "netuid": netuid,
                    "amount_staked": amount_rao,
                },
            )
        except Exception as e:
            return False, f"compose_call failed: {e!r}"

        baseline_rao = int(self.balance.rao)  # RAO baseline for the current attempt

        last_err: Optional[str] = None

        for attempt in range(1, max_retries + 1):
            try:
                resp, err_msg = await self.subtensor.sign_and_send_extrinsic(
                    call=call,
                    wallet=self.wallet,
                    wait_for_inclusion=True,
                    wait_for_finalization=False,
                    nonce_key="coldkeypub",
                    sign_with="coldkey",
                    use_nonce=True,
                    period=None,
                )
                if resp is True:
                    # Confirmed by RPC
                    await self.refresh_balance()
                    bt.logging.success(
                        "✅ Stake succeeded | "
                        f"netuid={netuid} | amount={amount_tao} TAO | "
                        f"balance={self.balance.tao} TAO"
                    )
                    return True, None

                last_err = f"staking returned False: {err_msg}"
            except Exception as e:
                last_err = f"staking exception: {e!r}"

            # Give chain a tiny moment, then refresh and check delta
            try:
                if balance_settle_sleep_s > 0:
                    await asyncio.sleep(balance_settle_sleep_s)
                await self.refresh_balance()

                current_rao = int(self.balance.rao)
                spent_rao = baseline_rao - current_rao
                spent_tao = float(bt.Balance.from_rao(spent_rao).tao)

                if spent_rao >= amount_rao:
                    bt.logging.success(
                        "✅ Stake assumed_ok via balance delta | "
                        f"netuid={netuid} | amount={amount_tao} TAO | spent≈{spent_tao} TAO | "
                        f"balance={self.balance.tao} TAO"
                    )
                    return True, None

                # Not enough delta → next attempt. Update baseline to the latest balance.
                bt.logging.warning(
                    f"⚠️ Attempt {attempt}/{max_retries} not confirmed "
                    f"(RPC fail & delta≈{spent_tao} TAO < {amount_tao} TAO)"
                )
                baseline_rao = current_rao

            except Exception as e2:
                bt.logging.warning(
                    f"⚠️ Attempt {attempt}/{max_retries}: balance refresh/check failed: {e2!r}"
                )
                # keep previous baseline_rao; try again

        return False, (last_err or "staking failed after retries")

    async def handle_signal(self, payload: Dict[str, Any]) -> bool:
        """
        Process one logical staking signal.
        Return True on success (for the caller to commit), False otherwise.

        We return True also when we *skip* old/future/invalid events,
        so the consumer can commit and move on (no poison pills).
        """
        # 1) Validate 'block' early
        event_block = self._parse_block_from_payload(payload)
        if event_block is None:
            bt.logging.warning(f"Skipping payload with invalid 'block': {payload}")
            return True

        # 2) Current block required
        current_block = await self._ensure_current_block()
        if current_block is None:
            bt.logging.warning("Skipping payload: current block unknown")
            return True

        # 3) Drop too-old events
        if self._is_event_too_old(event_block, current_block):
            age = current_block - event_block
            bt.logging.info(
                f"⏭️  Skipping old event: age={age} (event={event_block}, "
                f"current={current_block}, max_delay={self.max_delay_in_blocks})"
            )
            return True

        # 4) Throttle per-netuid using Redis cache of last successful trade
        netuid = self._parse_netuid_from_payload(payload)
        if netuid is None:
            bt.logging.warning(f"Skipping payload with invalid 'netuid': {payload}")
            return True

        if self._should_ignore(netuid):
            bt.logging.info(f"⏭️ Skipping ignored netuid={netuid}")
            return True

        last_success_block = await self._last_success_block_for_netuid(netuid)
        if (
            last_success_block is not None
            and (current_block - last_success_block) < self.min_spacing_in_blocks
        ):
            wait_blocks = self.min_spacing_in_blocks - (
                current_block - last_success_block
            )
            bt.logging.info(
                f"⏭️  Skipping event due to spacing window: "
                f"netuid={netuid} last_success={last_success_block} "
                f"current={current_block} window={self.min_spacing_in_blocks} "
                f"(wait ~{wait_blocks} more blocks)"
            )
            return True

        # 5) Execute
        bt.logging.info(
            f"📢 Execute trade for: {json.dumps(payload, ensure_ascii=False)}"
        )
        ok, err = await self.execute_trade(payload)
        if ok:
            # 6) Persist success to Redis: per-wallet JSON map {netuid: payload}
            await self._redis_save_payload(netuid, payload)
            return True

        bt.logging.error(f"❌ Trade execution failed: {err} | payload={payload}")
        return True
