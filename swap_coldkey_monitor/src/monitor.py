import json
import asyncio
import bittensor as bt
from bittensor.core.async_subtensor import AsyncSubtensor
from bittensor.core.async_subtensor import get_async_subtensor
from typing import Optional
from bittensor import DynamicInfo


from extrinsic import process_call_args
from kafka_producer import KafkaProducerManager
from utils import now_iso_utc
from calls import ACCEPTABLE_CALLS


class SwapColdkeyMonitor:
    def __init__(self, subtensor: AsyncSubtensor, producer: KafkaProducerManager):
        self.subtensor = subtensor
        self.kafka = producer
        self.current_block: Optional[int] = None
        self.subnets: Optional[list[DynamicInfo]] = None
        self._sem = asyncio.Semaphore(20)

    async def refresh_subnet(self):
        self.subnets = await self.subtensor.all_subnets()

    async def process_extrinsic(self, extrinsic):
        """
        Process an extrinsic and check if it matches the acceptable calls.
        """
        data = getattr(extrinsic, "value_serialized", {}) or {}
        parsed = process_call_args(data)
        if not parsed:
            return
        coldkey = data.get("address", "not found")

        netuid = None
        # for subnet in self.subnets:
        #     if subnet.owner_coldkey == subnet:
        #         netuid = subnet.netuid
        #         break
        # if netuid is None:
        #     bt.logging.warning(f"Not found subnet for extrinsic: {data}")
        #     return

        for p in parsed:
            type_ext = p.get("type", "unknow")
            # double check
            if type_ext not in ACCEPTABLE_CALLS:
                continue
            new_coldkey = p.get("new_coldkey", "not_found")
            bt.logging.info(f"{coldkey} extrinsic: {p}")

            payload = {
                "netuid": netuid,
                "type": type_ext,
                "observed_at": now_iso_utc(),
                "block": self.current_block,
                "coldkey": coldkey,
                "new_coldkey": new_coldkey,
                "parsed": p,
                "raw": data,  # keep raw for debugging/auditing
            }

            await self.kafka.send_json(payload, "signal")

    async def _process_extrinsic_bounded(self, extrinsic):
        """
        Wrapper that enforces the concurrency limit via a semaphore.
        """
        async with self._sem:
            await self.process_extrinsic(extrinsic)

    async def fetch_block_data(self):
        """
        Fetch all extrinsics for the current block.
        """
        extrinsics = await self.subtensor.substrate.get_extrinsics(
            block_number=self.current_block
        )
        return extrinsics

    async def handle_extrinsics(self, extrinsics):
        if not extrinsics:
            # Still refresh subnets even if the block had no extrinsics
            await self.refresh_subnet()
            return

        tasks = [
            asyncio.create_task(self._process_extrinsic_bounded(ext))
            for ext in extrinsics
        ]
        # Await completion and log any exceptions without crashing the loop
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, res in enumerate(results):
            if isinstance(res, Exception):
                bt.logging.error(
                    f"❌ Error while processing extrinsic #{idx} in block {self.current_block}",
                    exc_info=res,
                )

    async def handle_new_block(self, ws):
        """
        Handle new block events from the websocket.
        """
        raw = await ws.recv()
        msg = json.loads(raw)
        if msg.get("method") != "chain_newHead":
            return
        header = msg["params"]["result"]
        self.current_block = int(header["number"], 16)
        bt.logging.info(f"📦 New block {self.current_block}")
        extrinsics = await self.fetch_block_data()
        await self.handle_extrinsics(extrinsics)
        await self.refresh_subnet()

    async def subscribe_new_heads(self, ws):
        """
        Subscribe to new block headers and process them continuously.
        """
        # We can not set subnets in init, so we do it here
        await self.refresh_subnet()
        await ws.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "chain_subscribeNewHeads",
                    "params": [],
                }
            )
        )
        resp = json.loads(await ws.recv())
        bt.logging.info(f"✅ Subscribed to new blocks (id={resp.get('result')})")

        while True:
            await self.handle_new_block(ws)
