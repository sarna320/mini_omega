import json
import asyncio
import bittensor as bt
from bittensor.core.async_subtensor import AsyncSubtensor
from typing import Optional
from bittensor import DynamicInfo
from typing import Any, Dict, List, Optional

from kafka_producer import KafkaProducerManager
from utils import now_iso_utc


class SubnetMonitor:
    def __init__(self, subtensor: AsyncSubtensor, producer: KafkaProducerManager):
        self.subtensor = subtensor
        self.kafka = producer
        self.current_block: Optional[int] = None
        self.subnets: Optional[list[DynamicInfo]] = None

    async def refresh_subnets(self):
        self.subnets = await self.subtensor.all_subnets()

    def _to_subnet_identity_dict(self, info: Any) -> Optional[Dict[str, Any]]:
        """Extract subnet_identity as a plain dict (or None)."""
        si = getattr(info, "subnet_identity", None)
        if si is None:
            return None
        if hasattr(si, "__dict__"):
            return dict(si.__dict__)
        try:
            from dataclasses import is_dataclass, asdict

            if is_dataclass(si):
                return asdict(si)
        except Exception:
            pass
        try:
            return dict(si)
        except Exception:
            return None

    def diff_subnet_identity(
        self,
        old_infos: List[Any],
        new_infos: List[Any],
        *,
        strict: bool = False,  # default: don't raise on mismatch
        include_removed: bool = False,  # set True if you also want removed subnets
    ) -> Dict[str, Any]:
        """
        Compare only subnet_identity between snapshots.

        Returns:
        {
            "changed": { netuid: { field: {"old": ..., "new": ...}, ... }, ... },
            "added":   { netuid: {<full subnet_identity dict or None>}, ... },
            "removed": { netuid: {<full subnet_identity dict or None>}, ... }  # only if include_removed=True
        }
        """
        old_map = {
            getattr(i, "netuid"): self._to_subnet_identity_dict(i) for i in old_infos
        }
        new_map = {
            getattr(i, "netuid"): self._to_subnet_identity_dict(i) for i in new_infos
        }

        old_uids, new_uids = set(old_map), set(new_map)

        # Optional strict mode error
        if strict and old_uids != new_uids:
            only_old = sorted(old_uids - new_uids)
            only_new = sorted(new_uids - old_uids)
            raise ValueError(
                f"Netuid mismatch between snapshots (strict mode). "
                f"Only in old: {only_old}; only in new: {only_new}"
            )

        # Compute changes for intersection
        changed: Dict[int, Dict[str, Dict[str, Any]]] = {}
        for netuid in sorted(old_uids & new_uids):
            old_si, new_si = old_map[netuid], new_map[netuid]
            if old_si == new_si:
                continue

            # If one side is None and the other isn't, report all visible fields
            if (old_si is None) ^ (new_si is None):
                full = (new_si or old_si) or {}
                changed[netuid] = {
                    k: {
                        "old": (old_si.get(k) if old_si else None),
                        "new": (new_si.get(k) if new_si else None),
                    }
                    for k in full.keys()
                }
                continue

            # Both dicts: compare field-by-field
            fields_diff: Dict[str, Dict[str, Any]] = {}
            for k in sorted(set(old_si.keys()) | set(new_si.keys())):
                ov, nv = old_si.get(k), new_si.get(k)
                if ov != nv:
                    fields_diff[k] = {"old": ov, "new": nv}
            if fields_diff:
                changed[netuid] = fields_diff

        # Added subnets: include full subnet_identity payload (may be None)
        added: Dict[int, Optional[Dict[str, Any]]] = {
            uid: new_map[uid] for uid in sorted(new_uids - old_uids)
        }

        # Optionally include removed subnets
        removed: Dict[int, Optional[Dict[str, Any]]] = {}
        if include_removed:
            removed = {uid: old_map[uid] for uid in sorted(old_uids - new_uids)}

        result: Dict[str, Any] = {"changed_subnet": changed, "added_subnet": added}
        if include_removed:
            result["removed_subnet"] = removed
        return result

    async def check_for_changes(self):
        current_info_subnets = self.subnets
        new_info_subnets = await self.subtensor.all_subnets()

        # # for test
        # new_info_subnets[62].subnet_identity.subnet_name = "62_new"

        changes = self.diff_subnet_identity(
            current_info_subnets,
            new_info_subnets,
            include_removed=True,
        )
        await self.send_diff_to_kafka(changes)
        bt.logging.info(f"Changes: {changes}")
        self.subnets = new_info_subnets

    async def send_diff_to_kafka(self, diff: Dict[str, Any]) -> None:
        """
        Send each changed/added/removed subnet as a separate Kafka record, concurrently.
        Payload shape:
        - changed_subnet: {event, netuid, block, timestamp, fields}
        - added_subnet:   {event, netuid, block, timestamp, subnet_identity}
        - removed_subnet: {event, netuid, block, timestamp, subnet_identity}
        """
        tasks = []
        ts = now_iso_utc()
        blk = self.current_block

        # changed_subnet: fields diffs only
        for netuid, fields in diff.get("changed_subnet", {}).items():
            payload = {
                "type": "changed_subnet",
                "netuid": netuid,
                "block": blk,
                "timestamp": ts,
                "fields": fields,  # e.g. {"subnet_name": {"old": "...", "new": "..."}, ...}
            }
            tasks.append(self.kafka.send_json(payload, key="signal"))

        # added_subnet: full subnet_identity (may be None)
        for netuid, si in diff.get("added_subnet", {}).items():
            payload = {
                "type": "added_subnet",
                "netuid": netuid,
                "block": blk,
                "timestamp": ts,
                "subnet_identity": si,  # dict or None
            }
            tasks.append(self.kafka.send_json(payload, key="signal"))

        # removed_subnet: full subnet_identity (may be None)
        for netuid, si in diff.get("removed_subnet", {}).items():
            payload = {
                "type": "removed_subnet",
                "netuid": netuid,
                "block": blk,
                "timestamp": ts,
                "subnet_identity": si,  # dict or None
            }
            tasks.append(self.kafka.send_json(payload, key="signal"))

        if not tasks:
            return

        # Fire them concurrently; log per-task failures if any.
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for idx, res in enumerate(results):
            if isinstance(res, Exception):
                bt.logging.error("❌ Kafka send task failed", exc_info=res)

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
        await self.check_for_changes()

    async def subscribe_new_heads(self, ws):
        """
        Subscribe to new block headers and process them continuously.
        """
        # We can not set subnets in init, so we do it here
        await self.refresh_subnets()
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
