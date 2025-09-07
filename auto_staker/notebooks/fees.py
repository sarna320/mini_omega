# English-only comments and code
import asyncio
from bittensor.core.async_subtensor import get_async_subtensor
from bittensor.utils.balance import Balance
import bittensor as bt

# Your provided setup (I keep these to match your test)
w = bt.wallet("trader")
amount = Balance.from_tao(1)

origin_netuid = 1
destination_netuid = 18

origin_hotkey_ss58 = "5E2LP6EnZ54m3wS8s1yPvD5c3xo71kQroBw7aUVK32TKeZ5u"
destination_hotkey_ss58 = "5E2LP6EnZ54m3wS8s1yPvD5c3xo71kQroBw7aUVK32TKeZ5u"

origin_coldkey_ss58 = w.coldkey.ss58_address
destination_coldkey_ss58 = w.coldkey.ss58_address


async def main():
    s = await get_async_subtensor("finney")

    scenarios = [
        {
            "name": "move_stake: same subnet (hotkey->hotkey)",
            "origin_netuid": origin_netuid,
            "destination_netuid": origin_netuid,  # same subnet => expect 0%
            "origin_hotkey_ss58": origin_hotkey_ss58,
            "destination_hotkey_ss58": destination_hotkey_ss58,
            "origin_coldkey_ss58": origin_coldkey_ss58,
            "destination_coldkey_ss58": destination_coldkey_ss58,
        },
        {
            "name": "move_stake: cross subnet (hotkey change allowed)",
            "origin_netuid": origin_netuid,
            "destination_netuid": destination_netuid,  # cross subnet => expect 0.05%
            "origin_hotkey_ss58": origin_hotkey_ss58,
            "destination_hotkey_ss58": destination_hotkey_ss58,
            "origin_coldkey_ss58": origin_coldkey_ss58,
            "destination_coldkey_ss58": destination_coldkey_ss58,
        },
        {
            "name": "swap_stake: cross subnet (same hotkey)",
            "origin_netuid": origin_netuid,
            "destination_netuid": destination_netuid,  # cross subnet => expect 0.05%
            "origin_hotkey_ss58": origin_hotkey_ss58,
            "destination_hotkey_ss58": origin_hotkey_ss58,  # same hotkey (swap)
            "origin_coldkey_ss58": origin_coldkey_ss58,
            "destination_coldkey_ss58": destination_coldkey_ss58,
        },
        {
            "name": "move_stake: cross subnet (same hotkey, just to confirm)",
            "origin_netuid": origin_netuid,
            "destination_netuid": destination_netuid,  # cross subnet => expect 0.05%
            "origin_hotkey_ss58": origin_hotkey_ss58,
            "destination_hotkey_ss58": origin_hotkey_ss58,  # same hotkey using move
            "origin_coldkey_ss58": origin_coldkey_ss58,
            "destination_coldkey_ss58": destination_coldkey_ss58,
        },
    ]

    for sc in scenarios:
        fee_info = await s.get_stake_movement_fee(
            amount=amount,
            origin_netuid=sc["origin_netuid"],
            origin_hotkey_ss58=sc["origin_hotkey_ss58"],
            origin_coldkey_ss58=sc["origin_coldkey_ss58"],
            destination_netuid=sc["destination_netuid"],
            destination_hotkey_ss58=sc["destination_hotkey_ss58"],
            destination_coldkey_ss58=sc["destination_coldkey_ss58"],
        )
        print(f"{sc['name']}: {fee_info}")


asyncio.run(main())
