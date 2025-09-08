from scalecodec.types import GenericCall
from bittensor.utils import format_error_message
from bittensor.core.errors import SubstrateRequestException
from typing import TYPE_CHECKING, TypedDict, Any, Dict, cast

if TYPE_CHECKING:
    from auto_staker import AutoStaker


class ExtrinsicData(TypedDict, total=False):
    """Lightweight typing for the kwargs expected by create_signed_extrinsic."""

    call: GenericCall
    keypair: Any  # substrate keypair object; keep as Any to avoid importing substrate types here
    nonce: int
    era: Dict[str, int]  # e.g. {"period": 4, "current": <block_num>}


async def swap_stake_ext(
    self: "AutoStaker",
    destination_netuid: int,
    amount_rao: int,
    origin_netuid: int = 0,
) -> ExtrinsicData:
    """Build the call and return partial extrinsic data (call + keypair)."""
    call = await self.subtensor.substrate.compose_call(
        call_module="SubtensorModule",
        call_function="swap_stake",
        call_params={
            "hotkey": self.hotkey_to_stake,
            "origin_netuid": origin_netuid,
            "destination_netuid": destination_netuid,
            "alpha_amount": amount_rao,
        },
    )
    # Choose which key to sign with
    sign_with: str = "coldkey"
    signing_keypair = getattr(self.wallet, sign_with)

    extrinsic_data: ExtrinsicData = {"call": call, "keypair": signing_keypair}
    return extrinsic_data


async def submit_extrinsic(
    self: "AutoStaker",
    extrinsic_data: ExtrinsicData,
) -> tuple[bool, str]:
    """Complete extrinsic_data (nonce/era), sign and submit."""
    # Compute next nonce using coldkeypub address
    nonce_key = "coldkeypub"
    next_nonce = await self.subtensor.substrate.get_account_next_index(
        getattr(self.wallet, nonce_key).ss58_address
    )
    extrinsic_data["nonce"] = next_nonce

    # Add a mortal era (example values)
    extrinsic_data["era"] = {"period": 4, "current": self.current_block - 2}

    # Create signed extrinsic (cast so **kwargs typing is satisfied)
    extrinsic = await self.subtensor.substrate.create_signed_extrinsic(
        **cast(Dict[str, Any], extrinsic_data)
    )

    try:
        response = await self.subtensor.substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=True,
            wait_for_finalization=False,
        )

        if await response.is_success:
            return True, ""

        return False, format_error_message(await response.error_message)

    except SubstrateRequestException as e:
        return False, format_error_message(e)
