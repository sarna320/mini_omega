from bittensor.core.async_subtensor import AsyncSubtensor, Balance
import bittensor as bt

from calls import ACCEPTABLE_CALLS


def parse_call_args(call_args_list):
    """Parse a list of call arguments into a dictionary."""
    try:
        return {item["name"]: item["value"] for item in call_args_list}
    except (TypeError, KeyError) as e:
        bt.logging.error(f"Error parsing call_args_list: {e}")
        return {}


def process_stake(call_args):
    """Handle stake-related call functions."""
    try:
        parsed = parse_call_args(call_args)
        parsed["netuid"] = parsed.get("netuid")
        amount = parsed.get("amount_staked") or parsed.get("amount_unstaked")
        if parsed.get("amount_staked") is not None:
            parsed["type"] = "stake"
        else:
            parsed["type"] = "unstake"
        if amount is not None:
            parsed["amount"] = amount
        parsed["limit_price"] = parsed.get("limit_price")
        parsed["allow_partial"] = parsed.get("allow_partial")
        # Lista kluczy które chcemy zostawić
        allowed_keys = {
            "hotkey",
            "netuid",
            "amount",
            "limit_price",
            "allow_partial",
            "type",
        }
        # Nowy słownik tylko z potrzebnymi kluczami
        filtered_parsed = {key: parsed.get(key) for key in allowed_keys}
        return filtered_parsed
    except Exception as e:
        bt.logging.error(f"Error processing stake call args: {e}")


def process_batch(call_args):
    try:
        # Value is list of calls
        # bt.logging.info(f"process_batch decoding call_args: {call_args}")
        calls = call_args[0].get("value")
        # bt.logging.info(f"process_batch decoding calls: {calls}")
        to_return = []
        for call in calls:
            # bt.logging.info(f"process_batch decoding call in calls: {call}")
            call_function = call.get("call_function")
            if call_function not in ACCEPTABLE_CALLS:
                # bt.logging.info(f"Call_function: {call_function} not in {ACCEPTABLE_CALLS}")
                continue
            # bt.logging.info(f"call_function: {call_function}")
            call_args = call.get("call_args")
            # bt.logging.info(f"call_args: {call_args}")
            # bt.logging.info(50 * "*")
            try:
                handler = HANDELER.get(call_function)
                if handler is None:
                    continue
                proccesed_call_args = HANDELER[call_function](call_args)
            except Exception as e:
                bt.logging.error(f"Error processing batch_all call for value: {e}")
            to_return.append(proccesed_call_args)
        # bt.logging.info(f"to_return: {to_return}")
        return to_return
    except Exception as e:
        bt.logging.error(f"Error processing batch_all call args: {e}")


def procces_proxy(call_args):
    try:
        call = call_args[2].get("value")
        call_function = call.get("call_function")
        if call_function not in ACCEPTABLE_CALLS:
            # bt.logging.info(f"Call_function: {call_function} not in {ACCEPTABLE_CALLS}")
            return None
        call_args = call.get("call_args")
        handler = HANDELER.get(call_function)
        if handler is None:
            return None
        proccesed_call_args = HANDELER[call_function](call_args)
        return proccesed_call_args
    except Exception as e:
        bt.logging.error(f"Error processing proxy call args: {e}")


def procces_schedule_swap_coldkey(call_args):
    try:
        parsed = parse_call_args(call_args)
        parsed["new_coldkey"] = parsed.get("new_coldkey")
        parsed["type"] = "swap_coldkey"
        allowed_keys = {
            "new_coldkey",
            "type",
        }
        # Nowy słownik tylko z potrzebnymi kluczami
        filtered_parsed = {key: parsed.get(key) for key in allowed_keys}
        return filtered_parsed
    except Exception as e:
        bt.logging.error(f"Error processing schedule swap coldkey: {e}")


HANDELER = {
    "add_stake": process_stake,
    "add_stake_limit": process_stake,
    "remove_stake": process_stake,
    "remove_stake_limit": process_stake,
    "batch_all": process_batch,
    "batch": process_batch,
    "force_batch": process_batch,
    "proxy": procces_proxy,
    "schedule_swap_coldkey": procces_schedule_swap_coldkey,
}


def process_call_args(extrinsic_data):
    """Process a single extrinsic."""
    try:
        call_function = extrinsic_data.get("call", {}).get("call_function")
        if call_function not in ACCEPTABLE_CALLS:
            # bt.logging.info(f"Call_function: {call_function} not in {ACCEPTABLE_CALLS}")
            return None
        # bt.logging.info(f"process_call_args decoding extrinsic_data: {extrinsic_data}")
        call_args = extrinsic_data.get("call", {}).get("call_args")
        handler_func = HANDELER.get(call_function)
        if handler_func is None:
            return None
        proccesed_call_args = handler_func(call_args)

        # proccesed_call_args["call_function"] = call_function
        if isinstance(proccesed_call_args, dict):
            return [proccesed_call_args]
        elif isinstance(proccesed_call_args, list):
            return proccesed_call_args
        else:
            return None
    except Exception as e:
        bt.logging.error(f"Error decoding extrinsic: {e}")


async def process_stake_price(proccesed_call_args, async_subtensor: AsyncSubtensor):
    try:
        if isinstance(proccesed_call_args, dict):
            proccesed_call_args = [proccesed_call_args]
        for op in proccesed_call_args:
            op_type = op.get("type")
            if op_type not in ["stake", "unstake"]:
                continue
            amount: int = int(op.get("amount"))
            netuid: int = int(op.get("netuid"))
            balance = Balance.from_rao(amount=amount, netuid=netuid)
            try:
                x = await async_subtensor.subnet(netuid=netuid)
            except Exception as e:
                bt.logging.error(f"Error getting subnet info from subtensor: {e}")
            potenial_price = x.price.tao
            op["price_for_current_block"] = potenial_price
            if op_type == "unstake":
                potenial_amount = x.alpha_to_tao_with_slippage(alpha=balance)
                potenial_amount_tao = potenial_amount[0].tao
                op["amount_in_alpha"] = balance.tao
                op["potenial_amount_in_tao_with_slippage"] = potenial_amount_tao
                bt.logging.info(f"amount: {balance.tao} aplha | netuid: {netuid}")
            elif op_type == "stake":
                bt.logging.info(f"amount: {balance.tao} tao | netuid: {netuid}")
                potenial_amount = x.tao_to_alpha_with_slippage(tao=balance.tao)
                potenial_amount_alpha = potenial_amount[0].tao
                op["amount_in_tao"] = balance.tao
                op["potenial_amount_in_alpha_with_slippage"] = potenial_amount_alpha
        return proccesed_call_args
    except Exception as e:
        bt.logging.error(f"Error processing stake price: {e}")
