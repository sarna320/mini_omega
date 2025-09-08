from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import bittensor as bt

from discord import build_skip_embed, DiscordAlerter


def event_type(payload: Dict[str, Any]) -> Optional[str]:
    """Extract event type from payload ('type' or 'event')."""
    t = payload.get("type") or payload.get("event")
    return str(t) if t is not None else None


def _field_changed(payload: Dict[str, Any], field: str) -> bool:
    """
    Return True only if the specified field actually changed.
    Expected (diff) format in payload['fields']:
      - Either {field: {'old': 'A', 'new': 'B'}, ...}
      - Or {field: 'B', ...}  # treat presence as a change
    """
    diff = payload.get("fields")
    if not isinstance(diff, dict):
        return False

    if field not in diff:
        return False

    val = diff[field]
    if isinstance(val, dict):
        old_v = val.get("old")
        new_v = val.get("new")
        return old_v != new_v
    # Primitive present → consider it a change signal
    return True


def _description_changed(payload: Dict[str, Any]) -> bool:
    """Return True only if 'description' actually changed."""
    return _field_changed(payload, "description")


class SignalHandler(ABC):
    """Strategy interface for deciding whether a signal is accepted."""

    @abstractmethod
    def accepts(self, payload: Dict[str, Any]) -> bool:
        """Return True if the signal should be accepted for execution."""
        raise NotImplementedError


class RejectAllHandler(SignalHandler):
    def accepts(self, payload: Dict[str, Any]) -> bool:
        return False


class ScheduleSwapColdkeyHandler(SignalHandler):
    """Always accept schedule_swap_coldkey."""

    def accepts(self, payload: Dict[str, Any]) -> bool:
        return True


class AddedSubnetHandler(SignalHandler):
    """Always reject added_subnet."""

    def accepts(self, payload: Dict[str, Any]) -> bool:
        return False


class RemovedSubnetHandler(SignalHandler):
    """Always reject removed_subnet."""

    def accepts(self, payload: Dict[str, Any]) -> bool:
        return False


class ChangedSubnetHandler(SignalHandler):
    """
    Accept changed_subnet only if 'description' changed.
    (We explicitly ignore 'subnet_name' now.)
    """

    def accepts(self, payload: Dict[str, Any]) -> bool:
        return _description_changed(payload)


def _rejection_reason(evt: str, handler: SignalHandler, payload: Dict[str, Any]) -> str:
    """Build a human-readable reason for why a signal was rejected."""
    try:
        if isinstance(handler, AddedSubnetHandler):
            return "policy: added_subnet is always rejected"
        if isinstance(handler, RemovedSubnetHandler):
            return "policy: removed_subnet is always rejected"
        if isinstance(handler, ChangedSubnetHandler):
            diff = payload.get("fields")
            if not isinstance(diff, dict):
                return "changed_subnet rejected: 'fields' missing or not a dict"
            if "description" not in diff:
                return "changed_subnet rejected: 'description' did not change"
            val = diff["description"]
            if isinstance(val, dict) and val.get("old") == val.get("new"):
                return f"changed_subnet rejected: 'description' unchanged ({val.get('old')})"
            # Fallback (should not hit if accepts() is correct)
            return "changed_subnet rejected by policy"
        if isinstance(handler, RejectAllHandler):
            return "no specific handler found; default reject"
        # Generic fallback
        return f"handler {handler.__class__.__name__} rejected the signal"
    except Exception as e:
        return f"rejection reason unavailable due to error: {e!r}"


class EventSignalRouter(SignalHandler):
    """
    Dispatch to per-event handlers using payload['type'] / ['event'].
    If there is no specific handler, 'default_handler' is used (reject by default).

    If notifier is set, a Discord embed is sent whenever a signal is rejected,
    containing a concise reason.
    """

    def __init__(
        self,
        handlers: Dict[str, SignalHandler],
        default_handler: Optional[SignalHandler] = None,
        notifier: Optional[DiscordAlerter] = None,
    ) -> None:
        self._handlers = dict(handlers)
        self._default = default_handler or RejectAllHandler()
        self._notifier = notifier

    def accepts(self, payload: Dict[str, Any]) -> bool:
        evt = event_type(payload) or ""
        handler = self._handlers.get(evt, self._default)
        try:
            ok = bool(handler.accepts(payload))
        except Exception as e:
            bt.logging.warning(f"signal handler error for event={evt!r}: {e!r}")
            ok = False

        if not ok:
            reason = _rejection_reason(evt, handler, payload)
            bt.logging.info(f"⏭️  Skipped by policy: event={evt!r} — {reason}")
            if self._notifier:
                try:
                    self._notifier.notify(
                        build_skip_embed(
                            source="router",
                            event=evt,
                            reason=reason,
                            payload=payload,
                        )
                    )
                except Exception as ne:
                    bt.logging.warning(f"discord notify failed (router): {ne!r}")

        return ok


def make_default_router(notifier: Optional[DiscordAlerter] = None) -> EventSignalRouter:
    """Factory for the policy you described."""
    return EventSignalRouter(
        handlers={
            "schedule_swap_coldkey": ScheduleSwapColdkeyHandler(),
            "added_subnet": AddedSubnetHandler(),
            "removed_subnet": RemovedSubnetHandler(),
            "changed_subnet": ChangedSubnetHandler(),
        },
        default_handler=RejectAllHandler(),
        notifier=notifier,
    )
