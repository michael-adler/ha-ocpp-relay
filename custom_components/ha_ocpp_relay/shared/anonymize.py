"""Mask personal identifiers in logs and recordings.

Two kinds of values are masked:

* idTags (RFID card numbers and other user tokens), shown as ``anon-tag-xxxx``.
* Identifiers of the charger itself: the charge point ID, which is often the
  serial number, the serial-number fields of BootNotification and the SIM
  identifiers (ICCID, IMSI), shown as ``anon-id-xxxx``.

Each masked value keeps a short, stable hash of the original so log readers can
still tell two cards or chargers apart and correlate them across log files,
without exposing the value itself.

Charge point IDs appear in many places besides OCPP payloads: websocket URL
paths, sensor unique IDs, MQTT topics and plain log arguments.  Code that
learns a CP ID calls register_identifier(), and from then on every occurrence
of that string in masked output is replaced.  Registering is cheap and has no
effect unless masking is active.
"""

from __future__ import annotations

import dataclasses
import hashlib
import logging
import os
import pkgutil
import re
import threading
from typing import Any

from .models import MessageData, OCPPSensorData

# OCPP 1.6 uses idTag / parentIdTag.  OCPP 2.0.1 uses idToken objects of the form
# {"idToken": "<value>", "type": ...}, also nested as groupIdToken.  Only string
# values under these keys are masked, so the outer idToken object is walked into
# rather than replaced.
ID_TAG_KEYS = frozenset({"idTag", "parentIdTag", "idToken"})

# Charger identifiers: BootNotification serial numbers (1.6 and 2.0.1), SIM
# identifiers, and the cp_id field of snoop stream records.
IDENTIFIER_KEYS = frozenset({
    "chargePointSerialNumber",
    "chargeBoxSerialNumber",
    "meterSerialNumber",
    "serialNumber",
    "iccid",
    "imsi",
    "cp_id",
})

_TAG_PREFIX = "anon-tag-"
_ID_PREFIX = "anon-id-"
_HASH_CHARS = 4
_MASKED_RE = re.compile(rf"^anon-(?:tag|id)-[0-9a-f]{{{_HASH_CHARS}}}$")

# Identifiers shorter than this are not registered for free-text replacement,
# where they would match unrelated text such as connector numbers.  They are
# still masked when they appear under a known key.
_MIN_REGISTERED_LEN = 4
# The relay accepts any URL path as a CP ID, so bound the registry.
_MAX_REGISTERED = 1000

_VALUE = r"""(?P<vq>["'])(?P<value>[^"'\\]*)(?P=vq)"""

# Matches "key": "value" (JSON), 'key': 'value' (Python repr) and key='value'
# (keyword-style summaries).  The key is any name ending in idTag or idToken
# (either case of the leading "i"), which also catches keys whose start was cut
# off, e.g. by the websockets library shortening long frames in its debug log
# ('..."tIdTag": "..."').
_TAG_TEXT_RE = re.compile(
    r"""(?P<prefix>["']?\w*[iI]d(?:Tag|Token)["']?\s*[:=]\s*)""" + _VALUE
)

_ID_TEXT_RE = re.compile(
    r"""(?P<prefix>["']?\b(?:%s)["']?\s*[:=]\s*)""" % "|".join(sorted(IDENTIFIER_KEYS)) + _VALUE
)

# Field order of the OCPPSensorData repr: ..., unique_id='..._idtag', name='...', value='...'
_SENSOR_REPR_RE = re.compile(
    r"""(?P<prefix>unique_id=(?P<uq>["'])[^"']*_idtag(?P=uq), name=(?P<nq>["'])[^"']*(?P=nq), value=)"""
    + _VALUE
)

# user:password@ in ws://, wss://, http:// and https:// URLs.
_URL_USERINFO_RE = re.compile(r"(?P<scheme>\b(?:wss?|https?)://)[^/\s@\"']+@")

# HTTP request line of a websocket handshake, as logged by the websockets
# library ("< GET /CP-ID HTTP/1.1").  The charger's ID is the last path segment.
# This line is logged before the relay sees the connection, so the segment is
# registered here.
_REQUEST_LINE_RE = re.compile(r"(?P<prefix>\bGET \S*/)(?P<value>[^/\s?]+)(?P<suffix> HTTP/)")


# Frame preview logged by the websockets library at debug level:
#   < TEXT '[2, "b1", "BootNotification", {"chargeP...i": "2040..."}]' [240 bytes]
# Long frames are shortened by cutting out the middle and inserting "...".  The
# cut can split keys and values, so neither key-based nor value-based masking
# is reliable on what remains.
_FRAME_PREVIEW_RE = re.compile(r"^(?P<head>[<>] (?:TEXT|BINARY) ')(?P<body>.*)(?P<tail>' \[\d+ bytes\])$", re.S)
# The start of an OCPP frame: message type, message ID and, for CALLs, the action.
_FRAME_START_RE = re.compile(r"""^\[\s*\d\s*,\s*"[^"]*"(?:\s*,\s*"\w+")?""")


def _shorten_truncated_preview(text: str) -> str:
    """Keep only the OCPP frame start of a shortened websockets frame preview."""
    match = _FRAME_PREVIEW_RE.match(text)
    if match is None or "..." not in match.group("body"):
        return text
    start = _FRAME_START_RE.match(match.group("body"))
    kept = start.group(0) if start else ""
    return f"{match.group('head')}{kept}, ...{match.group('tail')}"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:_HASH_CHARS]


def mask_id_tag(tag: str) -> str:
    """Return a stable masked stand-in for one idTag value.

    Already-masked and empty values are returned unchanged, so masking is
    idempotent when more than one filter sees the same record.
    """
    if not tag or _MASKED_RE.match(tag):
        return tag
    return f"{_TAG_PREFIX}{_digest(tag)}"


def mask_identifier(value: str) -> str:
    """Return a stable masked stand-in for a charger identifier such as a CP ID."""
    if not value or _MASKED_RE.match(value):
        return value
    return f"{_ID_PREFIX}{_digest(value)}"


class _IdentifierRegistry:
    """Thread-safe set of identifiers to replace wherever they appear in text."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: set[str] = set()
        self._pattern: re.Pattern | None = None

    def add(self, value: Any) -> None:
        if (
            not isinstance(value, str)
            or len(value) < _MIN_REGISTERED_LEN
            or _MASKED_RE.match(value)
        ):
            return
        with self._lock:
            if value in self._values or len(self._values) >= _MAX_REGISTERED:
                return
            self._values.add(value)
            # Longest first, so an ID that contains another ID is replaced whole.
            alternatives = "|".join(
                re.escape(v) for v in sorted(self._values, key=len, reverse=True)
            )
            # Require non-alphanumeric neighbors so an ID is not replaced inside
            # a longer word.  Underscores and path separators count as
            # boundaries, so "OCPP_<id>_1_idtag" and "/ocpp/<id>" are matched.
            self._pattern = re.compile(rf"(?<![A-Za-z0-9])(?:{alternatives})(?![A-Za-z0-9])")

    def clear(self) -> None:
        with self._lock:
            self._values.clear()
            self._pattern = None

    def mask(self, text: str) -> str:
        pattern = self._pattern
        if pattern is None:
            return text
        return pattern.sub(lambda m: mask_identifier(m.group(0)), text)


_registry = _IdentifierRegistry()


def register_identifier(value: Any) -> None:
    """Mask *value* wherever it appears in masked output from now on.

    Call this as soon as a charge point ID is known, before it is logged.
    """
    _registry.add(value)


def clear_registered_identifiers() -> None:
    """Forget all registered identifiers (for tests)."""
    _registry.clear()


def anonymize_payload(obj: Any) -> Any:
    """Return a copy of a decoded OCPP frame, payload or snoop record with identifiers masked."""
    if isinstance(obj, dict):
        # Register identifiers first so that other strings in the same object
        # that mention them are masked too.
        for key, value in obj.items():
            if key in IDENTIFIER_KEYS:
                register_identifier(value)
        return {key: _anonymize_value(key, value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [anonymize_payload(item) for item in obj]
    if isinstance(obj, str):
        return _registry.mask(obj)
    return obj


def _anonymize_value(key: str, value: Any) -> Any:
    if isinstance(value, str):
        if key in ID_TAG_KEYS:
            return mask_id_tag(value)
        if key in IDENTIFIER_KEYS:
            return mask_identifier(value)
    return anonymize_payload(value)


def _mask_tag_match(match: re.Match) -> str:
    vq = match.group("vq")
    return f"{match.group('prefix')}{vq}{mask_id_tag(match.group('value'))}{vq}"


def _mask_id_match(match: re.Match) -> str:
    value = match.group("value")
    register_identifier(value)
    vq = match.group("vq")
    return f"{match.group('prefix')}{vq}{mask_identifier(value)}{vq}"


def _mask_request_line(match: re.Match) -> str:
    value = match.group("value")
    register_identifier(value)
    return f"{match.group('prefix')}{mask_identifier(value)}{match.group('suffix')}"


def anonymize_text(text: str) -> str:
    """Mask idTags, charger identifiers and URL credentials in free text."""
    text = _shorten_truncated_preview(text)
    text = _TAG_TEXT_RE.sub(_mask_tag_match, text)
    text = _SENSOR_REPR_RE.sub(_mask_tag_match, text)
    text = _ID_TEXT_RE.sub(_mask_id_match, text)
    text = _REQUEST_LINE_RE.sub(_mask_request_line, text)
    text = _URL_USERINFO_RE.sub(r"\g<scheme>***@", text)
    return _registry.mask(text)


def anonymize_message_data(msg: MessageData) -> MessageData:
    """Return a copy of a snoop stream message with identifiers masked."""
    register_identifier(msg.cp_id)
    return dataclasses.replace(
        msg,
        cp_id=mask_identifier(msg.cp_id) if msg.cp_id else msg.cp_id,
        payload=anonymize_payload(msg.payload),
    )


def anonymize_sensor(data: OCPPSensorData) -> OCPPSensorData:
    """Return a copy of a sensor reading with its CP ID and any idTag value masked.

    The CP ID is masked in every field, including unique_id, name and topic.
    """
    register_identifier(data.cp_id)
    masked = dataclasses.replace(
        data,
        **{
            f.name: _registry.mask(getattr(data, f.name))
            for f in dataclasses.fields(data)
            if isinstance(getattr(data, f.name), str)
        },
    )
    if masked.unique_id.endswith("_idtag") and isinstance(masked.value, str):
        masked = dataclasses.replace(masked, value=mask_id_tag(masked.value))
    return masked


class IdentifierMaskingFilter(logging.Filter):
    """Logging filter that masks idTags, charger identifiers and URL credentials."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Rewrite the record's message in place; never drop records."""
        if isinstance(record.args, tuple) and record.args:
            record.args = tuple(_anonymize_arg(arg) for arg in record.args)
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001
            # Leave malformed records for the logging machinery to report.
            return True
        masked = anonymize_text(message)
        if masked != message:
            record.msg = masked
            record.args = None
        return True


def _anonymize_arg(arg: Any) -> Any:
    """Mask structured log arguments before they are formatted."""
    if isinstance(arg, OCPPSensorData):
        return anonymize_sensor(arg)
    if isinstance(arg, MessageData):
        return anonymize_message_data(arg)
    if isinstance(arg, (dict, list)):
        return anonymize_payload(arg)
    return arg


def _has_masking_filter(filterer: logging.Filterer) -> bool:
    return any(isinstance(existing, IdentifierMaskingFilter) for existing in filterer.filters)


def install_masking_on_handlers(logger: logging.Logger | None = None) -> None:
    """Mask identifiers in everything written by *logger*'s handlers (default: root).

    Handler-level filters see records propagated from every logger, including
    third-party libraries such as websockets.
    """
    logger = logger or logging.getLogger()
    for handler in logger.handlers:
        if not _has_masking_filter(handler):
            handler.addFilter(IdentifierMaskingFilter())


def package_logger_names(package_name: str, package_path: list[str]) -> list[str]:
    """Return the logger names of a package and all of its modules.

    Names follow the logging.getLogger(__name__) convention.  Modules are
    discovered from the filesystem without importing them, so this is safe to
    call during package import.  It does blocking directory reads, so call it
    at import time (or in an executor), not on an event loop.
    """
    names = [package_name]
    # pkgutil.walk_packages would import each subpackage to recurse into it;
    # recurse on paths instead so this is safe to call during package import.
    pending = [(package_name, list(package_path))]
    while pending:
        prefix, paths = pending.pop()
        for info in pkgutil.iter_modules(paths, prefix=f"{prefix}."):
            names.append(info.name)
            if info.ispkg:
                leaf = info.name.rsplit(".", 1)[1]
                pending.append((info.name, [os.path.join(p, leaf) for p in paths]))
    return names


def install_masking_on_loggers(names: list[str]) -> None:
    """Mask identifiers in records logged directly on each named logger.

    Logger-level filters only apply to records created on that exact logger,
    not on its children, so every module's logger needs its own filter.
    """
    for name in names:
        module_logger = logging.getLogger(name)
        if not _has_masking_filter(module_logger):
            module_logger.addFilter(IdentifierMaskingFilter())
