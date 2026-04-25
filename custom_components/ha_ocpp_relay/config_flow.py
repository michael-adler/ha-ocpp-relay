"""Home Assistant config and options flows for configuring relay and snoop endpoints."""

from __future__ import annotations

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult

from .const import (
    CONF_CPMS_URL,
    CONF_RELAY_IS_LOCAL,
    CONF_RELAY_OCPP_HOST,
    CONF_RELAY_OCPP_PORT,
    CONF_RELAY_SNOOP_HOST,
    CONF_RELAY_SNOOP_PORT,
    CONF_SNOOP_SOCKET,
    DEFAULT_RELAY_IS_LOCAL,
    DEFAULT_RELAY_OCPP_HOST,
    DEFAULT_RELAY_OCPP_PORT,
    DEFAULT_RELAY_SNOOP_HOST,
    DEFAULT_RELAY_SNOOP_PORT,
    DEFAULT_SNOOP_SOCKET,
    DOMAIN,
    default_snoop_socket_for_container,
    normalize_relay_config,
)


def _config_schema(defaults: dict) -> vol.Schema:
    return _details_schema(defaults, defaults[CONF_RELAY_IS_LOCAL])


def _mode_schema(default_is_local: bool) -> vol.Schema:
    return vol.Schema(
        {
            vol.Required(CONF_RELAY_IS_LOCAL, default=default_is_local): bool,
        }
    )


def _details_schema(defaults: dict, is_local: bool) -> vol.Schema:
    fields: dict = {
        vol.Required(CONF_RELAY_OCPP_HOST, default=defaults[CONF_RELAY_OCPP_HOST]): str,
        vol.Required(CONF_RELAY_OCPP_PORT, default=defaults[CONF_RELAY_OCPP_PORT]): int,
        vol.Required(CONF_RELAY_SNOOP_PORT, default=defaults[CONF_RELAY_SNOOP_PORT]): int,
    }

    if is_local:
        fields[vol.Required(CONF_CPMS_URL, default=defaults[CONF_CPMS_URL])] = str
    else:
        fields[vol.Optional(CONF_CPMS_URL, default=defaults[CONF_CPMS_URL])] = str
        fields[vol.Required(CONF_RELAY_SNOOP_HOST, default=defaults[CONF_RELAY_SNOOP_HOST])] = str
        fields[vol.Required(CONF_SNOOP_SOCKET, default=defaults[CONF_SNOOP_SOCKET])] = str

    return vol.Schema(fields)


def _normalize_config(user_input: dict) -> dict:
    return normalize_relay_config(user_input)


def _validate_config(config: dict) -> dict[str, str]:
    errors: dict[str, str] = {}
    if config[CONF_RELAY_IS_LOCAL] and not config.get(CONF_CPMS_URL):
        errors[CONF_CPMS_URL] = "required"
    return errors


def _defaults_from_mapping(mapping: dict | None) -> dict:
    normalized = normalize_relay_config(mapping or {})
    relay_snoop_port = normalized[CONF_RELAY_SNOOP_PORT]
    return {
        CONF_RELAY_IS_LOCAL: normalized[CONF_RELAY_IS_LOCAL],
        CONF_CPMS_URL: normalized[CONF_CPMS_URL],
        CONF_RELAY_OCPP_HOST: normalized[CONF_RELAY_OCPP_HOST],
        CONF_RELAY_OCPP_PORT: normalized[CONF_RELAY_OCPP_PORT],
        CONF_RELAY_SNOOP_HOST: normalized[CONF_RELAY_SNOOP_HOST],
        CONF_RELAY_SNOOP_PORT: relay_snoop_port,
        CONF_SNOOP_SOCKET: normalized.get(
            CONF_SNOOP_SOCKET,
            default_snoop_socket_for_container(relay_snoop_port),
        ),
    }


class HaOcppRelayConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    def __init__(self) -> None:
        self._is_local = DEFAULT_RELAY_IS_LOCAL
        self._defaults = _defaults_from_mapping({})

    async def async_step_user(self, user_input=None) -> FlowResult:
        if user_input is not None:
            self._is_local = user_input[CONF_RELAY_IS_LOCAL]
            self._defaults[CONF_RELAY_IS_LOCAL] = self._is_local
            return await self.async_step_user_details()

        return self.async_show_form(
            step_id="user",
            data_schema=_mode_schema(self._is_local),
        )

    async def async_step_user_details(self, user_input=None) -> FlowResult:
        if user_input is not None:
            raw = dict(self._defaults)
            raw.update(user_input)
            raw[CONF_RELAY_IS_LOCAL] = self._is_local

            config = _normalize_config(raw)
            errors = _validate_config(config)
            if errors:
                self._defaults = _defaults_from_mapping(config)
                return self.async_show_form(
                    step_id="user_details",
                    data_schema=_details_schema(self._defaults, self._is_local),
                    errors=errors,
                )

            await self.async_set_unique_id(config[CONF_SNOOP_SOCKET])
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title="HA OCPP Relay", data=config)

        self._defaults = _defaults_from_mapping(self._defaults)
        return self.async_show_form(
            step_id="user_details",
            data_schema=_details_schema(self._defaults, self._is_local),
        )

    @staticmethod
    def async_get_options_flow(config_entry):
        return HaOcppRelayOptionsFlow(config_entry)


class HaOcppRelayOptionsFlow(config_entries.OptionsFlow):
    def __init__(self, config_entry) -> None:
        self._config_entry = config_entry
        merged = dict(config_entry.data)
        merged.update(config_entry.options)
        self._defaults = _defaults_from_mapping(merged)
        self._is_local = self._defaults[CONF_RELAY_IS_LOCAL]

    async def async_step_init(self, user_input=None) -> FlowResult:
        if user_input is not None:
            self._is_local = user_input[CONF_RELAY_IS_LOCAL]
            self._defaults[CONF_RELAY_IS_LOCAL] = self._is_local
            return await self.async_step_details()

        return self.async_show_form(
            step_id="init",
            data_schema=_mode_schema(self._is_local),
        )

    async def async_step_details(self, user_input=None) -> FlowResult:
        if user_input is not None:
            raw = dict(self._defaults)
            raw.update(user_input)
            raw[CONF_RELAY_IS_LOCAL] = self._is_local

            config = _normalize_config(raw)
            errors = _validate_config(config)
            if errors:
                self._defaults = _defaults_from_mapping(config)
                return self.async_show_form(
                    step_id="details",
                    data_schema=_details_schema(self._defaults, self._is_local),
                    errors=errors,
                )

            return self.async_create_entry(title="", data=config)

        self._defaults = _defaults_from_mapping(self._defaults)
        return self.async_show_form(
            step_id="details",
            data_schema=_details_schema(self._defaults, self._is_local),
        )
