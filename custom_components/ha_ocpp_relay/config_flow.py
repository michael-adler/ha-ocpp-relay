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
    SKIP_NEXT_UPDATE_RELOAD,
    default_snoop_socket_for_container,
    normalize_relay_config,
)


def _config_schema(defaults: dict) -> vol.Schema:
    """Build the initial schema used when opening the flow."""
    return _details_schema(defaults, defaults[CONF_RELAY_IS_LOCAL])


def _mode_schema(default_is_local: bool) -> vol.Schema:
    """Build schema for the mode selector step (local relay vs external)."""
    return vol.Schema(
        {
            vol.Required(CONF_RELAY_IS_LOCAL, default=default_is_local): bool,
        }
    )


def _details_schema(defaults: dict, is_local: bool) -> vol.Schema:
    """Build detail fields based on selected deployment mode.

    Local mode requires OCPP/snoop bind addresses and the upstream CSMS URL.
    External mode only needs the snoop socket URL to consume from a remote relay.
    """
    fields: dict = {}

    if is_local:
        fields[vol.Required(CONF_RELAY_OCPP_HOST, default=defaults[CONF_RELAY_OCPP_HOST])] = str
        fields[vol.Required(CONF_RELAY_OCPP_PORT, default=defaults[CONF_RELAY_OCPP_PORT])] = int
        fields[vol.Required(CONF_RELAY_SNOOP_HOST, default=defaults[CONF_RELAY_SNOOP_HOST])] = str
        fields[vol.Required(CONF_RELAY_SNOOP_PORT, default=defaults[CONF_RELAY_SNOOP_PORT])] = int
        fields[vol.Required(CONF_CPMS_URL, default=defaults[CONF_CPMS_URL])] = str
    else:
        fields[vol.Required(CONF_SNOOP_SOCKET, default=defaults[CONF_SNOOP_SOCKET])] = str

    return vol.Schema(fields)


def _normalize_config(user_input: dict) -> dict:
    """Apply defaults and invariants before persisting flow input."""
    return normalize_relay_config(user_input)


def _validate_config(config: dict) -> dict[str, str]:
    """Return form errors for invalid combinations not expressible in schema."""
    errors: dict[str, str] = {}
    if config[CONF_RELAY_IS_LOCAL] and not config.get(CONF_CPMS_URL):
        errors[CONF_CPMS_URL] = "required"
    return errors


def _defaults_from_mapping(mapping: dict | None) -> dict:
    """Project stored config into defaults for flow form fields."""
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


def _merge_flow_input(defaults: dict, user_input: dict, is_local: bool) -> dict:
    """Merge current defaults with submitted form values and selected mode."""
    raw = dict(defaults)
    raw.update(user_input)
    raw[CONF_RELAY_IS_LOCAL] = is_local
    return _normalize_config(raw)


def _detail_form(step_id: str, defaults: dict, is_local: bool, errors: dict | None = None) -> dict:
    """Build parameters for the detailed config form step."""
    return {
        "step_id": step_id,
        "data_schema": _details_schema(defaults, is_local),
        "errors": errors or {},
    }


class HaOcppRelayConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    def __init__(self) -> None:
        """Initialize the instance state."""
        self._is_local = DEFAULT_RELAY_IS_LOCAL
        self._external_config = {}
        self._defaults = _defaults_from_mapping({CONF_RELAY_SNOOP_HOST: DEFAULT_RELAY_SNOOP_HOST})

    def _show_details_form(self, *, errors: dict | None = None) -> FlowResult:
        """Show the detailed settings form for initial setup."""
        return self.async_show_form(
            **_detail_form("user_details", self._defaults, self._is_local, errors)
        )

    def _validate_detail_input(self, user_input: dict) -> tuple[dict, dict[str, str]]:
        """Normalize submitted detail values and return any form errors."""
        config = _merge_flow_input(self._defaults, user_input, self._is_local)
        return config, _validate_config(config)

    async def async_step_user(self, user_input=None) -> FlowResult:
        """Collect integration mode, then route to the detailed settings step."""
        if user_input is not None:
            new_is_local = user_input[CONF_RELAY_IS_LOCAL]
            # If toggling to local, save external config fields
            if new_is_local and not self._is_local:
                self._external_config = {
                    CONF_RELAY_SNOOP_HOST: self._defaults.get(CONF_RELAY_SNOOP_HOST),
                    CONF_RELAY_SNOOP_PORT: self._defaults.get(CONF_RELAY_SNOOP_PORT),
                    CONF_SNOOP_SOCKET: self._defaults.get(CONF_SNOOP_SOCKET),
                }
            # If toggling to external, restore external config fields if available
            if not new_is_local and self._is_local and self._external_config:
                self._defaults.update(self._external_config)
            self._is_local = new_is_local
            self._defaults[CONF_RELAY_IS_LOCAL] = self._is_local
            if self._is_local:
                self._defaults[CONF_RELAY_SNOOP_HOST] = DEFAULT_RELAY_SNOOP_HOST
            return await self.async_step_user_details()

        return self.async_show_form(
            step_id="user",
            data_schema=_mode_schema(self._is_local),
        )

    async def async_step_reconfigure(self, user_input=None) -> FlowResult:
        """Handle Home Assistant reconfigure action for an existing entry."""
        reconfigure_entry = self._get_reconfigure_entry()
        merged = dict(reconfigure_entry.data)
        merged.update(reconfigure_entry.options)
        self._external_config = reconfigure_entry.options.get(
            "external_config",
            {
                CONF_RELAY_SNOOP_HOST: merged.get(CONF_RELAY_SNOOP_HOST),
                CONF_RELAY_SNOOP_PORT: merged.get(CONF_RELAY_SNOOP_PORT),
                CONF_SNOOP_SOCKET: merged.get(CONF_SNOOP_SOCKET),
            },
        )
        self._defaults = _defaults_from_mapping(merged)
        self._is_local = self._defaults[CONF_RELAY_IS_LOCAL]

        if user_input is not None:
            new_is_local = user_input[CONF_RELAY_IS_LOCAL]
            if new_is_local and not self._is_local:
                self._external_config = {
                    CONF_RELAY_SNOOP_HOST: self._defaults.get(CONF_RELAY_SNOOP_HOST),
                    CONF_RELAY_SNOOP_PORT: self._defaults.get(CONF_RELAY_SNOOP_PORT),
                    CONF_SNOOP_SOCKET: self._defaults.get(CONF_SNOOP_SOCKET),
                }
            if not new_is_local and self._is_local and self._external_config:
                self._defaults.update(self._external_config)
            self._is_local = new_is_local
            self._defaults[CONF_RELAY_IS_LOCAL] = self._is_local
            if self._is_local:
                self._defaults[CONF_RELAY_SNOOP_HOST] = DEFAULT_RELAY_SNOOP_HOST
            return await self.async_step_reconfigure_details()

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=_mode_schema(self._is_local),
        )

    async def async_step_reconfigure_details(self, user_input=None) -> FlowResult:
        """Validate and persist reconfigure changes for an existing entry."""
        if user_input is not None:
            if CONF_RELAY_IS_LOCAL in user_input and user_input[CONF_RELAY_IS_LOCAL] != self._is_local:
                new_is_local = user_input[CONF_RELAY_IS_LOCAL]
                if new_is_local and not self._is_local:
                    self._external_config = {
                        CONF_RELAY_SNOOP_HOST: user_input.get(CONF_RELAY_SNOOP_HOST, self._defaults.get(CONF_RELAY_SNOOP_HOST)),
                        CONF_RELAY_SNOOP_PORT: user_input.get(CONF_RELAY_SNOOP_PORT, self._defaults.get(CONF_RELAY_SNOOP_PORT)),
                        CONF_SNOOP_SOCKET: user_input.get(CONF_SNOOP_SOCKET, self._defaults.get(CONF_SNOOP_SOCKET)),
                    }
                if not new_is_local and self._is_local and self._external_config:
                    self._defaults.update(self._external_config)
                self._is_local = new_is_local
                self._defaults[CONF_RELAY_IS_LOCAL] = self._is_local
                if self._is_local:
                    self._defaults[CONF_RELAY_SNOOP_HOST] = DEFAULT_RELAY_SNOOP_HOST
                return self.async_show_form(
                    **_detail_form("reconfigure_details", self._defaults, self._is_local)
                )

            config, errors = self._validate_detail_input(user_input)
            if errors:
                self._defaults = _defaults_from_mapping(config)
                return self.async_show_form(
                    **_detail_form("reconfigure_details", self._defaults, self._is_local, errors)
                )

            reconfigure_entry = self._get_reconfigure_entry()
            # Update both data and options so merged runtime config cannot be
            # masked by stale options from previous edits.
            runtime = self.hass.data.get(DOMAIN, {}).get(reconfigure_entry.entry_id)
            if runtime is not None:
                runtime[SKIP_NEXT_UPDATE_RELOAD] = True
            return self.async_update_reload_and_abort(
                reconfigure_entry,
                title=reconfigure_entry.title,
                data_updates=config,
                options_updates=config,
            )

        return self.async_show_form(
            **_detail_form("reconfigure_details", self._defaults, self._is_local)
        )

    async def async_step_user_details(self, user_input=None) -> FlowResult:
        """Validate and persist initial integration settings.

        The snoop socket is used as unique ID so duplicate relay endpoints are
        not configured twice.
        """
        if user_input is not None:
            # If relay_is_local was toggled in this step, update defaults and re-render
            if CONF_RELAY_IS_LOCAL in user_input and user_input[CONF_RELAY_IS_LOCAL] != self._is_local:
                new_is_local = user_input[CONF_RELAY_IS_LOCAL]
                # Save external config fields if toggling to local
                if new_is_local and not self._is_local:
                    self._external_config = {
                        CONF_RELAY_SNOOP_HOST: user_input.get(CONF_RELAY_SNOOP_HOST, self._defaults.get(CONF_RELAY_SNOOP_HOST)),
                        CONF_RELAY_SNOOP_PORT: user_input.get(CONF_RELAY_SNOOP_PORT, self._defaults.get(CONF_RELAY_SNOOP_PORT)),
                        CONF_SNOOP_SOCKET: user_input.get(CONF_SNOOP_SOCKET, self._defaults.get(CONF_SNOOP_SOCKET)),
                    }
                # Restore external config fields if toggling to external
                if not new_is_local and self._is_local and self._external_config:
                    self._defaults.update(self._external_config)
                self._is_local = new_is_local
                self._defaults[CONF_RELAY_IS_LOCAL] = self._is_local
                if self._is_local:
                    self._defaults[CONF_RELAY_SNOOP_HOST] = DEFAULT_RELAY_SNOOP_HOST
                return self._show_details_form()

            config, errors = self._validate_detail_input(user_input)
            if errors:
                self._defaults = _defaults_from_mapping(config)
                return self._show_details_form(errors=errors)

            # Persist external config fields in options if in external mode
            options = {}
            if not config[CONF_RELAY_IS_LOCAL]:
                options["external_config"] = {
                    CONF_RELAY_SNOOP_HOST: config.get(CONF_RELAY_SNOOP_HOST),
                    CONF_RELAY_SNOOP_PORT: config.get(CONF_RELAY_SNOOP_PORT),
                    CONF_SNOOP_SOCKET: config.get(CONF_SNOOP_SOCKET),
                }
            await self.async_set_unique_id(config[CONF_SNOOP_SOCKET])
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title="OCPP Relay", data=config, options=options)

        self._defaults = _defaults_from_mapping(self._defaults)
        return self._show_details_form()

    @staticmethod
    def async_get_options_flow(config_entry):
        """Return the options flow used to edit an existing entry."""
        return HaOcppRelayOptionsFlow(config_entry)


class HaOcppRelayOptionsFlow(config_entries.OptionsFlow):
    def __init__(self, config_entry) -> None:
        """Initialize the instance state."""
        self._config_entry = config_entry
        merged = dict(config_entry.data)
        merged.update(config_entry.options)
        self._external_config = config_entry.options.get(
            "external_config",
            {
                CONF_RELAY_SNOOP_HOST: merged.get(CONF_RELAY_SNOOP_HOST),
                CONF_RELAY_SNOOP_PORT: merged.get(CONF_RELAY_SNOOP_PORT),
                CONF_SNOOP_SOCKET: merged.get(CONF_SNOOP_SOCKET),
            },
        )
        self._defaults = _defaults_from_mapping(merged)
        self._is_local = self._defaults[CONF_RELAY_IS_LOCAL]

    def _show_details_form(self, *, errors: dict | None = None) -> FlowResult:
        """Show the detailed settings form for options editing."""
        return self.async_show_form(
            **_detail_form("details", self._defaults, self._is_local, errors)
        )

    def _validate_detail_input(self, user_input: dict) -> tuple[dict, dict[str, str]]:
        """Normalize submitted detail values and return any form errors."""
        config = _merge_flow_input(self._defaults, user_input, self._is_local)
        return config, _validate_config(config)

    async def async_step_init(self, user_input=None) -> FlowResult:
        """Collect mode selection for options editing."""
        if user_input is not None:
            new_is_local = user_input[CONF_RELAY_IS_LOCAL]
            # If toggling to local, save external config fields
            if new_is_local and not self._is_local:
                self._external_config = {
                    CONF_RELAY_SNOOP_HOST: self._defaults.get(CONF_RELAY_SNOOP_HOST),
                    CONF_RELAY_SNOOP_PORT: self._defaults.get(CONF_RELAY_SNOOP_PORT),
                    CONF_SNOOP_SOCKET: self._defaults.get(CONF_SNOOP_SOCKET),
                }
            # If toggling to external, restore external config fields if available
            if not new_is_local and self._is_local and self._external_config:
                self._defaults.update(self._external_config)
            self._is_local = new_is_local
            self._defaults[CONF_RELAY_IS_LOCAL] = self._is_local
            if self._is_local:
                self._defaults[CONF_RELAY_SNOOP_HOST] = DEFAULT_RELAY_SNOOP_HOST
            return await self.async_step_details()

        return self.async_show_form(
            step_id="init",
            data_schema=_mode_schema(self._is_local),
        )

    async def async_step_details(self, user_input=None) -> FlowResult:
        """Validate and save edited options for an existing entry."""
        if user_input is not None:
            # If relay_is_local was toggled in this step, update defaults and re-render
            if CONF_RELAY_IS_LOCAL in user_input and user_input[CONF_RELAY_IS_LOCAL] != self._is_local:
                new_is_local = user_input[CONF_RELAY_IS_LOCAL]
                # Save external config fields if toggling to local
                if new_is_local and not self._is_local:
                    self._external_config = {
                        CONF_RELAY_SNOOP_HOST: user_input.get(CONF_RELAY_SNOOP_HOST, self._defaults.get(CONF_RELAY_SNOOP_HOST)),
                        CONF_RELAY_SNOOP_PORT: user_input.get(CONF_RELAY_SNOOP_PORT, self._defaults.get(CONF_RELAY_SNOOP_PORT)),
                        CONF_SNOOP_SOCKET: user_input.get(CONF_SNOOP_SOCKET, self._defaults.get(CONF_SNOOP_SOCKET)),
                    }
                # Restore external config fields if toggling to external
                if not new_is_local and self._is_local and self._external_config:
                    self._defaults.update(self._external_config)
                self._is_local = new_is_local
                self._defaults[CONF_RELAY_IS_LOCAL] = self._is_local
                if self._is_local:
                    self._defaults[CONF_RELAY_SNOOP_HOST] = DEFAULT_RELAY_SNOOP_HOST
                return self._show_details_form()

            config, errors = self._validate_detail_input(user_input)
            if errors:
                self._defaults = _defaults_from_mapping(config)
                return self._show_details_form(errors=errors)

            return self.async_create_entry(data=config)

        self._defaults = _defaults_from_mapping(self._defaults)
        return self._show_details_form()
