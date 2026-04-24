from __future__ import annotations

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult

from .const import CONF_SNOOP_SOCKET, DEFAULT_SNOOP_SOCKET, DOMAIN


class HaOcppRelayConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input=None) -> FlowResult:
        if user_input is not None:
            await self.async_set_unique_id(user_input[CONF_SNOOP_SOCKET])
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title="HA OCPP Relay", data=user_input)

        schema = vol.Schema(
            {
                vol.Required(CONF_SNOOP_SOCKET, default=DEFAULT_SNOOP_SOCKET): str,
            }
        )
        return self.async_show_form(step_id="user", data_schema=schema)
