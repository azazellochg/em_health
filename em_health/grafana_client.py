# **************************************************************************
# *
# * Authors:     Grigory Sharov (gsharov@mrc-lmb.cam.ac.uk) [1]
# *
# * [1] MRC Laboratory of Molecular Biology (MRC-LMB)
# *
# * This program is free software; you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation; either version 3 of the License, or
# * (at your option) any later version.
# *
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# *
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
# * 02111-1307  USA
# *
# *  All comments concerning this program package may be sent to the
# *  e-mail address 'gsharov@mrc-lmb.cam.ac.uk'
# *
# **************************************************************************

import os
import requests
from requests import Response

from em_health.utils.tools import logger

GRAFANA_URL = "http://localhost:3000/api/"


class GrafanaClient:
    """Client for interacting with the Grafana HTTP API."""

    def __init__(self):
        self.base_url = GRAFANA_URL
        api_token = os.getenv("GRAFANA_API_TOKEN")
        if api_token in ["None", "", None]:
            raise ValueError("GRAFANA_API_TOKEN env var is not set")

        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }

    def __request(self, method: str, endpoint: str, payload: dict = None) -> dict:
        url = f"{self.base_url}{endpoint.lstrip('/')}"
        logger.debug("Grafana API %s request to %s: %s", method, url, payload)

        resp: Response = requests.request(method=method, url=url,
                                          headers=self.headers, json=payload)

        if not resp.ok:
            logger.error("Grafana API request failed [%s]: %s", resp.status_code, resp.text)
            raise requests.HTTPError(f"Grafana API request failed [{resp.status_code}]: {resp.text}", response=resp)

        try:
            return resp.json()
        except ValueError:
            logger.error("Invalid JSON in Grafana response: %s", resp.text)
            raise

    def find_dashboard_by_name(self, name: str, tag: str = None):
        query_params = f"search?query={name}"
        if tag:
            query_params += f"&tag={tag}"

        return self.__request("GET", query_params)

    def update_org_prefs(self, home_dashboard_name="Fleet overview", tag="overview") -> dict:
        dashboards = self.find_dashboard_by_name(home_dashboard_name, tag=tag)
        payload = {
            "theme": "system",
            "weekStart": "monday",
            "homeDashboardUID": dashboards[0]["uid"] if dashboards else ""
        }
        return self.__request("PUT", "org/preferences", payload)

if __name__ == "__main__":
    client = GrafanaClient()
    prefs = client.update_org_prefs()
    print(prefs)
