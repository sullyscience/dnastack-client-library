from pprint import pformat
from time import time, sleep
from typing import Dict, Any, List

from imagination import container
from requests import Session

from dnastack.common.console import Console
from dnastack.common.environments import env
from dnastack.common.tracing import Span
from dnastack.feature_flags import in_global_debug_mode
from dnastack.http.authenticators.oauth2_adapter.abstract import OAuth2Adapter, AuthException
from dnastack.http.authenticators.oauth2_adapter.models import OAuth2Authentication


class DeviceCodeFlowAdapter(OAuth2Adapter):
    __grant_type = 'urn:ietf:params:oauth:grant-type:device_code'

    def __init__(self, auth_info: OAuth2Authentication):
        super(DeviceCodeFlowAdapter, self).__init__(auth_info)
        self.__console: Console = container.get(Console)

    @staticmethod
    def get_expected_auth_info_fields() -> List[str]:
        return [
            'client_id',
            # 'client_secret',
            'device_code_endpoint',
            'grant_type',
            'resource_url',
            'token_endpoint',
        ]

    def exchange_tokens(self, trace_context: Span) -> Dict[str, Any]:
        session = Session()

        auth_info = self._auth_info
        grant_type = auth_info.grant_type
        login_url = auth_info.device_code_endpoint
        resource_url = auth_info.resource_url
        client_id = auth_info.client_id

        if grant_type != self.__grant_type:
            raise AuthException(resource_url, f'Invalid Grant Type (expected: {self.__grant_type})')

        if not login_url:
            raise AuthException(resource_url, "There is no device code URL specified.")

        device_code_params = {
            "grant_type": self.__grant_type,
            "client_id": client_id,
            "resource": self._prepare_resource_urls_for_request(resource_url),
        }

        if auth_info.scope:
            device_code_params['scope'] = auth_info.scope

        with trace_context.new_span(metadata={'oauth': 'device-code', 'step': 'init', 'init_url': login_url}) as sub_span:
            span_headers = sub_span.create_http_headers()
            device_code_res = session.post(login_url,
                                           params=device_code_params,
                                           allow_redirects=False,
                                           headers=span_headers)

        device_code_json = device_code_res.json()

        if in_global_debug_mode:
            self._logger.debug(f'Response from {login_url}:\n{pformat(device_code_json, indent=2)}')

        if device_code_res.ok:
            device_code = device_code_json["device_code"]
            device_verify_uri = device_code_json["verification_uri_complete"]
            poll_interval = int(device_code_json["interval"])
            expiry = time() + int(env('DEVICE_CODE_TTL', required=False) or device_code_json["expires_in"])

            # user_code = device_code_json['user_code']
            self.__console.print(f"Please go to {device_verify_uri} to continue.\n", to_stderr=True)
            self._events.dispatch('blocking-response-required', dict(kind='user_verification', url=device_verify_uri))
        else:
            if "error" in device_code_res.json():
                error_message = f'The device code request failed with message "{device_code_json["error"]}"'
            else:
                error_message = "The device code request failed"

            self._logger.error(f'Failed to initiate the device code flow ({device_code_params})')
            raise AuthException(url=login_url, msg=error_message)

        token_url = auth_info.token_endpoint

        while time() < expiry:
            with trace_context.new_span(metadata={'oauth': 'device-code', 'step': 'confirm', 'init_url': login_url}) \
                    as sub_span:
                auth_token_res = session.post(
                    token_url,
                    data={
                        "grant_type": self.__grant_type,
                        "device_code": device_code,
                        "client_id": client_id,
                    },
                    headers=sub_span.create_http_headers()
                )

                auth_token_json = auth_token_res.json()
                if in_global_debug_mode:
                    self._logger.debug(f'Response from {token_url}:\n{pformat(auth_token_json, indent=2)}')

                if auth_token_res.ok:
                    self._logger.debug('Response: Authorized')
                    session.close()
                    return auth_token_json
                elif "error" in auth_token_json:
                    if auth_token_json.get("error") == "authorization_pending":
                        self._logger.debug('Response: Pending on authorization...')
                        sleep(poll_interval)
                        continue

                    error_msg = "Failed to retrieve a token"
                    if "error_description" in auth_token_json:
                        error_msg += f": {auth_token_json['error_description']}"

                    self._logger.error('Exceeded the waiting time limit for the device code')
                    raise AuthException(url=token_url, msg=error_msg)
                else:
                    self._logger.debug('Response: Unknown state')
                    sleep(poll_interval)

        raise AuthException(url=token_url, msg="the authorize step timed out.")