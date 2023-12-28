from copy import deepcopy
from json import JSONDecodeError
from time import time
from typing import Optional, Any, Dict, Union

import requests
from imagination import container
from requests import Request, Session, Response

from dnastack.client.models import ServiceEndpoint
from dnastack.common.logger import get_logger
from dnastack.common.tracing import Span
from dnastack.feature_flags import in_global_debug_mode
from dnastack.http.authenticators.abstract import Authenticator, AuthenticationRequired, ReauthenticationRequired, \
    RefreshRequired, InvalidStateError, NoRefreshToken, AuthState, ReauthenticationRequiredDueToConfigChange, \
    AuthStateStatus
from dnastack.http.authenticators.oauth2_adapter.factory import OAuth2AdapterFactory
from dnastack.http.authenticators.oauth2_adapter.models import OAuth2Authentication
from dnastack.http.session_info import SessionInfo, SessionManager, SessionInfoHandler


class OAuth2MisconfigurationError(RuntimeError):
    pass


class OAuth2Authenticator(Authenticator):
    def __init__(self,
                 endpoint: ServiceEndpoint,
                 auth_info: Dict[str, Any],
                 session_manager: Optional[SessionManager] = None,
                 adapter_factory: Optional[OAuth2AdapterFactory] = None):
        super().__init__()

        self._endpoint = endpoint
        self._auth_info = auth_info
        self._logger = get_logger(f'{type(self).__name__}: E/T:{endpoint.type}/ID:{endpoint.id}'
                                  if endpoint
                                  else f'{type(self).__name__}: A/SID:{self.session_id}')
        self._adapter_factory: OAuth2AdapterFactory = adapter_factory or container.get(OAuth2AdapterFactory)
        self._session_manager: SessionManager = session_manager or container.get(SessionManager)
        self._session_info: Optional[SessionInfo] = None

    @property
    def session_id(self):
        return OAuth2Authentication(**self._auth_info).get_content_hash()

    def get_state(self) -> AuthState:
        status = AuthStateStatus.READY
        session_info: Dict[str, Any] = dict()

        try:
            session = self.restore_session()
            session_info.update(session.dict())
        except RefreshRequired as e:
            status = AuthStateStatus.REFRESH_REQUIRED
            session_info.update(e.session.dict())
        except AuthenticationRequired:
            status = AuthStateStatus.UNINITIALIZED
        except (ReauthenticationRequiredDueToConfigChange, ReauthenticationRequired):
            status = AuthStateStatus.REAUTH_REQUIRED

        return AuthState(
            authenticator=self.fully_qualified_class_name,
            id=self.session_id,
            auth_info=deepcopy(self._auth_info),
            session_info=session_info,
            status=status,
        )

    def authenticate(self, trace_context: Optional[Span] = None) -> SessionInfo:
        session_id = self.session_id
        event_details = dict(session_id=session_id,
                             auth_info=self._auth_info)

        self.events.dispatch('authentication-before', event_details)

        auth_info = OAuth2Authentication(**self._auth_info)
        adapter = self._adapter_factory.get_from(auth_info)

        if not adapter:
            event_details['reason'] = 'No compatible OAuth2 adapter'
            self.events.dispatch('authentication-failure', event_details)

            raise OAuth2MisconfigurationError('Cannot determine the type of authentication '
                                              f'({auth_info.json(sort_keys=True, exclude_none=True)})')

        adapter.check_config_readiness()
        for auth_event_type in ['blocking-response-required', 'blocking-response-ok', 'blocking-response-failed']:
            self.events.relay_from(adapter.events, auth_event_type)

        raw_response = adapter.exchange_tokens(trace_context)
        self._session_info = self._convert_token_response_to_session(auth_info.dict(), raw_response)
        self._session_manager.save(session_id, self._session_info)

        event_details['session_info'] = self._session_info

        self.events.dispatch('authentication-ok', event_details)

        return self._session_info

    def refresh(self, trace_context: Optional[Span] = None) -> SessionInfo:
        trace_context = trace_context or Span(origin='OAuth2Authenticator.refresh')

        session_id = self.session_id
        event_details = dict(cached=self._session_info is not None,
                             cache_hash=session_id)

        self.events.dispatch('refresh-before', event_details)

        session_info = self._session_info or self._session_manager.restore(session_id)

        if session_info is None:
            raise ReauthenticationRequired('No existing session information available')

        if session_info.dnastack_schema_version < 3:
            event_details['reason'] = f'Not enough information for token refresh'
            self.events.dispatch('refresh-failure', event_details)

            raise ReauthenticationRequired(f'The stored session information does not provide enough information to '
                                           f'refresh token. (given: {session_info})')

        if not session_info.refresh_token:
            event_details['reason'] = 'No refresh token'
            self.events.dispatch('refresh-failure', event_details)

            raise NoRefreshToken()

        auth_info = OAuth2Authentication(**session_info.handler.auth_info)
        refresh_token = session_info.refresh_token
        refresh_token_res: Optional[Response] = None

        try:
            with trace_context.new_span({'method': 'post', 'url': auth_info.token_endpoint}):
                refresh_token_res = requests.post(
                    auth_info.token_endpoint,
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": refresh_token,
                        "scope": session_info.scope,
                    },
                    auth=(auth_info.client_id, auth_info.client_secret),
                )

            if refresh_token_res.ok:
                refresh_token_json = refresh_token_res.json()

                if in_global_debug_mode:
                    self._logger.debug(f'refresh_token_json = {refresh_token_json}')

                # Fill in the missing data.
                refresh_token_json['refresh_token'] = refresh_token

                # Update the session
                self._session_info = self._convert_token_response_to_session(auth_info.dict(), refresh_token_json)
                self._session_manager.save(session_id, self._session_info)

                event_details['session_info'] = self._session_info
                self.events.dispatch('refresh-ok', event_details)

                return self._session_info
            else:
                try:
                    error_json = refresh_token_res.json()
                    error_msg = error_json['error_description']
                except JSONDecodeError:
                    error_msg = refresh_token_res.text

                # Handle Wallet-specific implementation
                reauthentication_required = refresh_token_res.status_code == 400 and 'JWT expired' in error_msg

                if not reauthentication_required:
                    event_details['reason'] = 'Invalid state while refreshing tokens'
                    self.events.dispatch('refresh-failure', event_details)

                exception_details = {
                    'debug_mode': in_global_debug_mode,
                    'request': {
                        'url': auth_info.token_endpoint,
                    },
                    'response': {
                        'trace_id': refresh_token_res.headers.get('X-B3-Traceid'),
                        'status': refresh_token_res.status_code,
                    },
                    'reason': f'Unable to refresh tokens: {error_msg}',
                }

                if in_global_debug_mode:
                    exception_details['_internal'] = {
                        'session_info': session_info.dict(),
                        'session_manager': str(self._session_manager),
                    }

                if reauthentication_required:
                    raise ReauthenticationRequired('Refresh token expired')
                else:
                    raise InvalidStateError('Unable to refresh the access token',
                                            details=exception_details)
        finally:
            if refresh_token_res:
                refresh_token_res.close()

    def revoke(self):
        session_id = self.session_id

        # Clear the local cache
        self._session_info = None
        self._session_manager.delete(session_id)

        self.events.dispatch('session-revoked', dict(session_id=session_id))

    def restore_session(self) -> Optional[SessionInfo]:
        logger = self._logger

        session_id = self.session_id
        event_details = dict(cached=self._session_info is not None,
                             cache_hash=session_id)

        logger.debug(f'Existing Session Info: {self._session_info}')

        session = self._session_info or self._session_manager.restore(session_id)

        logger.debug(f'Restored Session Info: {session}')

        if not session:
            logger.debug('No session restored')

            event_details['reason'] = 'No session available'
            self.events.dispatch('session-not-restored', event_details)

            raise AuthenticationRequired('No session available')
        elif session.is_valid():
            logger.debug('The session is valid')

            current_auth_info = OAuth2Authentication(**self._auth_info)
            current_config_hash = current_auth_info.get_content_hash()
            stored_config_hash = session.config_hash

            if current_config_hash == stored_config_hash:
                return session
            else:
                event_details['reason'] = 'Authentication information has changed and the session is invalidated.'
                self.events.dispatch('session-not-restored', event_details)

                raise ReauthenticationRequiredDueToConfigChange(
                    'The session is invalidated as the endpoint configuration has changed.'
                )
        else:
            logger.debug('The session is INVALID.')

            if session.refresh_token:
                logger.debug('Require REFRESH')

                event_details['reason'] = 'The session is invalid but it can be refreshed.'
                self.events.dispatch('session-not-restored', event_details)

                raise RefreshRequired(session)
            else:
                logger.debug('Require RE-AUTH')

                event_details['reason'] = 'The session is invalid. Require re-authentication.'
                self.events.dispatch('session-not-restored', event_details)

                raise ReauthenticationRequired('The session is invalid and refreshing tokens is not possible.')

    def _convert_token_response_to_session(self,
                                           authentication: Dict[str, Any],
                                           response: Dict[str, Any]):
        assert 'access_token' in response, f'Failed to exchange tokens due to an unexpected response ({response})'

        created_time = time()
        expiry_time = created_time + response['expires_in']

        current_auth_info = OAuth2Authentication(**self._auth_info)

        return SessionInfo(
            model_version=4,
            config_hash=current_auth_info.get_content_hash(),
            access_token=response['access_token'],
            refresh_token=response.get('refresh_token'),
            scope=response.get('scope'),
            token_type=response['token_type'],
            issued_at=created_time,
            valid_until=expiry_time,
            handler=SessionInfoHandler(auth_info=authentication)
        )

    def update_request(self, session: SessionInfo, r: Union[Request, Session]) -> Union[Request, Session]:
        r.headers["Authorization"] = f"Bearer {session.access_token}"

        if in_global_debug_mode:
            self._logger.debug(f'Bearer Token Claims: {session.access_token.split(".")[1]}')

        return r

    @classmethod
    def make(cls, endpoint: ServiceEndpoint, auth_info: Dict[str, Any]):
        return cls(endpoint, auth_info)
