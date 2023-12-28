from copy import deepcopy
from time import time
from math import floor
from typing import Any, Dict

from dnastack.cli.helpers.printer import echo_dict_in_table, echo_result, echo_list, echo_header
from dnastack.common.events import Event
from dnastack.http.authenticators.abstract import AuthStateStatus, AuthState

_status_color_map = {
    AuthStateStatus.READY: 'green',
    AuthStateStatus.UNINITIALIZED: 'magenta',
    AuthStateStatus.REFRESH_REQUIRED: 'yellow',
    AuthStateStatus.REAUTH_REQUIRED: 'red',
}


def handle_auth_begin(event: Event):
    session_id = event.details['session_id']
    state: AuthState = event.details['state']

    if state.status != AuthStateStatus.READY:
        echo_result('Session',
                    'yellow',
                    'initializing',
                    f'Session {session_id}',
                    ' ')


def handle_auth_end(event: Event):
    session_id = event.details['session_id']
    state: AuthState = event.details['state']
    feedback: Dict[str, Any] = deepcopy(state.auth_info)
    if state.status == AuthStateStatus.READY:
        is_refreshable = bool(state.session_info.get('refresh_token'))
        access_grant_duration_in_seconds = floor(state.session_info['valid_until'] - time())

        if not is_refreshable:
            duration_label = f'{access_grant_duration_in_seconds} seconds.'
            if access_grant_duration_in_seconds > 60:
                minutes = floor(access_grant_duration_in_seconds / 60)
                seconds = access_grant_duration_in_seconds % 60
                duration_label = f'{minutes} minute{"s" if minutes > 1 else ""}'
                if seconds:
                    duration_label += f' {seconds} second{"s" if seconds > 1 else ""}'
            echo_header(f'Your session will expire in {duration_label.strip()}. Once it expires, you will '
                        'be asked to log in again. ',
                        bg='yellow')

        echo_result('Session',
                    _status_color_map[state.status],
                    state.status,
                    f'Session {session_id}',
                    '●')

        feedback['refreshable'] = is_refreshable
        feedback['access_expires_in_seconds'] = access_grant_duration_in_seconds
    else:
        echo_result('Session',
                    _status_color_map[state.status],
                    state.status,
                    f'Session {session_id}',
                    'x')

    echo_dict_in_table(feedback, left_padding_size=18)


def handle_revoke_begin(event: Event):
    session_id = event.details['session_id']
    echo_result('Session',
                'yellow',
                'removing',
                f'Session {session_id}',
                ' ')

    echo_dict_in_table(event.details['state'].auth_info, left_padding_size=21)


def handle_revoke_end(event: Event):
    session_id = event.details['session_id']
    result = event.details['result']
    successfully_removed = result == 'removed'
    echo_result('Session',
                'red' if successfully_removed else 'magenta',
                result,
                f'Session {session_id}',
                'x')

    if successfully_removed:
        endpoint_ids = event.details['endpoint_ids']
        echo_list('Affected endpoint(s):', endpoint_ids)


def handle_no_refresh_token(_: Event):
    echo_result('Session', 'yellow', 'notice', 'This auth server did not provide the refresh token.', '▲')


def handle_refresh_skipped(_: Event):
    echo_result('Session', 'yellow', 'notice', 'As the session is still valid, the session will not be refreshed.', '▲')
