import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, List
from unittest import TestCase
from unittest.mock import MagicMock, Mock

from dnastack import ServiceEndpoint
from dnastack.http.authenticators.abstract import Authenticator, AuthenticationRequired
from dnastack.http.authenticators.oauth2 import OAuth2Authenticator
from dnastack.http.authenticators.oauth2_adapter.factory import OAuth2AdapterFactory
from dnastack.http.session import HttpSession, ClientError
from dnastack.http.session_info import InMemorySessionStorage, SessionManager
from requests import Session
from pydantic import BaseModel, Field


class HandledRequest(BaseModel):
    path: str

    # Assume that only one header name can appear once per request.
    headers: Dict[str, str]


class DataCollection(BaseModel):
    handled_requests: List[HandledRequest] = Field(default_factory=list)

    def reset(self):
        self.handled_requests.clear()


class MockWebHandler(BaseHTTPRequestHandler):
    _data_collection = DataCollection()

    @classmethod
    def reset_collected_data(cls):
        """ Reset the collected data. """
        cls._data_collection.reset()

    @classmethod
    def get_collected_data(cls) -> DataCollection:
        """ Provide a copy of the collected data. """
        return cls._data_collection.copy(deep=True)

    def _collect_request_data(self):
        """ Collect the information on the incoming request """
        request_url = "localhost:8000" + self.path
        self._data_collection.handled_requests.append(
            HandledRequest(
                path=request_url,
                headers={
                    name: value
                    for name, value in self.headers.items()
                }
            )
        )

    def do_GET(self):
        self._collect_request_data()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        response = {'message': 'Test response'}
        self.wfile.write(json.dumps(response).encode('utf-8'))

    def do_POST(self):
        self._collect_request_data()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()

        response = dict(
            access_token='test_access_token',
            refresh_token='test_refresh_token',
            token_type='test_token_type',
            expires_in=60,
        )
        self.wfile.write(json.dumps(response).encode('utf-8'))


class TestHttpSession(TestCase):

    def test_submit_403_status_code(self):
        # Create a mock authenticator
        authenticator_mock = MagicMock(Authenticator)
        authenticator_mock.before_request.return_value = None
        authenticator_mock.session_id = 1

        # Create a mock response with status code 403
        response_mock = Mock()
        response_mock.status_code = 403
        response_mock.ok = False
        response_mock.text = "Test data"

        # Create a mock session
        session_mock = MagicMock(Session)
        session_mock.get.return_value = response_mock

        http_session = HttpSession(authenticators=[authenticator_mock], session=session_mock, suppress_error=False)
        with self.assertRaises(ClientError) as e:
            http_session.submit(method="get", url="http://example-url.com")
        self.assertEqual(e.exception.response.status_code, 403)

    def setUp(self):
        # Start the HTTP server
        MockWebHandler.reset_collected_data()
        self.server = HTTPServer(('localhost', 8000), MockWebHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self):
        # Stop the HTTP server
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join()

    def test_tracing_submit_function(self):
        session_storage = InMemorySessionStorage()
        session_manager = SessionManager(session_storage)
        adapter_factory = OAuth2AdapterFactory()
        session = Session()

        auth_info = dict(
            type='oauth2',
            client_id='client_id',
            client_secret='client_secret',
            grant_type='client_credentials',
            resource_url='http://localhost:8000',
            token_endpoint='http://localhost:8000',
        )

        service_endpoint = ServiceEndpoint(
            id='test_endpoint',
            adapter_type='test_adapter',
            url='http://localhost:8000',
            authentication=auth_info,
        )

        url = 'http://localhost:8000/'

        test_authenticator = OAuth2Authenticator(service_endpoint, auth_info, session_manager, adapter_factory)

        http_session = HttpSession(authenticators=[test_authenticator], session=session, suppress_error=False)
        response = http_session.submit("get", url)
        self.assertEqual(response.status_code, 200)

        self.assertEqual(response.json()['message'], 'Test response')

        collected_request_data = MockWebHandler.get_collected_data().handled_requests

        # In this test scenario, we expect to have two requests made where one is for authentication and one for
        # the actual request.
        self.assertEqual(len(collected_request_data), 2)

        first_request_data = collected_request_data[0]

        self.assertIn("X-B3-TraceId", first_request_data.headers.keys())
        self.assertIn("X-B3-SpanId", first_request_data.headers.keys())
        self.assertIsNotNone(first_request_data.path)
        self.assertIn("User-Agent", first_request_data.headers.keys())

        first_header_traceid = first_request_data.headers["X-B3-TraceId"]
        first_header_spanid = first_request_data.headers["X-B3-SpanId"]

        for item in collected_request_data[1:]:
            self.assertEqual(item.headers["X-B3-TraceId"], first_header_traceid)
            self.assertNotEqual(item.headers["X-B3-SpanId"], first_header_spanid)
            self.assertIsNotNone(item.path)
            self.assertIn("User-Agent", item.headers.keys())
