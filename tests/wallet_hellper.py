from base64 import b64encode
from typing import Optional, List
from urllib.parse import urljoin

from pydantic import BaseModel

from dnastack.http.session import HttpSession


class TestUser(BaseModel):
    id: str
    email: str
    personalAccessToken: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    id_token: Optional[str]
    refresh_token: Optional[str]
    expires_in: str


class Principal(BaseModel):
    type: str
    email: Optional[str]


class Resource(BaseModel):
    uri: str


class Statement(BaseModel):
    actions: List[str]
    principals: List[Principal]
    resources: List[Resource]


class Policy(BaseModel):
    id: str
    version: Optional[str]
    statements: List[Statement]
    tags: Optional[List[str]]


class WalletHelper:
    def __init__(self, wallet_base_uri: str, client_id: str, client_secret: str):
        self.__wallet_base_uri = wallet_base_uri
        self.__admin_client_id = client_id
        self.__admin_client_secret = client_secret
        self.__wallet_resource = f'{wallet_base_uri}/'

    @staticmethod
    def _create_http_session(suppress_error: bool = False, no_auth: bool = False) -> HttpSession:
        """Create HTTP session wrapper"""
        session = HttpSession(suppress_error=suppress_error, enable_auth=(not no_auth))
        return session

    def _basic_auth(self) -> str:
        token = b64encode(f"{self.__admin_client_id}:{self.__admin_client_secret}".encode('utf-8')).decode("ascii")
        return f'Basic {token}'

    def _bearer_auth(self) -> str:
        return f'Bearer {self.get_access_token(self.__wallet_resource)}'

    def get_access_token(self, resource: str, scope: Optional[str] = '') -> str:
        with self._create_http_session() as session:
            response = session.post(urljoin(self.__wallet_base_uri,
                                            '/oauth/token'),
                                    params=dict(grant_type='client_credentials',
                                                resource=resource,
                                                scope=scope),
                                    headers={'Authorization': self._basic_auth()})
            return TokenResponse(**response.json()).access_token

    def login_to_app(self, app_base_url: str, email: str, personal_access_token: str) -> HttpSession:
        session = self._create_http_session()
        session.get(urljoin(self.__wallet_base_uri, f'/login/token'),
                    params=dict(email=email, token=personal_access_token))
        response = session.get(urljoin(app_base_url, f'/oauth/login'), allow_redirects=False)
        first_redirect_url_without_prompt = response.headers['Location'].replace('&prompt=select_account', '')
        session.get(first_redirect_url_without_prompt)
        return session

    def create_test_user(self, username: str) -> TestUser:
        with self._create_http_session() as session:
            response = session.post(urljoin(self.__wallet_base_uri, f'/test/users?username={username}'),
                                    headers={'Authorization': self._bearer_auth()})
            return TestUser(**response.json())

    def delete_test_user(self, email: str) -> None:
        with self._create_http_session() as session:
            session.delete(urljoin(self.__wallet_base_uri, f'/test/users/{email}'),
                           headers={'Authorization': self._bearer_auth()})

    def create_access_policy(self, policy: Policy) -> Policy:
        with self._create_http_session() as session:
            response = session.post(urljoin(self.__wallet_base_uri, f'/policies'),
                                    json=policy.dict(),
                                    headers={'Authorization': self._bearer_auth()})
            created_policy = Policy(**response.json())
            created_policy.version = response.headers['ETag'].replace('\"', '')
            return created_policy

    def delete_access_policy(self, policy_id: str, policy_version: str) -> None:
        with self._create_http_session() as session:
            session.delete(urljoin(self.__wallet_base_uri, f'/policies/{policy_id}'),
                           headers={'Authorization': self._bearer_auth(), 'If-Match': policy_version})
