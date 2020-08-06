import os
from typing import Dict

from fluid.http import HttpClient

from .repo import GithubRepo


class Github(HttpClient):
    def __init__(self, token=None) -> None:
        self.token = token or get_token()

    @property
    def api_url(self):
        return "https://api.github.com"

    @property
    def uploads_url(self):
        return "https://uploads.github.com"

    def __repr__(self):
        return self.api_url

    __str__ = __repr__

    def repo(self, repo_name: str) -> GithubRepo:
        return GithubRepo(self, repo_name)

    def default_headers(self) -> Dict[str, str]:
        headers = super().default_headers()
        if self.token:
            headers["authorization"] = f"token {self.token}"
        return headers


def get_token() -> str:
    token = os.getenv("GITHUB_SECRET_TOKEN") or os.getenv("GITHUB_TOKEN")
    return token
