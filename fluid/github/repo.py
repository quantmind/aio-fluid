from base64 import b64encode
from typing import TYPE_CHECKING, Dict, List

from nacl import encoding, public

if TYPE_CHECKING:
    from .client import Github


class GithubRepo:
    def __init__(self, cli: "Github", repo_name: str) -> None:
        self.cli: "Github" = cli
        self.repo_name: str = repo_name
        self.branches: GithubBranches = GithubBranches(self, "branches")
        self.deployments: GithubDeployments = GithubDeployments(self, "deployments")
        self.secrets: GithubSecrets = GithubSecrets(self, "actions/secrets")

    @property
    def url(self) -> str:
        return f"{self.cli.api_url}/repos/{self.repo_name}"

    def __repr__(self) -> str:
        return self.url

    __str__ = __repr__

    async def info(self) -> Dict:
        return await self.cli.get(self.url)

    async def flat_info(self, raw: bool = False) -> Dict:
        info = await self.info()
        branch = await self.branches.get(info["default_branch"])
        if raw:
            info["default_branch"] = branch
            return info
        return {
            "name": info["full_name"],
            "delete branch on merge": info["delete_branch_on_merge"],
            "default branch": branch["name"],
            "default protected": branch["protected"],
            "default branch sha": branch["commit"]["sha"],
            "default branch date": branch["commit"]["commit"]["author"]["date"],
        }


class GithubRepoComponent:
    def __init__(self, repo: GithubRepo, path: str) -> None:
        self.repo: GithubRepo = repo
        self.path: str = path

    @property
    def url(self) -> str:
        return f"{self.repo.url}/{self.path}"

    @property
    def cli(self) -> "Github":
        return self.repo.cli

    def __repr__(self):
        return self.url

    __str__ = __repr__

    async def get_list(self) -> List[Dict]:
        return await self.cli.get(self.url)


class GithubBranches(GithubRepoComponent):
    async def get(self, branch: str) -> Dict:
        """Branch data"""
        return await self.cli.get(f"{self.url}/{branch}")


class GithubDeployments(GithubRepoComponent):
    special_accept = dict(
        in_progress="application/vnd.github.flash-preview+json",
        queued="application/vnd.github.flash-preview+json",
        inactive="application/vnd.github.ant-man-preview+json",
    )

    async def get(self, deployment_id: str) -> Dict:
        """Get a existing deployment"""
        return await self.cli.get(f"{self.url}/{deployment_id}")

    async def create(self, ref: str, auto_merge: bool = False, **params) -> Dict:
        """Create a new deployment"""
        return await self.cli.post(
            self.url,
            json=dict(ref=ref, auto_merge=auto_merge, **params),
        )

    async def update(self, deployment_id: str, **params) -> Dict:
        accept = self.special_accept.get(params.get("state"))
        headers = {}
        if accept:
            headers["accept"] = accept
        return await self.cli.post(
            f"{self.url}/{deployment_id}/statuses", json=params, headers=headers
        )


class GithubSecrets(GithubRepoComponent):
    public_key: Dict[str, str] = ""

    async def update(self, name: str, value: str) -> Dict:
        """Branch data"""
        public_key = await self.get_public_key()
        encrypted_value = encrypt(public_key["key"], value)
        return await self.cli.put(
            f"{self.url}/{name}",
            json=dict(encrypted_value=encrypted_value, key_id=public_key["key_id"]),
        )

    async def get_public_key(self) -> str:
        if not self.public_key:
            self.public_key = await self.cli.get(f"{self.url}/public-key")
        return self.public_key


def encrypt(public_key: str, secret_value: str) -> str:
    """Encrypt a Unicode string using the public key."""
    public_key = public.PublicKey(public_key.encode("utf-8"), encoding.Base64Encoder())
    sealed_box = public.SealedBox(public_key)
    encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
    return b64encode(encrypted).decode("utf-8")
