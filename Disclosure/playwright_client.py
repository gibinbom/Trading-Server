# playwright_client.py
from __future__ import annotations

import os
import random
import logging
from dataclasses import dataclass, field
from contextlib import contextmanager
from typing import Any, Iterator, List, Optional, Sequence, Dict

try:
    from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext, Playwright
    _PLAYWRIGHT_IMPORT_ERROR = None
except Exception as exc:
    sync_playwright = None
    Page = Browser = BrowserContext = Playwright = Any
    _PLAYWRIGHT_IMPORT_ERROR = exc

log = logging.getLogger("disclosure.playwright")


DEFAULT_USER_AGENTS: List[str] = [
    # (너무 공격적으로 "우회" 목적이 아니라) 렌더링/호환성 분산용 정도로만
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


def _split_csv(s: Optional[str]) -> List[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _env_bool(key: str, default: str = "0") -> bool:
    return os.getenv(key, default) in ("1", "true", "True", "YES", "yes")


def _in_docker() -> bool:
    """
    Docker/컨테이너 환경 감지:
    - /.dockerenv 존재
    - /proc/1/cgroup 에 docker/containerd/kubepods 흔적
    """
    try:
        if os.path.exists("/.dockerenv"):
            return True
        with open("/proc/1/cgroup", "rt") as f:
            s = f.read()
        return ("docker" in s) or ("containerd" in s) or ("kubepods" in s)
    except Exception:
        return False


@dataclass(frozen=True)
class PlaywrightClientConfig:
    headless: bool = True
    slow_mo_ms: int = 0

    # navigation defaults
    nav_timeout_ms: int = 15000
    wait_until: str = "domcontentloaded"  # networkidle은 사이트마다 hang 가능

    # UA
    user_agents: Sequence[str] = field(default_factory=lambda: list(DEFAULT_USER_AGENTS))
    rotate_user_agent: bool = True

    # speed / stability
    block_resources: bool = True
    block_resource_types: Sequence[str] = field(default_factory=lambda: ("image", "font", "media", "stylesheet"))

    # optional
    proxy_server: Optional[str] = None
    locale: str = "ko-KR"
    timezone_id: str = "Asia/Seoul"

    # viewport
    # headless에서는 고정 뷰포트가 더 안정적임
    viewport: Optional[Dict[str, int]] = field(default_factory=lambda: {"width": 1280, "height": 720})

    # ✅ 추가: chromium launch args (Ubuntu/Docker/Root 안정화 플래그 등)
    launch_args: Sequence[str] = field(default_factory=list)


class PlaywrightClient:
    """
    - playwright/browser는 프로세스 생애 동안 1번만 띄우고 재사용
    - page() 호출 시마다 "새 context + 새 page"를 만들어 UA/route를 안전하게 적용 후 사용
    - 호출 끝나면 page/context를 닫아 누수/상태오염 방지
    """

    def __init__(self, cfg: PlaywrightClientConfig):
        self.cfg = cfg
        self._pw: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._ua_idx: int = 0

    # ---------- factory ----------
    @classmethod
    def from_settings(
        cls,
        settings: Any,
        *,
        headless: Optional[bool] = None,
        slow_mo_ms: Optional[int] = None,
        nav_timeout_ms: Optional[int] = None,
        wait_until: Optional[str] = None,
        user_agents: Optional[Sequence[str]] = None,
        rotate_user_agent: Optional[bool] = None,
        block_resources: Optional[bool] = None,
        block_resource_types: Optional[Sequence[str]] = None,
        proxy_server: Optional[str] = None,
        viewport: Optional[Dict[str, int]] = None,
        launch_args: Optional[Sequence[str]] = None,  # ✅ 추가
        **kwargs,  # ✅ 앞으로 engine에서 인자 추가해도 안 터지게
    ) -> "PlaywrightClient":
        """
        settings에서 기본값을 읽고, 명시 인자(overrides)가 있으면 덮어쓴다.
        """
        # settings 기반 기본
        cfg = PlaywrightClientConfig(
            headless=bool(getattr(settings, "PLAYWRIGHT_HEADLESS", True)),
            slow_mo_ms=int(getattr(settings, "PLAYWRIGHT_SLOWMO_MS", 0)),
            nav_timeout_ms=int(getattr(settings, "PLAYWRIGHT_NAV_TIMEOUT_MS", 15000)),
            wait_until=str(getattr(settings, "PLAYWRIGHT_WAIT_UNTIL", "domcontentloaded")),
            user_agents=_split_csv(getattr(settings, "PLAYWRIGHT_USER_AGENTS", None)) or list(DEFAULT_USER_AGENTS),
            rotate_user_agent=bool(getattr(settings, "PLAYWRIGHT_ROTATE_UA", True)),
            block_resources=bool(getattr(settings, "PLAYWRIGHT_BLOCK_RESOURCES", True)),
            block_resource_types=_split_csv(getattr(settings, "PLAYWRIGHT_BLOCK_TYPES", None))
            or ("image", "font", "media", "stylesheet"),
            proxy_server=getattr(settings, "PLAYWRIGHT_PROXY", None),
            locale=str(getattr(settings, "PLAYWRIGHT_LOCALE", "ko-KR")),
            timezone_id=str(getattr(settings, "PLAYWRIGHT_TIMEZONE", "Asia/Seoul")),
            viewport=getattr(settings, "PLAYWRIGHT_VIEWPORT", {"width": 1280, "height": 720}),
            launch_args=_split_csv(getattr(settings, "PLAYWRIGHT_LAUNCH_ARGS", None)) or [],
        )

        # overrides
        if headless is not None:
            cfg = dataclass_replace(cfg, headless=bool(headless))
        if slow_mo_ms is not None:
            cfg = dataclass_replace(cfg, slow_mo_ms=int(slow_mo_ms))
        if nav_timeout_ms is not None:
            cfg = dataclass_replace(cfg, nav_timeout_ms=int(nav_timeout_ms))
        if wait_until is not None:
            cfg = dataclass_replace(cfg, wait_until=str(wait_until))
        if user_agents is not None:
            cfg = dataclass_replace(cfg, user_agents=list(user_agents))
        if rotate_user_agent is not None:
            cfg = dataclass_replace(cfg, rotate_user_agent=bool(rotate_user_agent))
        if block_resources is not None:
            cfg = dataclass_replace(cfg, block_resources=bool(block_resources))
        if block_resource_types is not None:
            cfg = dataclass_replace(cfg, block_resource_types=list(block_resource_types))
        if proxy_server is not None:
            cfg = dataclass_replace(cfg, proxy_server=proxy_server)
        if viewport is not None:
            cfg = dataclass_replace(cfg, viewport=viewport)
        if launch_args is not None:
            cfg = dataclass_replace(cfg, launch_args=list(launch_args))

        return cls(cfg)

    # ---------- lifecycle ----------
    def _start(self) -> None:
        if self._pw is not None:
            return
        if sync_playwright is None:
            raise RuntimeError(f"playwright is unavailable: {_PLAYWRIGHT_IMPORT_ERROR}")
        self._pw = sync_playwright().start()

        # ✅ base args
        args: List[str] = list(self.cfg.launch_args or [])

        # ✅ Root/Docker에서 sandbox 이슈 방지
        is_root = False
        try:
            is_root = (os.geteuid() == 0)
        except Exception:
            pass

        if is_root or _env_bool("PLAYWRIGHT_NO_SANDBOX", "0"):
            for a in ("--no-sandbox", "--disable-setuid-sandbox"):
                if a not in args:
                    args.append(a)

        # ✅ Docker에서 /dev/shm 작으면 크래시 방지
        # (가능하면 docker run --shm-size=1g 권장)
        if _in_docker() or _env_bool("PLAYWRIGHT_DISABLE_DEV_SHM", "1"):
            if "--disable-dev-shm-usage" not in args:
                args.append("--disable-dev-shm-usage")

        launch_kwargs: Dict[str, Any] = {
            "headless": self.cfg.headless,
            "slow_mo": self.cfg.slow_mo_ms,
        }
        if args:
            launch_kwargs["args"] = args

        if self.cfg.proxy_server:
            launch_kwargs["proxy"] = {"server": self.cfg.proxy_server}

        self._browser = self._pw.chromium.launch(**launch_kwargs)
        log.info(
            "[PW] started headless=%s slowmo=%sms proxy=%s block=%s args=%s",
            self.cfg.headless,
            self.cfg.slow_mo_ms,
            bool(self.cfg.proxy_server),
            self.cfg.block_resources,
            args,
        )

    def close(self) -> None:
        try:
            if self._browser is not None:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._pw is not None:
                self._pw.stop()
        except Exception:
            pass
        self._browser = None
        self._pw = None

    # ---------- UA ----------
    def _pick_ua(self) -> str:
        uas = list(self.cfg.user_agents) if self.cfg.user_agents else list(DEFAULT_USER_AGENTS)
        if not uas:
            return DEFAULT_USER_AGENTS[0]

        if not self.cfg.rotate_user_agent:
            return uas[0]

        # round-robin + 약간 랜덤 섞기
        self._ua_idx = (self._ua_idx + 1) % len(uas)
        if random.random() < 0.15:
            return random.choice(uas)
        return uas[self._ua_idx]

    # ---------- routing ----------
    def _route_handler(self, route, request) -> None:
        try:
            rtype = request.resource_type
            if self.cfg.block_resources and rtype in set(self.cfg.block_resource_types):
                return route.abort()
            return route.continue_()
        except Exception:
            try:
                return route.continue_()
            except Exception:
                return

    # ---------- page context manager ----------
    @contextmanager
    def page(self) -> Iterator[Page]:
        if self._browser is None:
            self._start()

        assert self._browser is not None

        ua = self._pick_ua()

        context_kwargs: Dict[str, Any] = {
            "user_agent": ua,
            "locale": self.cfg.locale,
            "timezone_id": self.cfg.timezone_id,
        }
        # headless 안정성 위해 viewport 지정(원하면 None으로 끌 수 있음)
        if self.cfg.viewport is not None:
            context_kwargs["viewport"] = self.cfg.viewport

        ctx: BrowserContext = self._browser.new_context(**context_kwargs)

        # 리소스 차단은 context 단에서 거는 게 안정적
        if self.cfg.block_resources:
            try:
                ctx.route("**/*", self._route_handler)
            except Exception:
                pass

        page = ctx.new_page()
        page.set_default_timeout(self.cfg.nav_timeout_ms)
        page.set_default_navigation_timeout(self.cfg.nav_timeout_ms)

        try:
            yield page
        finally:
            try:
                page.close()
            except Exception:
                pass
            try:
                ctx.close()
            except Exception:
                pass


def dataclass_replace(obj: Any, **changes: Any) -> Any:
    """
    frozen dataclass를 안전하게 replace
    """
    data = obj.__dict__.copy()
    data.update(changes)
    return obj.__class__(**data)
