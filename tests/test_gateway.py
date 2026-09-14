"""
Contract tests for the gateway.

The nginx config and the Compose file make promises to each other - Grafana
serves from a sub-path, Prometheus has its prefix stripped - and nginx has a few
rules that fail silently rather than loudly. Each test here pins one of those.
"""

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
NGINX = ROOT / "gateway" / "nginx.conf"
COMPOSE = ROOT / "docker-compose.yml"


def nginx() -> str:
    """The config with comments stripped, so prose can't satisfy a regex."""
    return re.sub(r"#[^\n]*", "", NGINX.read_text(encoding="utf-8"))


def location_bodies() -> dict[str, str]:
    conf = nginx()
    bodies = {}
    for match in re.finditer(r"location\s+([^{]+?)\s*\{", conf):
        depth, i = 1, match.end()
        while depth:
            if conf[i] == "{":
                depth += 1
            elif conf[i] == "}":
                depth -= 1
            i += 1
        bodies[match.group(1).strip()] = conf[match.end(): i - 1]
    return bodies


def service(name: str) -> str:
    text = COMPOSE.read_text(encoding="utf-8")
    match = re.search(
        rf"^  {re.escape(name)}:\n(.*?)(?=^  [\w-]+:\n|^\S)", text, re.S | re.M
    )
    assert match, f"service {name} not found in docker-compose.yml"
    return match.group(1)


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

def test_every_ui_has_a_route():
    locations = location_bodies()
    for path in ("/grafana/", "/prometheus/", "/ws", "/"):
        assert path in locations, f"no location for {path}"


def test_api_metrics_are_not_exposed_on_the_front_door():
    """Prometheus scrapes api:8000 internally; the public gateway hides /metrics."""
    body = location_bodies()["= /metrics"]
    assert "return 404" in body


def test_upstreams_are_resolved_per_request():
    """
    A literal `proxy_pass http://grafana:3000` is resolved when nginx starts, so
    the gateway would refuse to boot whenever the obs profile is not running.
    A resolver plus variables defers the lookup to request time.
    """
    conf = nginx()
    assert "resolver 127.0.0.11" in conf
    for body in location_bodies().values():
        for target in re.findall(r"proxy_pass\s+([^;]+);", body):
            assert "$" in target, f"proxy_pass {target} is resolved at startup"


def test_missing_obs_profile_gets_an_explanation_not_a_bare_502():
    locations = location_bodies()
    for path in ("/grafana/", "/prometheus/"):
        assert "@obs_not_running" in locations[path], path
    assert "--profile obs" in locations["@obs_not_running"]


# ---------------------------------------------------------------------------
# The nginx footgun
# ---------------------------------------------------------------------------

def test_proxy_headers_are_set_only_at_server_level():
    """
    nginx does not merge proxy_set_header across levels: a location that sets
    even one header silently drops every server-level one, including Host, which
    breaks Grafana's origin check with no error in the config.
    """
    for path, body in location_bodies().items():
        assert "proxy_set_header" not in body, f"location {path} would drop server headers"


def test_websocket_upgrade_is_forwarded():
    conf = nginx()
    assert "map $http_upgrade $connection_upgrade" in conf
    assert "proxy_set_header Upgrade" in conf
    assert "proxy_set_header Connection        $connection_upgrade" in conf
    assert "proxy_http_version 1.1" in conf


# ---------------------------------------------------------------------------
# nginx and Compose must agree about paths
# ---------------------------------------------------------------------------

def test_grafana_serves_from_the_sub_path_the_gateway_passes_through():
    """Grafana keeps its /grafana/ prefix, so nginx must not strip it."""
    grafana = service("grafana")
    assert 'GF_SERVER_SERVE_FROM_SUB_PATH: "true"' in grafana
    assert re.search(r"GF_SERVER_ROOT_URL:\s*\S+/grafana/", grafana)
    assert "rewrite" not in location_bodies()["/grafana/"]


def test_prometheus_prefix_is_stripped_and_prometheus_serves_from_root():
    """
    The two halves of one decision: nginx strips /prometheus/, so Prometheus
    must serve from / while generating links under /prometheus/. Change either
    alone and the UI loads with every asset path broken.
    """
    assert "rewrite ^/prometheus/(.*)$ /$1 break" in location_bodies()["/prometheus/"]
    prometheus = service("prometheus")
    assert "--web.route-prefix=/" in prometheus
    assert re.search(r"--web\.external-url=\S+/prometheus/", prometheus)


def test_only_the_gateway_publishes_a_web_port():
    """One front door: the app, Grafana and Prometheus are reachable only through it."""
    for name in ("api", "grafana", "prometheus"):
        assert "ports:" not in service(name), f"{name} publishes its own port"
    assert '"8000:80"' in service("gateway")
