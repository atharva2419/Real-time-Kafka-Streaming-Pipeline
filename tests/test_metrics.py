"""
Tests for the metrics surface.

These are contract tests, not behaviour tests: a dashboard panel and an alert
rule both hard-code a metric name, so a rename that slips through breaks a
graph silently rather than failing a build. Anything referenced from
observability/ is asserted here.
"""

import json
import pathlib
import re

import pytest
from prometheus_client import REGISTRY, generate_latest

from pipeline import metrics

OBSERVABILITY = pathlib.Path(__file__).resolve().parents[1] / "observability"


def exposed() -> str:
    return generate_latest(REGISTRY).decode()


# ---------------------------------------------------------------------------
# Names and labels
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name",
    [
        "wiki_events_produced_total",
        "wiki_events_dropped_total",
        "wiki_produce_errors_total",
        "wiki_sse_reconnects_total",
        "wiki_events_consumed_total",
        "wiki_windows_emitted_total",
        "wiki_late_events_total",
        "wiki_sink_errors_total",
    ],
)
def test_counters_carry_the_total_suffix(name):
    """prometheus_client appends `_total`, so the declared names omit it."""
    metrics.events_produced.inc(0)
    metrics.events_dropped.labels(reason="invalid").inc(0)
    metrics.produce_errors.inc(0)
    metrics.sse_reconnects.labels(reason="sse").inc(0)
    metrics.events_consumed.labels(partition="0").inc(0)
    metrics.windows_emitted.labels(partition="0").inc(0)
    metrics.late_events.labels(partition="0").inc(0)
    metrics.sink_errors.inc(0)

    assert name in exposed()


@pytest.mark.parametrize(
    "name",
    [
        "wiki_open_windows",
        "wiki_assigned_partitions",
        "wiki_history_windows",
        "wiki_api_ws_clients",
    ],
)
def test_gauges_are_exposed(name):
    metrics.open_windows.set(0)
    metrics.assigned_partitions.set(0)
    metrics.history_windows.set(0)
    metrics.ws_clients.set(0)

    assert name in exposed()


def test_histograms_expose_buckets_for_quantiles():
    """histogram_quantile() needs the _bucket series; the dashboard uses both."""
    metrics.window_emit_delay.observe(1.0)
    metrics.sink_write_seconds.observe(0.001)
    metrics.redis_read_seconds.labels(op="latest").observe(0.001)

    text = exposed()
    for name in (
        "wiki_window_emit_delay_seconds_bucket",
        "wiki_sink_write_seconds_bucket",
        "wiki_api_redis_read_seconds_bucket",
    ):
        assert name in text


def test_partition_is_the_only_high_churn_label():
    """
    Cardinality guard. `wiki` has hundreds of values and would multiply every
    series by that; it belongs in Redis, where it already is.
    """
    for metric in (metrics.events_consumed, metrics.windows_emitted, metrics.late_events):
        assert metric._labelnames == ("partition",)
    assert "wiki" not in metrics.events_dropped._labelnames


def test_emit_delay_buckets_bracket_the_freshness_target():
    """The 2.5s target has to fall on a bucket edge or the SLO can't be read off."""
    assert 2.5 in metrics.window_emit_delay._upper_bounds


# ---------------------------------------------------------------------------
# The dashboard and alerts must only reference metrics that exist
# ---------------------------------------------------------------------------

def declared_metric_names() -> set[str]:
    names = set()
    for metric in REGISTRY.collect():
        for sample in metric.samples:
            names.add(sample.name)
            # Counters expose `_total`; the base name is what rules may use.
            for suffix in ("_total", "_bucket", "_count", "_sum", "_created"):
                if sample.name.endswith(suffix):
                    names.add(sample.name[: -len(suffix)])
    return names


def referenced_wiki_metrics(text: str) -> set[str]:
    return set(re.findall(r"\bwiki_[a-z_]+", text))


def test_dashboard_only_references_real_metrics():
    metrics.window_emit_delay.observe(1.0)
    metrics.sink_write_seconds.observe(0.001)
    metrics.redis_read_seconds.labels(op="latest").observe(0.001)
    metrics.events_dropped.labels(reason="invalid").inc(0)
    metrics.sse_reconnects.labels(reason="sse").inc(0)
    for metric in (metrics.events_consumed, metrics.windows_emitted, metrics.late_events):
        metric.labels(partition="0").inc(0)
    metrics.watermark_lag_seconds.labels(partition="0").set(0)

    dashboard = json.loads(
        (OBSERVABILITY / "grafana" / "dashboards" / "wiki-stream.json").read_text(
            encoding="utf-8"
        )
    )
    exprs = " ".join(
        target["expr"]
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
    )

    unknown = referenced_wiki_metrics(exprs) - declared_metric_names()
    assert not unknown, f"dashboard references undefined metrics: {sorted(unknown)}"


def test_alert_rules_only_reference_real_metrics():
    rules = (OBSERVABILITY / "alerts.yml").read_text(encoding="utf-8")
    unknown = referenced_wiki_metrics(rules) - declared_metric_names()
    assert not unknown, f"alerts reference undefined metrics: {sorted(unknown)}"


def test_dashboard_panels_are_well_formed():
    dashboard = json.loads(
        (OBSERVABILITY / "grafana" / "dashboards" / "wiki-stream.json").read_text(
            encoding="utf-8"
        )
    )
    assert dashboard["uid"] and dashboard["title"]

    ids = [panel["id"] for panel in dashboard["panels"]]
    assert len(ids) == len(set(ids)), "duplicate panel ids"

    for panel in dashboard["panels"]:
        assert panel["datasource"]["uid"] == "prometheus", panel["title"]
        assert panel["gridPos"]["w"] <= 24
        # Every series-bearing panel needs a legend unless it has one series.
        if panel["type"] == "timeseries" and len(panel["targets"]) > 1:
            assert panel["options"]["legend"]["showLegend"], panel["title"]


def test_scrape_interval_matches_the_window_size():
    """
    A scrape slower than the window would alias per-window signals. Counters
    make rate() exact regardless, but the gauges are sampled.
    """
    from pipeline import config

    prometheus_yml = (OBSERVABILITY / "prometheus.yml").read_text(encoding="utf-8")
    interval = re.search(r"scrape_interval:\s*(\d+)s", prometheus_yml)
    assert interval is not None
    assert int(interval.group(1)) <= config.WINDOW_SECONDS


# ---------------------------------------------------------------------------
# Alert rules that can actually fire
# ---------------------------------------------------------------------------

def alert_expressions() -> dict[str, str]:
    text = (OBSERVABILITY / "alerts.yml").read_text(encoding="utf-8")
    return dict(re.findall(r"- alert:\s*(\w+)\s*\n\s*expr:\s*(.+)", text))


def test_a_dead_process_is_covered_by_an_up_rule():
    """
    Regression, found by freezing the aggregator for 250 seconds.

    `sum(rate(x[1m])) == 0` reads like "nothing is happening" but cannot match a
    dead process: it publishes no series, the expression returns an empty
    vector, and there is nothing for `== 0` to compare. The stall rule stayed
    inactive for the whole outage. `up == 0` is what catches it.
    """
    assert any(expr.strip() == "up == 0" for expr in alert_expressions().values())


def test_rate_zero_rules_carry_an_absent_arm():
    """Same trap: a `rate(...) == 0` rule needs absent() to cover a vanished series."""
    for name, expr in alert_expressions().items():
        if "rate(" in expr and "== 0" in expr:
            assert "absent(" in expr, f"{name} cannot fire when its series is gone"


def test_every_alert_has_a_severity():
    text = (OBSERVABILITY / "alerts.yml").read_text(encoding="utf-8")
    assert text.count("- alert:") == text.count("severity:")
