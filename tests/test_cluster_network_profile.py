"""Cluster security-group plan, administrator profile validation and tenant redaction."""

from __future__ import annotations

import pytest

from drover.api.clusters import _cluster_to_info
from drover.services import provisioner
from drover.services.resource_policy_store import RuntimeSettingValidationError, _validate_cluster_profiles

SG = "sg-cluster"
PROFILE = {
    "network_id": "net-1",
    "ssh_cidrs": ["172.30.0.50/32"],
    "api_cidrs": ["172.30.0.11/32", "172.30.0.12/32"],
    "nodeport_cidrs": ["172.30.0.11/32"],
    "http_cidrs": [],
}


def _tcp(rules, port_min, port_max=None):
    port_max = port_min if port_max is None else port_max
    return [r for r in rules if r["protocol"] == "tcp" and (r["port_range_min"], r["port_range_max"]) == (port_min, port_max)]


@pytest.mark.parametrize("cidrs", [None, ["203.0.113.0/24"], ["172.30.0.11/32"]])
def test_agents_can_always_reach_the_api_from_the_cluster_group(cidrs):
    rules = provisioner._cluster_security_group_rules(cidrs, SG)

    assert any(r.get("remote_group_id") == SG for r in _tcp(rules, 6443))


def test_unprofiled_plan_keeps_requester_cidrs_and_world_open_service_ports():
    rules = provisioner._cluster_security_group_rules(["203.0.113.0/24"], SG)

    assert {r["remote_ip_prefix"] for r in _tcp(rules, 22)} == {"203.0.113.0/24"}
    assert {r["remote_ip_prefix"] for r in _tcp(rules, 6443) if "remote_ip_prefix" in r} == {"203.0.113.0/24"}
    for ports in ((80, 80), (443, 443), (30000, 32767)):
        assert {r["remote_ip_prefix"] for r in _tcp(rules, *ports)} == {"0.0.0.0/0"}


def test_profile_plan_takes_every_source_from_the_profile_and_opens_nothing_to_the_world():
    rules = provisioner._cluster_security_group_rules(["198.51.100.0/24"], SG, PROFILE)

    assert {r["remote_ip_prefix"] for r in _tcp(rules, 22)} == {"172.30.0.50/32"}
    assert {r["remote_ip_prefix"] for r in _tcp(rules, 6443) if "remote_ip_prefix" in r} == set(PROFILE["api_cidrs"])
    assert {r["remote_ip_prefix"] for r in _tcp(rules, 30000, 32767)} == {"172.30.0.11/32"}
    assert _tcp(rules, 80) == [] and _tcp(rules, 443) == []
    assert "0.0.0.0/0" not in {r.get("remote_ip_prefix") for r in rules}
    assert any(r.get("remote_group_id") == SG for r in _tcp(rules, 6443))
    assert any(r.get("remote_group_id") == SG for r in _tcp(rules, 2379, 2380))


def test_profile_http_cidrs_open_both_web_ports_only_for_those_sources():
    rules = provisioner._cluster_security_group_rules(None, SG, {**PROFILE, "http_cidrs": ["192.0.2.0/24"]})

    assert {r["remote_ip_prefix"] for r in _tcp(rules, 80)} == {"192.0.2.0/24"}
    assert {r["remote_ip_prefix"] for r in _tcp(rules, 443)} == {"192.0.2.0/24"}


def test_valid_profiles_are_normalized_and_deduplicated():
    profiles = _validate_cluster_profiles({
        "sandbox": {
            "network_id": " net-1 ",
            "ssh_cidrs": ["172.30.0.50/32", "172.30.0.50"],
            "api_cidrs": ["172.30.0.0/16"],
            "nodeport_cidrs": ["172.30.0.11/32"],
        }
    })

    assert profiles["sandbox"] == {
        "network_id": "net-1",
        "ssh_cidrs": ["172.30.0.50/32"],
        "api_cidrs": ["172.30.0.0/16"],
        "nodeport_cidrs": ["172.30.0.11/32"],
        "http_cidrs": [],
    }


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {"Bad_Name": PROFILE},
        {"p": {**PROFILE, "extra": 1}},
        {"p": {k: v for k, v in PROFILE.items() if k != "nodeport_cidrs"}},
        {"p": {**PROFILE, "ssh_cidrs": []}},
        {"p": {**PROFILE, "api_cidrs": ["not-a-cidr"]}},
        {"p": {**PROFILE, "nodeport_cidrs": "172.30.0.11/32"}},
        {"p": {**PROFILE, "network_id": "  "}},
        {"p": {**PROFILE, "http_cidrs": ["10.0.0.0/8"] * 21}},
    ],
)
def test_invalid_profiles_are_rejected(value):
    with pytest.raises(RuntimeSettingValidationError):
        _validate_cluster_profiles(value)


def _cluster(**extra):
    return {
        "id": "c1",
        "project_id": "p1",
        "name": "n",
        "status": "ERROR",
        "status_reason": "Reconciliation failed: Required neutron security_group_rule 'abc' (abc) is missing",
        "network_id": "net-1",
        "drift_status": {
            "has_drift": True,
            "missing_count": 1,
            "missing": [{"resource_id": "abc", "reason": "security_group_rule abc"}],
            "orphans": [],
            "mismatches": [],
            "reconciled_at": "2026-11-14T00:00:00+00:00",
        },
        **extra,
    }


def test_profile_cluster_hides_network_and_rule_identifiers_from_tenants_but_not_administrators():
    cluster = _cluster(resource_policy_snapshot={"cluster_profile": {"name": "sandbox", **PROFILE}})

    tenant = _cluster_to_info(cluster)
    admin = _cluster_to_info(cluster, admin=True)

    assert tenant.network_id is None
    assert "abc" not in (tenant.status_reason or "")
    assert tenant.drift_status == {
        "has_drift": True, "missing_count": 1, "reconciled_at": "2026-11-14T00:00:00+00:00", "details_hidden": True,
    }
    assert admin.network_id == "net-1"
    assert "abc" in admin.status_reason
    assert admin.drift_status["missing"][0]["resource_id"] == "abc"


def test_unprofiled_cluster_responses_are_unchanged_for_tenants():
    cluster = _cluster(resource_policy_snapshot={"k3s.default_network": {"id": "net-1"}})

    info = _cluster_to_info(cluster)

    assert info.network_id == "net-1"
    assert "abc" in info.status_reason
    assert info.drift_status["missing"][0]["resource_id"] == "abc"


def test_request_hash_of_a_request_without_profile_matches_the_pre_profile_canonical_json():
    import hashlib
    import json

    from drover.api.clusters import compute_request_hash
    from drover.models.schemas import CreateK3sClusterRequest

    # Canonical JSON of the request model before the `profile` field existed. A replayed Idempotency-Key must
    # still match the stored hash after the upgrade.
    legacy = {
        "name": "k", "agent_count": 1, "agent_flavor_id": None, "network_id": None, "key_name": None,
        "os_type": "ubuntu", "allowed_cidrs": None, "template_id": None, "master_count": 1,
        "stampede_enabled": True,
    }
    expected = hashlib.sha256(json.dumps(legacy, sort_keys=True).encode("utf-8")).hexdigest()

    assert compute_request_hash(CreateK3sClusterRequest(name="k")) == expected
    assert compute_request_hash(CreateK3sClusterRequest(name="k", profile="sandbox")) != expected


def _event(payload):
    from drover.models.orm import DroverOperationEvent

    return DroverOperationEvent(id=1, operation_id="op", sequence=1, phase="reconcile_complete", message="m", payload_json=payload)


def test_operation_event_drift_details_are_hidden_only_when_requested():
    from drover.api.operations import _event_to_info

    payload = {"has_drift": True, "missing_count": 1, "missing": [{"resource_id": "abc"}], "orphans": [], "mismatches": []}

    hidden = _event_to_info(_event(payload), hide_drift_details=True).payload_json
    shown = _event_to_info(_event(payload)).payload_json

    assert hidden == {"has_drift": True, "missing_count": 1, "details_hidden": True}
    assert shown == payload
    assert _event_to_info(_event({"step": "security_group", "progress": 5}), hide_drift_details=True).payload_json == {
        "step": "security_group", "progress": 5,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "is_admin,snapshot,lookup_fails,expected",
    [
        (False, {"cluster_profile": {"name": "sandbox"}}, False, True),
        (True, {"cluster_profile": {"name": "sandbox"}}, False, False),
        (False, {"k3s.default_network": {"id": "net"}}, False, False),
        (False, None, True, True),
    ],
)
async def test_event_redaction_applies_to_non_admin_readers_of_profile_clusters(
    monkeypatch, is_admin, snapshot, lookup_fails, expected
):
    from types import SimpleNamespace

    from drover.api import operations as operations_api

    async def get_cluster(project_id, cluster_id):
        if lookup_fails:
            raise RuntimeError("store unavailable")
        return {"resource_policy_snapshot": snapshot}

    monkeypatch.setattr(operations_api.store, "get_cluster", get_cluster)
    monkeypatch.setattr(operations_api, "authorize", lambda *args, **kwargs: is_admin)
    op = SimpleNamespace(id="op", project_id="p", cluster_id="c")

    assert await operations_api._hides_profile_details(op, {}) is expected
