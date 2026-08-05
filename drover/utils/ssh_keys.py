import re

# 코멘트 필드는 공백 포함 자유 문자열 허용 (newline/carriage return은 별도 차단)
_SSH_KEY_RE = re.compile(
    r"^(ssh-rsa|ssh-ed25519|ecdsa-sha2-nistp256|ecdsa-sha2-nistp384|ecdsa-sha2-nistp521|sk-ssh-ed25519@openssh\.com|sk-ecdsa-sha2-nistp256@openssh\.com)"
    r"\s+[A-Za-z0-9+/=]+"
    r"(\s+.+)?$"
)


def validate_ssh_public_key(key: str) -> None:
    """SSH 공개키 형식 검증. 유효하지 않으면 ValueError 발생."""
    if "\n" in key or "\r" in key:
        raise ValueError("SSH 공개키에 개행 문자가 포함될 수 없습니다")
    if not _SSH_KEY_RE.match(key.strip()):
        raise ValueError(f"유효하지 않은 SSH 공개키 형식입니다: {key[:40]!r}...")
