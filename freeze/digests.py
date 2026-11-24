from hashlib import sha256


def content_digest(content: bytes) -> str:
    return sha256(content).hexdigest()
