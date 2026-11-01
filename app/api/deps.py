from __future__ import annotations

import hmac

from fastapi import Depends, Header, HTTPException, Request


def get_ctx(request: Request):
    return request.app.state.ctx


def require_key(request: Request, x_api_key: str | None = Header(default=None)) -> None:
    expected = request.app.state.ctx.settings.api_key
    if expected and not hmac.compare_digest(x_api_key or "", expected):
        raise HTTPException(401, "invalid or missing X-API-Key")


Auth = Depends(require_key)
