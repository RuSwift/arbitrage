"""FastAPI app: admin UI and API."""

from pathlib import Path

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.web.dependencies import get_current_admin_from_request
from app.web.routers.admin import router as admin_router
from app.web.routers.auth import router as auth_router

# Paths relative to this file (app/web/main.py)
_WEB_DIR = Path(__file__).resolve().parent
_TEMPLATES_DIR = _WEB_DIR / "templates"
_STATIC_DIR = _WEB_DIR / "static"


def create_app():
    from fastapi import FastAPI
    app = FastAPI(title="Arbitrage Admin")
    app.include_router(admin_router)
    app.include_router(auth_router)
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")
    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

    # Side menu for admin panel (garantex-style: id, href, icon_class, label, sub, page)
    _SIDE_MENU = [
        {
            "header": "Инструменты",
            "items": [
                {
                    "id": "crawler",
                    "href": "/admin",
                    "icon_class": "bi bi-diagram-3",
                    "label": "Crawler",
                    "sub": [],
                    "page": "Crawler2",
                },
                {
                    "id": "tokens",
                    "href": "/admin/tokens",
                    "icon_class": "bi bi-currency-bitcoin",
                    "label": "Токены",
                    "sub": [],
                    "page": "Tokens",
                },
                {
                    "id": "configs",
                    "href": "/admin/configs",
                    "icon_class": "bi bi-gear",
                    "label": "Конфиги",
                    "sub": [],
                    "page": "Configs",
                },
                {
                    "id": "bookdepth",
                    "href": "/bookdepth",
                    "icon_class": "bi bi-graph-up",
                    "label": "Book Depth",
                    "sub": [],
                    "page": None,
                },
            ],
        },
    ]

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request):
        return templates.TemplateResponse("login.html", {"request": request})

    @app.get("/admin", response_class=HTMLResponse)
    async def admin_page(request: Request):
        user = await get_current_admin_from_request(request)
        if not user:
            return RedirectResponse(url="/login", status_code=302)
        return templates.TemplateResponse(
            "admin.html",
            {
                "request": request,
                "app_name": "Arbitrage",
                "side_menu": _SIDE_MENU,
                "selected_menu": "crawler",
                "current_page": "Crawler2",
                "user_login": user.sub,
            },
        )


    return app


app = create_app()
