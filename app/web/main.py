"""FastAPI app: admin UI and API."""

from pathlib import Path

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.web.routers.admin import router as admin_router

# Paths relative to this file (app/web/main.py)
_WEB_DIR = Path(__file__).resolve().parent
_TEMPLATES_DIR = _WEB_DIR / "templates"
_STATIC_DIR = _WEB_DIR / "static"


def create_app():
    from fastapi import FastAPI
    app = FastAPI(title="Arbitrage Admin")
    app.include_router(admin_router)
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
                    "page": "Crawler",
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

    @app.get("/admin", response_class=HTMLResponse)
    def admin_page(request: Request):
        return templates.TemplateResponse(
            "admin.html",
            {
                "request": request,
                "app_name": "Arbitrage",
                "side_menu": _SIDE_MENU,
                "selected_menu": "crawler",
                "current_page": "Crawler",
            },
        )

    return app


app = create_app()
