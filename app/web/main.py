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

    @app.get("/admin", response_class=HTMLResponse)
    def admin_page(request: Request):
        return templates.TemplateResponse("admin.html", {"request": request})

    return app


app = create_app()
