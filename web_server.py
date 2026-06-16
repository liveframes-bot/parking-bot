"""Простой HTTP-сервер для health-check (антисон Render)"""
import os
import json
from aiohttp import web
import asyncio
from threading import Thread

# Импортируем основной бот
from bot import bot, sheet, mask_fio, normalize_plate, find_by_plate

async def health_handler(request):
    """Простой endpoint для пинга"""
    return web.Response(text="OK", status=200)

async def stats_handler(request):
    """Статистика: сколько пользователей в базе"""
    try:
        records = sheet.get_all_values()
        return web.json_response({
            "status": "ok",
            "users": len(records) - 1,
            "bot": "running"
        })
    except Exception as e:
        return web.json_response({"status": "error", "msg": str(e)}, status=500)

async def search_handler(request):
    """Поиск владельца по гос.номеру через HTTP API"""
    plate = request.query.get('plate', '').strip()
    if not plate:
        return web.json_response({"error": "use ?plate=А123БВ777"}, status=400)
    result = find_by_plate(plate)
    if result:
        return web.json_response({
            "found": True,
            "plate": plate.upper(),
            "fio": mask_fio(result['fio']),
            "phone": result['phone'],
            "category": result['category']
        })
    return web.json_response({"found": False}, status=404)

def start_web():
    app = web.Application()
    app.router.add_get('/', health_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/stats', stats_handler)
    app.router.add_get('/search', search_handler)
    web.run_app(app, host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))

if __name__ == "__main__":
    start_web()
