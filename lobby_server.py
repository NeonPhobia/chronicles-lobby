#!/usr/bin/env python3
"""Сервер лобби и ретранслятор для кооператива «Хроник петли».

Хранит список открытых лобби и пересылает сообщения между игроками одного лобби (WebSocket).
Игровую логику не знает: хозяин лобби - его игра ведёт мир, сервер только передаёт.

Если у игрока пропала связь (свернул игру, плохая сеть), его место держится GRACE секунд: он может
вернуться командой resume с выданным токеном, и никто не вылетает из лобби.

Запуск:  pip install websockets   затем   python lobby_server.py
Порт берётся из переменной окружения PORT (так хотят хостинги), по умолчанию 8765.
"""
import asyncio
import json
import os
import itertools
import secrets
import time

import websockets

MAX_PLAYERS = 4
GRACE = 180.0       # сколько секунд держится место игрока без связи
lobbies = {}        # id -> {"id","name","info","members":{idx: ws},"names":{idx: str},"tokens":{idx: str},"away":{idx: deadline},"started"}
where = {}          # ws -> (lobby_id, idx)
ids = itertools.count(1)


def pub(lb):
    return {"id": lb["id"], "name": lb["name"], "info": lb["info"], "count": len(lb["names"]),
            "max": MAX_PLAYERS, "started": lb["started"]}


async def send(ws, obj):
    try:
        await ws.send(json.dumps(obj))
    except Exception:
        pass


async def to_lobby(lb, obj, skip=None):
    for idx, w in list(lb["members"].items()):
        if idx != skip:
            await send(w, obj)


async def remove_member(lb, idx):
    """Окончательно убрать игрока (вышел сам или не вернулся)."""
    lb["members"].pop(idx, None)
    lb["names"].pop(idx, None)
    lb["tokens"].pop(idx, None)
    lb["away"].pop(idx, None)
    if idx == 1:
        # хозяин ушёл насовсем - лобби закрыто
        await to_lobby(lb, {"op": "closed"})
        for w in list(lb["members"].values()):
            where.pop(w, None)
        lobbies.pop(lb["id"], None)
    else:
        await to_lobby(lb, {"op": "member", "idx": idx, "joined": False})


async def leave(ws):
    """Игрок вышел из лобби сам."""
    if ws not in where:
        return
    lid, idx = where.pop(ws)
    lb = lobbies.get(lid)
    if lb is None:
        return
    lb["members"].pop(idx, None)
    await remove_member(lb, idx)


async def drop(ws):
    """Связь оборвалась: место держится GRACE секунд."""
    if ws not in where:
        return
    lid, idx = where.pop(ws)
    lb = lobbies.get(lid)
    if lb is None or lb["members"].get(idx) is not ws:
        return
    lb["members"].pop(idx, None)
    lb["away"][idx] = time.time() + GRACE
    await to_lobby(lb, {"op": "away", "idx": idx})


async def janitor():
    while True:
        await asyncio.sleep(5)
        now = time.time()
        for lb in list(lobbies.values()):
            for idx, dl in list(lb["away"].items()):
                if dl < now:
                    await remove_member(lb, idx)


async def handler(ws):
    try:
        async for raw in ws:
            try:
                m = json.loads(raw)
            except Exception:
                continue
            op = m.get("op")
            if op == "list":
                await send(ws, {"op": "lobbies", "list": [pub(l) for l in lobbies.values() if len(l["names"]) < MAX_PLAYERS]})
            elif op == "create":
                await leave(ws)
                lid = next(ids)
                name = str(m.get("name", "Герой"))[:24]
                tok = secrets.token_hex(8)
                lb = {"id": lid, "name": name, "info": str(m.get("info", ""))[:40], "members": {1: ws}, "names": {1: name},
                      "tokens": {1: tok}, "away": {}, "started": False}
                lobbies[lid] = lb
                where[ws] = (lid, 1)
                await send(ws, {"op": "created", "id": lid, "me": 1, "names": lb["names"], "token": tok})
            elif op == "join":
                await leave(ws)
                lb = lobbies.get(int(m.get("id", 0)))
                if lb is None or len(lb["names"]) >= MAX_PLAYERS:
                    await send(ws, {"op": "error", "msg": "Лобби не найдено или заполнено"})
                    continue
                idx = next(i for i in range(2, MAX_PLAYERS + 2) if i not in lb["names"])
                name = str(m.get("name", "Герой"))[:24]
                tok = secrets.token_hex(8)
                lb["members"][idx] = ws
                lb["names"][idx] = name
                lb["tokens"][idx] = tok
                where[ws] = (lb["id"], idx)
                await send(ws, {"op": "joined", "id": lb["id"], "me": idx, "names": lb["names"], "started": lb["started"], "token": tok})
                await to_lobby(lb, {"op": "member", "idx": idx, "name": name, "joined": True}, skip=idx)
            elif op == "resume":
                # вернуться на своё место после обрыва связи
                lb = lobbies.get(int(m.get("id", 0)))
                idx = int(m.get("me", 0))
                if lb is None or lb["tokens"].get(idx) != m.get("token"):
                    await send(ws, {"op": "error", "msg": "Лобби уже закрыто", "resume_failed": True})
                    continue
                old = lb["members"].get(idx)
                if old is not None and old is not ws:
                    where.pop(old, None)
                lb["members"][idx] = ws
                lb["away"].pop(idx, None)
                where[ws] = (lb["id"], idx)
                await send(ws, {"op": "resumed", "id": lb["id"], "me": idx, "names": lb["names"], "started": lb["started"]})
                await to_lobby(lb, {"op": "back", "idx": idx}, skip=idx)
            elif op == "start":
                if ws in where and where[ws][1] == 1:
                    lobbies[where[ws][0]]["started"] = True
            elif op == "leave":
                await leave(ws)
            elif op == "relay":
                if ws not in where:
                    continue
                lid, idx = where[ws]
                lb = lobbies.get(lid)
                if lb is None:
                    continue
                to = int(m.get("to", 0))
                obj = {"op": "relay", "from": idx, "t": m.get("t", ""), "d": m.get("d", "")}
                if to == 0:
                    await to_lobby(lb, obj, skip=idx)
                elif to in lb["members"]:
                    await send(lb["members"][to], obj)
    except websockets.ConnectionClosed:
        pass
    finally:
        await drop(ws)


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with websockets.serve(handler, "0.0.0.0", port, max_size=2 ** 20, ping_interval=20, ping_timeout=60):
        print("Сервер лобби запущен, порт", port, flush=True)
        asyncio.create_task(janitor())
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
