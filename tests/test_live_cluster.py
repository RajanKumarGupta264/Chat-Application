"""Live Multi-Process ASGI Worker End-to-End WebSocket Network Test."""

import asyncio
import json
import socket
import time
import pytest
import uvicorn
import websockets
import fakeredis.aioredis
from fakeredis import FakeServer

from app.config import Settings
from app.main import create_app
from app.schemas import EventType


class BackgroundServer:
    """Uvicorn server running in an asyncio task on a dedicated local port."""

    def __init__(self, app, host: str = "127.0.0.1", port: int = 8000):
        self.app = app
        self.host = host
        self.port = port
        self.config = uvicorn.Config(
            app=self.app,
            host=self.host,
            port=self.port,
            log_level="warning",
            loop="asyncio",
        )
        self.server = uvicorn.Server(config=self.config)
        self.task = None

    async def start(self):
        self.task = asyncio.create_task(self.server.serve())
        # Wait until server is listening
        for _ in range(50):
            if self.server.started:
                break
            await asyncio.sleep(0.1)

    async def stop(self):
        self.server.should_exit = True
        if self.task:
            await self.task


async def drain_async_handshake(ws):
    """Consume initial frames (welcome, join, history)."""
    frames = []
    for _ in range(3):
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            frames.append(json.loads(raw))
        except Exception:
            break
    return frames


@pytest.mark.asyncio
async def test_live_network_cross_worker_websockets():
    """Verify live cross-worker WebSocket communication over real TCP ports."""
    shared_server = FakeServer()

    # Worker 1 on port 8101
    redis1 = fakeredis.aioredis.FakeRedis(server=shared_server, decode_responses=True)
    settings1 = Settings(WORKER_ID="worker-live-1", PORT=8101)
    app1 = create_app(settings=settings1, redis_client=redis1)
    server1 = BackgroundServer(app1, host="127.0.0.1", port=8101)

    # Worker 2 on port 8102
    redis2 = fakeredis.aioredis.FakeRedis(server=shared_server, decode_responses=True)
    settings2 = Settings(WORKER_ID="worker-live-2", PORT=8102)
    app2 = create_app(settings=settings2, redis_client=redis2)
    server2 = BackgroundServer(app2, host="127.0.0.1", port=8102)

    await server1.start()
    await server2.start()

    try:
        uri1 = "ws://127.0.0.1:8101/ws/live-room/alice?password=live_secret_123"
        uri2 = "ws://127.0.0.1:8102/ws/live-room/bob?password=live_secret_123"

        async with websockets.connect(uri1) as ws_alice:
            await drain_async_handshake(ws_alice)

            async with websockets.connect(uri2) as ws_bob:
                await drain_async_handshake(ws_bob)

                # Alice receives Bob's join notification across workers
                join_bob_on_alice = json.loads(await asyncio.wait_for(ws_alice.recv(), timeout=2.0))
                assert join_bob_on_alice["client_id"] == "bob"
                assert join_bob_on_alice["worker_id"] == "worker-live-2"

                # Send message from Alice on Worker 1 (port 8101)
                await ws_alice.send(json.dumps({
                    "action": "message",
                    "content": "Real-time packet across ASGI instances!",
                    "client_sent_time": time.time(),
                }))

                # Alice receives own message reflection
                msg_alice = json.loads(await asyncio.wait_for(ws_alice.recv(), timeout=2.0))
                # Bob receives message on Worker 2 (port 8102)
                msg_bob = json.loads(await asyncio.wait_for(ws_bob.recv(), timeout=2.0))

                assert msg_alice["content"] == "Real-time packet across ASGI instances!"
                assert msg_alice["sender_id"] == "alice"

                assert msg_bob["content"] == "Real-time packet across ASGI instances!"
                assert msg_bob["sender_id"] == "alice"
                assert msg_bob["worker_id"] == "worker-live-1"

    finally:
        await server1.stop()
        await server2.stop()
