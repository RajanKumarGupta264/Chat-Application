"""Tests for sequential creator role transfer, creator remove controls, exit, and World Chat."""

import asyncio
import json
import fakeredis.aioredis
from fakeredis import FakeServer
from fastapi.testclient import TestClient
import pytest
import uvicorn
import websockets
from websockets.exceptions import ConnectionClosed

from app.config import Settings
from app.main import create_app
from app.schemas import EventType


class BackgroundServer:
    """Uvicorn server running in an asyncio task for async testing."""
    def __init__(self, app, host: str = "127.0.0.1", port: int = 8200):
        self.app = app
        self.host = host
        self.port = port
        self.config = uvicorn.Config(self.app, host=self.host, port=self.port, log_level="warning", loop="asyncio")
        self.server = uvicorn.Server(config=self.config)
        self.task = None

    async def start(self):
        self.task = asyncio.create_task(self.server.serve())
        for _ in range(50):
            if self.server.started:
                break
            await asyncio.sleep(0.05)

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


@pytest.fixture
def fake_redis_server():
    return FakeServer()


@pytest.fixture
def app_with_fake_redis(fake_redis_server):
    redis_client = fakeredis.aioredis.FakeRedis(server=fake_redis_server, decode_responses=True)
    settings = Settings(
        WORKER_ID="transfer-worker-1",
        PORT=8000,
        REDIS_URL="redis://cluster:6379/0",
        MAX_ROOM_MEMBERS=20,
    )
    return create_app(settings=settings, redis_client=redis_client)


def test_world_chat_open_access_no_password(app_with_fake_redis):
    """Verify that World Chat allows anyone to connect without password and chat in real-time."""
    with TestClient(app_with_fake_redis) as client:
        # Verify API
        verify_res = client.post("/api/rooms/verify", json={"room_id": "world", "password": ""})
        assert verify_res.status_code == 200

        # Connect Alice to World Chat
        with client.websocket_connect("/ws/world/Alice") as ws_alice:
            for _ in range(3):
                ws_alice.receive_text()

            # Connect Bob to World Chat
            with client.websocket_connect("/ws/world/Bob") as ws_bob:
                for _ in range(3):
                    ws_bob.receive_text()
                _ = ws_alice.receive_text()  # Alice receives Bob join

                # Alice sends message in World Chat
                ws_alice.send_text(json.dumps({"action": "message", "content": "Hello World Chat!"}))
                _ = ws_alice.receive_text()  # Alice reflection
                msg_bob = json.loads(ws_bob.receive_text())
                assert msg_bob["content"] == "Hello World Chat!"
                assert msg_bob["sender_id"] == "Alice"


@pytest.mark.asyncio
async def test_sequential_creator_transfer_and_moderation():
    """Verify sequential creator transfer Alice -> Bob -> Charlie and creator moderation."""
    server_fake = FakeServer()
    redis_client = fakeredis.aioredis.FakeRedis(server=server_fake, decode_responses=True)
    settings = Settings(WORKER_ID="worker-seq-test", PORT=8210)
    app = create_app(settings=settings, redis_client=redis_client)
    server = BackgroundServer(app, host="127.0.0.1", port=8210)

    await server.start()

    try:
        room_name = "dyn-room"
        room_pwd = "dyn_password"
        base_uri = f"ws://127.0.0.1:8210/ws/{room_name}"

        # 1. Alice connects as Creator
        ws_alice = await websockets.connect(f"{base_uri}/Alice?password={room_pwd}")
        await drain_async_handshake(ws_alice)

        # 2. Bob joins as 2nd member
        ws_bob = await websockets.connect(f"{base_uri}/Bob?password={room_pwd}")
        await drain_async_handshake(ws_bob)
        _ = await ws_alice.recv()  # Bob join notice on Alice

        # 3. Charlie joins as 3rd member
        ws_charlie = await websockets.connect(f"{base_uri}/Charlie?password={room_pwd}")
        await drain_async_handshake(ws_charlie)
        _ = await ws_alice.recv()    # Charlie join notice on Alice
        _ = await ws_bob.recv()      # Charlie join notice on Bob

        # 4. Bob (not creator) attempts to terminate room -> should get error
        await ws_bob.send(json.dumps({"action": "terminate_room"}))
        bob_err = json.loads(await asyncio.wait_for(ws_bob.recv(), timeout=1.0))
        assert bob_err["event_type"] == "error"

        # 5. Alice (Current Creator) disconnects / exits
        await ws_alice.close()

        # Both Bob and Charlie receive CreatorTransferredEvent and UserLeftEvent
        frames_bob = []
        for _ in range(2):
            raw = await asyncio.wait_for(ws_bob.recv(), timeout=2.0)
            frames_bob.append(json.loads(raw))

        transfer_frame = next(f for f in frames_bob if f.get("event_type") == "creator_transferred")
        assert transfer_frame["new_creator_id"] == "Bob"
        assert transfer_frame["old_creator_id"] == "Alice"

        # Drain Charlie frames from Alice's departure
        for _ in range(2):
            await asyncio.wait_for(ws_charlie.recv(), timeout=2.0)

        # 6. Now Bob is the Creator! Bob removes Charlie
        await ws_bob.send(json.dumps({
            "action": "kick_user",
            "target_client_id": "Charlie",
            "reason": "Test removal",
        }))

        # Bob receives kick broadcast
        kick_frame = json.loads(await asyncio.wait_for(ws_bob.recv(), timeout=2.0))
        assert kick_frame["event_type"] == "user_kicked"
        assert kick_frame["client_id"] == "Charlie"
        assert kick_frame["kicked_by"] == "Bob"

        # Charlie is disconnected by server with code 4003
        try:
            raw_charlie = await asyncio.wait_for(ws_charlie.recv(), timeout=2.0)
            parsed = json.loads(raw_charlie)
            assert parsed["event_type"] == "user_kicked"
        except ConnectionClosed as exc:
            assert exc.rcvd.code == 4003 or exc.code == 4003

        # Bob receives Charlie's user_left frame from departure
        leave_charlie = json.loads(await asyncio.wait_for(ws_bob.recv(), timeout=2.0))
        assert leave_charlie["event_type"] == "user_left"
        assert leave_charlie["client_id"] == "Charlie"

        # 7. Bob terminates the room
        await ws_bob.send(json.dumps({"action": "terminate_room"}))
        try:
            term_frame = json.loads(await asyncio.wait_for(ws_bob.recv(), timeout=2.0))
            assert term_frame["event_type"] == "room_destroyed"
        except ConnectionClosed:
            pass  # Expected when server closes connection on termination

        try:
            await ws_bob.close()
        except Exception:
            pass

        try:
            await ws_charlie.close()
        except Exception:
            pass

    finally:
        await server.stop()


def test_world_chat_has_no_member_limit(fake_redis_server):
    """Verify that while private rooms enforce the MAX_ROOM_MEMBERS limit, World Chat has no member limit."""
    redis_client = fakeredis.aioredis.FakeRedis(server=fake_redis_server, decode_responses=True)
    # Set limit of 2 for private rooms
    settings = Settings(
        WORKER_ID="worker-no-limit",
        PORT=8000,
        MAX_ROOM_MEMBERS=2,
    )
    custom_app = create_app(settings=settings, redis_client=redis_client)

    with TestClient(custom_app) as client:
        # 1. Connect 4 members to World Chat (exceeding private limit of 2)
        with client.websocket_connect("/ws/world/User1") as ws1:
            with client.websocket_connect("/ws/world/User2") as ws2:
                with client.websocket_connect("/ws/world/User3") as ws3:
                    with client.websocket_connect("/ws/world/User4") as ws4:
                        # All 4 succeed in World Chat!
                        verify_world = client.post("/api/rooms/verify", json={"room_id": "world", "password": ""})
                        assert verify_world.status_code == 200
                        assert verify_world.json()["active_count"] == 4

        # 2. Private room enforces the limit of 2
        client.post("/api/rooms/create", json={"room_id": "priv-lim", "password": "pwd", "created_by": "U1"})
        with client.websocket_connect("/ws/priv-lim/U1?password=pwd") as p1:
            with client.websocket_connect("/ws/priv-lim/U2?password=pwd") as p2:
                # 3rd connection is rejected for private room
                res_priv = client.post("/api/rooms/verify", json={"room_id": "priv-lim", "password": "pwd"})
                assert res_priv.status_code == 403
                assert "Room is full" in res_priv.json()["detail"]


