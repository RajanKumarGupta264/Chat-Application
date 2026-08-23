"""Integration and unit test suite for connection lifecycle and password security."""

import json
from typing import AsyncGenerator
import fakeredis.aioredis
from fakeredis import FakeServer
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
import pytest

from app.config import Settings
from app.main import create_app
from app.schemas import EventType


@pytest.fixture
def fake_redis():
    """In-memory Redis server fixture providing isolated test storage."""
    server = FakeServer()
    redis_client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    return redis_client


@pytest.fixture
def app_instance(fake_redis):
    """FastAPI application fixture with an attached mock Redis client."""
    settings = Settings(
        WORKER_ID="test-worker-1",
        PORT=8000,
        REDIS_URL="redis://fake-host:6379/0",
        MAX_ROOM_MEMBERS=20,
    )
    return create_app(settings=settings, redis_client=fake_redis)


def drain_handshake(ws):
    """Consume initial connection frames (welcome, join, history)."""
    frames = []
    # Read 3 initial frames: welcome, join, history
    for _ in range(3):
        try:
            frames.append(json.loads(ws.receive_text()))
        except Exception:
            break
    return frames


def test_health_endpoint(app_instance):
    """Test HTTP health check endpoint."""
    with TestClient(app_instance) as client:
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["worker_id"] == "test-worker-1"
        assert "uptime_seconds" in data


def test_index_page(app_instance):
    """Test frontend index page retrieval."""
    with TestClient(app_instance) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert "Distributed Real-Time Chat Engine" in response.text
        assert 'href="/static/style.css"' in response.text
        assert 'src="/static/app.js"' in response.text


def test_static_assets(app_instance):
    """Test that static CSS and JavaScript files are served with status 200."""
    with TestClient(app_instance) as client:
        css_res = client.get("/static/style.css")
        assert css_res.status_code == 200
        assert "--primary" in css_res.text

        js_res = client.get("/static/app.js")
        assert js_res.status_code == 200
        assert "connectWebSocket" in js_res.text


def test_room_create_and_verify_api(app_instance):
    """Test room creation API and password verification."""
    with TestClient(app_instance) as client:
        # 1. Create a password-protected room
        create_res = client.post("/api/rooms/create", json={
            "room_id": "vip-vault",
            "password": "mypassword123",
            "created_by": "Alice"
        })
        assert create_res.status_code == 200
        data = create_res.json()
        assert data["success"] is True
        assert data["is_new"] is True
        assert data["creator_id"] == "Alice"
        assert data["is_creator"] is True

        # 2. Verify with correct password
        verify_ok = client.post("/api/rooms/verify", json={
            "room_id": "vip-vault",
            "password": "mypassword123",
            "client_id": "Alice"
        })
        assert verify_ok.status_code == 200
        assert verify_ok.json()["success"] is True
        assert verify_ok.json()["is_creator"] is True

        # 3. Verify with wrong password
        verify_wrong = client.post("/api/rooms/verify", json={
            "room_id": "vip-vault",
            "password": "wrong_password",
            "client_id": "Bob"
        })
        assert verify_wrong.status_code == 401


def test_websocket_password_auth_and_rejection(app_instance):
    """Test WebSocket connection success with correct password and rejection with wrong password."""
    with TestClient(app_instance) as client:
        # Create room
        client.post("/api/rooms/create", json={
            "room_id": "crypto-club",
            "password": "secure_pass_456"
        })

        # 1. Connect with wrong password -> Should be disconnected
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with client.websocket_connect("/ws/crypto-club/hacker?password=wrongpass"):
                pass
        assert exc_info.value.code == 1008

        # 2. Connect with correct password -> Should succeed
        with client.websocket_connect("/ws/crypto-club/alice?password=secure_pass_456") as ws:
            welcome_raw = ws.receive_text()
            welcome = json.loads(welcome_raw)
            assert welcome["event_type"] == EventType.SYSTEM_NOTICE.value
            assert "crypto-club" in welcome["message"]


def test_room_auto_destruction_when_empty(app_instance, fake_redis):
    """Test that when all participants leave, the room metadata and data are permanently destroyed."""
    with TestClient(app_instance) as client:
        room_name = "ephemeral-room-99"
        room_pass = "temporary123"

        # Create and connect Alice
        client.post("/api/rooms/create", json={"room_id": room_name, "password": room_pass, "created_by": "alice"})

        with client.websocket_connect(f"/ws/{room_name}/alice?password={room_pass}") as ws_alice:
            drain_handshake(ws_alice)

            # Connect Bob
            with client.websocket_connect(f"/ws/{room_name}/bob?password={room_pass}") as ws_bob:
                drain_handshake(ws_bob)
                _ = ws_alice.receive_text()  # alice receives bob join

            # Bob left -> Alice should receive USER_LEFT
            leave_bob = json.loads(ws_alice.receive_text())
            assert leave_bob["event_type"] == EventType.USER_LEFT.value
            assert leave_bob["active_count"] == 1

        # Now Alice has also left -> Room active count dropped to 0!
        import asyncio
        async def check_redis_key():
            val = await fake_redis.get(f"chat:room_meta:{room_name}")
            assert val is None
            msg_val = await fake_redis.get(f"chat:messages:{room_name}")
            assert msg_val is None

        asyncio.run(check_redis_key())


def test_websocket_chat_echo(app_instance):
    """Test sending a chat message and receiving it back over local fan-out."""
    with TestClient(app_instance) as client:
        with client.websocket_connect("/ws/dev-team/bob?password=pass123") as websocket:
            drain_handshake(websocket)

            # Send chat message payload
            send_payload = {
                "action": "message",
                "content": "Hello ASGI Engine!",
                "client_sent_time": 1700000000.0,
            }
            websocket.send_text(json.dumps(send_payload))

            msg_raw = websocket.receive_text()
            msg = json.loads(msg_raw)
            assert msg["event_type"] == EventType.CHAT_MESSAGE.value
            assert msg["sender_id"] == "bob"
            assert msg["content"] == "Hello ASGI Engine!"


def test_websocket_heartbeat_ping_pong(app_instance):
    """Test PING frame receiving immediate PONG reply with latency metadata."""
    with TestClient(app_instance) as client:
        with client.websocket_connect("/ws/lobby/charlie?password=lobby_pass") as websocket:
            drain_handshake(websocket)

            ping_payload = {
                "action": "ping",
                "client_sent_time": 1700000050.123,
            }
            websocket.send_text(json.dumps(ping_payload))

            pong_raw = websocket.receive_text()
            pong = json.loads(pong_raw)
            assert pong["event_type"] == EventType.PONG.value
            assert pong["client_id"] == "charlie"
            assert pong["client_sent_time"] == 1700000050.123


def test_websocket_typing_indicator_broadcast(app_instance):
    """Test that typing events from User 2 are broadcast to User 1 in real-time."""
    with TestClient(app_instance) as client:
        room_name = "typing-lounge"
        room_pass = "typepass123"

        with client.websocket_connect(f"/ws/{room_name}/user1?password={room_pass}") as ws1:
            drain_handshake(ws1)

            with client.websocket_connect(f"/ws/{room_name}/user2?password={room_pass}") as ws2:
                drain_handshake(ws2)
                _ = ws1.receive_text()  # ws1 receives user2 join

                # User 2 sends a typing event
                ws2.send_text(json.dumps({"action": "typing", "is_typing": True}))

                # User 1 should receive the TypingEvent
                typing_raw = ws1.receive_text()
                typing_data = json.loads(typing_raw)
                assert typing_data["event_type"] == EventType.TYPING.value
                assert typing_data["client_id"] == "user2"
                assert typing_data["is_typing"] is True


def test_max_room_members_limit(fake_redis):
    """Test that a room enforces the maximum member limit of 20 members."""
    from app.config import Settings
    from app.main import create_app

    # Create app with small limit of 2 members for easy testing
    custom_settings = Settings(
        WORKER_ID="worker-limit-test",
        MAX_ROOM_MEMBERS=2,
    )
    custom_app = create_app(settings=custom_settings, redis_client=fake_redis)

    with TestClient(custom_app) as client:
        room = "limited-room"
        pwd = "limit_pass"

        with client.websocket_connect(f"/ws/{room}/member1?password={pwd}") as ws1:
            drain_handshake(ws1)

            with client.websocket_connect(f"/ws/{room}/member2?password={pwd}") as ws2:
                drain_handshake(ws2)

                # Room is now at capacity (2/2)
                # Verify API rejects member3
                res = client.post("/api/rooms/verify", json={"room_id": room, "password": pwd})
                assert res.status_code == 403
                assert "Room is full" in res.json()["detail"]

                # Verify WebSocket rejects 3rd connection
                with pytest.raises(Exception):
                    with client.websocket_connect(f"/ws/{room}/member3?password={pwd}"):
                        pass
