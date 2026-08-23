"""Unit and integration tests for message persistence, history replay, and creator controls."""

import json
import fakeredis.aioredis
from fakeredis import FakeServer
from fastapi.testclient import TestClient
import pytest

from app.config import Settings
from app.main import create_app
from app.schemas import EventType, MessageEvent


@pytest.fixture
def fake_redis_server():
    return FakeServer()


@pytest.fixture
def app_with_fake_redis(fake_redis_server):
    redis_client = fakeredis.aioredis.FakeRedis(server=fake_redis_server, decode_responses=True)
    settings = Settings(
        WORKER_ID="persist-worker-1",
        PORT=8000,
        REDIS_URL="redis://cluster:6379/0",
        MAX_ROOM_MEMBERS=20,
    )
    return create_app(settings=settings, redis_client=redis_client)


def drain_handshake_and_get_history(ws):
    """Consume handshake frames and return history event."""
    history_event = None
    for _ in range(3):
        try:
            frame = json.loads(ws.receive_text())
            if frame.get("event_type") == EventType.HISTORY:
                history_event = frame
        except Exception:
            break
    return history_event


def test_creator_registration_and_verification(app_with_fake_redis):
    """Test that room creator is registered and identified correctly."""
    with TestClient(app_with_fake_redis) as client:
        # Create room as Alice
        create_res = client.post("/api/rooms/create", json={
            "room_id": "vip-club",
            "password": "secretpass123",
            "created_by": "Alice",
        })
        assert create_res.status_code == 200
        data = create_res.json()
        assert data["is_new"] is True
        assert data["creator_id"] == "Alice"
        assert data["is_creator"] is True

        # Verify as Bob (non-creator)
        verify_bob = client.post("/api/rooms/verify", json={
            "room_id": "vip-club",
            "password": "secretpass123",
            "client_id": "Bob",
        })
        assert verify_bob.status_code == 200
        data_bob = verify_bob.json()
        assert data_bob["creator_id"] == "Alice"
        assert data_bob["is_creator"] is False

        # Verify as Alice (creator)
        verify_alice = client.post("/api/rooms/verify", json={
            "room_id": "vip-club",
            "password": "secretpass123",
            "client_id": "Alice",
        })
        assert verify_alice.status_code == 200
        data_alice = verify_alice.json()
        assert data_alice["creator_id"] == "Alice"
        assert data_alice["is_creator"] is True


def test_chat_history_persistence_and_replay(app_with_fake_redis):
    """Test that chat messages sent in a room are preserved and replayed upon reconnect."""
    with TestClient(app_with_fake_redis) as client:
        # 1. Alice connects as creator and sends 2 messages
        with client.websocket_connect("/ws/persist-room/Alice?password=pwd123") as ws_alice:
            hist_alice = drain_handshake_and_get_history(ws_alice)
            assert hist_alice is not None
            assert hist_alice["is_creator"] is True
            assert len(hist_alice["messages"]) == 0

            # Alice sends message 1
            ws_alice.send_text(json.dumps({"action": "message", "content": "First message from Alice"}))
            msg1 = json.loads(ws_alice.receive_text())
            assert msg1["content"] == "First message from Alice"

            # Alice sends message 2
            ws_alice.send_text(json.dumps({"action": "message", "content": "Second message from Alice"}))
            msg2 = json.loads(ws_alice.receive_text())
            assert msg2["content"] == "Second message from Alice"

            # 2. Bob joins the same room while Alice is in it
            with client.websocket_connect("/ws/persist-room/Bob?password=pwd123") as ws_bob:
                hist_bob = drain_handshake_and_get_history(ws_bob)
                assert hist_bob is not None
                assert hist_bob["is_creator"] is False
                assert hist_bob["creator_id"] == "Alice"
                assert len(hist_bob["messages"]) == 2
                assert hist_bob["messages"][0]["content"] == "First message from Alice"
                assert hist_bob["messages"][1]["content"] == "Second message from Alice"

                # Drain Bob join notification on Alice
                _ = ws_alice.receive_text()

                # Bob sends a reply
                ws_bob.send_text(json.dumps({"action": "message", "content": "Hello Alice, I see your past messages!"}))

            # Drain Bob departure from Alice socket
            _ = ws_alice.receive_text()

        # 3. Test reload scenario:
        with client.websocket_connect("/ws/room-reload/Alice?password=pwd123") as ws1:
            drain_handshake_and_get_history(ws1)
            ws1.send_text(json.dumps({"action": "message", "content": "Persisted message 1"}))
            _ = ws1.receive_text()

            # Second client connects
            with client.websocket_connect("/ws/room-reload/Bob?password=pwd123") as ws2:
                hist2 = drain_handshake_and_get_history(ws2)
                assert hist2 is not None
                assert len(hist2["messages"]) == 1
                assert hist2["messages"][0]["content"] == "Persisted message 1"


def test_creator_terminate_room_and_data_purge(app_with_fake_redis):
    """Test that creator can terminate the room and permanently purge all message history."""
    with TestClient(app_with_fake_redis) as client:
        with client.websocket_connect("/ws/term-room/Alice?password=pwd123") as ws_alice:
            drain_handshake_and_get_history(ws_alice)
            ws_alice.send_text(json.dumps({"action": "message", "content": "Confidential Message"}))
            _ = ws_alice.receive_text()

            with client.websocket_connect("/ws/term-room/Bob?password=pwd123") as ws_bob:
                drain_handshake_and_get_history(ws_bob)
                _ = ws_alice.receive_text()  # Bob joined notice on Alice

                # Bob (non-creator) tries to terminate room -> should receive ErrorEvent
                ws_bob.send_text(json.dumps({"action": "terminate_room"}))
                bob_err = json.loads(ws_bob.receive_text())
                assert bob_err["event_type"] == EventType.ERROR
                assert "Only the room creator" in bob_err["detail"]

                # Alice (creator) terminates the room
                ws_alice.send_text(json.dumps({"action": "terminate_room"}))

                # Bob should receive room destroyed event
                bob_destroyed = json.loads(ws_bob.receive_text())
                assert bob_destroyed["event_type"] == EventType.ROOM_DESTROYED


def test_creator_kick_user(app_with_fake_redis):
    """Test that creator can kick another user from the room."""
    with TestClient(app_with_fake_redis) as client:
        with client.websocket_connect("/ws/kick-room/Alice?password=pwd123") as ws_alice:
            drain_handshake_and_get_history(ws_alice)

            with client.websocket_connect("/ws/kick-room/Bob?password=pwd123") as ws_bob:
                drain_handshake_and_get_history(ws_bob)
                _ = ws_alice.receive_text()  # Bob join notice on Alice

                # Bob tries to kick Alice -> Error
                ws_bob.send_text(json.dumps({"action": "kick_user", "target_client_id": "Alice"}))
                err_frame = json.loads(ws_bob.receive_text())
                assert err_frame["event_type"] == EventType.ERROR

                # Alice kicks Bob
                ws_alice.send_text(json.dumps({
                    "action": "kick_user",
                    "target_client_id": "Bob",
                    "reason": "Violating rules",
                }))

                # Alice receives kick notification broadcast
                alice_kick_notice = json.loads(ws_alice.receive_text())
                assert alice_kick_notice["event_type"] == EventType.USER_KICKED
                assert alice_kick_notice["client_id"] == "Bob"
                assert alice_kick_notice["kicked_by"] == "Alice"

