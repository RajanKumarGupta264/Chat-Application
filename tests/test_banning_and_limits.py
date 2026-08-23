"""Comprehensive verification tests for:
1. Creator kicking a user -> User banned & rejected from rejoining via API and WebSocket.
2. World Chat 200-message buffer trimming.
3. World Chat quiet presence (no global join notice broadcast).
4. Sub-45ms message dispatch latency.
"""

import asyncio
import json
import time
import fakeredis.aioredis
from fakeredis import FakeServer
from fastapi.testclient import TestClient
import pytest

from app.config import Settings
from app.main import create_app
from app.schemas import MessageEvent


@pytest.fixture
def fake_redis_server():
    return FakeServer()


@pytest.fixture
def test_app(fake_redis_server):
    redis_client = fakeredis.aioredis.FakeRedis(server=fake_redis_server, decode_responses=True)
    settings = Settings(
        WORKER_ID="worker-ban-test",
        PORT=8000,
        REDIS_URL="redis://127.0.0.1:6379/0",
        MAX_ROOM_MEMBERS=20,
    )
    return create_app(settings=settings, redis_client=redis_client)


def test_creator_kick_and_ban_rejoin_prevention(test_app):
    """Verify that when creator kicks a member, they are permanently banned from rejoining the room."""
    with TestClient(test_app) as client:
        # 1. Alice creates private room
        create_res = client.post("/api/rooms/create", json={"room_id": "vault-ban", "password": "pass", "created_by": "Alice"})
        assert create_res.status_code == 200

        # Alice connects
        with client.websocket_connect("/ws/vault-ban/Alice?password=pass") as ws_alice:
            # Drain handshake
            for _ in range(3):
                ws_alice.receive_text()

            # Bob joins
            with client.websocket_connect("/ws/vault-ban/Bob?password=pass") as ws_bob:
                for _ in range(3):
                    ws_bob.receive_text()
                _ = ws_alice.receive_text()  # Alice sees Bob join

                # Alice kicks Bob
                ws_alice.send_text(json.dumps({"action": "kick_user", "target_client_id": "Bob"}))

                # Alice receives kick broadcast
                kick_msg = json.loads(ws_alice.receive_text())
                assert kick_msg["event_type"] == "user_kicked"
                assert kick_msg["client_id"] == "Bob"
                assert kick_msg["kicked_by"] == "Alice"

                # 2. Bob attempts to verify room API -> Rejected with 403
                rejoin_api = client.post("/api/rooms/verify", json={"room_id": "vault-ban", "password": "pass", "client_id": "Bob"})
                assert rejoin_api.status_code == 403
                assert "removed from this room" in rejoin_api.json()["detail"]

                # 3. Bob attempts WebSocket connection -> Rejected with 1008
                with pytest.raises(Exception):
                    with client.websocket_connect("/ws/vault-ban/Bob?password=pass") as ws_banned:
                        pass


def test_world_chat_quiet_presence(test_app):
    """Verify that World Chat updates online count without sending join notices to other users."""
    with TestClient(test_app) as client:
        # Alice connects to World Chat
        with client.websocket_connect("/ws/world/Alice") as ws_alice:
            welcome_a = json.loads(ws_alice.receive_text())
            assert welcome_a["event_type"] == "system_notice"
            assert "Connected to World Chat" in welcome_a["message"]

            # Presence update published on join
            pres_a = json.loads(ws_alice.receive_text())
            assert pres_a["event_type"] == "presence_update"

            hist_a = json.loads(ws_alice.receive_text())
            assert hist_a["event_type"] == "history"

            # Bob connects to World Chat
            with client.websocket_connect("/ws/world/Bob") as ws_bob:
                welcome_b = json.loads(ws_bob.receive_text())
                assert welcome_b["event_type"] == "system_notice"

                # Alice receives PresenceUpdateEvent, NOT a chat notification message
                event_for_alice = json.loads(ws_alice.receive_text())
                assert event_for_alice["event_type"] == "presence_update"
                assert event_for_alice["active_count"] == 2
                assert "Bob" in event_for_alice["members"]
                assert event_for_alice["event_type"] == "presence_update"
                assert event_for_alice["active_count"] == 2
                assert "Bob" in event_for_alice["members"]


@pytest.mark.asyncio
async def test_world_chat_200_message_limit(fake_redis_server):
    """Verify that World Chat message buffer is strictly capped to the latest 200 messages."""
    redis_client = fakeredis.aioredis.FakeRedis(server=fake_redis_server, decode_responses=True)
    app_inst = create_app(redis_client=redis_client)
    bridge = app_inst.state.redis_bridge
    await bridge.start()

    try:
        # Push 220 messages to World Chat
        for i in range(220):
            msg = MessageEvent(
                room_id="world",
                sender_id=f"User_{i}",
                content=f"Message #{i}",
            )
            await bridge.save_message("world", msg)

        # Retrieve history
        history = await bridge.get_room_history("world")
        assert len(history) == 200
        # Earliest message should be #20 (0..19 pruned)
        assert history[0].content == "Message #20"
        # Latest message should be #219
        assert history[-1].content == "Message #219"

    finally:
        await bridge.stop()


@pytest.mark.asyncio
async def test_sub_45ms_message_latency(fake_redis_server):
    """Verify that message save and cluster fanout takes well under 45ms."""
    redis_client = fakeredis.aioredis.FakeRedis(server=fake_redis_server, decode_responses=True)
    app_inst = create_app(redis_client=redis_client)
    bridge = app_inst.state.redis_bridge
    await bridge.start()

    try:
        msg = MessageEvent(
            room_id="perf-room",
            sender_id="Speedy",
            content="Benchmark message",
        )

        start = time.perf_counter()
        await asyncio.gather(
            bridge.save_message("perf-room", msg),
            bridge.publish("perf-room", msg),
        )
        duration_ms = (time.perf_counter() - start) * 1000.0

        print(f"Message dispatch latency: {duration_ms:.2f} ms")
        assert duration_ms < 45.0, f"Latency {duration_ms}ms exceeded 45ms threshold"

    finally:
        await bridge.stop()

