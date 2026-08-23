"""Distributed multi-worker message fan-out and presence synchronization tests with room security."""

import json
import pytest
import fakeredis.aioredis
from fakeredis import FakeServer
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.schemas import EventType


@pytest.fixture
def shared_fake_redis_server():
    """Shared Redis backend server instance simulating a unified distributed broker."""
    return FakeServer()


@pytest.fixture
def worker_1_app(shared_fake_redis_server):
    """FastAPI Worker 1 instance."""
    redis_client = fakeredis.aioredis.FakeRedis(
        server=shared_fake_redis_server,
        decode_responses=True,
    )
    settings = Settings(
        WORKER_ID="worker-node-1",
        PORT=8000,
        REDIS_URL="redis://fake-broker:6379/0",
    )
    return create_app(settings=settings, redis_client=redis_client)


@pytest.fixture
def worker_2_app(shared_fake_redis_server):
    """FastAPI Worker 2 instance."""
    redis_client = fakeredis.aioredis.FakeRedis(
        server=shared_fake_redis_server,
        decode_responses=True,
    )
    settings = Settings(
        WORKER_ID="worker-node-2",
        PORT=8001,
        REDIS_URL="redis://fake-broker:6379/0",
    )
    return create_app(settings=settings, redis_client=redis_client)


def drain_handshake(ws):
    """Consume initial connection frames (welcome, join, history)."""
    frames = []
    for _ in range(3):
        try:
            frames.append(json.loads(ws.receive_text()))
        except Exception:
            break
    return frames


def test_cross_worker_message_fanout(worker_1_app, worker_2_app):
    """Verify that a message sent by a client on Worker 1 is delivered to a client on Worker 2."""
    with TestClient(worker_1_app) as client_worker_1, TestClient(worker_2_app) as client_worker_2:
        room_pass = "finance_secure_pass"
        # Connect Alice to Worker 1
        with client_worker_1.websocket_connect(f"/ws/finance/alice?password={room_pass}") as ws_alice:
            drain_handshake(ws_alice)

            # Connect Bob to Worker 2 (same room: 'finance', same password)
            with client_worker_2.websocket_connect(f"/ws/finance/bob?password={room_pass}") as ws_bob:
                drain_handshake(ws_bob)

                # Alice on Worker 1 receives Bob's join event from Worker 2
                join_bob_on_alice = json.loads(ws_alice.receive_text())
                assert join_bob_on_alice["client_id"] == "bob"
                assert join_bob_on_alice["worker_id"] == "worker-node-2"

                # Alice on Worker 1 sends a message
                alice_message = {
                    "action": "message",
                    "content": "Quarterly financial report is ready.",
                    "client_sent_time": 1700000100.0,
                }
                ws_alice.send_text(json.dumps(alice_message))

                # Both Alice (Worker 1) and Bob (Worker 2) should receive the message via Redis Pub/Sub
                alice_recv = json.loads(ws_alice.receive_text())
                bob_recv = json.loads(ws_bob.receive_text())

                assert alice_recv["content"] == "Quarterly financial report is ready."
                assert alice_recv["sender_id"] == "alice"
                assert alice_recv["worker_id"] == "worker-node-1"

                assert bob_recv["content"] == "Quarterly financial report is ready."
                assert bob_recv["sender_id"] == "alice"
                assert bob_recv["worker_id"] == "worker-node-1"

                # Bob on Worker 2 replies back to Alice on Worker 1
                bob_reply = {
                    "action": "message",
                    "content": "Received loud and clear, Alice!",
                }
                ws_bob.send_text(json.dumps(bob_reply))

                # Alice receives Bob's reply originated from Worker 2
                alice_recv_reply = json.loads(ws_alice.receive_text())
                assert alice_recv_reply["content"] == "Received loud and clear, Alice!"
                assert alice_recv_reply["sender_id"] == "bob"
                assert alice_recv_reply["worker_id"] == "worker-node-2"


def test_channel_room_isolation_across_workers(worker_1_app, worker_2_app):
    """Verify that messages in room A are isolated from room B across different workers."""
    with TestClient(worker_1_app) as client_worker_1, TestClient(worker_2_app) as client_worker_2:
        with client_worker_1.websocket_connect("/ws/room-alpha/user-a?password=alpha_pass") as ws_a:
            drain_handshake(ws_a)

            with client_worker_2.websocket_connect("/ws/room-beta/user-b?password=beta_pass") as ws_b:
                drain_handshake(ws_b)

                # Send message in room-alpha
                ws_a.send_text(json.dumps({"action": "message", "content": "Alpha secret"}))

                # User A receives Alpha secret
                msg_a = json.loads(ws_a.receive_text())
                assert msg_a["content"] == "Alpha secret"

                # User B on room-beta sends a ping to ensure queue is processed
                ws_b.send_text(json.dumps({"action": "ping"}))
                pong_b = json.loads(ws_b.receive_text())
                assert pong_b["event_type"] == EventType.PONG.value


def test_cross_worker_disconnection_presence_propagation(worker_1_app, worker_2_app):
    """Verify that disconnecting a client on Worker 2 emits a leave event to Worker 1."""
    with TestClient(worker_1_app) as client_worker_1, TestClient(worker_2_app) as client_worker_2:
        room_pass = "presence_pass"
        with client_worker_1.websocket_connect(f"/ws/presence-room/alice?password={room_pass}") as ws_alice:
            drain_handshake(ws_alice)

            # Bob joins on Worker 2 and then exits the context
            with client_worker_2.websocket_connect(f"/ws/presence-room/bob?password={room_pass}") as ws_bob:
                drain_handshake(ws_bob)
                _ = ws_alice.receive_text()  # alice receives bob join

            # At this point, ws_bob is closed. Alice on Worker 1 should receive USER_LEFT for Bob
            leave_raw = ws_alice.receive_text()
            leave_event = json.loads(leave_raw)
            assert leave_event["event_type"] == EventType.USER_LEFT.value
            assert leave_event["client_id"] == "bob"
            assert leave_event["worker_id"] == "worker-node-2"
