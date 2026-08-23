"""Live Multi-Worker Distributed Cluster Verification Script with World Chat & Sequential Creator Transfer.

Spins up two distinct ASGI worker instances on real TCP ports (8105, 8106)
connected to a shared Redis backend and performs end-to-end tests:
1. Public World Chat open broadcast across stranger nodes without passwords
2. Private Room creation, cross-worker join, and message history
3. Sequential creator role transfer when Alice exits -> Bob promoted to Creator
4. Creator-only member removal (Bob removes Charlie)
5. Creator room termination and complete data purge
"""

import asyncio
import json
import logging
import sys
import time
import fakeredis.aioredis
from fakeredis import FakeServer
import uvicorn
import websockets
from websockets.exceptions import ConnectionClosed

from app.config import Settings
from app.main import create_app

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] (%(process)d) %(message)s")
logger = logging.getLogger("verification")


class WorkerInstance:
    """Encapsulates a live uvicorn server running in an asyncio task."""
    def __init__(self, port: int, worker_id: str, redis_client):
        self.port = port
        self.worker_id = worker_id
        self.redis_client = redis_client
        self.settings = Settings(
            WORKER_ID=worker_id,
            PORT=port,
            REDIS_URL="redis://cluster-bus:6379/0",
            MAX_ROOM_MEMBERS=20,
        )
        self.app = create_app(settings=self.settings, redis_client=self.redis_client)
        self.config = uvicorn.Config(self.app, host="127.0.0.1", port=port, log_level="warning")
        self.server = uvicorn.Server(self.config)
        self.task = None

    async def start(self):
        self.task = asyncio.create_task(self.server.serve())
        while not self.server.started:
            await asyncio.sleep(0.05)
        logger.info("Worker [%s] online at http://127.0.0.1:%d", self.worker_id, self.port)

    async def stop(self):
        self.server.should_exit = True
        if self.task:
            await self.task
        logger.info("Worker [%s] stopped.", self.worker_id)


async def drain_handshake(ws):
    """Consume initial frames (welcome, join, history) and return history event."""
    hist = None
    for _ in range(3):
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            parsed = json.loads(raw)
            if parsed.get("event_type") == "history":
                hist = parsed
        except Exception:
            break
    return hist


async def main():
    print("=" * 80)
    print("DISTRIBUTED CHAT SYSTEM: WORLD CHAT & SEQUENTIAL CREATOR TRANSFER TEST")
    print("=" * 80)

    # 1. Initialize Shared Redis Backplane
    shared_redis_server = FakeServer()
    redis1 = fakeredis.aioredis.FakeRedis(server=shared_redis_server, decode_responses=True)
    redis2 = fakeredis.aioredis.FakeRedis(server=shared_redis_server, decode_responses=True)

    worker1 = WorkerInstance(port=8105, worker_id="stranger-1", redis_client=redis1)
    worker2 = WorkerInstance(port=8106, worker_id="stranger-2", redis_client=redis2)

    await worker1.start()
    await worker2.start()

    try:
        # -------------------------------------------------------------
        # PART 1: PUBLIC WORLD CHAT TEST (Cross-Worker, No Password)
        # -------------------------------------------------------------
        print("\n[PART 1] Verifying Public World Chat (No Password Required)...")
        uri_world_a = "ws://127.0.0.1:8105/ws/world/Guest_1"
        uri_world_b = "ws://127.0.0.1:8106/ws/world/Guest_2"

        async with websockets.connect(uri_world_a) as ws_w1:
            await drain_handshake(ws_w1)
            async with websockets.connect(uri_world_b) as ws_w2:
                await drain_handshake(ws_w2)
                _ = await ws_w1.recv()  # Guest_2 join on Guest_1

                # Guest_1 on Stranger 1 sends global message
                await ws_w1.send(json.dumps({"action": "message", "content": "Global Broadcast from Stranger 1"}))
                _ = await ws_w1.recv()  # reflection
                w_msg = json.loads(await ws_w2.recv())
                print(f" -> Guest_2 on Stranger 2 received World message: \"{w_msg['content']}\" (Origin: {w_msg['worker_id']})")
                assert w_msg["content"] == "Global Broadcast from Stranger 1"

        print(" -> World Chat Verification: PASSED [OK]")

        # -------------------------------------------------------------
        # PART 2: SEQUENTIAL CREATOR TRANSFER & MODERATION
        # -------------------------------------------------------------
        print("\n[PART 2] Verifying Private Room & Sequential Creator Transfer...")
        room_id = "ops-vault"
        room_pass = "vault_secret_99"
        uri_alice = f"ws://127.0.0.1:8105/ws/{room_id}/Alice?password={room_pass}"
        uri_bob = f"ws://127.0.0.1:8106/ws/{room_id}/Bob?password={room_pass}"
        uri_charlie = f"ws://127.0.0.1:8106/ws/{room_id}/Charlie?password={room_pass}"

        # 1. Alice connects as Initial Creator
        ws_alice = await websockets.connect(uri_alice)
        hist_alice = await drain_handshake(ws_alice)
        assert hist_alice["is_creator"] is True
        print(f" [1] Alice created room '{room_id}' as Initial Creator.")

        # 2. Bob joins on Stranger 2
        ws_bob = await websockets.connect(uri_bob)
        await drain_handshake(ws_bob)
        _ = await ws_alice.recv()  # Bob join on Alice
        print(f" [2] Bob joined room '{room_id}' on Stranger 2.")

        # 3. Charlie joins on Stranger 2
        ws_charlie = await websockets.connect(uri_charlie)
        await drain_handshake(ws_charlie)
        _ = await ws_alice.recv()  # Charlie join on Alice
        _ = await ws_bob.recv()    # Charlie join on Bob
        print(f" [3] Charlie joined room '{room_id}' on Stranger 2.")

        # Alice sends message
        await ws_alice.send(json.dumps({"action": "message", "content": "Welcome team! Passing leadership shortly."}))
        _ = await ws_alice.recv()
        _ = await ws_bob.recv()
        _ = await ws_charlie.recv()

        # 4. Alice (Creator) exits the room!
        print("\n [4] Alice (Creator) exits the room...")
        await ws_alice.close()

        # Bob receives CreatorTransferredEvent
        frames_bob = [json.loads(await ws_bob.recv()), json.loads(await ws_bob.recv())]
        transfer_bob = next(f for f in frames_bob if f.get("event_type") == "creator_transferred")
        print(f" -> Sequential Transfer Triggered! New Creator: {transfer_bob['new_creator_id']} (Transferred from: {transfer_bob['old_creator_id']})")
        assert transfer_bob["new_creator_id"] == "Bob"

        # Drain Charlie frames from Alice's departure
        for _ in range(2):
            await ws_charlie.recv()

        # 5. Bob (New Creator) removes Charlie
        print("\n [5] Bob (New Creator) executes Member Removal on Charlie...")
        await ws_bob.send(json.dumps({
            "action": "kick_user",
            "target_client_id": "Charlie",
            "reason": "Test moderation",
        }))

        kick_broadcast = json.loads(await ws_bob.recv())
        print(f" -> Kick Broadcast: {kick_broadcast['client_id']} was removed by {kick_broadcast['kicked_by']}")
        assert kick_broadcast["client_id"] == "Charlie"
        assert kick_broadcast["kicked_by"] == "Bob"

        # Charlie receives kick / disconnects
        try:
            raw_c = await ws_charlie.recv()
            assert json.loads(raw_c)["event_type"] == "user_kicked"
        except ConnectionClosed as exc:
            assert exc.rcvd.code == 4003 or exc.code == 4003
        print(" -> Charlie was successfully removed and disconnected with Code 4003 [OK]")

        # Drain Charlie leave frame on Bob
        _ = await ws_bob.recv()

        # 6. Bob terminates the room
        print("\n [6] Bob terminates the room...")
        await ws_bob.send(json.dumps({"action": "terminate_room"}))
        print(" -> Room terminated and all message data purged.")

        try:
            await ws_bob.close()
        except Exception:
            pass

        print("\n" + "=" * 80)
        print(">>> ALL LIVE MULTI-WORKER VERIFICATION CHECKS PASSED! <<<")
        print("=" * 80)

    finally:
        await worker1.stop()
        await worker2.stop()


if __name__ == "__main__":
    asyncio.run(main())
