"""FastAPI Application & WebSocket Endpoints for Distributed Real-Time Chat Engine."""

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import redis.asyncio as aioredis

from app.config import STATIC_DIR, Settings, get_settings
from app.connection_manager import ConnectionManager
from app.redis_bridge import RedisPubSubBridge
from app.schemas import (
    ClusterEvent,
    ErrorEvent,
    EventType,
    HistoryEvent,
    InboundClientPayload,
    MessageEvent,
    PresenceEvent,
    PresenceUpdateEvent,
    RoomCreateRequest,
    RoomDestroyedEvent,
    RoomResponse,
    RoomVerifyRequest,
    SystemNoticeEvent,
    TypingEvent,
    UserKickedEvent,
)

logger = logging.getLogger("chat.main")


def create_app(
    settings: Optional[Settings] = None,
    redis_client: Optional[aioredis.Redis] = None,
) -> FastAPI:
    """Application factory for FastAPI instance with dependency injection."""
    app_settings = settings or get_settings()

    app = FastAPI(
        title=app_settings.APP_NAME,
        version="2.0.0",
        docs_url="/docs",
        redoc_url=None,
    )

    # Cross-Origin Resource Sharing (CORS)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Attach shared singleton state
    app.state.settings = app_settings
    app.state.start_time = time.time()
    app.state.connection_manager = ConnectionManager()
    app.state.redis_bridge = RedisPubSubBridge(
        connection_manager=app.state.connection_manager,
        settings=app_settings,
        redis_client=redis_client,
    )

    @app.on_event("startup")
    async def on_startup():
        """Initialize Redis connection and start background pub/sub bridge."""
        logging.basicConfig(level=getattr(logging, app_settings.LOG_LEVEL.upper(), logging.INFO))
        logger.info("Initializing ASGI Worker [%s] on port %d...", app_settings.WORKER_ID, app_settings.PORT)

        # Auto-boot embedded Redis broker if using 127.0.0.1 and port 6379 is available
        if "127.0.0.1" in app_settings.REDIS_URL or "localhost" in app_settings.REDIS_URL:
            import socket
            import threading
            from fakeredis import TcpFakeServer
            try:
                test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_sock.bind(("127.0.0.1", 6379))
                test_sock.close()
                server = TcpFakeServer(("127.0.0.1", 6379))
                t = threading.Thread(target=server.serve_forever, daemon=True)
                t.start()
                logger.info("Auto-started embedded Redis broker on 127.0.0.1:6379")
            except Exception:
                pass

        await app.state.redis_bridge.start()

    @app.on_event("shutdown")
    async def on_shutdown():
        """Gracefully disconnect all active sockets and terminate Redis bridge."""
        logger.info("Shutting down ASGI Worker [%s]...", app_settings.WORKER_ID)
        await app.state.redis_bridge.stop()
        await app.state.connection_manager.disconnect_all()

    # -------------------------------------------------------------
    # REST API ENDPOINTS
    # -------------------------------------------------------------

    @app.get("/health")
    async def health_check() -> Dict[str, Any]:
        """Health check endpoint reporting worker instance & Redis backplane status."""
        bridge: RedisPubSubBridge = app.state.redis_bridge
        manager: ConnectionManager = app.state.connection_manager
        return {
            "status": "healthy",
            "worker_id": app_settings.WORKER_ID,
            "port": app_settings.PORT,
            "redis_connected": bridge.is_connected,
            "uptime_seconds": time.time() - getattr(app.state, "start_time", time.time()),
            "local_rooms": manager.get_all_local_rooms(),
            "local_connections_count": manager.get_total_local_connections(),
        }

    @app.post("/api/rooms/create", response_model=RoomResponse)
    async def create_room_api(req: RoomCreateRequest) -> RoomResponse:
        """Create a new ephemeral room with a password."""
        room_id = req.room_id.strip()
        if room_id.lower() == "world":
            return RoomResponse(
                success=True,
                room_id="world",
                message="World Chat is an open public channel.",
                is_new=False,
                active_count=await app.state.redis_bridge.get_cluster_active_count("world"),
                creator_id="World Server",
                is_creator=False,
            )

        bridge: RedisPubSubBridge = app.state.redis_bridge

        if await bridge.is_user_banned(room_id, req.created_by or ""):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You have been removed from this room by the creator and cannot rejoin.",
            )

        success, msg, is_new = await bridge.create_or_verify_room(
            room_id=room_id,
            password=req.password,
            creator_id=req.created_by or "Anonymous",
        )

        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=msg,
            )

        active_count = await bridge.get_cluster_active_count(room_id)
        creator_id = await bridge.get_room_creator(room_id)
        is_creator = await bridge.is_room_creator(room_id, req.created_by or "")

        return RoomResponse(
            success=True,
            room_id=room_id,
            message=msg,
            is_new=is_new,
            active_count=active_count,
            creator_id=creator_id,
            is_creator=is_creator,
        )

    @app.post("/api/rooms/verify", response_model=RoomResponse)
    async def verify_room_api(req: RoomVerifyRequest) -> RoomResponse:
        """Validate room password before opening WebSocket connection."""
        room_id = req.room_id.strip()
        bridge: RedisPubSubBridge = app.state.redis_bridge

        if room_id.lower() == "world":
            active_count = await bridge.get_cluster_active_count("world")
            return RoomResponse(
                success=True,
                room_id="world",
                message="Access granted to World Chat.",
                is_new=False,
                active_count=active_count,
                creator_id="World Server",
                is_creator=False,
            )

        # Check ban list
        if req.client_id and await bridge.is_user_banned(room_id, req.client_id):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You have been removed from this room by the creator and cannot rejoin.",
            )

        # Check room capacity (Max 20 members for private rooms)
        active_count = await bridge.get_cluster_active_count(room_id)
        if active_count >= app_settings.MAX_ROOM_MEMBERS:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Room is full. Maximum {app_settings.MAX_ROOM_MEMBERS} members allowed.",
            )

        success, msg = await bridge.verify_room_password(room_id=room_id, password=req.password, client_id=req.client_id or "")

        if not success:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=msg,
            )

        creator_id = await bridge.get_room_creator(room_id)
        is_creator = await bridge.is_room_creator(room_id, req.client_id or "")

        return RoomResponse(
            success=True,
            room_id=room_id,
            message=msg,
            is_new=False,
            active_count=active_count,
            creator_id=creator_id,
            is_creator=is_creator,
        )

    # -------------------------------------------------------------
    # WEBSOCKET REAL-TIME DISPATCHER
    # -------------------------------------------------------------

    @app.websocket("/ws/{room_id}/{client_id}")
    async def websocket_endpoint(
        websocket: WebSocket,
        room_id: str,
        client_id: str,
        password: Optional[str] = None,
    ) -> None:
        """Core real-time bi-directional WebSocket handler."""
        room_id = room_id.strip()
        client_id = client_id.strip()

        if not room_id or not client_id:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        manager: ConnectionManager = app.state.connection_manager
        bridge: RedisPubSubBridge = app.state.redis_bridge
        is_world_chat = (room_id.lower() == "world")

        # 1. Authenticate Room Password & Ban Check (unless World Chat)
        if not is_world_chat:
            if await bridge.is_user_banned(room_id, client_id):
                logger.warning("Rejected banned user '%s' from entering room '%s'", client_id, room_id)
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="You have been removed from this room by the creator")
                return

            if password:
                auth_ok, auth_msg, _ = await bridge.create_or_verify_room(room_id, password, client_id)
                if not auth_ok:
                    logger.warning("Rejected unauthorized connection from '%s' to room '%s': %s", client_id, room_id, auth_msg)
                    await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid room password")
                    return
            else:
                auth_ok, auth_msg = await bridge.verify_room_password(room_id, "", client_id)
                if not auth_ok:
                    await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Password required")
                    return

            # 2. Check room capacity (Max 20 members for private rooms)
            current_members = await bridge.get_cluster_active_count(room_id)
            if current_members >= app_settings.MAX_ROOM_MEMBERS:
                logger.warning("Rejected connection from '%s' to room '%s': Room is full (%d/%d)", client_id, room_id, current_members, app_settings.MAX_ROOM_MEMBERS)
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=f"Room is full (Maximum {app_settings.MAX_ROOM_MEMBERS} members)")
                return

        # 3. Accept socket handshake
        await websocket.accept()

        # 4. Register local connection
        await manager.connect(room_id, client_id, websocket)

        # 5. Register presence and ordered members
        cluster_count, members_list = await bridge.register_presence(room_id, client_id)

        # 6. Broadcast presence event to cluster
        if not is_world_chat:
            # Private Room: send USER_JOINED notice to room
            join_event = PresenceEvent(
                event_type=EventType.USER_JOINED,
                room_id=room_id,
                client_id=client_id,
                active_count=cluster_count,
                members=members_list,
                worker_id=app_settings.WORKER_ID,
            )
            await bridge.publish(room_id, join_event)
        else:
            # World Chat: Quiet presence update (no chat spam, updates online counter)
            update_event = PresenceUpdateEvent(
                room_id="world",
                active_count=cluster_count,
                members=members_list,
                worker_id=app_settings.WORKER_ID,
            )
            await bridge.publish("world", update_event)

        # 7. Send welcome confirmation directly to connecting user
        if is_world_chat:
            welcome_msg = f"Connected to World Chat [{app_settings.WORKER_ID}]. Active members: {cluster_count}"
        else:
            welcome_msg = f"Connected to Private Room '{room_id}' [{app_settings.WORKER_ID}]. Active members: {cluster_count}/{app_settings.MAX_ROOM_MEMBERS}"

        welcome_notice = SystemNoticeEvent(
            room_id=room_id,
            message=welcome_msg,
            worker_id=app_settings.WORKER_ID,
        )
        await websocket.send_text(welcome_notice.model_dump_json())

        # 8. Send full chat history and creator role info
        creator_id = await bridge.get_room_creator(room_id)
        is_creator = await bridge.is_room_creator(room_id, client_id)
        room_history = await bridge.get_room_history(room_id)

        history_event = HistoryEvent(
            room_id=room_id,
            creator_id=creator_id,
            is_creator=is_creator,
            members=members_list,
            messages=room_history,
        )
        await websocket.send_text(history_event.model_dump_json())

        try:
            while True:
                # Receive raw frame from client
                raw_text = await websocket.receive_text()

                try:
                    payload_dict = json.loads(raw_text)
                    payload = InboundClientPayload.model_validate(payload_dict)
                except Exception as parse_err:
                    error_event = SystemNoticeEvent(
                        room_id=room_id,
                        message=f"Invalid message format: {str(parse_err)}",
                        worker_id=app_settings.WORKER_ID,
                    )
                    await websocket.send_text(error_event.model_dump_json())
                    continue

                action = (payload.action or "message").lower()

                # Action: CHAT MESSAGE (Optimized for <= 45ms latency)
                if action == "message" and payload.content:
                    msg_text = payload.content[:app_settings.MAX_MESSAGE_LENGTH]
                    msg_event = MessageEvent(
                        message_id=payload.message_id or str(uuid.uuid4()),
                        room_id=room_id,
                        sender_id=client_id,
                        content=msg_text,
                        worker_id=app_settings.WORKER_ID,
                        client_sent_time=payload.client_sent_time,
                    )
                    # Concurrently persist and fan out
                    await asyncio.gather(
                        bridge.save_message(room_id, msg_event),
                        bridge.publish(room_id, msg_event),
                    )

                # Action: TYPING STATUS
                elif action == "typing":
                    typing_status = bool(payload.is_typing)
                    typing_event = TypingEvent(
                        room_id=room_id,
                        client_id=client_id,
                        is_typing=typing_status,
                        worker_id=app_settings.WORKER_ID,
                    )
                    await bridge.publish(room_id, typing_event)

                # Action: KICK / REMOVE MEMBER (Creator Only)
                elif action == "kick_user" and payload.target_client_id:
                    target_client = payload.target_client_id.strip()
                    if not await bridge.is_room_creator(room_id, client_id):
                        err_evt = ErrorEvent(detail="Permission denied: Only the room creator can remove members.")
                        await websocket.send_text(err_evt.model_dump_json())
                        continue

                    if target_client.lower() == client_id.lower():
                        err_evt = ErrorEvent(detail="You cannot remove yourself from the room.")
                        await websocket.send_text(err_evt.model_dump_json())
                        continue

                    # Record ban in Redis so user cannot rejoin
                    await bridge.ban_user(room_id, target_client)

                    # Broadcast UserKickedEvent across the cluster
                    kick_event = UserKickedEvent(
                        room_id=room_id,
                        client_id=target_client,
                        kicked_by=client_id,
                        reason=payload.reason or "Removed by creator.",
                        worker_id=app_settings.WORKER_ID,
                    )
                    await bridge.publish(room_id, kick_event)

                    # Disconnect target client socket if connected on this worker
                    await manager.close_client_sockets(room_id, target_client, code=4003, reason="Removed by creator")
                    logger.info("User '%s' kicked and banned from room '%s' by creator '%s'", target_client, room_id, client_id)

                # Action: TERMINATE ROOM (Creator Only)
                elif action == "terminate_room":
                    if not await bridge.is_room_creator(room_id, client_id):
                        err_evt = ErrorEvent(detail="Permission denied: Only the room creator can terminate the room.")
                        await websocket.send_text(err_evt.model_dump_json())
                        continue

                    logger.info("Room '%s' terminated by creator '%s'", room_id, client_id)
                    await bridge.destroy_room(room_id, reason=f"Room '{room_id}' was terminated by creator {client_id}.")

                # Action: HEARTBEAT PING
                elif action == "ping":
                    pong_frame = {
                        "event_type": "pong",
                        "client_id": client_id,
                        "worker_id": app_settings.WORKER_ID,
                        "client_sent_time": payload.client_sent_time,
                    }
                    await websocket.send_text(json.dumps(pong_frame))

        except WebSocketDisconnect:
            logger.info("WebSocket disconnected: room=%s client=%s", room_id, client_id)
        except Exception as err:
            logger.warning("WebSocket loop exception for %s in %s: %s", client_id, room_id, err)
        finally:
            await manager.disconnect(room_id, client_id)
            remaining_count, remaining_members, new_creator = await bridge.unregister_presence(room_id, client_id)

            if not is_world_chat:
                if remaining_count > 0:
                    leave_event = PresenceEvent(
                        event_type=EventType.USER_LEFT,
                        room_id=room_id,
                        client_id=client_id,
                        active_count=remaining_count,
                        members=remaining_members,
                        worker_id=app_settings.WORKER_ID,
                    )
                    await bridge.publish(room_id, leave_event)
                else:
                    logger.info("All users have departed room '%s'. Room and all ephemeral data have been permanently erased.", room_id)
            else:
                # World Chat: quiet presence update on departure
                update_event = PresenceUpdateEvent(
                    room_id="world",
                    active_count=remaining_count,
                    members=remaining_members,
                    worker_id=app_settings.WORKER_ID,
                )
                await bridge.publish("world", update_event)

    # -------------------------------------------------------------
    # STATIC ASSET & SPA SERVING
    # -------------------------------------------------------------

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/")
    async def get_index() -> FileResponse:
        """Serve the responsive SPA HTML interface."""
        index_file = STATIC_DIR / "index.html"
        if not index_file.exists():
            raise HTTPException(status_code=404, detail="Index HTML not found.")
        return FileResponse(
            index_file,
            media_type="text/html",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

    return app


# Default ASGI entrypoint
app = create_app()
