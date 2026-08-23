"""Thread-safe, asyncio-locked local WebSocket connection manager."""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Set, Union
from fastapi import WebSocket
from pydantic import BaseModel

logger = logging.getLogger("chat.connection_manager")


class ConnectionManager:
    """Manages active local WebSocket connections partitioned by room_id.
    
    Guarantees thread-safe / coroutine-safe mutations via an asyncio.Lock.
    """

    def __init__(self) -> None:
        # room_id -> set of active WebSockets
        self._rooms: Dict[str, Set[WebSocket]] = {}
        # WebSocket -> metadata dict (room_id, client_id, connect_time)
        self._socket_info: Dict[WebSocket, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, room_id: str, client_id: str, websocket: WebSocket) -> int:
        """Register a new active WebSocket connection for a room.
        
        Returns the new total count of local active sockets for the room.
        """
        async with self._lock:
            if room_id not in self._rooms:
                self._rooms[room_id] = set()

            self._rooms[room_id].add(websocket)
            self._socket_info[websocket] = {
                "room_id": room_id,
                "client_id": client_id,
            }
            count = len(self._rooms[room_id])
            logger.debug("Socket registered: room=%s client=%s (local_active=%d)", room_id, client_id, count)
            return count

    async def disconnect(self, room_id: str, client_id: str, websocket: Optional[WebSocket] = None) -> int:
        """Unregister an existing WebSocket connection.
        
        Returns the remaining count of local active sockets for the room.
        """
        async with self._lock:
            if websocket is not None:
                if room_id in self._rooms:
                    self._rooms[room_id].discard(websocket)
                    if not self._rooms[room_id]:
                        del self._rooms[room_id]
                self._socket_info.pop(websocket, None)
            else:
                # Disconnect all sockets belonging to this client_id in room_id
                to_remove = [
                    ws for ws, info in self._socket_info.items()
                    if info.get("room_id") == room_id and info.get("client_id") == client_id
                ]
                for ws in to_remove:
                    if room_id in self._rooms:
                        self._rooms[room_id].discard(ws)
                        if not self._rooms[room_id]:
                            del self._rooms[room_id]
                    self._socket_info.pop(ws, None)

            count = len(self._rooms.get(room_id, set()))
            logger.debug("Socket unregistered: room=%s client=%s (local_active=%d)", room_id, client_id, count)
            return count

    async def disconnect_all(self) -> None:
        """Close and clear all active WebSocket connections across all rooms."""
        async with self._lock:
            all_sockets = list(self._socket_info.keys())
            self._rooms.clear()
            self._socket_info.clear()

        for ws in all_sockets:
            try:
                await ws.close(code=1001, reason="Server shutting down")
            except Exception:
                pass

    async def close_client_sockets(self, room_id: str, client_id: str, code: int = 1000, reason: str = "Kicked") -> None:
        """Close active sockets for a specific client_id in room_id."""
        async with self._lock:
            sockets_to_close = [
                ws for ws, info in self._socket_info.items()
                if info.get("room_id") == room_id and info.get("client_id") == client_id
            ]
        for ws in sockets_to_close:
            try:
                await ws.close(code=code, reason=reason)
            except Exception:
                pass

    async def broadcast_local(self, room_id: str, payload: Union[str, dict, BaseModel]) -> int:
        """Fan out a message to all local WebSocket connections subscribed to room_id.
        
        Dead or failed connections are automatically scheduled for eviction.
        Returns the number of successfully delivered sockets.
        """
        # Serialize payload
        if isinstance(payload, BaseModel):
            message_text = payload.model_dump_json()
        elif isinstance(payload, dict):
            message_text = json.dumps(payload)
        else:
            message_text = str(payload)

        # Snapshot sockets under lock to avoid race conditions during iteration
        async with self._lock:
            target_sockets = list(self._rooms.get(room_id, set()))

        if not target_sockets:
            return 0

        # Concurrently send to all local sockets in the room
        send_tasks = [self._safe_send(ws, message_text) for ws in target_sockets]
        results = await asyncio.gather(*send_tasks, return_exceptions=True)

        delivered = 0
        dead_sockets: List[WebSocket] = []

        for ws, res in zip(target_sockets, results):
            if isinstance(res, Exception) or res is False:
                dead_sockets.append(ws)
            else:
                delivered += 1

        # Clean up any dead sockets detected during send
        if dead_sockets:
            async with self._lock:
                for ws in dead_sockets:
                    if room_id in self._rooms:
                        self._rooms[room_id].discard(ws)
                        if not self._rooms[room_id]:
                            del self._rooms[room_id]
                    self._socket_info.pop(ws, None)
            logger.warning("Evicted %d dead socket(s) from room %s", len(dead_sockets), room_id)

        return delivered

    async def _safe_send(self, websocket: WebSocket, text: str) -> bool:
        """Safely send text to a websocket, returning True if successful."""
        try:
            await websocket.send_text(text)
            return True
        except Exception as exc:
            logger.debug("Failed sending to socket: %s", exc)
            return False

    def get_local_active_count(self, room_id: str) -> int:
        """Return the count of local active sockets for room_id."""
        return len(self._rooms.get(room_id, set()))

    def get_all_local_rooms(self) -> List[str]:
        """Return a list of all room IDs with active local connections."""
        return list(self._rooms.keys())

    def get_total_local_connections(self) -> int:
        """Return total active WebSocket connections across all rooms on this worker."""
        return len(self._socket_info)

