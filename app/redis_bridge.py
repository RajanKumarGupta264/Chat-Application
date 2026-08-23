"""Bi-directional Redis Pub/Sub Bridge with password-protected ephemeral rooms, presence ordering, banning, and World Chat."""

import asyncio
import hashlib
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from pydantic import BaseModel
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError, RedisError, TimeoutError

from app.config import Settings, get_settings
from app.connection_manager import ConnectionManager
from app.schemas import (
    CreatorTransferredEvent,
    EventType,
    MessageEvent,
    RoomDestroyedEvent,
    UserKickedEvent,
)

logger = logging.getLogger("chat.redis_bridge")


class RedisPubSubBridge:
    """Bi-directional Redis bridge for distributed multi-worker message fan-out and ephemeral room security."""

    def __init__(
        self,
        connection_manager: ConnectionManager,
        settings: Optional[Settings] = None,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.manager = connection_manager
        self.custom_redis = redis_client
        self._redis: Optional[aioredis.Redis] = redis_client
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._listener_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._is_connected = False
        self._channel_prefix = self.settings.CHANNEL_PREFIX
        self._presence_prefix = self.settings.PRESENCE_PREFIX
        self._presence_order_prefix = "chat:presence_order:"
        self._banned_prefix = "chat:banned:"
        self._meta_prefix = "chat:room_meta:"
        self._messages_prefix = "chat:messages:"
        # Local in-memory fallback storage when Redis is offline
        self._local_room_meta: Dict[str, Dict[str, Any]] = {}
        self._local_room_messages: Dict[str, List[Dict[str, Any]]] = {}
        self._local_presence_order: Dict[str, List[str]] = {}
        self._local_banned: Dict[str, Set[str]] = {}
        self._processed_msg_ids = set()

    @property
    def is_connected(self) -> bool:
        """Return boolean indicating active Redis connection."""
        return self._is_connected

    def _hash_password(self, room_id: str, password: str) -> str:
        """Derive a secure salted hash for room password verification."""
        salt = f"ephemeral_salt_{room_id}"
        return hashlib.sha256(f"{salt}:{password}".encode("utf-8")).hexdigest()

    async def start(self) -> None:
        """Initialize Redis connection and start the background listener coroutine."""
        if self._is_running:
            return

        self._is_running = True
        if self._redis is None:
            self._redis = aioredis.from_url(
                self.settings.REDIS_URL,
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                health_check_interval=15,
            )

        # Verify initial connectivity
        try:
            await self._redis.ping()
            self._is_connected = True
            logger.info("Connected to Redis backplane at %s", self.settings.REDIS_URL)
        except Exception as exc:
            self._is_connected = False
            logger.warning("Initial Redis connection failed (%s). Listener will retry in background.", exc)

        # Launch background listener task
        self._listener_task = asyncio.create_task(
            self._resilient_listener_loop(),
            name="redis_pubsub_listener",
        )

    async def stop(self) -> None:
        """Gracefully terminate listener task, unsubscribe, and close Redis connections."""
        self._is_running = False
        self._is_connected = False

        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None

        if self._pubsub:
            try:
                await self._pubsub.punsubscribe(f"{self._channel_prefix}*")
                if hasattr(self._pubsub, "aclose"):
                    await self._pubsub.aclose()
                else:
                    await self._pubsub.close()
            except Exception as exc:
                logger.debug("Error closing pubsub: %s", exc)
            self._pubsub = None

        if self._redis and not self.custom_redis:
            try:
                await self._redis.aclose()
            except Exception as exc:
                logger.debug("Error closing redis client: %s", exc)
            self._redis = None

        logger.info("Redis bridge stopped gracefully.")

    async def ban_user(self, room_id: str, client_id: str) -> None:
        """Record banned user to prevent them from rejoining the private room."""
        if room_id.lower() == "world":
            return

        normalized = client_id.strip().lower()
        if self._redis and self._is_connected:
            key = f"{self._banned_prefix}{room_id}"
            try:
                await self._redis.sadd(key, normalized)
                await self._redis.expire(key, 86400)
            except Exception as exc:
                logger.warning("Failed to ban user in Redis: %s", exc)

        self._local_banned.setdefault(room_id, set()).add(normalized)
        logger.info("User '%s' added to ban list for room '%s'", client_id, room_id)

    async def is_user_banned(self, room_id: str, client_id: str) -> bool:
        """Check if user has been kicked/banned from the room."""
        if room_id.lower() == "world":
            return False

        normalized = client_id.strip().lower()
        if self._redis and self._is_connected:
            key = f"{self._banned_prefix}{room_id}"
            try:
                is_banned = await self._redis.sismember(key, normalized)
                if is_banned:
                    return True
            except Exception as exc:
                logger.warning("Failed to check banned status in Redis: %s", exc)

        return normalized in self._local_banned.get(room_id, set())

    async def create_or_verify_room(
        self,
        room_id: str,
        password: Optional[str] = "",
        creator_id: str = "Anonymous",
    ) -> Tuple[bool, str, bool]:
        """Create room with password or verify credentials if room already exists.
        
        Returns: (success: bool, message: str, is_new: bool)
        """
        # World Chat exemption (open public channel)
        if room_id.lower() == "world":
            return True, "Welcome to World Chat.", False

        password_str = (password or "").strip()
        if not password_str:
            return False, "Password is required to create or join a private room.", False

        # Check ban list
        if await self.is_user_banned(room_id, creator_id):
            return False, "You have been removed from this room by the creator and cannot rejoin.", False

        target_hash = self._hash_password(room_id, password_str)
        meta_key = f"{self._meta_prefix}{room_id}"

        # 1. Check Redis if connected
        if self._redis and self._is_connected:
            try:
                existing_raw = await self._redis.get(meta_key)
                if existing_raw:
                    meta = json.loads(existing_raw)
                    if meta.get("password_hash") == target_hash:
                        return True, "Access granted.", False
                    else:
                        return False, "Incorrect password for this room.", False

                # Room does not exist -> Create new ephemeral room
                new_meta = {
                    "room_id": room_id,
                    "password_hash": target_hash,
                    "created_at": time.time(),
                    "created_by": creator_id,
                }
                # Set TTL (24 hours) as safety fallback
                await self._redis.set(meta_key, json.dumps(new_meta), ex=86400)
                logger.info("Created new ephemeral room '%s' by '%s'", room_id, creator_id)
                return True, "Room created successfully.", True
            except Exception as exc:
                logger.warning("Redis room metadata query failed (%s). Falling back to local store.", exc)

        # 2. Local in-memory fallback
        if room_id in self._local_room_meta:
            meta = self._local_room_meta[room_id]
            if meta.get("password_hash") == target_hash:
                return True, "Access granted.", False
            else:
                return False, "Incorrect password for this room.", False

        # Create locally
        self._local_room_meta[room_id] = {
            "room_id": room_id,
            "password_hash": target_hash,
            "created_at": time.time(),
            "created_by": creator_id,
        }
        logger.info("Created local ephemeral room '%s' by '%s'", room_id, creator_id)
        return True, "Room created successfully.", True

    async def verify_room_password(self, room_id: str, password: Optional[str], client_id: str = "") -> Tuple[bool, str]:
        """Verify whether the supplied password matches the existing room's password."""
        if room_id.lower() == "world":
            return True, "Access granted to World Chat."

        if client_id and await self.is_user_banned(room_id, client_id):
            return False, "You have been removed from this room by the creator and cannot rejoin."

        if not password:
            return False, "Password required to enter this room."

        target_hash = self._hash_password(room_id, password.strip())
        meta_key = f"{self._meta_prefix}{room_id}"

        if self._redis and self._is_connected:
            try:
                existing_raw = await self._redis.get(meta_key)
                if not existing_raw:
                    return False, "Room does not exist or has been destroyed."
                meta = json.loads(existing_raw)
                if meta.get("password_hash") == target_hash:
                    return True, "Access granted."
                return False, "Incorrect password."
            except Exception as exc:
                logger.warning("Redis verify failed: %s", exc)

        if room_id in self._local_room_meta:
            meta = self._local_room_meta[room_id]
            if meta.get("password_hash") == target_hash:
                return True, "Access granted."
            return False, "Incorrect password."

        return False, "Room does not exist or has been destroyed."

    async def get_room_creator(self, room_id: str) -> Optional[str]:
        """Retrieve creator username of the room."""
        if room_id.lower() == "world":
            return "World Server"

        meta_key = f"{self._meta_prefix}{room_id}"
        if self._redis and self._is_connected:
            try:
                existing_raw = await self._redis.get(meta_key)
                if existing_raw:
                    meta = json.loads(existing_raw)
                    return meta.get("created_by")
            except Exception as exc:
                logger.warning("Failed to get room creator from Redis: %s", exc)

        if room_id in self._local_room_meta:
            return self._local_room_meta[room_id].get("created_by")

        return None

    async def is_room_creator(self, room_id: str, client_id: str) -> bool:
        """Check whether client_id is the designated creator of room_id."""
        if room_id.lower() == "world":
            return False

        creator = await self.get_room_creator(room_id)
        if not creator or not client_id:
            return False
        return creator.strip().lower() == client_id.strip().lower()

    async def set_room_creator(self, room_id: str, new_creator_id: str) -> None:
        """Update the designated creator of room_id."""
        meta_key = f"{self._meta_prefix}{room_id}"
        if self._redis and self._is_connected:
            try:
                existing_raw = await self._redis.get(meta_key)
                if existing_raw:
                    meta = json.loads(existing_raw)
                    meta["created_by"] = new_creator_id
                    await self._redis.set(meta_key, json.dumps(meta), ex=86400)
            except Exception as exc:
                logger.warning("Failed to update room creator in Redis: %s", exc)

        if room_id in self._local_room_meta:
            self._local_room_meta[room_id]["created_by"] = new_creator_id

    async def save_message(self, room_id: str, message: MessageEvent) -> None:
        """Persist chat message to the room's message buffer (200 limit for world, 1000 for private)."""
        msg_json = message.model_dump_json()
        key = f"{self._messages_prefix}{room_id}"
        limit = 200 if room_id.lower() == "world" else 1000

        if self._redis and self._is_connected:
            try:
                await self._redis.rpush(key, msg_json)
                await self._redis.ltrim(key, -limit, -1)  # Keep latest limit messages
                await self._redis.expire(key, 86400)
            except Exception as exc:
                logger.warning("Failed to save message to Redis history for room '%s': %s", room_id, exc)

        # Also store in local buffer
        if room_id not in self._local_room_messages:
            self._local_room_messages[room_id] = []
        self._local_room_messages[room_id].append(message.model_dump())
        if len(self._local_room_messages[room_id]) > limit:
            self._local_room_messages[room_id] = self._local_room_messages[room_id][-limit:]

    async def get_room_history(self, room_id: str) -> List[MessageEvent]:
        """Fetch all stored chat messages for room_id in chronological order."""
        key = f"{self._messages_prefix}{room_id}"

        if self._redis and self._is_connected:
            try:
                raw_items = await self._redis.lrange(key, 0, -1)
                messages: List[MessageEvent] = []
                for item in raw_items:
                    try:
                        messages.append(MessageEvent.model_validate_json(item))
                    except Exception:
                        pass
                if messages:
                    return messages
            except Exception as exc:
                logger.warning("Failed to fetch message history from Redis: %s", exc)

        # Fallback to local memory buffer
        local_items = self._local_room_messages.get(room_id, [])
        result = []
        for item in local_items:
            try:
                result.append(MessageEvent.model_validate(item))
            except Exception:
                pass
        return result

    async def destroy_room(self, room_id: str, reason: Optional[str] = None) -> None:
        """Permanently delete room metadata, presence sets, banned sets, message history, and broadcast destruction."""
        if room_id.lower() == "world":
            return  # World chat is never destroyed

        meta_key = f"{self._meta_prefix}{room_id}"
        presence_key = f"{self._presence_prefix}{room_id}"
        order_key = f"{self._presence_order_prefix}{room_id}"
        banned_key = f"{self._banned_prefix}{room_id}"
        messages_key = f"{self._messages_prefix}{room_id}"

        # Clean Redis keys
        if self._redis and self._is_connected:
            try:
                await self._redis.delete(meta_key, presence_key, order_key, banned_key, messages_key)
                logger.info("[AUTO-DESTROY] Permanently erased Redis ephemeral room metadata, messages & presence for '%s'", room_id)
            except Exception as exc:
                logger.warning("Failed to delete Redis keys for room '%s': %s", room_id, exc)

        # Clean local memory
        self._local_room_meta.pop(room_id, None)
        self._local_room_messages.pop(room_id, None)
        self._local_presence_order.pop(room_id, None)
        self._local_banned.pop(room_id, None)

        # Publish ROOM_DESTROYED event to cluster
        destroy_event = RoomDestroyedEvent(
            room_id=room_id,
            worker_id=self.settings.WORKER_ID,
            message=reason or f"Room '{room_id}' has been permanently terminated and all chat data erased.",
        )
        await self.publish(room_id, destroy_event)
        logger.info("Room '%s' has been permanently erased from the cluster.", room_id)

    async def publish(self, room_id: str, payload: Union[BaseModel, dict, str]) -> int:
        """Publish payload to the Redis channel for room_id."""
        if isinstance(payload, BaseModel):
            raw_data = payload.model_dump_json()
        elif isinstance(payload, dict):
            raw_data = json.dumps(payload)
        else:
            raw_data = str(payload)

        channel = f"{self._channel_prefix}{room_id}"

        if self._redis and self._is_connected:
            try:
                receivers = await self._redis.publish(channel, raw_data)
                logger.debug("Published to %s -> %d receivers", channel, receivers)
                return receivers
            except Exception as exc:
                logger.warning(
                    "Redis publish failed (%s). Falling back to direct local broadcast for room %s.",
                    exc,
                    room_id,
                )
                self._is_connected = False
                return await self.manager.broadcast_local(room_id, raw_data)

        # Fallback to local broadcast when Redis is offline
        logger.debug("Redis not connected. Dispatching directly to local broadcast for room %s", room_id)
        return await self.manager.broadcast_local(room_id, raw_data)

    async def register_presence(self, room_id: str, client_id: str) -> Tuple[int, List[str]]:
        """Add client to distributed presence & order sets. Returns (active_count, ordered_members)."""
        join_timestamp = time.time()

        if self._redis and self._is_connected:
            key = f"{self._presence_prefix}{room_id}"
            order_key = f"{self._presence_order_prefix}{room_id}"
            try:
                await self._redis.sadd(key, client_id)
                await self._redis.expire(key, 86400)
                await self._redis.zadd(order_key, {client_id: join_timestamp})
                await self._redis.expire(order_key, 86400)
                count = int(await self._redis.scard(key))
                members = list(await self._redis.zrange(order_key, 0, -1))
                return count, members
            except Exception as exc:
                logger.warning("Failed to register presence in Redis for %s: %s", client_id, exc)

        # Local in-memory fallback
        order_list = self._local_presence_order.setdefault(room_id, [])
        if client_id not in order_list:
            order_list.append(client_id)
        count = len(order_list)
        return count, list(order_list)

    async def unregister_presence(self, room_id: str, client_id: str) -> Tuple[int, List[str], Optional[str]]:
        """Remove client from presence sets.
        
        If departing user was the creator, automatically transfers creator role sequentially to the next member.
        Returns: (remaining_count, remaining_members, new_creator_id)
        """
        new_creator: Optional[str] = None
        remaining_members: List[str] = []
        count = 0

        was_creator = await self.is_room_creator(room_id, client_id)

        if self._redis and self._is_connected:
            key = f"{self._presence_prefix}{room_id}"
            order_key = f"{self._presence_order_prefix}{room_id}"
            try:
                await self._redis.srem(key, client_id)
                await self._redis.zrem(order_key, client_id)
                count = int(await self._redis.scard(key))
                remaining_members = list(await self._redis.zrange(order_key, 0, -1))
            except Exception as exc:
                logger.warning("Failed to unregister presence in Redis for %s: %s", client_id, exc)
                count = self.manager.get_local_active_count(room_id)
        else:
            order_list = self._local_presence_order.get(room_id, [])
            if client_id in order_list:
                order_list.remove(client_id)
            remaining_members = list(order_list)
            count = len(remaining_members)

        # Check auto-destruction for private rooms
        if room_id.lower() != "world":
            if count <= 0:
                logger.info("Room '%s' has 0 remaining participants. Initiating auto-destruction...", room_id)
                await self.destroy_room(room_id)
            elif was_creator and remaining_members:
                # Sequential creator transfer to earliest-joined member
                new_creator = remaining_members[0]
                await self.set_room_creator(room_id, new_creator)
                logger.info("Room '%s': Creator role transferred from '%s' to '%s'", room_id, client_id, new_creator)
                transfer_event = CreatorTransferredEvent(
                    room_id=room_id,
                    new_creator_id=new_creator,
                    old_creator_id=client_id,
                    members=remaining_members,
                    worker_id=self.settings.WORKER_ID,
                    message=f"Creator role has been automatically transferred to {new_creator}.",
                )
                await self.publish(room_id, transfer_event)

        return count, remaining_members, new_creator

    async def get_room_members(self, room_id: str) -> List[str]:
        """Query ordered list of active member usernames in room_id."""
        if self._redis and self._is_connected:
            order_key = f"{self._presence_order_prefix}{room_id}"
            try:
                members = await self._redis.zrange(order_key, 0, -1)
                return list(members)
            except Exception as exc:
                logger.warning("Failed to query presence_order zrange: %s", exc)

        return list(self._local_presence_order.get(room_id, []))

    async def get_cluster_active_count(self, room_id: str) -> int:
        """Query total cluster-wide active member count for room_id."""
        if not self._redis or not self._is_connected:
            return self.manager.get_local_active_count(room_id)
        key = f"{self._presence_prefix}{room_id}"
        try:
            count = await self._redis.scard(key)
            return int(count)
        except Exception as exc:
            logger.warning("Failed to query presence scard: %s", exc)
            return self.manager.get_local_active_count(room_id)

    async def _resilient_listener_loop(self) -> None:
        """Continuous listener loop with exponential backoff for broker disconnections."""
        pattern = f"{self._channel_prefix}*"
        attempt = 0

        while self._is_running:
            try:
                if self._redis is None:
                    self._redis = aioredis.from_url(
                        self.settings.REDIS_URL,
                        decode_responses=True,
                        socket_timeout=5.0,
                        socket_connect_timeout=5.0,
                    )

                await self._redis.ping()
                self._is_connected = True
                # Clean any lingering pubsub subscription before creating a new one
                if self._pubsub:
                    try:
                        await self._pubsub.punsubscribe(pattern)
                        if hasattr(self._pubsub, "aclose"):
                            await self._pubsub.aclose()
                        else:
                            await self._pubsub.close()
                    except Exception:
                        pass
                    self._pubsub = None

                self._pubsub = self._redis.pubsub()
                await self._pubsub.psubscribe(pattern)
                logger.info("Subscribed to Redis pattern '%s'", pattern)

                while self._is_running:
                    message = await self._pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=1.0,
                    )

                    if message is not None:
                        await self._process_redis_message(message)

                    await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except (ConnectionError, TimeoutError, RedisError, Exception) as exc:
                self._is_connected = False
                if not self._is_running:
                    break

                attempt += 1
                delay = min(
                    self.settings.MAX_RECONNECT_DELAY,
                    self.settings.RECONNECT_BACKOFF_BASE * (1.5 ** (attempt - 1)),
                )
                logger.warning(
                    "Redis connection dropped (%s). Reconnecting in %.1fs (attempt %d)...",
                    exc,
                    delay,
                    attempt,
                )

                if self._pubsub:
                    try:
                        if hasattr(self._pubsub, "aclose"):
                            await self._pubsub.aclose()
                        else:
                            await self._pubsub.close()
                    except Exception:
                        pass
                    self._pubsub = None

                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break

    async def _process_redis_message(self, message: Dict[str, Any]) -> None:
        """Process inbound Redis PubSub frame, deduplicate, and fan out to local room connections."""
        try:
            msg_type = message.get("type")
            if msg_type not in ("pmessage", "message"):
                return

            channel = message.get("channel", "")
            data = message.get("data")
            if not data or not isinstance(data, str):
                return

            if channel.startswith(self._channel_prefix):
                room_id = channel[len(self._channel_prefix):]
            else:
                room_id = channel

            # Backend Deduplication by message_id
            try:
                parsed = json.loads(data)
                msg_id = parsed.get("message_id")
                if msg_id:
                    if msg_id in self._processed_msg_ids:
                        logger.debug("Suppressed duplicate Redis message ID: %s", msg_id)
                        return
                    self._processed_msg_ids.add(msg_id)
                    if len(self._processed_msg_ids) > 10000:
                        # Trim oldest entries
                        self._processed_msg_ids = set(list(self._processed_msg_ids)[5000:])
            except Exception:
                pass

            await self.manager.broadcast_local(room_id, data)

        except Exception as exc:
            logger.error("Error processing Redis message: %s", exc)
