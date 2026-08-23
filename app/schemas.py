"""Data schemas and typing definitions powered by Pydantic v2."""

import time
import uuid
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    """Enumeration of event types across WebSocket and Redis backplane."""
    CHAT_MESSAGE = "chat_message"
    USER_JOINED = "user_joined"
    USER_LEFT = "user_left"
    USER_KICKED = "user_kicked"
    PRESENCE_UPDATE = "presence_update"
    CREATOR_TRANSFERRED = "creator_transferred"
    HISTORY = "history"
    TYPING = "typing"
    PING = "ping"
    PONG = "pong"
    SYSTEM_NOTICE = "system_notice"
    ROOM_DESTROYED = "room_destroyed"
    ERROR = "error"


class BaseEvent(BaseModel):
    """Base event payload structure."""
    event_type: EventType
    timestamp: float = Field(default_factory=lambda: time.time())


class MessageEvent(BaseEvent):
    """Chat message event sent across the cluster."""
    event_type: Literal[EventType.CHAT_MESSAGE] = EventType.CHAT_MESSAGE
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    room_id: str
    sender_id: str
    content: str
    worker_id: Optional[str] = None
    client_sent_time: Optional[float] = None

    @field_validator("content")
    @classmethod
    def validate_content(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("Message content cannot be empty or whitespace only")
        return cleaned


class PresenceEvent(BaseEvent):
    """User presence event (join/leave) propagated across workers."""
    event_type: Literal[EventType.USER_JOINED, EventType.USER_LEFT]
    room_id: str
    client_id: str
    active_count: int
    members: List[str] = Field(default_factory=list)
    worker_id: Optional[str] = None


class PresenceUpdateEvent(BaseEvent):
    """Lightweight presence count and member list synchronization without chat feed notice."""
    event_type: Literal[EventType.PRESENCE_UPDATE] = EventType.PRESENCE_UPDATE
    room_id: str
    active_count: int
    members: List[str] = Field(default_factory=list)
    worker_id: Optional[str] = None


class TypingEvent(BaseEvent):
    """Real-time user typing status event propagated across room participants."""
    event_type: Literal[EventType.TYPING] = EventType.TYPING
    room_id: str
    client_id: str
    is_typing: bool
    worker_id: Optional[str] = None


class HistoryEvent(BaseEvent):
    """Event emitted to replay stored message history upon connection."""
    event_type: Literal[EventType.HISTORY] = EventType.HISTORY
    room_id: str
    creator_id: Optional[str] = None
    is_creator: bool = False
    members: List[str] = Field(default_factory=list)
    messages: List[MessageEvent] = Field(default_factory=list)


class CreatorTransferredEvent(BaseEvent):
    """Event emitted when the room creator role is automatically transferred."""
    event_type: Literal[EventType.CREATOR_TRANSFERRED] = EventType.CREATOR_TRANSFERRED
    room_id: str
    new_creator_id: str
    old_creator_id: Optional[str] = None
    members: List[str] = Field(default_factory=list)
    worker_id: Optional[str] = None
    message: str = "Creator role has been transferred."


class UserKickedEvent(BaseEvent):
    """Event emitted when a user is kicked by the room creator."""
    event_type: Literal[EventType.USER_KICKED] = EventType.USER_KICKED
    room_id: str
    client_id: str
    kicked_by: str
    reason: str = "Removed from room by creator."
    worker_id: Optional[str] = None


class RoomDestroyedEvent(BaseEvent):
    """Event emitted when a room is permanently destroyed."""
    event_type: Literal[EventType.ROOM_DESTROYED] = EventType.ROOM_DESTROYED
    room_id: str
    worker_id: Optional[str] = None
    message: str = "Room has been permanently destroyed."


class HeartbeatEvent(BaseEvent):
    """Heartbeat PING/PONG frame payload."""
    event_type: Literal[EventType.PING, EventType.PONG]
    client_id: Optional[str] = None
    worker_id: Optional[str] = None
    client_sent_time: Optional[float] = None
    server_time: float = Field(default_factory=lambda: time.time())


class SystemNoticeEvent(BaseEvent):
    """System notice or status advisory event."""
    event_type: Literal[EventType.SYSTEM_NOTICE] = EventType.SYSTEM_NOTICE
    room_id: Optional[str] = None
    message: str
    worker_id: Optional[str] = None


class ErrorEvent(BaseEvent):
    """Error event communicated to clients."""
    event_type: Literal[EventType.ERROR] = EventType.ERROR
    detail: str
    code: Optional[str] = None


class InboundClientPayload(BaseModel):
    """Typed parser for raw client frames received over WebSocket."""
    action: Optional[str] = "message"  # "message", "typing", "ping", "pong", "terminate_room", "kick_user"
    content: Optional[str] = None
    message_id: Optional[str] = None
    is_typing: Optional[bool] = None
    client_sent_time: Optional[float] = None
    target_client_id: Optional[str] = None
    reason: Optional[str] = None


# REST API Models for Room Management
class RoomCreateRequest(BaseModel):
    """Payload for creating a password-protected room."""
    room_id: str = Field(..., min_length=2, max_length=64)
    password: Optional[str] = Field(default="")
    created_by: Optional[str] = "Anonymous"


class RoomVerifyRequest(BaseModel):
    """Payload for validating a room ID and password."""
    room_id: str = Field(..., min_length=2, max_length=64)
    password: Optional[str] = Field(default="")
    client_id: Optional[str] = None


class RoomResponse(BaseModel):
    """Response returned for room creation or validation."""
    success: bool
    room_id: str
    message: str
    is_new: bool = False
    active_count: int = 0
    creator_id: Optional[str] = None
    is_creator: bool = False


# Discriminated union of all cluster broadcast payloads
ClusterEvent = Union[
    MessageEvent,
    PresenceEvent,
    PresenceUpdateEvent,
    TypingEvent,
    HistoryEvent,
    CreatorTransferredEvent,
    UserKickedEvent,
    RoomDestroyedEvent,
    HeartbeatEvent,
    SystemNoticeEvent,
    ErrorEvent,
]
