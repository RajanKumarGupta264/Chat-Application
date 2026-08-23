/**
 * Distributed Real-Time Chat Engine Client Application
 * Features:
 * - World Chat by default (unlimited members, quiet presence, 200-message limit)
 * - Private Room Pop-up Modal (Join & Create options)
 * - Clean Left Sidebar in Private Rooms (Active Members focused)
 * - Single prominent Exit button in chat header
 * - Creator-exclusive member kick & ban restrictions
 * - Ultra-low message latency (<45ms) without duplicate bubbles
 * - Multi-worker cluster synchronization across Stranger nodes
 */

// -------------------------------------------------------------
// GLOBAL APPLICATION STATE
// -------------------------------------------------------------

let socket = null;
let currentRoom = "world";
let currentPassword = "";
let currentUser = "";
let roomCreator = null;
let isRoomCreator = false;
let activeMembers = [];
let receivedCount = 0;
let typingTimeout = null;
let isTypingSent = false;
let currentMode = "world"; // "world" or "private"
let modalTab = "join"; // "join" or "create"

// Dedicated message caches to prevent data loss on tab switching
let worldMessages = [];
let privateMessages = [];

// Predefined palette for unique user colors
const SENDER_COLORS = [
    '#6366f1', '#10b981', '#f59e0b', '#ec4899', '#8b5cf6',
    '#06b6d4', '#3b82f6', '#14b8a6', '#f97316', '#a855f7'
];

function getSenderColor(name) {
    if (!name) return SENDER_COLORS[0];
    let hash = 0;
    for (let i = 0; i < name.length; i++) {
        hash = name.charCodeAt(i) + ((hash << 5) - hash);
    }
    const idx = Math.abs(hash) % SENDER_COLORS.length;
    return SENDER_COLORS[idx];
}

// -------------------------------------------------------------
// INITIALIZATION & USER IDENTIFICATION
// -------------------------------------------------------------

document.addEventListener("DOMContentLoaded", () => {
    // 1. Initialize user display name
    let storedUser = localStorage.getItem("chat_username");
    if (!storedUser) {
        const randId = Math.floor(1000 + Math.random() * 9000);
        storedUser = `Stranger_${randId}`;
        localStorage.setItem("chat_username", storedUser);
    }
    currentUser = storedUser;

    const userInput = document.getElementById("username-input");
    if (userInput) userInput.value = currentUser;

    const modalUserInput = document.getElementById("modal-username-input");
    if (modalUserInput) modalUserInput.value = currentUser;

    // 2. Check for active private room session in sessionStorage
    const savedRoom = sessionStorage.getItem("chat_active_room");
    const savedPass = sessionStorage.getItem("chat_active_password");

    if (savedRoom && savedPass) {
        // Restore Private Room Session
        currentMode = "private";
        applyUIMode("private", savedRoom);
        connectWebSocket(savedRoom, savedPass);
    } else {
        // Default: Connect to World Chat
        currentMode = "world";
        applyUIMode("world", "world");
        connectWebSocket("world", "");
    }

    // Enter key shortcuts
    document.getElementById("modal-password-input")?.addEventListener("keydown", (e) => {
        if (e.key === "Enter") submitPrivateRoomModal();
    });
    document.getElementById("modal-room-input")?.addEventListener("keydown", (e) => {
        if (e.key === "Enter") submitPrivateRoomModal();
    });
});

function updateUsername(newName) {
    const trimmed = (newName || "").trim();
    if (trimmed) {
        currentUser = trimmed;
        localStorage.setItem("chat_username", currentUser);
        const modalUserInput = document.getElementById("modal-username-input");
        if (modalUserInput) modalUserInput.value = currentUser;

        // Reconnect if currently active
        if (socket && socket.readyState === WebSocket.OPEN) {
            connectWebSocket(currentRoom, currentPassword);
        }
    }
}

// -------------------------------------------------------------
// UI MODE SWITCHING & LAYOUT RESTRICTIONS
// -------------------------------------------------------------

function onPrivateNavClick() {
    const savedRoom = sessionStorage.getItem("chat_active_room");
    const savedPass = sessionStorage.getItem("chat_active_password");

    if (savedRoom && savedPass && currentRoom === savedRoom && socket && socket.readyState === WebSocket.OPEN) {
        // Already connected to active private room
        switchToPrivateModeView();
    } else {
        // Open Private Room Pop-up Modal
        openPrivateModal();
    }
}

function switchToWorldChat() {
    if (currentRoom === "world" && socket && socket.readyState === WebSocket.OPEN) {
        return;
    }

    currentMode = "world";
    applyUIMode("world", "world");
    connectWebSocket("world", "");
}

function switchToPrivateModeView() {
    const savedRoom = sessionStorage.getItem("chat_active_room");
    if (!savedRoom) {
        openPrivateModal();
        return;
    }
    currentMode = "private";
    applyUIMode("private", savedRoom);
    renderCurrentFeed();
}

function applyUIMode(mode, roomId) {
    const isWorld = (mode === "world" || roomId === "world");

    const navWorld = document.getElementById("nav-world-btn");
    const navPrivate = document.getElementById("nav-private-btn");
    const navPrivateLabel = document.getElementById("nav-private-label");

    if (isWorld) {
        navWorld.classList.add("active");
        navPrivate.classList.remove("active");
        navPrivateLabel.textContent = "Private Room";

        // WORLD CHAT SIDEBAR LAYOUT
        document.getElementById("sidebar-top-section").style.display = "block";
        document.getElementById("sidebar-private-info").style.display = "none";
        document.getElementById("chat-exit-btn").style.display = "none";
        document.getElementById("badge-members-label").textContent = "Online:";
        document.getElementById("members-card-title").innerHTML = `👥 Online Users (<span id="members-count-badge">0</span>)`;

        document.getElementById("chat-header-room").textContent = "world";
        document.getElementById("room-lock-icon").textContent = "🌍";
        document.getElementById("stat-room").textContent = "World";
    } else {
        navWorld.classList.remove("active");
        navPrivate.classList.add("active");
        navPrivateLabel.textContent = `Room: ${roomId}`;

        // PRIVATE ROOM SIDEBAR LAYOUT: HIDE SECTION ABOVE ACTIVE MEMBERS
        document.getElementById("sidebar-top-section").style.display = "none";
        document.getElementById("sidebar-private-info").style.display = "block";
        document.getElementById("sidebar-room-name").textContent = roomId;
        document.getElementById("chat-exit-btn").style.display = "inline-flex"; // Single Exit Button!
        document.getElementById("badge-members-label").textContent = "Members:";
        document.getElementById("members-card-title").innerHTML = `👥 Active Members (<span id="members-count-badge">0</span>)`;

        document.getElementById("chat-header-room").textContent = roomId;
        document.getElementById("room-lock-icon").textContent = "🔒";
        document.getElementById("stat-room").textContent = roomId;
    }
}

// -------------------------------------------------------------
// PRIVATE ROOM POP-UP MODAL
// -------------------------------------------------------------

function openPrivateModal() {
    const modal = document.getElementById("private-modal");
    const errBox = document.getElementById("modal-error");
    if (errBox) errBox.style.display = "none";

    const modalUserInput = document.getElementById("modal-username-input");
    if (modalUserInput) modalUserInput.value = currentUser;

    modal.style.display = "flex";
    switchModalTab(modalTab);
}

function closePrivateModal() {
    const modal = document.getElementById("private-modal");
    if (modal) modal.style.display = "none";
}

function switchModalTab(tab) {
    modalTab = tab;
    const tabJoin = document.getElementById("modal-tab-join");
    const tabCreate = document.getElementById("modal-tab-create");
    const submitBtn = document.getElementById("modal-submit-btn");
    const genWrap = document.getElementById("modal-gen-wrap");
    const errBox = document.getElementById("modal-error");

    if (errBox) errBox.style.display = "none";

    if (tab === "create") {
        tabCreate.classList.add("active");
        tabJoin.classList.remove("active");
        submitBtn.textContent = "Create Private Room";
        genWrap.style.display = "inline";
    } else {
        tabJoin.classList.add("active");
        tabCreate.classList.remove("active");
        submitBtn.textContent = "Enter Private Room";
        genWrap.style.display = "none";
    }
}

function generateRandomModalRoom() {
    const adjectives = ["nebula", "cyber", "quantum", "stealth", "shadow", "apex", "solar", "matrix", "crypto"];
    const randWord = adjectives[Math.floor(Math.random() * adjectives.length)];
    const randNum = Math.floor(100 + Math.random() * 900);
    const roomInput = document.getElementById("modal-room-input");
    if (roomInput) roomInput.value = `${randWord}-${randNum}`;
}

function toggleModalPasswordVisibility() {
    const pwdInput = document.getElementById("modal-password-input");
    if (pwdInput) {
        pwdInput.type = (pwdInput.type === "password") ? "text" : "password";
    }
}

async function submitPrivateRoomModal() {
    const username = (document.getElementById("modal-username-input").value || "").trim();
    const roomId = (document.getElementById("modal-room-input").value || "").trim();
    const password = (document.getElementById("modal-password-input").value || "").trim();
    const submitBtn = document.getElementById("modal-submit-btn");

    if (!username) {
        showModalError("Please enter your display name.");
        return;
    }
    if (!roomId) {
        showModalError("Please enter a Room ID.");
        return;
    }
    if (roomId.toLowerCase() === "world") {
        showModalError("'world' is reserved for World Chat. Please choose another Room ID.");
        return;
    }
    if (!password) {
        showModalError("Room password is required.");
        return;
    }

    currentUser = username;
    localStorage.setItem("chat_username", currentUser);
    const mainUserInput = document.getElementById("username-input");
    if (mainUserInput) mainUserInput.value = currentUser;

    submitBtn.disabled = true;
    submitBtn.textContent = (modalTab === "create") ? "Creating Room..." : "Verifying Password...";

    try {
        const endpoint = (modalTab === "create") ? "/api/rooms/create" : "/api/rooms/verify";
        const res = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                room_id: roomId,
                password: password,
                created_by: currentUser,
                client_id: currentUser,
            })
        });

        const data = await res.json();
        if (!res.ok) {
            showModalError(data.detail || "Access rejected.");
            submitBtn.disabled = false;
            submitBtn.textContent = (modalTab === "create") ? "Create Private Room" : "Enter Private Room";
            return;
        }

        // Save session
        sessionStorage.setItem("chat_active_room", roomId);
        sessionStorage.setItem("chat_active_password", password);

        // Close modal and transition into private room
        closePrivateModal();
        currentMode = "private";
        applyUIMode("private", roomId);
        connectWebSocket(roomId, password);

        submitBtn.disabled = false;
        submitBtn.textContent = (modalTab === "create") ? "Create Private Room" : "Enter Private Room";
    } catch (err) {
        showModalError("Connection error: " + err.message);
        submitBtn.disabled = false;
        submitBtn.textContent = (modalTab === "create") ? "Create Private Room" : "Enter Private Room";
    }
}

function showModalError(msg) {
    const errBox = document.getElementById("modal-error");
    if (errBox) {
        errBox.textContent = msg;
        errBox.style.display = "block";
    }
}

// -------------------------------------------------------------
// WEBSOCKET CONNECTION & MANAGEMENT
// -------------------------------------------------------------

function connectWebSocket(roomId, password) {
    if (socket) {
        socket.onclose = null;
        socket.close();
        socket = null;
    }

    currentRoom = roomId;
    currentPassword = password;
    const isWorld = (roomId === "world");

    updateConnState("connecting");
    applyUIMode(isWorld ? "world" : "private", roomId);
    updateMembersCountBadge(0);

    // Render cached messages or placeholder
    renderCurrentFeed();
    appendSystemNotice(`Connecting to ${isWorld ? 'World Chat' : 'Private Room ' + roomId}...`);

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const host = window.location.host;
    const encodedUser = encodeURIComponent(currentUser);
    const passParam = password ? `?password=${encodeURIComponent(password)}` : "";
    const wsUrl = `${protocol}//${host}/ws/${encodeURIComponent(roomId)}/${encodedUser}${passParam}`;

    socket = new WebSocket(wsUrl);

    socket.onopen = () => {
        updateConnState("connected");
        document.getElementById("message-input").disabled = false;
        document.getElementById("send-btn").disabled = false;
    };

    socket.onmessage = (event) => {
        handleInboundFrame(event.data);
    };

    socket.onclose = (event) => {
        updateConnState("disconnected");
        document.getElementById("message-input").disabled = true;
        document.getElementById("send-btn").disabled = true;

        if (event.code === 4003) {
            alert("You have been removed from the room by the creator and cannot rejoin.");
            sessionStorage.removeItem("chat_active_room");
            sessionStorage.removeItem("chat_active_password");
            switchToWorldChat();
        } else if (event.code === 1008) {
            alert(event.reason || "Room policy violation or authorization error.");
            if (currentRoom !== "world") {
                sessionStorage.removeItem("chat_active_room");
                sessionStorage.removeItem("chat_active_password");
                switchToWorldChat();
            }
        }
    };

    socket.onerror = () => {
        updateConnState("disconnected");
    };
}

function exitCurrentRoom() {
    if (currentRoom === "world") return;
    if (confirm(`Leave room '${currentRoom}' and return to World Chat?`)) {
        sessionStorage.removeItem("chat_active_room");
        sessionStorage.removeItem("chat_active_password");
        switchToWorldChat();
    }
}

function confirmTerminateRoom() {
    if (!isRoomCreator) {
        alert("Only the room creator can permanently terminate this room.");
        return;
    }
    if (confirm(`⚠️ PERMANENT ACTION: Destroy room '${currentRoom}' and erase all chat data for everyone?`)) {
        if (socket && socket.readyState === WebSocket.OPEN) {
            socket.send(JSON.stringify({ action: "terminate_room" }));
        }
    }
}

function removeMember(targetClientId) {
    if (!isRoomCreator) {
        alert("Only the room creator can remove members.");
        return;
    }
    if (confirm(`Remove '${targetClientId}' from this room? They will not be allowed to rejoin.`)) {
        if (socket && socket.readyState === WebSocket.OPEN) {
            socket.send(JSON.stringify({
                action: "kick_user",
                target_client_id: targetClientId,
                reason: "Removed by creator."
            }));
        }
    }
}

// -------------------------------------------------------------
// FRAME DISPATCHER & EVENT HANDLERS
// -------------------------------------------------------------

function handleInboundFrame(rawData) {
    try {
        const data = JSON.parse(rawData);
        receivedCount++;
        document.getElementById("stat-received").textContent = receivedCount;

        const eventType = data.event_type;

        switch (eventType) {
            case "history":
                handleHistoryEvent(data);
                break;
            case "chat_message":
                handleChatMessage(data);
                break;
            case "presence_update":
                handlePresenceUpdate(data);
                break;
            case "user_joined":
                handleUserJoined(data);
                break;
            case "user_left":
                handleUserLeft(data);
                break;
            case "creator_transferred":
                handleCreatorTransferred(data);
                break;
            case "user_kicked":
                handleUserKicked(data);
                break;
            case "typing":
                handleTypingEvent(data);
                break;
            case "system_notice":
                let noticeMsg = data.message || "";
                if (currentRoom === "world") {
                    if (noticeMsg.includes("Active members") || noticeMsg.includes("ephemeral room")) {
                        noticeMsg = `Connected to World Chat [${data.worker_id || 'remote'}].`;
                    }
                }
                appendSystemNotice(noticeMsg);
                if (data.worker_id) {
                    document.getElementById("worker-display").textContent = data.worker_id;
                }
                break;
            case "room_destroyed":
                appendSystemNotice(`💥 ${data.message}`);
                sessionStorage.removeItem("chat_active_room");
                sessionStorage.removeItem("chat_active_password");
                setTimeout(() => switchToWorldChat(), 1500);
                break;
            case "error":
                appendSystemNotice(`⚠️ Error: ${data.detail}`);
                break;
            default:
                console.debug("Unhandled event:", data);
        }
    } catch (err) {
        console.error("Frame parse error:", err);
    }
}

function handleHistoryEvent(data) {
    // Creator info
    roomCreator = data.creator_id;
    isRoomCreator = Boolean(roomCreator && roomCreator.toLowerCase() === currentUser.toLowerCase());
    activeMembers = data.members || [];

    updateCreatorUI();
    updateMembersCountBadge(activeMembers.length);
    renderMembersList();

    const incomingMsgs = data.messages || [];
    if (currentRoom === "world") {
        worldMessages = incomingMsgs;
    } else {
        privateMessages = incomingMsgs;
    }

    renderCurrentFeed();
    if (incomingMsgs.length > 0) {
        appendSystemNotice(`📜 Replayed ${incomingMsgs.length} message(s).`);
    }
}

function handleChatMessage(data) {
    hideTypingIndicator(data.sender_id);

    const isWorld = (data.room_id === "world");
    const targetBuffer = isWorld ? worldMessages : privateMessages;

    // Cap World Chat to 200 messages
    targetBuffer.push(data);
    if (isWorld && targetBuffer.length > 200) {
        targetBuffer.splice(0, targetBuffer.length - 200);
    }

    // Only render to DOM if currently viewing that room
    if (data.room_id === currentRoom) {
        appendMessageBubble(data);
    }
}

function handlePresenceUpdate(data) {
    if (data.members) activeMembers = data.members;
    updateMembersCountBadge(data.active_count || activeMembers.length);
    renderMembersList();
}

function handleUserJoined(data) {
    if (data.members) activeMembers = data.members;
    else if (!activeMembers.includes(data.client_id)) activeMembers.push(data.client_id);

    updateMembersCountBadge(data.active_count || activeMembers.length);
    renderMembersList();

    if (currentRoom !== "world" && data.client_id.toLowerCase() !== currentUser.toLowerCase()) {
        appendSystemNotice(`👋 ${data.client_id} joined the room [${data.worker_id || 'remote'}].`);
    }
}

function handleUserLeft(data) {
    if (data.members) {
        activeMembers = data.members;
    } else {
        activeMembers = activeMembers.filter(m => m.toLowerCase() !== data.client_id.toLowerCase());
    }

    updateMembersCountBadge(data.active_count || activeMembers.length);
    renderMembersList();

    if (currentRoom !== "world") {
        appendSystemNotice(`🚪 ${data.client_id} left the room.`);
    }
}

function handleCreatorTransferred(data) {
    roomCreator = data.new_creator_id;
    isRoomCreator = Boolean(roomCreator && roomCreator.toLowerCase() === currentUser.toLowerCase());
    if (data.members) activeMembers = data.members;

    updateCreatorUI();
    updateMembersCountBadge(activeMembers.length);
    renderMembersList();

    if (isRoomCreator) {
        appendSystemNotice(`👑 You are now the Creator of room '${currentRoom}'!`);
    } else {
        appendSystemNotice(`👑 Creator role transferred to ${data.new_creator_id}.`);
    }
}

function handleUserKicked(data) {
    if (data.client_id.toLowerCase() === currentUser.toLowerCase()) {
        alert(`You were removed from room '${currentRoom}' by ${data.kicked_by} and cannot rejoin.`);
        sessionStorage.removeItem("chat_active_room");
        sessionStorage.removeItem("chat_active_password");
        switchToWorldChat();
    } else {
        activeMembers = activeMembers.filter(m => m.toLowerCase() !== data.client_id.toLowerCase());
        updateMembersCountBadge(activeMembers.length);
        renderMembersList();
        appendSystemNotice(`🚫 ${data.client_id} was removed from the room by ${data.kicked_by}.`);
    }
}

// -------------------------------------------------------------
// UI RENDERING & MEMBERS LIST
// -------------------------------------------------------------

function updateMembersCountBadge(count) {
    const isWorld = (currentRoom === "world");
    const val = (typeof count === "number" && count > 0) ? count : (activeMembers.length || 1);
    const displayEl = document.getElementById("members-display");
    const badgeEl = document.getElementById("members-count-badge");

    if (displayEl) {
        displayEl.textContent = isWorld ? `${val}` : `${val} / 20`;
    }
    if (badgeEl) {
        badgeEl.textContent = isWorld ? `${val}` : `${val}/20`;
    }
}

function updateCreatorUI() {
    const creatorBadge = document.getElementById("creator-badge");
    const termBtn = document.getElementById("terminate-btn");
    const creatorStat = document.getElementById("stat-creator");
    const sidebarCreator = document.getElementById("sidebar-creator-name");

    const isWorld = (currentRoom === "world");

    if (!isWorld) {
        if (creatorStat) creatorStat.textContent = roomCreator || "None";
        if (sidebarCreator) sidebarCreator.textContent = roomCreator || "Loading...";

        if (isRoomCreator) {
            if (creatorBadge) creatorBadge.style.display = "inline-flex";
            if (termBtn) termBtn.style.display = "block";
        } else {
            if (creatorBadge) creatorBadge.style.display = "none";
            if (termBtn) termBtn.style.display = "none";
        }
    } else {
        if (creatorBadge) creatorBadge.style.display = "none";
        if (termBtn) termBtn.style.display = "none";
        if (creatorStat) creatorStat.textContent = "World";
        if (sidebarCreator) sidebarCreator.textContent = "World Server";
    }
}

function renderMembersList() {
    const listEl = document.getElementById("members-list");
    if (!listEl) return;

    if (!activeMembers || activeMembers.length === 0) {
        listEl.innerHTML = `<div class="members-empty">No active members</div>`;
        return;
    }

    const isWorld = (currentRoom === "world");

    listEl.innerHTML = activeMembers.map((member) => {
        const isMe = (member.toLowerCase() === currentUser.toLowerCase());
        const isCreator = (!isWorld && roomCreator && member.toLowerCase() === roomCreator.toLowerCase());
        const color = getSenderColor(member);

        let badges = "";
        if (isCreator) badges += `<span class="member-creator-tag">👑 Creator</span>`;
        if (isMe) badges += `<span class="member-you-tag">You</span>`;

        let removeBtn = "";
        if (!isWorld && isRoomCreator && !isMe) {
            removeBtn = `<button class="btn-remove-member" onclick="removeMember('${escapeHtml(member)}')" title="Remove this member">✕ Remove</button>`;
        }

        return `
            <div class="member-row ${isMe ? 'member-row-me' : ''}">
                <div class="member-info">
                    <span class="member-dot" style="background-color: ${color};"></span>
                    <span class="member-name ${isMe ? 'me-name' : ''}">${escapeHtml(member)}</span>
                    ${badges}
                </div>
                ${removeBtn}
            </div>
        `;
    }).join("");
}

// -------------------------------------------------------------
// CHAT RENDERING & INSTANT MESSAGING
// -------------------------------------------------------------

function renderCurrentFeed() {
    const feed = document.getElementById("chat-feed");
    feed.innerHTML = "";

    const isWorld = (currentRoom === "world");
    const buffer = isWorld ? worldMessages : privateMessages;

    if (buffer.length === 0) {
        appendSystemNotice(`Welcome to ${isWorld ? 'World Chat' : 'Private Room ' + currentRoom}! Be the first to say hello.`);
    } else {
        buffer.forEach(msg => {
            appendMessageBubble(msg);
        });
    }
}

function sendMessage() {
    const input = document.getElementById("message-input");
    const text = input.value.trim();
    if (!text || !socket || socket.readyState !== WebSocket.OPEN) return;

    // Send WebSocket frame to cluster (< 5ms local dispatch)
    socket.send(JSON.stringify({
        action: "message",
        content: text,
        client_sent_time: Date.now(),
    }));

    input.value = "";
    input.focus();

    // Reset typing flag
    if (isTypingSent) {
        isTypingSent = false;
        socket.send(JSON.stringify({ action: "typing", is_typing: false }));
    }
}

function appendMessageBubble(data) {
    const feed = document.getElementById("chat-feed");
    if (!feed) return;

    const isMe = (data.sender_id || "").toLowerCase() === currentUser.toLowerCase();
    const isWorld = (currentRoom === "world");

    // Cap DOM message nodes in World Chat to 200
    if (isWorld) {
        const bubbles = feed.querySelectorAll(".message-bubble");
        if (bubbles.length >= 200) {
            bubbles[0].remove();
        }
    }

    const wrap = document.createElement("div");
    wrap.className = `message-bubble ${isMe ? 'outgoing' : 'incoming'}`;

    const timeStr = data.timestamp 
        ? new Date(data.timestamp * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        : new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

    const workerTag = data.worker_id ? `<span class="message-worker-tag">[${escapeHtml(data.worker_id)}]</span>` : "";

    let removeBtnHtml = "";
    if (!isWorld && isRoomCreator && !isMe) {
        removeBtnHtml = `<button class="msg-remove-btn" onclick="removeMember('${escapeHtml(data.sender_id)}')" title="Remove member">✕ Remove</button>`;
    }

    wrap.innerHTML = `
        <div class="message-meta">
            <span class="message-sender">${escapeHtml(data.sender_id || 'Unknown')}</span>
            ${workerTag}
            <span class="message-time">${timeStr}</span>
            ${removeBtnHtml}
        </div>
        <div class="message-content">${escapeHtml(data.content || '')}</div>
    `;

    feed.appendChild(wrap);
    scrollFeedToBottom();
}

function appendSystemNotice(text) {
    const feed = document.getElementById("chat-feed");
    if (!feed) return;

    const div = document.createElement("div");
    div.className = "system-bubble";
    div.innerHTML = `<span>ℹ️ ${escapeHtml(text)}</span>`;
    feed.appendChild(div);
    scrollFeedToBottom();
}

function scrollFeedToBottom() {
    const feed = document.getElementById("chat-feed");
    if (feed) feed.scrollTop = feed.scrollHeight;
}

function handleTypingInput() {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;

    if (!isTypingSent) {
        isTypingSent = true;
        socket.send(JSON.stringify({ action: "typing", is_typing: true }));
    }

    clearTimeout(typingTimeout);
    typingTimeout = setTimeout(() => {
        isTypingSent = false;
        if (socket && socket.readyState === WebSocket.OPEN) {
            socket.send(JSON.stringify({ action: "typing", is_typing: false }));
        }
    }, 1800);
}

function handleTypingEvent(data) {
    if (data.client_id.toLowerCase() === currentUser.toLowerCase()) return;

    const bar = document.getElementById("typing-indicator-bar");
    const text = document.getElementById("typing-text");

    if (data.is_typing) {
        text.textContent = `${data.client_id} is typing...`;
        bar.style.display = "flex";
    } else {
        bar.style.display = "none";
    }
}

function hideTypingIndicator(senderId) {
    const bar = document.getElementById("typing-indicator-bar");
    if (bar) bar.style.display = "none";
}

function updateConnState(state) {
    const dot = document.getElementById("conn-dot");
    const label = document.getElementById("conn-status");
    const transport = document.getElementById("stat-transport");

    if (state === "connected") {
        dot.className = "dot connected";
        label.textContent = "Connected";
        if (transport) transport.textContent = "Redis Cluster (Active)";
    } else if (state === "connecting") {
        dot.className = "dot connecting";
        label.textContent = "Connecting...";
        if (transport) transport.textContent = "Handshaking...";
    } else {
        dot.className = "dot disconnected";
        label.textContent = "Disconnected";
        if (transport) transport.textContent = "Offline";
    }
}

function toggleMobileSidebar(force) {
    const sidebar = document.getElementById("sidebar");
    const backdrop = document.getElementById("sidebar-backdrop");
    const open = (typeof force === "boolean") ? force : !sidebar.classList.contains("mobile-open");

    if (open) {
        sidebar.classList.add("mobile-open");
        backdrop.classList.add("active");
    } else {
        sidebar.classList.remove("mobile-open");
        backdrop.classList.remove("active");
    }
}

function escapeHtml(str) {
    if (!str) return "";
    return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}
