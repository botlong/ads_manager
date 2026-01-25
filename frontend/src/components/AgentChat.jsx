import React, { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import { MessageSquare, X, Menu, Plus, Send, Trash2, Bot, Edit, Check, Sparkles } from 'lucide-react';

export default function AgentChat() {
    const [isOpen, setIsOpen] = useState(false);
    const [conversations, setConversations] = useState([]);
    const [currentId, setCurrentId] = useState(null);
    const [input, setInput] = useState("");
    const [loading, setLoading] = useState(false);
    const [showSidebar, setShowSidebar] = useState(false);

    // --- Rename State ---
    const [editingId, setEditingId] = useState(null);
    const [editTitle, setEditTitle] = useState("");

    // --- Window Size State ---
    const [windowSize, setWindowSize] = useState({ width: 450, height: 650 }); // Increased size

    const messagesEndRef = useRef(null);

    // Load from localStorage on mount
    useEffect(() => {
        const saved = localStorage.getItem('agent_conversations');
        if (saved) {
            try {
                const parsed = JSON.parse(saved);
                setConversations(parsed);
                if (parsed.length > 0) {
                    setCurrentId(parsed[0].id);
                }
            } catch (e) {
                console.error("Failed to parse conversations", e);
                createNewChat();
            }
        } else {
            createNewChat();
        }
    }, []);

    // Save to localStorage whenever conversations change
    useEffect(() => {
        if (conversations.length > 0) {
            localStorage.setItem('agent_conversations', JSON.stringify(conversations));
        }
    }, [conversations]);

    const getCurrentChat = () => conversations.find(c => c.id === currentId) || { messages: [] };

    const createNewChat = () => {
        const newChat = {
            id: Date.now(),
            title: `Analysis ${new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`,
            messages: [{ role: 'agent', content: '你好！我是广告诊断专家。\n\n**我可以帮你**：\n- 扫描所有广告系列，发现异常\n- 深度分析具体的 Performance Max 或 Search 广告系列\n\n请告诉我你需要什么？例如：\n- "扫描所有广告系列"\n- "分析 Sales-Performance Max-UK"' }]
        };
        setConversations(prev => [newChat, ...prev]);
        setCurrentId(newChat.id);
        setShowSidebar(false);
    };

    const deleteChat = (e, id) => {
        e.stopPropagation();
        const filtered = conversations.filter(c => c.id !== id);
        setConversations(filtered);
        if (id === currentId) {
            setCurrentId(filtered.length > 0 ? filtered[0].id : null);
        }
        localStorage.setItem('agent_conversations', JSON.stringify(filtered));
    };

    // --- Rename Logic ---
    const startEditing = (e, chat) => {
        e.stopPropagation();
        setEditingId(chat.id);
        setEditTitle(chat.title);
    };

    const saveTitle = (e) => {
        e.stopPropagation();
        if (editingId) {
            const updated = conversations.map(c =>
                c.id === editingId ? { ...c, title: editTitle } : c
            );
            setConversations(updated);
            setEditingId(null);
            setEditTitle("");
        }
    };

    const cancelEditing = (e) => {
        e.stopPropagation();
        setEditingId(null);
        setEditTitle("");
    };

    const sendMessage = async () => {
        if (!input.trim() || !currentId) return;

        const userMsg = { role: 'user', content: input };

        // Optimistically update UI with user message
        setConversations(prev => prev.map(c => {
            if (c.id === currentId) {
                return { ...c, messages: [...c.messages, userMsg] };
            }
            return c;
        }));

        const currentInput = input;
        setInput("");
        setLoading(true);

        // Prepare History
        // Prepare History (Send structured messages instead of string)
        const currentMsgs = conversations.find(c => c.id === currentId)?.messages || [];
        const messageHistory = currentMsgs.map(m => ({ role: m.role, content: m.content })).slice(-10); // increased context to 10

        try {
            const response = await fetch('http://localhost:8000/api/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: currentInput, messages: messageHistory })
            });

            if (!response.body) throw new Error("No response body");

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let agentMsgContent = "";

            // Add empty agent message to start streaming into
            setConversations(prev => prev.map(c => {
                if (c.id === currentId) {
                    return { ...c, messages: [...c.messages, { role: 'agent', content: "" }] };
                }
                return c;
            }));

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                const chunk = decoder.decode(value, { stream: true });
                agentMsgContent += chunk;

                // Update the last message (agent's) with new content
                setConversations(prev => prev.map(c => {
                    if (c.id === currentId) {
                        const msgs = [...c.messages];
                        msgs[msgs.length - 1] = { role: 'agent', content: agentMsgContent };
                        return { ...c, messages: msgs };
                    }
                    return c;
                }));
            }

        } catch (e) {
            console.error(e);
            setConversations(prev => prev.map(c => {
                if (c.id === currentId) {
                    return { ...c, messages: [...c.messages, { role: 'agent', content: "Error: Could not connect to backend." }] };
                }
                return c;
            }));
        }
        setLoading(false);
    };

    // --- Improved Scroll Logic ---
    const scrollContainerRef = useRef(null);
    const shouldAutoScroll = useRef(true);

    const onScroll = () => {
        if (!scrollContainerRef.current) return;
        const { scrollTop, scrollHeight, clientHeight } = scrollContainerRef.current;
        // If user is within 50px of the bottom, enable auto-scroll. Otherwise disable it.
        shouldAutoScroll.current = scrollHeight - scrollTop - clientHeight < 50;
    };

    const scrollToBottom = (force = false) => {
        if (shouldAutoScroll.current || force) {
            messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
        }
    };

    useEffect(() => {
        scrollToBottom();
    }, [conversations, currentId, isOpen]);

    // --- Drag (Move) Logic ---
    const [position, setPosition] = useState(() => ({
        x: window.innerWidth - 470, // Adjusted for wider window
        y: window.innerHeight - 700 // Adjusted for taller window
    }));

    const isDragging = useRef(false);
    const hasMoved = useRef(false);
    const dragOffset = useRef({ x: 0, y: 0 });

    const onMouseDown = (e) => {
        isDragging.current = true;
        hasMoved.current = false;
        dragOffset.current = {
            x: e.clientX - position.x,
            y: e.clientY - position.y
        };
        document.addEventListener('mousemove', onMouseMove);
        document.addEventListener('mouseup', onMouseUp);
        e.preventDefault();
    };

    const onMouseMove = (e) => {
        if (!isDragging.current) return;
        hasMoved.current = true;

        let newX = e.clientX - dragOffset.current.x;
        let newY = e.clientY - dragOffset.current.y;

        const currentWidth = isOpen ? windowSize.width : 56;

        if (newY < 0) newY = 0;
        if (newX < -currentWidth + 20) newX = -currentWidth + 20;
        if (newX > window.innerWidth - 20) newX = window.innerWidth - 20;
        if (newY > window.innerHeight - 20) newY = window.innerHeight - 20;

        setPosition({ x: newX, y: newY });
    };

    const onMouseUp = () => {
        isDragging.current = false;
        document.removeEventListener('mousemove', onMouseMove);
        document.removeEventListener('mouseup', onMouseUp);
    };

    // --- Advanced Resize Logic ---
    const isResizing = useRef(false);
    const resizeDir = useRef(null); // 'n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw'
    const resizeStart = useRef({ x: 0, y: 0, w: 0, h: 0, l: 0, t: 0 });

    const onResizeMouseDown = (e, direction) => {
        e.preventDefault();
        e.stopPropagation();
        isResizing.current = true;
        resizeDir.current = direction;

        resizeStart.current = {
            x: e.clientX,
            y: e.clientY,
            w: windowSize.width,
            h: windowSize.height,
            l: position.x,
            t: position.y
        };

        document.addEventListener('mousemove', onResizeMouseMove);
        document.addEventListener('mouseup', onResizeMouseUp);
    };

    const onResizeMouseMove = (e) => {
        if (!isResizing.current) return;

        const deltaX = e.clientX - resizeStart.current.x;
        const deltaY = e.clientY - resizeStart.current.y;

        let newWidth = resizeStart.current.w;
        let newHeight = resizeStart.current.h;
        let newLeft = resizeStart.current.l;
        let newTop = resizeStart.current.t;

        const dir = resizeDir.current;

        // X-axis (Width & Left)
        if (dir.includes('e')) {
            newWidth = Math.max(300, resizeStart.current.w + deltaX);
        } else if (dir.includes('w')) {
            const proposedWidth = resizeStart.current.w - deltaX;
            if (proposedWidth >= 300) {
                newWidth = proposedWidth;
                newLeft = resizeStart.current.l + deltaX;
            }
        }

        // Y-axis (Height & Top)
        if (dir.includes('s')) {
            newHeight = Math.max(400, resizeStart.current.h + deltaY);
        } else if (dir.includes('n')) {
            const proposedHeight = resizeStart.current.h - deltaY;
            if (proposedHeight >= 400) {
                newHeight = proposedHeight;
                newTop = resizeStart.current.t + deltaY;
            }
        }

        setWindowSize({ width: newWidth, height: newHeight });
        setPosition({ x: newLeft, y: newTop });
    };

    const onResizeMouseUp = () => {
        isResizing.current = false;
        document.removeEventListener('mousemove', onResizeMouseMove);
        document.removeEventListener('mouseup', onResizeMouseUp);
    };


    const toggleChat = () => {
        if (!hasMoved.current) {
            if (!isOpen) {
                let newX = position.x;
                let newY = position.y;
                if (newX + windowSize.width > window.innerWidth) newX = window.innerWidth - (windowSize.width + 10);
                if (newY + windowSize.height > window.innerHeight) newY = window.innerHeight - (windowSize.height + 10);
                if (newY < 0) newY = 0;
                if (newX < 0) newX = 10;
                setPosition({ x: newX, y: newY });
            }
            setIsOpen(!isOpen);
        }
    };

    // Styles
    const widgetStyle = {
        position: 'fixed',
        left: position.x,
        top: position.y,
        zIndex: 10000,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'flex-start',
    };

    const windowStyle = {
        width: `${windowSize.width}px`,
        height: `${windowSize.height}px`,
        backgroundColor: '#ffffff',
        borderRadius: '16px',
        boxShadow: '0 10px 25px -5px rgba(0, 0, 0, 0.1), 0 8px 10px -6px rgba(0, 0, 0, 0.1)',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
        border: '1px solid #e2e8f0',
        transition: isResizing.current ? 'none' : 'opacity 0.2s ease',
        position: 'relative'
    };

    const headerStyle = {
        padding: '12px 16px',
        background: 'linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%)',
        color: 'white',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        cursor: 'move',
        userSelect: 'none',
        borderBottom: '1px solid rgba(255,255,255,0.1)'
    };

    // Resize Handle Style Helper
    const handleStyle = (pos, cursor) => ({
        position: 'absolute',
        ...pos,
        zIndex: 2000,
        backgroundColor: 'transparent', // Make visible for debugging with 'rgba(255,0,0,0.3)'
        cursor: cursor
    });

    return (
        <div style={widgetStyle}>
            {isOpen && (
                <div style={windowStyle} className="chat-window-container">

                    {/* Resize Handles - 8 Directions */}
                    {/* Corners */}
                    <div onMouseDown={(e) => onResizeMouseDown(e, 'nw')} style={handleStyle({ top: 0, left: 0, width: '15px', height: '15px' }, 'nw-resize')} />
                    <div onMouseDown={(e) => onResizeMouseDown(e, 'ne')} style={handleStyle({ top: 0, right: 0, width: '15px', height: '15px' }, 'ne-resize')} />
                    <div onMouseDown={(e) => onResizeMouseDown(e, 'sw')} style={handleStyle({ bottom: 0, left: 0, width: '15px', height: '15px' }, 'sw-resize')} />
                    <div onMouseDown={(e) => onResizeMouseDown(e, 'se')} style={handleStyle({ bottom: 0, right: 0, width: '15px', height: '15px' }, 'se-resize')} />

                    {/* Edges */}
                    <div onMouseDown={(e) => onResizeMouseDown(e, 'n')} style={handleStyle({ top: 0, left: '15px', right: '15px', height: '8px' }, 'n-resize')} />
                    <div onMouseDown={(e) => onResizeMouseDown(e, 's')} style={handleStyle({ bottom: 0, left: '15px', right: '15px', height: '8px' }, 's-resize')} />
                    <div onMouseDown={(e) => onResizeMouseDown(e, 'w')} style={handleStyle({ left: 0, top: '15px', bottom: '15px', width: '8px' }, 'w-resize')} />
                    <div onMouseDown={(e) => onResizeMouseDown(e, 'e')} style={handleStyle({ right: 0, top: '15px', bottom: '15px', width: '8px' }, 'e-resize')} />

                    {/* Sidebar Overlay */}
                    {showSidebar && (
                        <>
                            <div
                                onClick={() => setShowSidebar(false)}
                                style={{
                                    position: 'absolute',
                                    top: 0, left: 0, right: 0, bottom: 0,
                                    backgroundColor: 'rgba(0,0,0,0.3)',
                                    zIndex: 19
                                }}
                            />
                            <div style={{
                                position: 'absolute',
                                top: 0, left: 0, bottom: 0,
                                width: '240px',
                                backgroundColor: '#f8fafc',
                                borderRight: '1px solid #e2e8f0',
                                zIndex: 20,
                                display: 'flex',
                                flexDirection: 'column',
                                boxShadow: '4px 0 15px rgba(0,0,0,0.1)'
                            }}>
                                <div style={{ padding: '16px', borderBottom: '1px solid #e2e8f0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    <span style={{ fontWeight: 600, color: '#334155' }}>History</span>
                                    <div style={{ display: 'flex', gap: '8px' }}>
                                        <button onClick={createNewChat} title="New Chat" style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#3b82f6' }}>
                                            <Plus size={20} />
                                        </button>
                                        <button onClick={() => setShowSidebar(false)} title="Close Sidebar" style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#64748b' }}>
                                            <X size={20} />
                                        </button>
                                    </div>
                                </div>
                                <div style={{ flex: 1, overflowY: 'auto' }}>
                                    {conversations.map(c => (
                                        <div
                                            key={c.id}
                                            onClick={() => { setCurrentId(c.id); setShowSidebar(false); }}
                                            style={{
                                                padding: '12px 16px',
                                                cursor: 'pointer',
                                                backgroundColor: c.id === currentId ? '#e0f2fe' : 'transparent',
                                                borderLeft: c.id === currentId ? '3px solid #3b82f6' : '3px solid transparent',
                                                display: 'flex',
                                                justifyContent: 'space-between',
                                                alignItems: 'center',
                                                minHeight: '44px'
                                            }}
                                        >
                                            {editingId === c.id ? (
                                                <div style={{ display: 'flex', alignItems: 'center', gap: '4px', width: '100%' }} onClick={e => e.stopPropagation()}>
                                                    <input
                                                        value={editTitle}
                                                        onChange={e => setEditTitle(e.target.value)}
                                                        onKeyDown={e => e.key === 'Enter' && saveTitle(e)}
                                                        autoFocus
                                                        style={{ width: '100%', fontSize: '0.85rem', padding: '2px 4px', border: '1px solid #3b82f6', borderRadius: '4px' }}
                                                    />
                                                    <button onClick={saveTitle} style={{ border: 'none', background: 'none', color: '#16a34a', cursor: 'pointer', padding: 0 }}><Check size={14} /></button>
                                                    <button onClick={cancelEditing} style={{ border: 'none', background: 'none', color: '#ef4444', cursor: 'pointer', padding: 0 }}><X size={14} /></button>
                                                </div>
                                            ) : (
                                                <>
                                                    <span style={{ fontSize: '0.9rem', color: '#475569', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '120px' }}>
                                                        {c.title}
                                                    </span>
                                                    <div style={{ display: 'flex', gap: '4px' }}>
                                                        <button
                                                            onClick={(e) => startEditing(e, c)}
                                                            title="Rename"
                                                            style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#94a3b8', padding: 0 }}
                                                        >
                                                            <Edit size={14} />
                                                        </button>
                                                        <button
                                                            onClick={(e) => deleteChat(e, c.id)}
                                                            title="Delete"
                                                            style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#94a3b8', padding: 0 }}
                                                        >
                                                            <Trash2 size={14} />
                                                        </button>
                                                    </div>
                                                </>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </>
                    )}

                    {/* Header */}
                    <div style={headerStyle} onMouseDown={onMouseDown}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <button
                                onClick={() => setShowSidebar(!showSidebar)}
                                onMouseDown={e => e.stopPropagation()}
                                style={{ background: 'rgba(255,255,255,0.2)', border: 'none', borderRadius: '4px', padding: '4px', cursor: 'pointer', color: 'white', display: 'flex' }}
                            >
                                <Menu size={18} />
                            </button>
                            <span style={{ fontWeight: 600, fontSize: '0.95rem' }}>{getCurrentChat().title}</span>
                        </div>
                        <button
                            onClick={() => setIsOpen(false)}
                            onMouseDown={e => e.stopPropagation()}
                            style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'white', opacity: 0.8 }}
                        >
                            <X size={20} />
                        </button>
                    </div>

                    {/* Messages Area */}
                    <div
                        ref={scrollContainerRef}
                        onScroll={onScroll}
                        style={{
                            flex: 1,
                            minHeight: 0, // CRITICAL for nested flex scrolling
                            overflowY: 'auto',
                            padding: '16px',
                            backgroundColor: '#f8fafc',
                            display: 'flex',
                            flexDirection: 'column',
                            gap: '12px'
                        }}
                    >
                        {getCurrentChat().messages?.map((m, i) => (
                            <div key={i} style={{
                                alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start',
                                maxWidth: '85%',
                                backgroundColor: m.role === 'user' ? '#3b82f6' : '#ffffff',
                                color: m.role === 'user' ? 'white' : '#1e293b',
                                padding: '10px 14px',
                                borderRadius: m.role === 'user' ? '16px 16px 2px 16px' : '16px 16px 16px 2px',
                                boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
                                fontSize: '0.95rem',
                                lineHeight: '1.5'
                            }}>
                                <ReactMarkdown>{m.content}</ReactMarkdown>
                            </div>
                        ))}
                        {loading && !getCurrentChat().messages.some(m => m.role === 'agent' && m.content === "") && (
                            <div style={{ alignSelf: 'flex-start', backgroundColor: '#fff', padding: '10px 14px', borderRadius: '16px', color: '#64748b', fontSize: '0.9rem', fontStyle: 'italic' }}>
                                Agent is thinking...
                            </div>
                        )}
                        <div ref={messagesEndRef} />
                    </div>

                    {/* Input Area */}
                    <div style={{
                        padding: '12px',
                        paddingRight: '24px',
                        backgroundColor: 'white',
                        borderTop: '1px solid #e2e8f0',
                        display: 'flex',
                        gap: '8px'
                    }}>
                        <input
                            value={input}
                            onChange={e => setInput(e.target.value)}
                            onKeyDown={e => e.key === 'Enter' && sendMessage()}
                            placeholder="Ask about your ads..."
                            style={{
                                flex: 1,
                                padding: '10px 14px',
                                borderRadius: '24px',
                                border: '1px solid #cbd5e1',
                                outline: 'none',
                                fontSize: '0.95rem',
                                backgroundColor: '#f1f5f9'
                            }}
                        />
                        <button
                            onClick={sendMessage}
                            disabled={loading}
                            style={{
                                width: '40px',
                                height: '40px',
                                borderRadius: '50%',
                                background: 'linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%)',
                                color: 'white',
                                border: 'none',
                                cursor: loading ? 'default' : 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                transition: 'opacity 0.2s',
                                flexShrink: 0
                            }}
                        >
                            <Send size={18} />
                        </button>
                    </div>
                </div>
            )}

            {/* Toggle Button (Floating Icon - Gemini Style) */}
            {!isOpen && (
                <button
                    onClick={toggleChat}
                    onMouseDown={onMouseDown}
                    style={{
                        width: '56px',
                        height: '56px',
                        borderRadius: '28px',
                        background: 'linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%)',
                        color: 'white',
                        border: 'none',
                        boxShadow: '0 4px 12px rgba(59, 130, 246, 0.5)',
                        cursor: 'move',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        transition: 'transform 0.2s',
                    }}
                    onMouseEnter={e => e.currentTarget.style.transform = 'scale(1.05)'}
                    onMouseLeave={e => e.currentTarget.style.transform = 'scale(1)'}
                >
                    <Sparkles size={28} />
                </button>
            )}
        </div>
    );

}
