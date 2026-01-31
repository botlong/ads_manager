import React, { useState, useRef, useEffect } from 'react';
import Markdown from 'react-markdown';
import { MessageSquare, X, Menu, Plus, Send, Trash2, Edit, Check, Sparkles, Maximize2, Minimize2, Minus, Layout, Table, AlertTriangle, ChevronDown, CheckSquare, Square } from 'lucide-react';
import CustomRuleEditor from './CustomRuleEditor';
import PerAgentRuleEditor from './PerAgentRuleEditor';
import { API_BASE_URL } from '../config';
import { useAuth } from '../context/AuthContext';

export default function AgentChat() {
    const [isOpen, setIsOpen] = useState(false);
    const [isFullScreen, setIsFullScreen] = useState(false);
    const [conversations, setConversations] = useState([]);
    const [currentId, setCurrentId] = useState(null);
    const [input, setInput] = useState("");
    const [loading, setLoading] = useState(false);
    const [showSidebar, setShowSidebar] = useState(false);
    const { authFetch } = useAuth();

    // --- Table Selection State ---
    const [selectedTables, setSelectedTables] = useState(['Anomalies', 'Campaigns', 'Products', 'search_term', 'asset', 'audience', 'age', 'gender', 'location', 'ad_schedule', 'channel', 'seo']);
    const [isContextOpen, setIsContextOpen] = useState(false);

    // --- Custom Rule Editor State ---
    const [customRuleText, setCustomRuleText] = useState("");
    const [showRuleEditor, setShowRuleEditor] = useState(false);
    const [ruleSaveStatus, setRuleSaveStatus] = useState(null); // 'saving', 'saved', 'error'

    const [editingId, setEditingId] = useState(null);
    const [editTitle, setEditTitle] = useState("");
    const [windowSize, setWindowSize] = useState({ width: 450, height: 650 });

    const messagesEndRef = useRef(null);
    const contextRef = useRef(null);

    // Load from localStorage on mount
    useEffect(() => {
        const saved = localStorage.getItem('agent_conversations');
        if (saved) {
            try {
                const parsed = JSON.parse(saved);
                const validList = Array.isArray(parsed) ? parsed : [];
                setConversations(validList);
                if (validList.length > 0) {
                    setCurrentId(validList[0].id);
                } else {
                    createNewChat();
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
        if (Array.isArray(conversations) && conversations.length > 0) {
            localStorage.setItem('agent_conversations', JSON.stringify(conversations));
        }
    }, [conversations]);

    // Close context menu when clicking outside
    useEffect(() => {
        const handleClickOutside = (event) => {
            if (contextRef.current && !contextRef.current.contains(event.target)) {
                setIsContextOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const getCurrentChat = () => {
        if (!Array.isArray(conversations)) return { title: 'Analysis', messages: [] };
        const chat = conversations.find(c => c.id === currentId);
        return chat || { title: 'Analysis', messages: [] };
    };

    const createNewChat = () => {
        const now = new Date();
        const month = String(now.getMonth() + 1).padStart(2, '0');
        const day = String(now.getDate()).padStart(2, '0');
        const hour = String(now.getHours()).padStart(2, '0');
        const minute = String(now.getMinutes()).padStart(2, '0');
        const dateTimeStr = `${month}-${day} ${hour}:${minute}`;
        const newChat = {
            id: Date.now(),
            title: `Analysis ${dateTimeStr}`,
            messages: [{ role: 'agent', content: '您好！我是 **AdsManager 专家系统 (v1.0)**。\n\n**核心能力**：\n- 🛡️ **Rule-First 诊断**：基于确定性规则，拒绝幻觉。\n- 🩺 **多维度专家**：搜词、渠道、商品、地域深度审计。\n- ⚖️ **风控保护**：大促期、冷启动期自动降级风险动作。\n\n**您可以**：\n- 直接点击 **"Send"** (空消息) 进行全账户自动巡检。\n- 输入 **"分析 [系列名]"** 调遣专家组进行深度诊断。' }]
        };
        setConversations(prev => [newChat, ...(Array.isArray(prev) ? prev : [])]);
        setCurrentId(newChat.id);
        setShowSidebar(false);
    };

    const deleteChat = (e, id) => {
        e.stopPropagation();
        setConversations(prev => {
            const filtered = (Array.isArray(prev) ? prev : []).filter(c => c.id !== id);
            if (id === currentId) {
                setCurrentId(filtered.length > 0 ? filtered[0].id : null);
            }
            return filtered;
        });
    };

    const startEditing = (e, chat) => {
        e.stopPropagation();
        setEditingId(chat.id);
        setEditTitle(chat.title);
    };

    const saveTitle = (e) => {
        e.stopPropagation();
        if (editingId) {
            setConversations(prev => (Array.isArray(prev) ? prev : []).map(c =>
                c.id === editingId ? { ...c, title: editTitle } : c
            ));
            setEditingId(null);
            setEditTitle("");
        }
    };

    const cancelEditing = (e) => {
        e.stopPropagation();
        setEditingId(null);
        setEditTitle("");
    };

    const toggleTable = (tableName) => {
        setSelectedTables(prev =>
            prev.includes(tableName)
                ? prev.filter(t => t !== tableName)
                : [...prev, tableName]
        );
    };

    // --- Custom Rule Handlers ---
    const applyRuleOnce = () => {
        // The rule will be passed with the next message only
        if (customRuleText.trim()) {
            setRuleSaveStatus('applied');
            setTimeout(() => setRuleSaveStatus(null), 2000);
        }
    };

    const applyRulePermanently = async () => {
        if (!customRuleText.trim()) return;
        setRuleSaveStatus('saving');
        try {
            const response = await authFetch(`${API_BASE_URL}/api/agent-rules`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    table_name: 'global',
                    rule_prompt: customRuleText
                })
            });
            if (response.ok) {
                setRuleSaveStatus('saved');
                setTimeout(() => setRuleSaveStatus(null), 2000);
            } else {
                setRuleSaveStatus('error');
            }
        } catch (e) {
            console.error('Failed to save rule:', e);
            setRuleSaveStatus('error');
        }
    };

    const sendMessage = async () => {
        if (!currentId) return;

        // "Quick Scan" Feature: If input is empty, treat it as a request to scan
        const isQuickScan = !input.trim();
        const effectiveInput = isQuickScan ? "Invoke the Expert System to scan for anomalies. (Auto-Scan)" : input;

        const userMsg = { role: 'user', content: effectiveInput };
        setConversations(prev => (Array.isArray(prev) ? prev : []).map(c => {
            if (c.id === currentId) {
                return { ...c, messages: [...(Array.isArray(c.messages) ? c.messages : []), userMsg] };
            }
            return c;
        }));

        const currentInput = effectiveInput;
        setInput("");
        setLoading(true);

        const currentChat = getCurrentChat();
        const currentMsgs = Array.isArray(currentChat.messages) ? currentChat.messages : [];
        const messageHistory = currentMsgs.map(m => ({ role: m.role, content: m.content })).slice(-10);

        try {
            // 如果选中了 SEO agent，从 localStorage 获取 SEO 页面数据
            let seoPagesData = null;
            if (selectedTables.includes('seo')) {
                try {
                    const stored = localStorage.getItem('seo_pages_data');
                    if (stored) {
                        seoPagesData = JSON.parse(stored);
                    }
                } catch (e) {
                    console.error('Failed to parse SEO pages data:', e);
                }
            }

            const response = await authFetch(`${API_BASE_URL}/api/chat`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: currentInput,
                    messages: messageHistory,
                    selectedTables: selectedTables,
                    seo_pages_data: seoPagesData
                })
            });

            if (!response.ok) {
                throw new Error(`Server Error: ${response.status} ${response.statusText}`);
            }

            if (!response.body) throw new Error("No response body");

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let agentMsgContent = "";

            setConversations(prev => (Array.isArray(prev) ? prev : []).map(c => {
                if (c.id === currentId) {
                    return { ...c, messages: [...(Array.isArray(c.messages) ? c.messages : []), { role: 'agent', content: "" }] };
                }
                return c;
            }));

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                const chunk = decoder.decode(value, { stream: true });
                agentMsgContent += chunk;

                setConversations(prev => (Array.isArray(prev) ? prev : []).map(c => {
                    if (c.id === currentId) {
                        const msgs = [...(Array.isArray(c.messages) ? c.messages : [])];
                        if (msgs.length > 0) {
                            msgs[msgs.length - 1] = { role: 'agent', content: agentMsgContent };
                        }
                        return { ...c, messages: msgs };
                    }
                    return c;
                }));
            }

        } catch (e) {
            console.error("Chat Error:", e);
            setConversations(prev => (Array.isArray(prev) ? prev : []).map(c => {
                if (c.id === currentId) {
                    return { ...c, messages: [...(Array.isArray(c.messages) ? c.messages : []), { role: 'agent', content: `Error: ${e.message || "Could not connect to backend."}` }] };
                }
                return c;
            }));
        }
        setLoading(false);
    };

    const scrollContainerRef = useRef(null);
    const shouldAutoScroll = useRef(true);

    const onScroll = () => {
        if (!scrollContainerRef.current) return;
        const { scrollTop, scrollHeight, clientHeight } = scrollContainerRef.current;
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

    const [position, setPosition] = useState(() => ({
        x: window.innerWidth - 470,
        y: window.innerHeight - 700
    }));

    const isDragging = useRef(false);
    const hasMoved = useRef(false);
    const dragOffset = useRef({ x: 0, y: 0 });

    const onMouseDown = (e) => {
        if (isFullScreen) return;
        isDragging.current = true;
        hasMoved.current = false;
        dragOffset.current = {
            x: e.clientX - position.x,
            y: e.clientY - position.y
        };
        document.addEventListener('mousemove', onMouseMove);
        document.addEventListener('mouseup', onMouseUp);
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

    const isResizing = useRef(false);
    const resizeDir = useRef(null);
    const resizeStart = useRef({ x: 0, y: 0, w: 0, h: 0, l: 0, t: 0 });

    const onResizeMouseDown = (e, direction) => {
        if (isFullScreen) return;
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

        if (dir.includes('e')) {
            newWidth = Math.max(350, resizeStart.current.w + deltaX);
        } else if (dir.includes('w')) {
            const proposedWidth = resizeStart.current.w - deltaX;
            if (proposedWidth >= 350) {
                newWidth = proposedWidth;
                newLeft = resizeStart.current.l + deltaX;
            }
        }

        if (dir.includes('s')) {
            newHeight = Math.max(450, resizeStart.current.h + deltaY);
        } else if (dir.includes('n')) {
            const proposedHeight = resizeStart.current.h - deltaY;
            if (proposedHeight >= 450) {
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

    const currentChat = getCurrentChat();
    const messages = Array.isArray(currentChat.messages) ? currentChat.messages : [];

    const ALL_TABLES = [
        { id: 'Anomalies', label: '🛡️ Anomaly Guard' },
        { id: 'Campaigns', label: '📊 Campaign Manager' },
        { id: 'Products', label: '📦 Product Specialist' },
        { id: 'search_term', label: '🔍 Search Term Analyst' },
        { id: 'asset', label: '🎨 Creative Asset Expert' },
        { id: 'audience', label: '👥 Audience Strategist' },
        { id: 'age', label: '🎂 Age Demographics' },
        { id: 'gender', label: '⚧ Gender Demographics' },
        { id: 'location', label: '🌍 Location & Geo Expert' },
        { id: 'ad_schedule', label: '⏰ Time/Schedule Analyst' },
        { id: 'channel', label: '📡 Channel (PMax) Auditor' },
        { id: 'seo', label: '🔎 SEO Analyst' }
    ];

    return (
        <div
            style={{
                position: 'fixed',
                left: isFullScreen ? 0 : position.x,
                top: isFullScreen ? 0 : position.y,
                zIndex: 10000,
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'flex-start',
            }}
        >
            {isOpen && (
                <div
                    style={{
                        width: isFullScreen ? '100vw' : `${windowSize.width}px`,
                        height: isFullScreen ? '100vh' : `${windowSize.height}px`,
                        backgroundColor: '#ffffff',
                        borderRadius: isFullScreen ? '0' : '16px',
                        boxShadow: '0 20px 50px rgba(0, 0, 0, 0.15)',
                        display: 'flex',
                        flexDirection: 'column',
                        overflow: 'hidden',
                        border: isFullScreen ? 'none' : '1px solid #eef2f6',
                        transition: isResizing.current ? 'none' : 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                        position: 'relative',
                    }}
                    className="chat-window-container"
                >
                    {!isFullScreen && (
                        <>
                            <div onMouseDown={(e) => onResizeMouseDown(e, 'nw')} style={{ position: 'absolute', top: 0, left: 0, width: '15px', height: '15px', zIndex: 2000, cursor: 'nw-resize' }} />
                            <div onMouseDown={(e) => onResizeMouseDown(e, 'ne')} style={{ position: 'absolute', top: 0, right: 0, width: '15px', height: '15px', zIndex: 2000, cursor: 'ne-resize' }} />
                            <div onMouseDown={(e) => onResizeMouseDown(e, 'sw')} style={{ position: 'absolute', bottom: 0, left: 0, width: '15px', height: '15px', zIndex: 2000, cursor: 'sw-resize' }} />
                            <div onMouseDown={(e) => onResizeMouseDown(e, 'se')} style={{ position: 'absolute', bottom: 0, right: 0, width: '15px', height: '15px', zIndex: 2000, cursor: 'se-resize' }} />
                            <div onMouseDown={(e) => onResizeMouseDown(e, 'n')} style={{ position: 'absolute', top: 0, left: '15px', right: '15px', height: '8px', zIndex: 2000, cursor: 'n-resize' }} />
                            <div onMouseDown={(e) => onResizeMouseDown(e, 's')} style={{ position: 'absolute', bottom: 0, left: '15px', right: '15px', height: '8px', zIndex: 2000, cursor: 's-resize' }} />
                            <div onMouseDown={(e) => onResizeMouseDown(e, 'w')} style={{ position: 'absolute', left: 0, top: '15px', bottom: '15px', width: '8px', zIndex: 2000, cursor: 'w-resize' }} />
                            <div onMouseDown={(e) => onResizeMouseDown(e, 'e')} style={{ position: 'absolute', right: 0, top: '15px', bottom: '15px', width: '8px', zIndex: 2000, cursor: 'e-resize' }} />
                        </>
                    )}

                    {showSidebar && (
                        <>
                            <div onClick={() => setShowSidebar(false)} style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(0,0,0,0.4)', zIndex: 19, backdropFilter: 'blur(2px)' }} />
                            <div style={{ position: 'absolute', top: 0, left: 0, bottom: 0, width: '260px', backgroundColor: '#ffffff', borderRight: '1px solid #f1f5f9', zIndex: 20, display: 'flex', flexDirection: 'column', boxShadow: '10px 0 25px rgba(0,0,0,0.1)', animation: 'slideIn 0.2s ease-out' }}>
                                <div style={{ padding: '20px', borderBottom: '1px solid #f1f5f9', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    <span style={{ fontWeight: 700, color: '#1e293b', fontSize: '1rem' }}>History</span>
                                    <div style={{ display: 'flex', gap: '8px' }}>
                                        <button onClick={createNewChat} title="New Chat" style={{ background: '#f1f5f9', border: 'none', borderRadius: '6px', padding: '6px', cursor: 'pointer', color: '#0f172a' }}><Plus size={18} /></button>
                                        <button onClick={() => setShowSidebar(false)} title="Close Sidebar" style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#94a3b8' }}><X size={20} /></button>
                                    </div>
                                </div>
                                <div style={{ flex: 1, overflowY: 'auto', padding: '10px' }}>
                                    {Array.isArray(conversations) && conversations.map(c => (
                                        <div key={c.id} onClick={() => { setCurrentId(c.id); setShowSidebar(false); }} style={{ padding: '12px 14px', borderRadius: '10px', cursor: 'pointer', backgroundColor: c.id === currentId ? '#f1f5f9' : 'transparent', display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '4px' }}>
                                            {editingId === c.id ? (
                                                <div style={{ display: 'flex', alignItems: 'center', gap: '4px', width: '100%' }} onClick={e => e.stopPropagation()}>
                                                    <input value={editTitle} onChange={e => setEditTitle(e.target.value)} onKeyDown={e => e.key === 'Enter' && saveTitle(e)} autoFocus style={{ width: '100%', fontSize: '0.85rem', padding: '4px 6px', border: '1px solid #cbd5e1', borderRadius: '4px' }} />
                                                    <button onClick={saveTitle} style={{ border: 'none', background: '#22c55e', color: 'white', borderRadius: '4px', cursor: 'pointer', padding: '2px' }}><Check size={14} /></button>
                                                </div>
                                            ) : (
                                                <>
                                                    <span style={{ fontSize: '0.85rem', fontWeight: c.id === currentId ? '600' : '400', color: '#334155', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '140px' }}>{c.title}</span>
                                                    <div style={{ display: 'flex', gap: '6px' }} className="chat-actions">
                                                        <button onClick={(e) => startEditing(e, c)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#94a3b8', padding: 0 }}><Edit size={14} /></button>
                                                        <button onClick={(e) => deleteChat(e, c.id)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#94a3b8', padding: 0 }}><Trash2 size={14} /></button>
                                                    </div>
                                                </>
                                            )}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </>
                    )}

                    <div
                        style={{
                            padding: '14px 20px',
                            background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 100%)',
                            color: 'white',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'space-between',
                            cursor: isFullScreen ? 'default' : 'move',
                            userSelect: 'none',
                            flexShrink: 0
                        }}
                        onMouseDown={onMouseDown}
                    >
                        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                            <button onClick={() => setShowSidebar(!showSidebar)} title="Menu" onMouseDown={e => e.stopPropagation()} style={{ background: 'rgba(255,255,255,0.08)', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '8px', padding: '6px', cursor: 'pointer', color: 'white', display: 'flex' }}><Menu size={18} /></button>
                            <span style={{ fontWeight: 600, fontSize: '0.9rem', letterSpacing: '0.01em' }}>{currentChat.title}</span>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <button onClick={() => setIsFullScreen(!isFullScreen)} title={isFullScreen ? "Exit Fullscreen" : "Fullscreen"} onMouseDown={e => e.stopPropagation()} style={{ background: 'none', border: 'none', borderRadius: '6px', padding: '6px', cursor: 'pointer', color: 'white', opacity: 0.7, display: 'flex' }}>
                                {isFullScreen ? <Minimize2 size={18} /> : <Maximize2 size={18} />}
                            </button>
                            <button onClick={() => { setIsOpen(false); setIsFullScreen(false); }} title="Minimize" onMouseDown={e => e.stopPropagation()} style={{ background: 'none', border: 'none', borderRadius: '6px', padding: '6px', cursor: 'pointer', color: 'white', opacity: 0.7, display: 'flex' }}>
                                <Minus size={20} />
                            </button>
                        </div>
                    </div>

                    <div
                        ref={scrollContainerRef}
                        onScroll={onScroll}
                        style={{
                            flex: 1,
                            minHeight: 0,
                            overflowY: 'auto',
                            padding: isFullScreen ? '24px 10%' : '24px 20px',
                            backgroundColor: '#fdfdfe',
                            display: 'flex',
                            flexDirection: 'column',
                            gap: '16px'
                        }}
                    >
                        {messages.map((m, i) => (
                            <div
                                key={i}
                                style={{
                                    alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start',
                                    maxWidth: isFullScreen ? '95%' : '88%',
                                    width: (isFullScreen && m.role === 'agent') ? '100%' : 'auto',
                                    backgroundColor: m.role === 'user' ? '#0f172a' : '#f1f5f9',
                                    color: m.role === 'user' ? 'white' : '#1e293b',
                                    padding: '16px 22px',
                                    borderRadius: m.role === 'user' ? '20px 20px 4px 20px' : '20px 20px 20px 4px',
                                    boxShadow: '0 2px 10px rgba(0,0,0,0.05)',
                                    fontSize: '0.95rem',
                                    lineHeight: '1.7'
                                }}
                            >
                                <div className="markdown-content">
                                    <Markdown>{String(m.content || '')}</Markdown>
                                </div>
                            </div>
                        ))}
                        {loading && !messages.some(m => m.role === 'agent' && m.content === "") && (
                            <div style={{ alignSelf: 'flex-start', backgroundColor: '#f1f5f9', padding: '10px 16px', borderRadius: '18px', color: '#64748b', fontSize: '0.85rem', display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <Sparkles size={14} className="spinning-icon" />
                                Analyzing data...
                            </div>
                        )}
                        <div ref={messagesEndRef} />
                    </div>

                    {/* Consolidated Context Selection Dropdown */}
                    <div style={{
                        padding: isFullScreen ? '15px 10%' : '12px 20px',
                        backgroundColor: '#f8fafc',
                        borderTop: '1px solid #f1f5f9',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '12px',
                        position: 'relative',
                        userSelect: 'none'
                    }} ref={contextRef}>
                        <div
                            onClick={() => setIsContextOpen(!isContextOpen)}
                            style={{
                                flex: 1,
                                padding: '8px 14px',
                                backgroundColor: 'white',
                                border: '1px solid #e2e8f0',
                                borderRadius: '10px',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'space-between',
                                cursor: 'pointer',
                                fontSize: '0.85rem',
                                color: '#475569',
                                fontWeight: '500',
                                boxShadow: '0 1px 2px rgba(0,0,0,0.05)'
                            }}
                        >
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <Layout size={16} color="#64748b" />
                                <span>{selectedTables.length === 0 ? "Select Active Agents" : `${selectedTables.length} Agents Active`}</span>
                            </div>
                            <ChevronDown size={16} />
                        </div>

                        {isContextOpen && (
                            <div style={{
                                position: 'absolute',
                                bottom: '100%',
                                left: isFullScreen ? '10%' : '20px',
                                right: isFullScreen ? '10%' : '20px',
                                marginBottom: '8px',
                                backgroundColor: 'white',
                                borderRadius: '12px',
                                boxShadow: '0 10px 30px rgba(0,0,0,0.15)',
                                border: '1px solid #f1f5f9',
                                overflow: 'hidden',
                                zIndex: 30,
                                maxHeight: '300px',
                                overflowY: 'auto',
                                animation: 'slideInUp 0.2s ease-out'
                            }}>
                                <div style={{ padding: '12px 15px', backgroundColor: '#f8fafc', borderBottom: '1px solid #f1f5f9', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    <span style={{ fontSize: '11px', fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase' }}>Available Experts</span>
                                    <div style={{ display: 'flex', gap: '8px' }}>
                                        <button onClick={() => setSelectedTables(ALL_TABLES.map(t => t.id))} style={{ border: 'none', background: 'none', fontSize: '11px', color: '#3b82f6', cursor: 'pointer', fontWeight: 600 }}>Enable All</button>
                                        <button onClick={() => setSelectedTables([])} style={{ border: 'none', background: 'none', fontSize: '11px', color: '#f43f5e', cursor: 'pointer', fontWeight: 600 }}>Disable All</button>
                                    </div>
                                </div>
                                <div style={{ padding: '8px' }}>
                                    {ALL_TABLES.map(table => (
                                        <div
                                            key={table.id}
                                            onClick={() => toggleTable(table.id)}
                                            style={{
                                                padding: '10px 12px',
                                                borderRadius: '8px',
                                                cursor: 'pointer',
                                                display: 'flex',
                                                alignItems: 'center',
                                                justifyContent: 'space-between',
                                                fontSize: '0.85rem',
                                                backgroundColor: selectedTables.includes(table.id) ? '#f0f7ff' : 'transparent',
                                                color: selectedTables.includes(table.id) ? '#1e40af' : '#475569',
                                                transition: 'background 0.2s'
                                            }}
                                            onMouseOver={(e) => !selectedTables.includes(table.id) && (e.currentTarget.style.backgroundColor = '#f8fafc')}
                                            onMouseOut={(e) => !selectedTables.includes(table.id) && (e.currentTarget.style.backgroundColor = 'transparent')}
                                        >
                                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', position: 'relative' }}>
                                                {selectedTables.includes(table.id) ? <CheckSquare size={16} /> : <Square size={16} color="#cbd5e1" />}
                                                <span style={{ fontWeight: selectedTables.includes(table.id) ? '600' : '400' }}>{table.label}</span>
                                            </div>
                                            <PerAgentRuleEditor tableId={table.id} tableLabel={table.label} isSelected={selectedTables.includes(table.id)} />
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>

                    <div
                        style={{
                            padding: isFullScreen ? '20px 10% 40px 10%' : '16px 20px 24px 20px',
                            backgroundColor: 'white',
                            borderTop: '1px solid #f1f5f9',
                            display: 'flex',
                            gap: '12px',
                            alignItems: 'center'
                        }}
                    >
                        <input value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && sendMessage()} placeholder="Ask me anything..." style={{ flex: 1, padding: '14px 20px', borderRadius: '12px', border: '1px solid #e2e8f0', outline: 'none', fontSize: '0.95rem', backgroundColor: '#f8fafc' }} />
                        <button onClick={sendMessage} disabled={loading} style={{ width: '48px', height: '48px', borderRadius: '12px', background: '#0f172a', color: 'white', border: 'none', cursor: loading ? 'default' : 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}><Send size={18} /> </button>
                    </div>
                </div>
            )}

            {!isOpen && (
                <button
                    onClick={toggleChat}
                    onMouseDown={onMouseDown}
                    style={{
                        width: '64px',
                        height: '64px',
                        borderRadius: '24px',
                        background: '#0f172a',
                        color: 'white',
                        border: 'none',
                        boxShadow: '0 10px 30px rgba(15, 23, 42, 0.4)',
                        cursor: 'move',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        transition: 'all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275)',
                        position: 'relative',
                        overflow: 'hidden'
                    }}
                >
                    <Sparkles size={30} fill="currentColor" style={{ opacity: 0.9 }} />
                    <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, background: 'linear-gradient(45deg, transparent, rgba(255,255,255,0.1), transparent)', pointerEvents: 'none' }} />
                </button>
            )}
        </div>
    );
}
