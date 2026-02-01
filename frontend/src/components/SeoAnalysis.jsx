import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft, Search, Loader2, Calendar, X, Sparkles, Send } from 'lucide-react';
import Markdown from 'react-markdown';
import { API_BASE_URL } from '../config';
import { useAuth } from '../context/AuthContext';

export default function SeoAnalysis() {
    const { authFetch } = useAuth();
    const datePopupRef = useRef(null);

    // 可用日期范围（从数据库获取）
    const [dateRange, setDateRange] = useState({ start: '', end: '' });

    // 从 localStorage 读取上次的值
    const [siteUrl] = useState('baofengradio.co.uk');
    const [ctrThreshold, setCtrThreshold] = useState(() => {
        const saved = localStorage.getItem('seo_ctr_threshold');
        return saved ? parseInt(saved) : 2;
    });
    const [startDate, setStartDate] = useState('');
    const [endDate, setEndDate] = useState('');
    const [rowLimit, setRowLimit] = useState(() => {
        const saved = localStorage.getItem('seo_row_limit');
        return saved ? parseInt(saved) : 100;
    });
    const [loading, setLoading] = useState(false);
    const [pages, setPages] = useState([]);
    const [error, setError] = useState('');
    const [showDatePopup, setShowDatePopup] = useState(false);

    // SEO Agent 对话状态
    const [agentLoading, setAgentLoading] = useState(false);
    const [agentResponse, setAgentResponse] = useState('');
    const responseRef = useRef(null);

    // 页面加载时获取可用日期范围
    useEffect(() => {
        const fetchDateRange = async () => {
            try {
                const res = await authFetch(`${API_BASE_URL}/api/seo/date-range`);
                const data = await res.json();
                if (data.status === 'success') {
                    setDateRange({ start: data.start_date, end: data.end_date });
                    setStartDate(data.start_date);
                    setEndDate(data.end_date);
                }
            } catch (e) {
                console.error('获取日期范围失败:', e);
            }
        };
        fetchDateRange();
    }, [authFetch]);

    // 点击外部关闭日期弹窗
    useEffect(() => {
        const handleClickOutside = (e) => {
            if (datePopupRef.current && !datePopupRef.current.contains(e.target)) {
                setShowDatePopup(false);
            }
        };
        if (showDatePopup) {
            document.addEventListener('mousedown', handleClickOutside);
        }
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, [showDatePopup]);

    // 保存到 localStorage
    useEffect(() => {
        localStorage.setItem('seo_ctr_threshold', ctrThreshold);
    }, [ctrThreshold]);

    useEffect(() => {
        localStorage.setItem('seo_start_date', startDate);
    }, [startDate]);

    useEffect(() => {
        localStorage.setItem('seo_end_date', endDate);
    }, [endDate]);

    useEffect(() => {
        localStorage.setItem('seo_row_limit', rowLimit);
    }, [rowLimit]);

    // 获取数据
    const handleFetch = useCallback(async () => {
        setLoading(true);
        setError('');
        try {
            const params = new URLSearchParams({
                ctr_threshold: ctrThreshold,
                start_date: startDate,
                end_date: endDate,
                row_limit: rowLimit
            });
            const response = await authFetch(`${API_BASE_URL}/api/seo/low-ctr-pages?${params}`);
            const data = await response.json();

            if (data.status === 'success') {
                setPages(data.data || []);
                // 存储到 localStorage 供 SEO Agent 使用
                localStorage.setItem('seo_pages_data', JSON.stringify(data.data || []));
            } else {
                setError(data.message || '获取数据失败');
                setPages([]);
            }
        } catch (e) {
            setError('请求失败: ' + e.message);
            setPages([]);
        } finally {
            setLoading(false);
        }
    }, [authFetch, ctrThreshold, startDate, endDate, rowLimit]);

    // 页面加载时自动获取数据
    useEffect(() => {
        handleFetch();
    }, []);

    // 格式化日期显示
    const formatDateRange = () => {
        return `${startDate} ~ ${endDate}`;
    };

    return (
        <div style={{
            width: '100vw',
            height: '100vh',
            background: '#f8fafc',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden'
        }}>
            {/* TOP NAVIGATION BAR */}
            <div style={{
                height: '60px',
                background: 'white',
                borderBottom: '1px solid #e2e8f0',
                display: 'flex',
                alignItems: 'center',
                padding: '0 30px',
                justifyContent: 'space-between',
                flexShrink: 0,
                zIndex: 10
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
                    <Link
                        to="/"
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '8px',
                            color: '#6366f1',
                            textDecoration: 'none',
                            fontWeight: '500',
                            fontSize: '14px',
                            padding: '8px 12px',
                            borderRadius: '8px',
                            background: '#eef2ff',
                            transition: 'all 0.2s'
                        }}
                    >
                        <ArrowLeft size={18} />
                        返回主页
                    </Link>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '10px', color: '#10b981', fontWeight: 'bold', fontSize: '18px' }}>
                        <Search size={20} />
                        SEO分析页
                    </div>
                </div>
            </div>

            {/* SCROLLABLE CONTENT AREA */}
            <div style={{ flex: 1, overflowY: 'auto', padding: '20px 30px' }}>
                <div style={{ maxWidth: '1200px', margin: '0 auto', display: 'flex', flexDirection: 'column', gap: '25px' }}>

                    {/* 筛选区域 */}
                    <div style={{
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e2e8f0',
                        padding: '16px 20px',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        gap: '16px'
                    }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <label style={{ fontWeight: '500', color: '#374151', fontSize: '14px' }}>站点:</label>
                                <span style={{ color: '#6366f1', fontWeight: '600', fontSize: '14px' }}>{siteUrl}</span>
                            </div>

                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <label style={{ fontWeight: '500', color: '#374151', fontSize: '14px' }}>CTR&lt;</label>
                                <input
                                    type="number"
                                    min="1"
                                    max="100"
                                    value={ctrThreshold}
                                    onChange={(e) => setCtrThreshold(Math.min(100, Math.max(1, parseInt(e.target.value) || 2)))}
                                    style={{
                                        width: '50px',
                                        padding: '6px 8px',
                                        borderRadius: '6px',
                                        border: '1px solid #e2e8f0',
                                        fontSize: '14px',
                                        textAlign: 'center'
                                    }}
                                />
                                <span style={{ color: '#6b7280', fontSize: '14px' }}>%</span>
                            </div>

                            {/* 日期范围按钮 */}
                            <div style={{ position: 'relative' }} ref={datePopupRef}>
                                <button
                                    onClick={() => setShowDatePopup(!showDatePopup)}
                                    style={{
                                        padding: '6px 12px',
                                        background: showDatePopup ? '#eef2ff' : '#f8fafc',
                                        border: '1px solid #e2e8f0',
                                        borderRadius: '6px',
                                        cursor: 'pointer',
                                        display: 'flex',
                                        alignItems: 'center',
                                        gap: '6px',
                                        fontSize: '14px',
                                        color: '#374151'
                                    }}
                                >
                                    <Calendar size={14} />
                                    {formatDateRange()}
                                </button>

                                {/* 日期弹窗 */}
                                {showDatePopup && (
                                    <div style={{
                                        position: 'absolute',
                                        top: '100%',
                                        left: 0,
                                        marginTop: '8px',
                                        background: 'white',
                                        borderRadius: '12px',
                                        border: '1px solid #e2e8f0',
                                        boxShadow: '0 10px 40px rgba(0,0,0,0.15)',
                                        padding: '16px',
                                        zIndex: 100,
                                        minWidth: '280px'
                                    }}>
                                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                                            <span style={{ fontWeight: '600', color: '#374151' }}>选择日期范围</span>
                                            <button onClick={() => setShowDatePopup(false)} style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '4px' }}>
                                                <X size={16} color="#9ca3af" />
                                            </button>
                                        </div>
                                        <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                                            <div>
                                                <label style={{ display: 'block', marginBottom: '4px', fontSize: '13px', color: '#6b7280' }}>开始日期</label>
                                                <input
                                                    type="date"
                                                    value={startDate}
                                                    min={dateRange.start}
                                                    max={dateRange.end}
                                                    onChange={(e) => setStartDate(e.target.value)}
                                                    style={{
                                                        width: '100%',
                                                        padding: '8px 12px',
                                                        borderRadius: '8px',
                                                        border: '1px solid #e2e8f0',
                                                        fontSize: '14px'
                                                    }}
                                                />
                                            </div>
                                            <div>
                                                <label style={{ display: 'block', marginBottom: '4px', fontSize: '13px', color: '#6b7280' }}>结束日期</label>
                                                <input
                                                    type="date"
                                                    value={endDate}
                                                    min={dateRange.start}
                                                    max={dateRange.end}
                                                    onChange={(e) => setEndDate(e.target.value)}
                                                    style={{
                                                        width: '100%',
                                                        padding: '8px 12px',
                                                        borderRadius: '8px',
                                                        border: '1px solid #e2e8f0',
                                                        fontSize: '14px'
                                                    }}
                                                />
                                            </div>
                                            {dateRange.start && (
                                                <div style={{ fontSize: '12px', color: '#9ca3af', textAlign: 'center' }}>
                                                    可用范围: {dateRange.start} ~ {dateRange.end}
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                )}
                            </div>

                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <label style={{ fontWeight: '500', color: '#374151', fontSize: '14px' }}>行数:</label>
                                <input
                                    type="number"
                                    min="1"
                                    max="25000"
                                    value={rowLimit}
                                    onChange={(e) => setRowLimit(Math.min(25000, Math.max(1, parseInt(e.target.value) || 100)))}
                                    style={{
                                        width: '70px',
                                        padding: '6px 8px',
                                        borderRadius: '6px',
                                        border: '1px solid #e2e8f0',
                                        fontSize: '14px',
                                        textAlign: 'center'
                                    }}
                                />
                            </div>
                        </div>

                        <button
                            onClick={handleFetch}
                            disabled={loading}
                            style={{
                                padding: '8px 20px',
                                background: loading ? '#94a3b8' : '#3b82f6',
                                color: 'white',
                                border: 'none',
                                borderRadius: '8px',
                                fontWeight: '600',
                                cursor: loading ? 'default' : 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                gap: '6px',
                                flexShrink: 0,
                                fontSize: '14px'
                            }}
                        >
                            {loading && <Loader2 size={14} className="spinning-icon" />}
                            获取
                        </button>
                    </div>

                    {/* 错误提示 */}
                    {error && (
                        <div style={{
                            background: '#fef2f2',
                            border: '1px solid #fecaca',
                            borderRadius: '8px',
                            padding: '12px 16px',
                            color: '#dc2626'
                        }}>
                            {error}
                        </div>
                    )}

                    {/* URL/CTR 表格 */}
                    <div style={{
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e2e8f0',
                        overflow: 'hidden'
                    }}>
                        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                            <thead>
                                <tr style={{ background: '#f8fafc' }}>
                                    <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: '600', color: '#374151', borderBottom: '1px solid #e2e8f0', width: '60px' }}>序号</th>
                                    <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: '600', color: '#374151', borderBottom: '1px solid #e2e8f0' }}>URL</th>
                                    <th style={{ padding: '12px 16px', textAlign: 'right', fontWeight: '600', color: '#374151', borderBottom: '1px solid #e2e8f0', width: '100px' }}>CTR</th>
                                </tr>
                            </thead>
                            <tbody>
                                {loading ? (
                                    <tr>
                                        <td colSpan="3" style={{ padding: '40px', textAlign: 'center', color: '#6b7280' }}>
                                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '10px' }}>
                                                <Loader2 size={20} className="spinning-icon" />
                                                加载中...
                                            </div>
                                        </td>
                                    </tr>
                                ) : pages.length === 0 ? (
                                    <tr>
                                        <td colSpan="3" style={{ padding: '40px', textAlign: 'center', color: '#9ca3af' }}>
                                            暂无数据
                                        </td>
                                    </tr>
                                ) : (
                                    pages.map((page, index) => (
                                        <tr key={index} style={{ borderBottom: '1px solid #f1f5f9' }}>
                                            <td style={{ padding: '12px 16px', color: '#6b7280' }}>{index + 1}</td>
                                            <td style={{ padding: '12px 16px', color: '#374151', wordBreak: 'break-all' }}>{page.url}</td>
                                            <td style={{ padding: '12px 16px', textAlign: 'right', color: '#ef4444', fontWeight: '500' }}>{page.ctr}%</td>
                                        </tr>
                                    ))
                                )}
                            </tbody>
                        </table>
                    </div>

                    {/* SEO Agent 对话框 */}
                    <div style={{
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e2e8f0',
                        marginTop: '20px',
                        overflow: 'hidden'
                    }}>
                        <div style={{
                            padding: '16px 20px',
                            borderBottom: '1px solid #e2e8f0',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'space-between',
                            background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 100%)'
                        }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', color: 'white' }}>
                                <Sparkles size={20} />
                                <span style={{ fontWeight: '600' }}>SEO Agent 分析</span>
                            </div>
                            <button
                                onClick={async () => {
                                    if (pages.length === 0) {
                                        setError('请先获取数据');
                                        return;
                                    }
                                    setAgentLoading(true);
                                    setAgentResponse('');
                                    try {
                                        const response = await authFetch(`${API_BASE_URL}/api/chat`, {
                                            method: 'POST',
                                            headers: { 'Content-Type': 'application/json' },
                                            body: JSON.stringify({
                                                message: '分析这些页面的SEO并给出优化建议',
                                                messages: [],
                                                selectedTables: ['seo'],
                                                seo_pages_data: pages
                                            })
                                        });
                                        const reader = response.body.getReader();
                                        const decoder = new TextDecoder();
                                        let result = '';
                                        while (true) {
                                            const { done, value } = await reader.read();
                                            if (done) break;
                                            result += decoder.decode(value, { stream: true });
                                            setAgentResponse(result);
                                            if (responseRef.current) {
                                                responseRef.current.scrollTop = responseRef.current.scrollHeight;
                                            }
                                        }
                                    } catch (e) {
                                        setAgentResponse('分析失败: ' + e.message);
                                    } finally {
                                        setAgentLoading(false);
                                    }
                                }}
                                disabled={agentLoading || pages.length === 0}
                                style={{
                                    padding: '8px 16px',
                                    background: agentLoading ? '#64748b' : '#3b82f6',
                                    color: 'white',
                                    border: 'none',
                                    borderRadius: '8px',
                                    cursor: agentLoading ? 'default' : 'pointer',
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: '6px',
                                    fontWeight: '500',
                                    fontSize: '14px'
                                }}
                            >
                                {agentLoading ? (
                                    <><Loader2 size={16} className="spinning-icon" /> 分析中...</>
                                ) : (
                                    <><Send size={16} /> 开始分析</>
                                )}
                            </button>
                        </div>
                        <div
                            ref={responseRef}
                            style={{
                                padding: '20px',
                                minHeight: '200px',
                                maxHeight: '500px',
                                overflowY: 'auto',
                                background: '#f8fafc',
                                fontFamily: 'system-ui, -apple-system, sans-serif',
                                fontSize: '14px',
                                lineHeight: '1.6'
                            }}
                        >
                            {agentResponse ? (
                                <Markdown
                                    components={{
                                        h2: ({ node, ...props }) => <h2 style={{ marginTop: '20px', marginBottom: '10px', color: '#1e293b', borderBottom: '1px solid #e2e8f0', paddingBottom: '8px' }} {...props} />,
                                        strong: ({ node, ...props }) => <strong style={{ color: '#0f172a' }} {...props} />,
                                        hr: ({ node, ...props }) => <hr style={{ border: 'none', borderTop: '1px solid #e2e8f0', margin: '20px 0' }} {...props} />,
                                        p: ({ node, ...props }) => <p style={{ margin: '8px 0' }} {...props} />
                                    }}
                                >
                                    {agentResponse}
                                </Markdown>
                            ) : (
                                <div style={{ color: '#9ca3af', textAlign: 'center', paddingTop: '60px' }}>
                                    {pages.length === 0 ? '请先获取数据，再点击"开始分析"' : '点击"开始分析"按钮，SEO Agent 将分析页面并给出优化建议'}
                                </div>
                            )}
                        </div>
                    </div>

                </div>
            </div>
        </div>
    );
}
