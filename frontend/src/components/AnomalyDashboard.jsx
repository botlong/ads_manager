import React, { useState, useEffect, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { AlertTriangle, TrendingDown, ArrowRight, Calendar, ArrowUp, ArrowDown } from 'lucide-react';
import { API_BASE_URL } from '../config';
import { useAuth } from '../context/AuthContext';

const AnomalyDashboard = () => {
    const navigate = useNavigate();
    const [anomalies, setAnomalies] = useState([]);
    const [loading, setLoading] = useState(true);
    const [isOpen, setIsOpen] = useState(true);
    const [targetDate, setTargetDate] = useState(() => {
        return localStorage.getItem('anomaly_target_date') || '';
    });
    const { authFetch } = useAuth();

    // Sorting state
    const [sortConfig, setSortConfig] = useState({ key: null, direction: 'desc' });

    // Date range state
    const [minDate, setMinDate] = useState('');
    const [maxDate, setMaxDate] = useState('');

    // Fetch analyzable date range on mount
    useEffect(() => {
        const fetchDateRange = async () => {
            try {
                const res = await authFetch(`${API_BASE_URL}/api/anomalies/campaign/date-range`);
                const data = await res.json();
                if (data.min_date) setMinDate(data.min_date);
                if (data.max_date) {
                    setMaxDate(data.max_date);
                    // Set default target date to max_date if not already set
                    if (!targetDate) {
                        setTargetDate(data.max_date);
                    }
                }
            } catch (error) {
                console.error('Failed to fetch date range', error);
            }
        };
        fetchDateRange();
    }, []);

    useEffect(() => {
        if (targetDate) {
            localStorage.setItem('anomaly_target_date', targetDate);
        }
        fetchAnomalies();
    }, [targetDate]);

    const fetchAnomalies = async () => {
        setLoading(true);
        try {
            const url = targetDate
                ? `${API_BASE_URL}/api/anomalies/campaign?target_date=${targetDate}`
                : `${API_BASE_URL}/api/anomalies/campaign`;

            const res = await authFetch(url);
            const data = await res.json();

            if (Array.isArray(data)) {
                setAnomalies(data);
                if (data.length > 0) {
                    setIsOpen(true);
                }
            }
        } catch (error) {
            console.error('Failed to fetch anomalies', error);
        } finally {
            setLoading(false);
        }
    };

    const handleSort = (key) => {
        setSortConfig(prev => ({
            key,
            direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
        }));
    };

    // Sorted anomalies
    const sortedAnomalies = useMemo(() => {
        if (!sortConfig.key) return anomalies;

        return [...anomalies].sort((a, b) => {
            let aVal, bVal;

            switch (sortConfig.key) {
                case 'conversions':
                    aVal = a.current_conv || 0;
                    bVal = b.current_conv || 0;
                    break;
                case 'roas':
                    aVal = a.curr_roas || 0;
                    bVal = b.curr_roas || 0;
                    break;
                case 'cpa':
                    aVal = a.curr_cpa || 0;
                    bVal = b.curr_cpa || 0;
                    break;
                case 'roas_change':
                    aVal = a.prev_roas ? ((a.curr_roas - a.prev_roas) / a.prev_roas) : 0;
                    bVal = b.prev_roas ? ((b.curr_roas - b.prev_roas) / b.prev_roas) : 0;
                    break;
                case 'cpa_change':
                    aVal = a.prev_cpa ? ((a.curr_cpa - a.prev_cpa) / a.prev_cpa) : 0;
                    bVal = b.prev_cpa ? ((b.curr_cpa - b.prev_cpa) / b.prev_cpa) : 0;
                    break;
                default:
                    aVal = a[sortConfig.key] || 0;
                    bVal = b[sortConfig.key] || 0;
            }

            if (sortConfig.direction === 'asc') {
                return aVal - bVal;
            }
            return bVal - aVal;
        });
    }, [anomalies, sortConfig]);

    // Note: Even if no anomalies are found for a selected date, 
    // we should still show the header if the user has a date selected 
    // so they can change the date back. 
    // But if it's the initial load and nothing found, we can stay hidden.

    if (loading && !targetDate) return <div style={{ padding: '20px', textAlign: 'center', color: '#666' }}>Loading Anomalies...</div>;

    // If no data and no date selected, hide (standard behavior)
    if (anomalies.length === 0 && !targetDate) return null;

    return (
        <div style={{
            marginBottom: '20px',
            background: '#fff',
            border: '1px solid #ffcaca',
            borderRadius: '12px',
            overflow: 'hidden',
            boxShadow: '0 4px 12px rgba(255, 0, 0, 0.05)'
        }}>
            {/* Header / Toolbar */}
            <div style={{
                padding: '12px 20px',
                background: '#ffe6e6',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                borderBottom: isOpen ? '1px solid #ffcaca' : 'none'
            }}>
                <div
                    onClick={() => setIsOpen(!isOpen)}
                    style={{ display: 'flex', alignItems: 'center', gap: '10px', color: '#d32f2f', cursor: 'pointer', flex: 1 }}
                >
                    <AlertTriangle size={20} fill="#d32f2f" color="#fff" />
                    <span style={{ fontWeight: '600', fontSize: '15px' }}>
                        Anomaly Monitor {anomalies.length > 0 ? `(${anomalies.length})` : '(Clear)'}
                    </span>
                </div>

                <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                    {/* Date Selector */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', background: '#fff', padding: '4px 10px', borderRadius: '6px', border: '1px solid #ffcaca' }}>
                        <Calendar size={14} color="#d32f2f" />
                        <span style={{ fontSize: '12px', color: '#d32f2f', fontWeight: '500' }}>Analysis Date:</span>
                        <input
                            type="date"
                            value={targetDate || (anomalies.length > 0 ? anomalies[0].date : '')}
                            onChange={(e) => setTargetDate(e.target.value)}
                            min={minDate}
                            max={maxDate}
                            style={{
                                border: 'none',
                                outline: 'none',
                                fontSize: '12px',
                                color: '#333',
                                cursor: 'pointer',
                                background: 'transparent'
                            }}
                        />
                    </div>

                    <div
                        onClick={() => setIsOpen(!isOpen)}
                        style={{ color: '#d32f2f', fontSize: '13px', cursor: 'pointer', fontWeight: '500' }}
                    >
                        {isOpen ? 'Collapse' : 'Expand'}
                    </div>
                </div>
            </div>

            {/* List */}
            {isOpen && (
                <div style={{ background: '#fff' }}>
                    {anomalies.length === 0 ? (
                        <div style={{ padding: '30px', textAlign: 'center', color: '#999', fontSize: '14px' }}>
                            No anomalies detected for the selected date.
                        </div>
                    ) : (
                        <>
                            {/* Sorting Header */}
                            <div style={{
                                padding: '10px 20px',
                                borderBottom: '1px solid #fdecde',
                                background: '#fef5f5',
                                display: 'flex',
                                gap: '10px',
                                alignItems: 'center',
                                fontSize: '11px',
                                color: '#888'
                            }}>
                                <span style={{ marginRight: '8px' }}>Sort by:</span>
                                {[
                                    { key: 'conversions', label: 'Conversions' },
                                    { key: 'roas', label: 'ROAS' },
                                    { key: 'roas_change', label: 'ROAS Change' },
                                    { key: 'cpa', label: 'CPA' },
                                    { key: 'cpa_change', label: 'CPA Change' }
                                ].map(({ key, label }) => (
                                    <button
                                        key={key}
                                        onClick={() => handleSort(key)}
                                        style={{
                                            padding: '4px 10px',
                                            border: sortConfig.key === key ? '1px solid #d32f2f' : '1px solid #ddd',
                                            borderRadius: '4px',
                                            background: sortConfig.key === key ? '#ffebee' : '#fff',
                                            color: sortConfig.key === key ? '#d32f2f' : '#666',
                                            cursor: 'pointer',
                                            fontSize: '11px',
                                            fontWeight: sortConfig.key === key ? '600' : '400',
                                            display: 'flex',
                                            alignItems: 'center',
                                            gap: '4px'
                                        }}
                                    >
                                        {label}
                                        {sortConfig.key === key && (
                                            sortConfig.direction === 'desc' ? <ArrowDown size={12} /> : <ArrowUp size={12} />
                                        )}
                                    </button>
                                ))}
                            </div>

                            {sortedAnomalies.map((item, index) => {
                                // Build anomaly-specific URL params
                                const anomalyParams = new URLSearchParams({
                                    anomaly: 'true',
                                    reason: item.reason || '',
                                    start_date: targetDate,
                                    end_date: targetDate,
                                    curr_roas: item.curr_roas?.toFixed(2) || '',
                                    prev_roas: item.prev_roas?.toFixed(2) || '',
                                    curr_cpa: item.curr_cpa?.toFixed(2) || '',
                                    prev_cpa: item.prev_cpa?.toFixed(2) || '',
                                    growth: item.growth?.toFixed(2) || ''
                                });
                                return (
                                    <div
                                        key={index}
                                        onClick={() => navigate(`/campaign/${encodeURIComponent(item.campaign)}?${anomalyParams.toString()}`)}
                                        style={{
                                            padding: '15px 20px',
                                            borderBottom: index === anomalies.length - 1 ? 'none' : '1px solid #fdecde',
                                            display: 'flex',
                                            alignItems: 'center',
                                            justifyContent: 'space-between',
                                            gap: '20px',
                                            cursor: 'pointer',
                                            transition: 'background 0.15s ease'
                                        }}
                                        onMouseEnter={(e) => e.currentTarget.style.background = '#fff5f5'}
                                        onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                                    >
                                        {/* Left: Campaign & Reason */}
                                        <div style={{ flex: 1 }}>
                                            <div style={{ fontSize: '15px', fontWeight: 'bold', color: '#333', marginBottom: '4px' }}>
                                                {item.campaign}
                                            </div>
                                            <div style={{ fontSize: '12px', color: '#c62828', display: 'flex', alignItems: 'center', gap: '6px', fontWeight: '500' }}>
                                                <TrendingDown size={14} />
                                                {item.reason}
                                            </div>
                                        </div>

                                        {/* Metrics */}
                                        <div style={{ display: 'flex', gap: '25px', fontSize: '12px', color: '#555' }}>
                                            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end' }}>
                                                <span style={{ color: '#999', fontSize: '10px', textTransform: 'uppercase' }}>Conv Volume</span>
                                                <span style={{ fontWeight: 600 }}>
                                                    {item.prev_conv.toFixed(2)} <ArrowRight size={10} style={{ margin: '0 4px' }} /> {item.current_conv.toFixed(2)}
                                                </span>
                                            </div>
                                            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', minWidth: '90px' }}>
                                                <span style={{ color: '#999', fontSize: '10px', textTransform: 'uppercase' }}>ROAS Trend</span>
                                                <span style={{ fontWeight: 600, color: item.curr_roas < item.prev_roas ? '#d32f2f' : '#333' }}>
                                                    {item.prev_roas.toFixed(2)} <ArrowRight size={10} style={{ margin: '0 4px' }} /> {item.curr_roas.toFixed(2)}
                                                </span>
                                            </div>
                                            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', minWidth: '90px' }}>
                                                <span style={{ color: '#999', fontSize: '10px', textTransform: 'uppercase' }}>CPA Trend</span>
                                                <span style={{ fontWeight: 600, color: item.curr_cpa > item.prev_cpa ? '#d32f2f' : '#333' }}>
                                                    {item.prev_cpa.toFixed(2)} <ArrowRight size={10} style={{ margin: '0 4px' }} /> {item.curr_cpa.toFixed(2)}
                                                </span>
                                            </div>
                                        </div>
                                    </div>
                                );
                            })}
                        </>
                    )}
                </div>
            )}
        </div>
    );
};

export default AnomalyDashboard;
