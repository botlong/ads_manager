import React, { useState, useEffect } from 'react';
import { AlertTriangle, TrendingDown, ArrowRight, Calendar } from 'lucide-react';

const AnomalyDashboard = () => {
    const [anomalies, setAnomalies] = useState([]);
    const [loading, setLoading] = useState(true);
    const [isOpen, setIsOpen] = useState(true);
    const [targetDate, setTargetDate] = useState(() => {
        return localStorage.getItem('anomaly_target_date') || '';
    });

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
                ? `http://localhost:8000/api/anomalies/campaign?target_date=${targetDate}`
                : 'http://localhost:8000/api/anomalies/campaign';

            const res = await fetch(url);
            const data = await res.json();

            if (Array.isArray(data)) {
                setAnomalies(data);
                // If it's the first load and we don't have a target date, 
                // we might want to set the targetDate to the date from the first anomaly 
                // but the user only asked for "default to last day in DB".
                // Backend already picks the last day if targetDate is empty.
                if (data.length > 0) {
                    setIsOpen(true);
                    // Update the input value to match the actual date being looked at if the user hasn't picked one
                    if (!targetDate && data[0].date) {
                        // setTargetDate(data[0].date); // Avoid infinite loop, only set if empty
                    }
                }
            }
        } catch (error) {
            console.error('Failed to fetch anomalies', error);
        } finally {
            setLoading(false);
        }
    };

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
                        anomalies.map((item, index) => (
                            <div key={index} style={{
                                padding: '15px 20px',
                                borderBottom: index === anomalies.length - 1 ? 'none' : '1px solid #fdecde',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'space-between',
                                gap: '20px'
                            }}>
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
                        ))
                    )}
                </div>
            )}
        </div>
    );
};

export default AnomalyDashboard;
