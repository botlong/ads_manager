import React, { useState, useEffect } from 'react';
import { Package, TrendingDown, ArrowRight, Calendar } from 'lucide-react';
import { API_BASE_URL } from '../config';
import { useAuth } from '../context/AuthContext';

const ProductMonitor = () => {
    const [anomalies, setAnomalies] = useState([]);
    const [loading, setLoading] = useState(true);
    const [isOpen, setIsOpen] = useState(true);
    const [targetDate, setTargetDate] = useState(() => {
        return localStorage.getItem('product_anomaly_target_date') || '';
    });
    const { authFetch } = useAuth();

    useEffect(() => {
        if (targetDate) {
            localStorage.setItem('product_anomaly_target_date', targetDate);
        }
        fetchAnomalies();
    }, [targetDate]);

    const fetchAnomalies = async () => {
        setLoading(true);
        try {
            const url = targetDate
                ? `${API_BASE_URL}/api/anomalies/product?target_date=${targetDate}`
                : `${API_BASE_URL}/api/anomalies/product`;

            const res = await authFetch(url);
            const data = await res.json();

            if (Array.isArray(data)) {
                setAnomalies(data);
                if (data.length > 0) {
                    setIsOpen(true);
                }
            }
        } catch (error) {
            console.error('Failed to fetch product anomalies', error);
        } finally {
            setLoading(false);
        }
    };

    if (loading && !targetDate) return <div style={{ padding: '20px', textAlign: 'center', color: '#666' }}>Loading Product Anomalies...</div>;

    if (anomalies.length === 0 && !targetDate) return null;

    return (
        <div style={{
            marginBottom: '20px',
            background: '#fff',
            border: '1px solid #ffd4a8',
            borderRadius: '12px',
            overflow: 'hidden',
            boxShadow: '0 4px 12px rgba(255, 140, 0, 0.05)'
        }}>
            {/* Header / Toolbar */}
            <div style={{
                padding: '12px 20px',
                background: '#fff4e6',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                borderBottom: isOpen ? '1px solid #ffd4a8' : 'none'
            }}>
                <div
                    onClick={() => setIsOpen(!isOpen)}
                    style={{ display: 'flex', alignItems: 'center', gap: '10px', color: '#e67700', cursor: 'pointer', flex: 1 }}
                >
                    <Package size={20} color="#e67700" />
                    <span style={{ fontWeight: '600', fontSize: '15px' }}>
                        Product Monitor {anomalies.length > 0 ? `(${anomalies.length})` : '(Clear)'}
                    </span>
                </div>

                <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                    {/* Date Selector */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', background: '#fff', padding: '4px 10px', borderRadius: '6px', border: '1px solid #ffd4a8' }}>
                        <Calendar size={14} color="#e67700" />
                        <span style={{ fontSize: '12px', color: '#e67700', fontWeight: '500' }}>Analysis Date:</span>
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
                        style={{ color: '#e67700', fontSize: '13px', cursor: 'pointer', fontWeight: '500' }}
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
                            No product anomalies detected for the selected date.
                        </div>
                    ) : (
                        anomalies.map((item, index) => (
                            <div key={index} style={{
                                padding: '15px 20px',
                                borderBottom: index === anomalies.length - 1 ? 'none' : '1px solid #fff4e6',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'space-between',
                                gap: '20px'
                            }}>
                                {/* Left: Product & Reason */}
                                <div style={{ flex: 1 }}>
                                    <div style={{ fontSize: '15px', fontWeight: 'bold', color: '#333', marginBottom: '4px' }}>
                                        {item.title}
                                    </div>
                                    <div style={{ fontSize: '11px', color: '#888', marginBottom: '4px' }}>
                                        ID: {item.item_id}
                                    </div>
                                    <div style={{ fontSize: '12px', color: '#e67700', display: 'flex', alignItems: 'center', gap: '6px', fontWeight: '500' }}>
                                        <TrendingDown size={14} />
                                        {item.reason}
                                    </div>
                                </div>

                                {/* Metrics */}
                                <div style={{ display: 'flex', gap: '25px', fontSize: '12px', color: '#555' }}>
                                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end' }}>
                                        <span style={{ color: '#999', fontSize: '10px', textTransform: 'uppercase' }}>Conv Volume</span>
                                        <span style={{ fontWeight: 600 }}>
                                            {item.prev_conv?.toFixed(2) || '0.00'} <ArrowRight size={10} style={{ margin: '0 4px' }} /> {item.current_conv?.toFixed(2) || '0.00'}
                                        </span>
                                    </div>
                                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', minWidth: '90px' }}>
                                        <span style={{ color: '#999', fontSize: '10px', textTransform: 'uppercase' }}>ROAS Trend</span>
                                        <span style={{ fontWeight: 600, color: (item.curr_roas < item.prev_roas) ? '#e67700' : '#333' }}>
                                            {item.prev_roas?.toFixed(2) || '0.00'} <ArrowRight size={10} style={{ margin: '0 4px' }} /> {item.curr_roas?.toFixed(2) || '0.00'}
                                        </span>
                                    </div>
                                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', minWidth: '90px' }}>
                                        <span style={{ color: '#999', fontSize: '10px', textTransform: 'uppercase' }}>CPA Trend</span>
                                        <span style={{ fontWeight: 600, color: (item.curr_cpa > item.prev_cpa) ? '#e67700' : '#333' }}>
                                            {item.prev_cpa?.toFixed(2) || '0.00'} <ArrowRight size={10} style={{ margin: '0 4px' }} /> {item.curr_cpa?.toFixed(2) || '0.00'}
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

export default ProductMonitor;
