import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { LayoutDashboard } from 'lucide-react';
import ResizableTable from './ResizableTable';

export default function CampaignListDashboard() {
    const navigate = useNavigate();
    const [tableData, setTableData] = useState([]);
    const [columns, setColumns] = useState([]);
    const [loading, setLoading] = useState(false);
    const [startDate, setStartDate] = useState('');
    const [endDate, setEndDate] = useState('');
    const [displayLimit, setDisplayLimit] = useState(50);
    const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });

    useEffect(() => {
        setDisplayLimit(50); // Reset limit
        fetchData();
    }, [startDate, endDate]);

    const fetchData = async () => {
        setLoading(true);
        try {
            // Construct query params
            const params = new URLSearchParams();
            if (startDate) params.append('start_date', startDate);
            if (endDate) params.append('end_date', endDate);

            const response = await fetch(`http://localhost:8000/api/tables/campaign?${params.toString()}`);
            const data = await response.json();

            if (data.error) {
                console.error('Error:', data.error);
            } else {
                setTableData(data.data || []);
                setColumns(data.columns || []);
            }
        } catch (error) {
            console.error('Fetch error:', error);
        } finally {
            setLoading(false);
        }
    };

    // Sort full dataset first
    const sortedTableData = React.useMemo(() => {
        if (!sortConfig.key) return tableData;

        return [...tableData].sort((a, b) => {
            let aVal = a[sortConfig.key];
            let bVal = b[sortConfig.key];

            // Normalize
            if (aVal === null || aVal === undefined) aVal = '';
            if (bVal === null || bVal === undefined) bVal = '';

            // Clean strings (remove $, %, ,) for reliable number detection
            const clean = (val) => {
                if (typeof val !== 'string') return val;
                return val.replace(/[$,%]/g, '').replace(/,/g, '');
            };

            const cleanA = clean(aVal);
            const cleanB = clean(bVal);

            // Strict number check: Must be a valid number and not an empty string
            // parseFloat("2026-01-01") is 2026 (Bad for dates), so we check if Number() is valid
            const isANum = !isNaN(cleanA) && !isNaN(parseFloat(cleanA)) && String(cleanA).trim() !== '';
            const isBNum = !isNaN(cleanB) && !isNaN(parseFloat(cleanB)) && String(cleanB).trim() !== '';

            if (isANum && isBNum) {
                return sortConfig.direction === 'asc' ? parseFloat(cleanA) - parseFloat(cleanB) : parseFloat(cleanB) - parseFloat(cleanA);
            }

            const aStr = String(aVal).toLowerCase();
            const bStr = String(bVal).toLowerCase();

            if (aStr < bStr) return sortConfig.direction === 'asc' ? -1 : 1;
            if (aStr > bStr) return sortConfig.direction === 'asc' ? 1 : -1;
            return 0;
        });
    }, [tableData, sortConfig]);

    // Check if a campaign has anomalies (ROAS drop or CPA rise)
    const hasAnomaly = (row) => {
        try {
            const conv = parseFloat(row.conversions) || 0;
            if (conv < 3) return false;

            // ROAS check
            const roas = parseFloat(row.roas) || 0;
            const roasComp = parseFloat(row.roascompare_to) || 0;
            if (roasComp > 0 && roas < (roasComp * 0.8)) return true;

            // CPA check
            const cpa = parseFloat(row.cost_conv) || 0;
            const cpaComp = parseFloat(row.cost_conv_compare_to) || 0;
            if (cpaComp > 0 && cpa > (cpaComp * 1.25)) return true;

            return false;
        } catch {
            return false;
        }
    };

    // Handle row click to navigate to campaign detail
    const handleRowClick = (row) => {
        if (row.campaign && row.campaign !== '--') {
            const params = new URLSearchParams();
            if (startDate) params.append('start_date', startDate);
            if (endDate) params.append('end_date', endDate);

            const search = params.toString();
            const url = `/campaign/${encodeURIComponent(row.campaign)}${search ? '?' + search : ''}`;
            navigate(url);
        }
    };

    return (
        <div style={{
            width: '100vw',
            height: '100vh',
            background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            padding: '20px',
            boxSizing: 'border-box',
            overflow: 'hidden'
        }}>
            <div style={{
                width: '100%',
                maxWidth: '1400px',
                height: '60vh',
                background: 'rgba(255, 255, 255, 0.95)',
                borderRadius: '20px',
                boxShadow: '0 20px 60px rgba(0,0,0,0.3)',
                display: 'flex',
                flexDirection: 'column',
                overflow: 'hidden'
            }}>
                {/* Header */}
                <div style={{
                    padding: '30px',
                    background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                    color: 'white',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    gap: '15px'
                }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                        <LayoutDashboard size={32} />
                        <h1 style={{ margin: 0, fontSize: '28px', fontWeight: '600' }}>
                            Campaigns Overview
                        </h1>
                    </div>

                    {/* Date Picker Section */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                        <div style={{ display: 'flex', flexDirection: 'column' }}>
                            <label style={{ fontSize: '12px', opacity: 0.8, marginBottom: '2px' }}>Start Date</label>
                            <input
                                type="date"
                                value={startDate}
                                onChange={(e) => setStartDate(e.target.value)}
                                style={{
                                    padding: '8px',
                                    borderRadius: '6px',
                                    border: 'none',
                                    outline: 'none',
                                    color: '#333'
                                }}
                            />
                        </div>
                        <span style={{ fontSize: '20px', fontWeight: 'bold' }}>-</span>
                        <div style={{ display: 'flex', flexDirection: 'column' }}>
                            <label style={{ fontSize: '12px', opacity: 0.8, marginBottom: '2px' }}>End Date</label>
                            <input
                                type="date"
                                value={endDate}
                                onChange={(e) => setEndDate(e.target.value)}
                                style={{
                                    padding: '8px',
                                    borderRadius: '6px',
                                    border: 'none',
                                    outline: 'none',
                                    color: '#333'
                                }}
                            />
                        </div>
                        {(startDate || endDate) && (
                            <button
                                onClick={() => { setStartDate(''); setEndDate(''); }}
                                style={{
                                    marginLeft: '10px',
                                    padding: '8px 12px',
                                    background: 'rgba(255,255,255,0.2)',
                                    border: '1px solid rgba(255,255,255,0.4)',
                                    color: 'white',
                                    borderRadius: '6px',
                                    cursor: 'pointer'
                                }}
                            >
                                Clear
                            </button>
                        )}
                    </div>
                </div>

                {/* Table Container with Infinite Scroll */}
                <div
                    style={{ flex: 1, overflow: 'auto', padding: '20px' }}
                    onScroll={(e) => {
                        const { scrollTop, scrollHeight, clientHeight } = e.target;
                        // Load more when scrolled to bottom (within 50px)
                        if (scrollHeight - scrollTop - clientHeight < 50) {
                            if (displayLimit < tableData.length) {
                                setDisplayLimit(prev => prev + 50);
                            }
                        }
                    }}
                >
                    {loading ? (
                        <div style={{ textAlign: 'center', padding: '50px', fontSize: '18px', color: '#666' }}>
                            Loading campaigns...
                        </div>
                    ) : (
                        <>
                            <ResizableTable
                                columns={columns}
                                data={sortedTableData.slice(0, displayLimit)}
                                onRowClick={handleRowClick}
                                sortConfig={sortConfig}
                                onSort={(key, direction) => setSortConfig({ key, direction })}
                                getRowStyle={(row) => {
                                    if (hasAnomaly(row)) {
                                        return { backgroundColor: '#ffe6e6' };
                                    }
                                    return {};
                                }}
                            />
                        </>
                    )}
                </div>

                {/* Footer Status */}
                <div style={{
                    padding: '10px 20px',
                    borderTop: '1px solid #dee2e6',
                    background: 'white',
                    fontSize: '13px',
                    color: '#999'
                }}>
                    ({Math.min(displayLimit, tableData.length)} / {tableData.length})
                </div>
            </div>
        </div>
    );
}
