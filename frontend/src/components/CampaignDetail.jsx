import React, { useState, useEffect } from 'react';
import { useParams, useNavigate, useSearchParams } from 'react-router-dom';
import { ArrowLeft, Loader } from 'lucide-react';
import ResizableTable from './ResizableTable';

// Helper for Anomaly Check
const hasAnomaly = (row) => {
    try {
        const conv = parseFloat(row.conversions) || 0;
        if (conv < 3) return false;

        const roas = parseFloat(row.roas || row.conv_value_cost) || 0;
        const roasComp = parseFloat(row.roas_compare) || 0;
        if (roasComp > 0 && roas < (roasComp * 0.8)) return true;

        const cpa = parseFloat(row.cpa || row.cost_conv) || 0;
        const cpaComp = parseFloat(row.cpa_compare) || 0;
        if (cpaComp > 0 && cpa > ((cpa - cpaComp) * 1.25)) return true;

        return false;
    } catch {
        return false;
    }
};

const DetailTable = ({ data, columns }) => {
    const [displayLimit, setDisplayLimit] = useState(50);
    const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });

    // Internal sorting
    const sortedData = React.useMemo(() => {
        if (!sortConfig.key) return data;

        return [...data].sort((a, b) => {
            let aVal = a[sortConfig.key];
            let bVal = b[sortConfig.key];

            if (aVal === null || aVal === undefined) aVal = '';
            if (bVal === null || bVal === undefined) bVal = '';

            const clean = (val) => {
                if (typeof val !== 'string') return val;
                return val.replace(/[$,%]/g, '').replace(/,/g, '');
            };

            const cleanA = clean(aVal);
            const cleanB = clean(bVal);

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
    }, [data, sortConfig]);

    return (
        <div style={{ display: 'flex', flexDirection: 'column' }}>
            <div
                style={{
                    maxHeight: '500px',
                    overflow: 'auto',
                    border: '1px solid #eee',
                    borderRadius: '8px'
                }}
                onScroll={(e) => {
                    const { scrollTop, scrollHeight, clientHeight } = e.target;
                    if (scrollHeight - scrollTop - clientHeight < 50) {
                        if (displayLimit < sortedData.length) {
                            setDisplayLimit(prev => prev + 50);
                        }
                    }
                }}
            >
                <ResizableTable
                    columns={columns}
                    data={sortedData.slice(0, displayLimit)}
                    sortConfig={sortConfig}
                    onSort={(key, direction) => setSortConfig({ key, direction })}
                    getRowStyle={(row) => {
                        if (hasAnomaly(row)) return { backgroundColor: '#ffe6e6' };
                        return {};
                    }}
                />
            </div>
            <div style={{
                padding: '8px 15px',
                fontSize: '12px',
                color: '#999',
                textAlign: 'right',
                background: '#fafafa',
                borderTop: '1px solid #eee'
            }}>
                ({Math.min(displayLimit, sortedData.length)} / {sortedData.length})
            </div>
        </div>
    );
};

export default function CampaignDetail() {

    const { campaignName } = useParams();
    const navigate = useNavigate();
    const [searchParams] = useSearchParams();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);

    const startDate = searchParams.get('start_date');
    const endDate = searchParams.get('end_date');

    useEffect(() => {
        fetchCampaignDetails();
    }, [campaignName, startDate, endDate]);

    const fetchCampaignDetails = async () => {
        setLoading(true);
        try {
            const params = new URLSearchParams();
            if (startDate) params.append('start_date', startDate);
            if (endDate) params.append('end_date', endDate);

            const queryString = params.toString();
            const url = `http://localhost:8000/api/campaigns/${encodeURIComponent(campaignName)}/details${queryString ? '?' + queryString : ''}`;

            const response = await fetch(url);
            const result = await response.json();

            // Post-process: Calculate real-time ROAS (Income / Cost) for strict accuracy
            Object.keys(result).forEach(tableName => {
                const tableInfo = result[tableName];
                if (tableInfo.data && Array.isArray(tableInfo.data)) {
                    tableInfo.data = tableInfo.data.map(row => {
                        const cost = parseFloat(row.cost) || 0;
                        const value = parseFloat(row.conv_value) || 0;

                        // Calculate strict ROAS
                        const calculatedRoas = cost > 0 ? (value / cost) : 0;

                        // Calculate strict CPA
                        const conv = parseFloat(row.conversions) || 0;
                        const calculatedCpa = conv > 0 ? (cost / conv) : 0;

                        // Overwrite standard columns with calculated value format (2 decimals)
                        if (row.hasOwnProperty('conv_value_cost')) {
                            row.conv_value_cost = calculatedRoas.toFixed(2);
                        }
                        if (row.hasOwnProperty('roas')) {
                            row.roas = calculatedRoas.toFixed(2);
                        }

                        // Overwrite CPA columns with calculated value
                        if (row.hasOwnProperty('cost_conv')) {
                            row.cost_conv = calculatedCpa.toFixed(2);
                        }
                        if (row.hasOwnProperty('cpa')) {
                            row.cpa = calculatedCpa.toFixed(2);
                        }

                        return row;
                    });
                }
            });

            setData(result);
        } catch (error) {
            console.error('Error fetching campaign details:', error);
        } finally {
            setLoading(false);
        }
    };

    // Anomaly check for all tables
    // Anomaly check moved to helper function above

    const renderTable = (tableName, tableData) => {
        if (!tableData || tableData.error) {
            return (
                <div style={{ padding: '20px', textAlign: 'center', color: '#999' }}>
                    Error loading {tableName} data
                </div>
            );
        }

        if (!tableData.data || tableData.data.length === 0) {
            return (
                <div style={{ padding: '20px', textAlign: 'center', color: '#999' }}>
                    No {tableName} data found for this campaign
                </div>
            );
        }

        return <DetailTable data={tableData.data} columns={tableData.columns} />;
    };

    const tableLabels = {
        search_term: 'Search Term',
        channel: 'Channel',
        asset: 'Asset',
        audience: 'Audience',
        age: 'Age',
        gender: 'Gender',
        location_by_cities_all_campaign: 'Location',
        ad_schedule: 'Ad Schedule'
    };

    if (loading) {
        return (
            <div style={{
                width: '100%',
                height: '100vh',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
            }}>
                <div style={{
                    background: 'white',
                    padding: '40px',
                    borderRadius: '12px',
                    boxShadow: '0 10px 40px rgba(0,0,0,0.2)',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '15px'
                }}>
                    <Loader size={24} className="spinner" style={{ animation: 'spin 1s linear infinite' }} />
                    <span style={{ fontSize: '16px', color: '#666' }}>Loading campaign details...</span>
                </div>
            </div>
        );
    }

    return (
        <div style={{
            width: '100%',
            minHeight: '100vh',
            background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
            padding: '20px'
        }}>
            <div style={{
                maxWidth: '1400px',
                margin: '0 auto',
                background: 'white',
                borderRadius: '16px',
                boxShadow: '0 10px 40px rgba(0,0,0,0.2)',
                overflow: 'hidden'
            }}>
                {/* Header */}
                <div style={{
                    padding: '25px 30px',
                    background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                    color: 'white',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '15px'
                }}>
                    <button
                        onClick={() => navigate('/')}
                        style={{
                            background: 'rgba(255,255,255,0.2)',
                            border: 'none',
                            color: 'white',
                            padding: '10px 20px',
                            borderRadius: '8px',
                            cursor: 'pointer',
                            display: 'flex',
                            alignItems: 'center',
                            gap: '8px',
                            fontSize: '14px',
                            transition: 'background 0.2s'
                        }}
                        onMouseOver={(e) => e.target.style.background = 'rgba(255,255,255,0.3)'}
                        onMouseOut={(e) => e.target.style.background = 'rgba(255,255,255,0.2)'}
                    >
                        <ArrowLeft size={18} />
                        Back to Campaigns
                    </button>
                    <div style={{ flex: 1 }}>
                        <h1 style={{ margin: 0, fontSize: '24px', fontWeight: '600' }}>
                            {decodeURIComponent(campaignName)}
                        </h1>
                        <p style={{ margin: '5px 0 0 0', fontSize: '14px', opacity: 0.9 }}>
                            Campaign Details
                        </p>
                    </div>
                </div>

                {/* Content */}
                <div style={{ padding: '30px' }}>
                    {data && Object.keys(tableLabels).map((tableName) => (
                        <div key={tableName} style={{
                            marginBottom: '30px',
                            background: '#f8f9fa',
                            borderRadius: '12px',
                            overflow: 'hidden',
                            boxShadow: '0 2px 8px rgba(0,0,0,0.1)'
                        }}>
                            <div style={{
                                padding: '15px 20px',
                                background: 'white',
                                borderBottom: '2px solid #667eea'
                            }}>
                                <h2 style={{
                                    margin: 0,
                                    fontSize: '18px',
                                    fontWeight: '600',
                                    color: '#333'
                                }}>
                                    {tableLabels[tableName]}
                                </h2>
                            </div>
                            {renderTable(tableName, data[tableName])}
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
