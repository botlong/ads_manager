import React, { useState, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { LayoutDashboard, Filter, X, Zap, Activity, ShoppingBag, RotateCcw, LogOut, User } from 'lucide-react';
import ResizableTable from './ResizableTable';
import AnomalyDashboard from './AnomalyDashboard';
import ProductMonitor from './ProductMonitor';
import { API_BASE_URL } from '../config';
import { useAuth } from '../context/AuthContext';

export default function App() {
    const navigate = useNavigate();
    const [searchParams, setSearchParams] = useSearchParams();
    const { user, logout, authFetch } = useAuth();

    // Shared State -> Removed redundant explicit dates.
    // Date filtering is now handled solely through the 'filters' and 'productFilters' arrays.

    // Campaign Table State
    const [tableData, setTableData] = useState([]);
    const [columns, setColumns] = useState([]);
    const [loading, setLoading] = useState(false);
    const [displayLimit, setDisplayLimit] = useState(50);
    const [sortConfig, setSortConfig] = useState(() => {
        const saved = localStorage.getItem('campaign_sort');
        return saved ? JSON.parse(saved) : { key: null, direction: 'asc' };
    });
    const [filters, setFilters] = useState(() => {
        const saved = localStorage.getItem('campaign_filters');
        return saved ? JSON.parse(saved) : [];
    });
    const [isFilterOpen, setIsFilterOpen] = useState(false);
    const [newFilter, setNewFilter] = useState({ field: 'roas', operator: '>', value: '' });

    // Product Table State
    const [productData, setProductData] = useState([]);
    const [productColumns, setProductColumns] = useState([]);
    const [productLoading, setProductLoading] = useState(false);
    const [productDisplayLimit, setProductDisplayLimit] = useState(50);
    const [productSortConfig, setProductSortConfig] = useState(() => {
        const saved = localStorage.getItem('product_sort');
        return saved ? JSON.parse(saved) : { key: 'cost', direction: 'desc' };
    });
    const [productFilters, setProductFilters] = useState(() => {
        const saved = localStorage.getItem('product_filters');
        return saved ? JSON.parse(saved) : [];
    });
    const [isProductFilterOpen, setIsProductFilterOpen] = useState(false);
    const [newProductFilter, setNewProductFilter] = useState({ field: 'cost', operator: '>', value: '' });

    // Field Config for Campaigns
    const FIELD_TYPES = {
        date: 'date',
        campaign: 'text',
        campaign_status: 'text',
        campaign_type: 'text',
        ad_group: 'text',
        roas: 'number',
        cpa: 'number',
        cost: 'number',
        conversions: 'number',
        conv_value: 'number',
        clicks: 'number',
        impressions: 'number',
        ctr: 'percent',
        conversions_rate: 'percent',
        search_impr_share: 'percent',
        budget: 'number'
    };

    // Field Config for Products
    const PRODUCT_FIELD_TYPES = {
        date: 'date',
        title: 'text',
        item_id: 'text',
        merchant_id: 'text',
        status: 'text',
        issues: 'text',
        price: 'number',
        clicks: 'number',
        impr: 'number',
        ctr: 'percent',
        avg_cpc: 'number',
        cost: 'number'
    };

    const getFieldType = (field, isProduct = false) => {
        const types = isProduct ? PRODUCT_FIELD_TYPES : FIELD_TYPES;
        return types[field] || 'text';
    };

    const getUniqueValues = (field, isProduct = false) => {
        const data = isProduct ? productData : tableData;
        if (!data) return [];
        const values = new Set(data.map(row => row[field]).filter(v => v !== null && v !== undefined && v !== ''));
        return Array.from(values).sort();
    };

    const parseValue = (val) => {
        if (typeof val === 'number') return val;
        if (!val) return 0;
        const str = String(val).replace(/[$,%]/g, '').replace(/,/g, '');
        return parseFloat(str) || 0;
    };



    // Persist Campaign State
    useEffect(() => {
        localStorage.setItem('campaign_filters', JSON.stringify(filters));
    }, [filters]);

    useEffect(() => {
        localStorage.setItem('campaign_sort', JSON.stringify(sortConfig));
    }, [sortConfig]);

    // Campaign Fetch Effect - Triggered by filter/sort changes
    useEffect(() => {
        setDisplayLimit(50);
        fetchData();
    }, [filters, sortConfig]);

    // Product Fetch Effect - Triggered by filter/sort changes
    useEffect(() => {
        setProductDisplayLimit(50);
        fetchProductData();
    }, [productFilters, productSortConfig]);

    // Persist Product State
    useEffect(() => {
        localStorage.setItem('product_filters', JSON.stringify(productFilters));
    }, [productFilters]);

    useEffect(() => {
        localStorage.setItem('product_sort', JSON.stringify(productSortConfig));
    }, [productSortConfig]);

    const fetchData = async () => {
        setLoading(true);
        try {
            // Extract date range from advanced filters if present
            const dateFilter = filters.find(f => f.field === 'date');
            const params = new URLSearchParams();

            if (dateFilter) {
                if (dateFilter.operator === 'range') {
                    if (dateFilter.value?.min) params.append('start_date', dateFilter.value.min);
                    if (dateFilter.value?.max) params.append('end_date', dateFilter.value.max);
                } else if (dateFilter.value) {
                    params.append('start_date', dateFilter.value);
                    params.append('end_date', dateFilter.value);
                }
            }

            // Use authFetch instead of fetch
            const response = await authFetch(`${API_BASE_URL}/api/tables/campaign?${params.toString()}`);
            const data = await response.json();
            if (!data.error) {
                setTableData(data.data || []);
                setColumns(data.columns || []);
            }
        } catch (e) { console.error(e); } finally { setLoading(false); }
    };

    const fetchProductData = async () => {
        setProductLoading(true);
        try {
            // Extract date range from advanced filters if present
            const dateFilter = productFilters.find(f => f.field === 'date');
            const params = new URLSearchParams();

            if (dateFilter) {
                if (dateFilter.operator === 'range') {
                    if (dateFilter.value?.min) params.append('start_date', dateFilter.value.min);
                    if (dateFilter.value?.max) params.append('end_date', dateFilter.value.max);
                } else if (dateFilter.value) {
                    params.append('start_date', dateFilter.value);
                    params.append('end_date', dateFilter.value);
                }
            }

            // Use authFetch instead of fetch
            const response = await authFetch(`${API_BASE_URL}/api/tables/product?${params.toString()}`);
            const data = await response.json();
            if (!data.error) {
                setProductData(data.data || []);
                setProductColumns(data.columns || []);
            }
        } catch (e) { console.error(e); } finally { setProductLoading(false); }
    };

    // Filter Logic Helper
    const applyFilters = (data, filtersToApply, isProduct = false) => {
        if (!filtersToApply.length) return data;

        return data.filter((row) => {
            return filtersToApply.every(filter => {
                const type = getFieldType(filter.field, isProduct);
                const rowValue = row[filter.field];

                if (type === 'date') {
                    const rowDate = new Date(rowValue).getTime();
                    if (filter.operator === 'range') {
                        const min = filter.value.min ? new Date(filter.value.min).getTime() : -Infinity;
                        const max = filter.value.max ? new Date(filter.value.max).getTime() : Infinity;
                        return rowDate >= min && rowDate <= max;
                    }
                    const filterDate = filter.value ? new Date(filter.value).getTime() : 0;
                    switch (filter.operator) {
                        case '>': return rowDate > filterDate;
                        case '<': return rowDate < filterDate;
                        case '>=': return rowDate >= filterDate;
                        case '<=': return rowDate <= filterDate;
                        case '=': return rowDate === filterDate;
                        default: return true;
                    }
                }

                if (type === 'number' || type === 'percent') {
                    const num = parseValue(rowValue);
                    if (filter.operator === 'range') {
                        const min = parseFloat(filter.value.min) || -Infinity;
                        const max = parseFloat(filter.value.max) || Infinity;
                        return num >= min && num <= max;
                    }
                    const val = parseFloat(filter.value) || 0;
                    switch (filter.operator) {
                        case '>': return num > val;
                        case '<': return num < val;
                        case '>=': return num >= val;
                        case '<=': return num <= val;
                        case '=': return Math.abs(num - val) < 0.0001;
                        default: return true;
                    }
                } else {
                    const str = String(rowValue || '').toLowerCase();
                    const val = String(filter.value || '').toLowerCase();
                    switch (filter.operator) {
                        case 'contains': return str.includes(val);
                        case '=': return str === val;
                        default: return true;
                    }
                }
            });
        });
    };

    // Campaign Memo
    const filteredTableData = React.useMemo(() => {
        return applyFilters(tableData, filters, false);
    }, [tableData, filters]);

    const sortedTableData = React.useMemo(() => {
        if (!sortConfig.key) return filteredTableData;
        return [...filteredTableData].sort((a, b) => {
            let av = a[sortConfig.key], bv = b[sortConfig.key];
            const clean = v => parseFloat(String(v || 0).replace(/[$,%]/g, '').replace(/,/g, '')) || 0;
            if (typeof av === 'number' || !isNaN(clean(av))) return sortConfig.direction === 'asc' ? clean(av) - clean(bv) : clean(bv) - clean(av);
            return sortConfig.direction === 'asc' ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
        });
    }, [filteredTableData, sortConfig]);

    // Product Memo
    const filteredProductData = React.useMemo(() => {
        return applyFilters(productData, productFilters, true);
    }, [productData, productFilters]);

    const sortedProductData = React.useMemo(() => {
        if (!productSortConfig.key) return filteredProductData;
        return [...filteredProductData].sort((a, b) => {
            let av = a[productSortConfig.key], bv = b[productSortConfig.key];
            const clean = v => parseFloat(String(v || 0).replace(/[$,%]/g, '').replace(/,/g, '')) || 0;
            if (typeof av === 'number' || !isNaN(clean(av))) return productSortConfig.direction === 'asc' ? clean(av) - clean(bv) : clean(bv) - clean(av);
            return productSortConfig.direction === 'asc' ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
        });
    }, [filteredProductData, productSortConfig]);

    const handleRowClick = (row) => {
        if (row.campaign !== '--') {
            const detailParams = new URLSearchParams();
            const dateFilter = filters.find(f => f.field === 'date');
            if (dateFilter) {
                if (dateFilter.operator === 'range') {
                    if (dateFilter.value?.min) detailParams.append('start_date', dateFilter.value.min);
                    if (dateFilter.value?.max) detailParams.append('end_date', dateFilter.value.max);
                } else if (dateFilter.value) {
                    detailParams.append('start_date', dateFilter.value);
                    detailParams.append('end_date', dateFilter.value);
                }
            }
            navigate(`/campaign/${encodeURIComponent(row.campaign)}?${detailParams.toString()}`);
        }
    };

    // Helper for filter UI
    const renderFilterControls = (isProduct = false) => {
        const filterState = isProduct ? productFilters : filters;
        const setFilterState = isProduct ? setProductFilters : setFilters;
        const isOpen = isProduct ? isProductFilterOpen : isFilterOpen;
        const setIsOpen = isProduct ? setIsProductFilterOpen : setIsFilterOpen;
        const draftFilter = isProduct ? newProductFilter : newFilter;
        const setDraftFilter = isProduct ? setNewProductFilter : setNewFilter;
        const fieldTypes = isProduct ? PRODUCT_FIELD_TYPES : FIELD_TYPES;

        return (
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <div style={{ position: 'relative' }}>
                    <button onClick={() => setIsOpen(!isOpen)} style={{
                        display: 'flex', alignItems: 'center', gap: '6px', padding: '6px 12px',
                        background: 'white', border: '1px solid #cbd5e1', borderRadius: '8px',
                        color: '#475569', fontSize: '13px', fontWeight: '500', cursor: 'pointer'
                    }}>
                        <Filter size={14} /> Filter
                    </button>

                    {isOpen && (
                        <div style={{
                            position: 'absolute', right: 0, top: '100%', marginTop: '8px',
                            background: 'white', borderRadius: '12px',
                            boxShadow: '0 10px 40px rgba(0,0,0,0.15)',
                            padding: '15px', zIndex: 1001,
                            minWidth: '280px', color: '#333'
                        }}>
                            <h3 style={{ margin: '0 0 10px 0', fontSize: '14px', fontWeight: '600' }}>Add Condition</h3>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                                <select
                                    value={draftFilter.field}
                                    onChange={(e) => setDraftFilter({ ...draftFilter, field: e.target.value, operator: getFieldType(e.target.value, isProduct) === 'text' ? 'contains' : '>', value: '' })}
                                    style={{ padding: '8px', borderRadius: '6px', border: '1px solid #ddd' }}
                                >
                                    <option value="date">Date</option>
                                    <option disabled>── Metrics ──</option>
                                    {Object.keys(fieldTypes).filter(k => fieldTypes[k] === 'number' || fieldTypes[k] === 'percent').map(k => (
                                        <option key={k} value={k}>{k.toUpperCase().replace('_', ' ')}</option>
                                    ))}
                                    <option disabled>── Text ──</option>
                                    {Object.keys(fieldTypes).filter(k => (fieldTypes[k] === 'text' && k !== 'date')).map(k => (
                                        <option key={k} value={k}>{k.toUpperCase().replace('_', ' ')}</option>
                                    ))}
                                </select>
                                <select
                                    value={draftFilter.operator}
                                    onChange={(e) => setDraftFilter({ ...draftFilter, operator: e.target.value })}
                                    style={{ padding: '8px', borderRadius: '6px', border: '1px solid #ddd' }}
                                >
                                    {(getFieldType(draftFilter.field, isProduct) === 'number' || getFieldType(draftFilter.field, isProduct) === 'percent' || getFieldType(draftFilter.field, isProduct) === 'date') ? (
                                        <>
                                            <option value=">">&gt; Greater</option>
                                            <option value="<">&lt; Less</option>
                                            <option value=">=">&gt;= Greater or Equal</option>
                                            <option value="<=">&lt;= Less or Equal</option>
                                            <option value="=" >= Equal</option>
                                            <option value="range">Range (Between)</option>
                                        </>
                                    ) : (
                                        <>
                                            <option value="contains">Contains</option>
                                            <option value="=">Equals</option>
                                        </>
                                    )}
                                </select>
                                {draftFilter.operator === 'range' ? (
                                    <div style={{ display: 'flex', gap: '5px' }}>
                                        <input
                                            type={getFieldType(draftFilter.field, isProduct) === 'date' ? 'date' : 'number'}
                                            placeholder="Min"
                                            value={draftFilter.value?.min || ''}
                                            onChange={(e) => setDraftFilter({ ...draftFilter, value: { ...draftFilter.value, min: e.target.value } })}
                                            style={{ flex: 1, padding: '8px', borderRadius: '6px', border: '1px solid #ddd' }}
                                        />
                                        <input
                                            type={getFieldType(draftFilter.field, isProduct) === 'date' ? 'date' : 'number'}
                                            placeholder="Max"
                                            value={draftFilter.value?.max || ''}
                                            onChange={(e) => setDraftFilter({ ...draftFilter, value: { ...draftFilter.value, max: e.target.value } })}
                                            style={{ flex: 1, padding: '8px', borderRadius: '6px', border: '1px solid #ddd' }}
                                        />
                                    </div>
                                ) : (getFieldType(draftFilter.field, isProduct) === 'text' && draftFilter.operator === '=') ? (
                                    <select
                                        value={draftFilter.value}
                                        onChange={(e) => setDraftFilter({ ...draftFilter, value: e.target.value })}
                                        style={{ padding: '8px', borderRadius: '6px', border: '1px solid #ddd' }}
                                    >
                                        <option value="">Select Value...</option>
                                        {getUniqueValues(draftFilter.field, isProduct).map(val => (
                                            <option key={val} value={val}>{val}</option>
                                        ))}
                                    </select>
                                ) : (
                                    <input
                                        type={getFieldType(draftFilter.field, isProduct) === 'text' ? 'text' : (getFieldType(draftFilter.field, isProduct) === 'date' ? 'date' : 'number')}
                                        placeholder="Value"
                                        value={draftFilter.value}
                                        onChange={(e) => setDraftFilter({ ...draftFilter, value: e.target.value })}
                                        style={{ padding: '8px', borderRadius: '6px', border: '1px solid #ddd' }}
                                    />
                                )}
                                <button
                                    onClick={() => {
                                        if (draftFilter.operator === 'range') {
                                            if (!draftFilter.value?.min && !draftFilter.value?.max) return;
                                        } else {
                                            if (draftFilter.value === '' && getFieldType(draftFilter.field, isProduct) !== 'text') return;
                                        }
                                        setFilterState([...filterState, { ...draftFilter, id: Date.now() }]);
                                        setIsOpen(false);
                                    }}
                                    style={{
                                        padding: '10px', background: '#3b82f6', color: 'white',
                                        border: 'none', borderRadius: '6px', cursor: 'pointer', fontWeight: '600'
                                    }}
                                >
                                    Add Condition
                                </button>
                            </div>
                        </div>
                    )}
                    {isOpen && <div style={{ position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, zIndex: 1000 }} onClick={() => setIsOpen(false)} />}
                </div>

                <button
                    onClick={() => {
                        setFilterState([]);
                        if (isProduct) {
                            localStorage.removeItem('product_filters');
                        } else {
                            localStorage.removeItem('campaign_filters');
                        }
                    }}
                    style={{
                        display: 'flex', alignItems: 'center', gap: '6px', padding: '6px 12px',
                        background: 'white', border: '1px solid #cbd5e1', borderRadius: '8px',
                        color: '#475569', fontSize: '13px', fontWeight: '500', cursor: 'pointer'
                    }}
                >
                    <RotateCcw size={14} /> 初始化
                </button>
            </div>
        );
    };

    const renderActiveChips = (isProduct = false) => {
        const filterState = isProduct ? productFilters : filters;
        const setFilterState = isProduct ? setProductFilters : setFilters;

        if (filterState.length === 0) return null;

        return (
            <div style={{ padding: '10px 20px', background: '#f8fafc', borderBottom: '1px solid #f1f5f9', display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                {filterState.map(f => (
                    <div key={f.id} style={{
                        display: 'flex', alignItems: 'center', gap: '6px', padding: '4px 12px',
                        background: '#e0e7ff', borderRadius: '20px', color: '#4338ca',
                        fontSize: '12px', fontWeight: '600'
                    }}>
                        <span>
                            {f.field.toUpperCase().replace('_', ' ')} {f.operator === 'range' ? `${f.value.min} - ${f.value.max}` : `${f.operator} ${f.value}`}
                        </span>
                        <X size={12} cursor="pointer" onClick={() => setFilterState(filterState.filter(x => x.id !== f.id))} />
                    </div>
                ))}
            </div>
        );
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
                <div style={{ display: 'flex', alignItems: 'center', gap: '30px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '10px', color: '#6366f1', fontWeight: 'bold', fontSize: '18px' }}>
                        <Zap size={20} fill="#6366f1" />
                        AdsManager
                    </div>
                </div>

                {/* User Profile & Logout */}
                <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: '#475569', fontSize: '14px', fontWeight: '500' }}>
                        <div style={{ width: '32px', height: '32px', borderRadius: '50%', background: '#e0e7ff', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#6366f1' }}>
                            <User size={18} />
                        </div>
                        <span>{user?.username || 'User'}</span>
                    </div>
                    <button 
                        onClick={logout}
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '6px',
                            padding: '8px 12px',
                            borderRadius: '8px',
                            border: '1px solid #e2e8f0',
                            background: 'white',
                            color: '#ef4444',
                            fontSize: '13px',
                            fontWeight: '600',
                            cursor: 'pointer',
                            transition: 'all 0.2s'
                        }}
                        onMouseOver={(e) => e.currentTarget.style.background = '#fef2f2'}
                        onMouseOut={(e) => e.currentTarget.style.background = 'white'}
                    >
                        <LogOut size={14} />
                        Logout
                    </button>
                </div>
            </div>

            {/* SCROLLABLE CONTENT AREA */}
            <div style={{ flex: 1, overflowY: 'auto', padding: '20px 30px' }}>
                <div style={{ maxWidth: '1600px', margin: '0 auto', display: 'flex', flexDirection: 'column', gap: '25px' }}>

                    {/* SECTION 1: ANOMALY MONITOR */}
                    <div style={{
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e2e8f0',
                        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                        overflow: 'hidden'
                    }}>
                        <div style={{ padding: '15px 20px', borderBottom: '1px solid #f1f5f9', background: '#fafafa' }}>
                            <h2 style={{ fontSize: '16px', fontWeight: '600', color: '#334155', margin: 0, display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <Activity size={18} color="#ef4444" />
                                Anomaly Monitor
                            </h2>
                        </div>
                        <div style={{ padding: '20px' }}>
                            <AnomalyDashboard />
                            <ProductMonitor />
                        </div>
                    </div>

                    {/* SECTION 2: CAMPAIGNS OVERVIEW */}
                    <div style={{
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e2e8f0',
                        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                        display: 'flex', flexDirection: 'column',
                        minHeight: '500px'
                    }}>
                        <div style={{ padding: '15px 20px', borderBottom: '1px solid #f1f5f9', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <h2 style={{ fontSize: '16px', fontWeight: '600', color: '#334155', margin: 0, display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <LayoutDashboard size={18} color="#3b82f6" />
                                Campaigns Overview
                            </h2>
                            {renderFilterControls(false)}
                        </div>

                        {renderActiveChips(false)}

                        <div style={{ height: '400px', overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
                            <div style={{ flex: 1, overflow: 'auto' }} onScroll={(e) => {
                                if (e.target.scrollHeight - e.target.scrollTop - e.target.clientHeight < 50 && displayLimit < tableData.length) setDisplayLimit(p => p + 50);
                            }}>
                                <ResizableTable
                                    columns={columns}
                                    data={sortedTableData.slice(0, displayLimit)}
                                    onRowClick={handleRowClick}
                                    sortConfig={sortConfig}
                                    onSort={(key, direction) => setSortConfig({ key, direction })}
                                />
                            </div>
                            <div style={{ padding: '10px 20px', borderTop: '1px solid #dee2e6', background: 'white', color: '#94a3b8', fontSize: '13px' }}>
                                Showing {Math.min(displayLimit, sortedTableData.length)} of {sortedTableData.length} records (Filtered)
                            </div>
                        </div>
                    </div>

                    {/* SECTION 3: PRODUCTS OVERVIEW */}
                    <div style={{
                        background: 'white',
                        borderRadius: '12px',
                        border: '1px solid #e2e8f0',
                        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                        display: 'flex', flexDirection: 'column',
                        minHeight: '500px'
                    }}>
                        <div style={{ padding: '15px 20px', borderBottom: '1px solid #f1f5f9', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <h2 style={{ fontSize: '16px', fontWeight: '600', color: '#334155', margin: 0, display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <ShoppingBag size={18} color="#10b981" />
                                Products Overview
                            </h2>
                            {renderFilterControls(true)}
                        </div>

                        {renderActiveChips(true)}

                        <div style={{ height: '400px', overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
                            <div style={{ flex: 1, overflow: 'auto' }} onScroll={(e) => {
                                if (e.target.scrollHeight - e.target.scrollTop - e.target.clientHeight < 50 && productDisplayLimit < productData.length) setProductDisplayLimit(p => p + 50);
                            }}>
                                <ResizableTable
                                    columns={productColumns}
                                    data={sortedProductData.slice(0, productDisplayLimit)}
                                    sortConfig={productSortConfig}
                                    onSort={(key, direction) => setProductSortConfig({ key, direction })}
                                />
                            </div>
                            <div style={{ padding: '10px 20px', borderTop: '1px solid #dee2e6', background: 'white', color: '#94a3b8', fontSize: '13px' }}>
                                Showing {Math.min(productDisplayLimit, sortedProductData.length)} of {sortedProductData.length} products (Filtered)
                            </div>
                        </div>
                    </div>

                </div>
            </div>
        </div>
    );
}
