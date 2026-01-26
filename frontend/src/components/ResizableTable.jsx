import React, { useState, useEffect, useRef } from 'react';
import { ArrowUpDown, ArrowUp, ArrowDown } from 'lucide-react';
import './ResizableTable.css';

const ResizableTable = ({
    columns = [],
    data = [],
    onRowClick = null,
    getRowStyle = null,
    sortConfig = null,
    onSort = null
}) => {
    // Initialize column widths - default 150px per column
    const [columnWidths, setColumnWidths] = useState({});
    const [resizing, setResizing] = useState(null);
    const tableRef = useRef(null);

    // Initialize column widths on mount
    useEffect(() => {
        const initialWidths = {};
        columns.forEach((_, index) => {
            initialWidths[index] = 150; // Default width
        });
        setColumnWidths(initialWidths);
    }, [columns]);

    // Internal sorting state (fallback if not controlled)
    const [internalSortConfig, setInternalSortConfig] = useState({ key: null, direction: 'asc' });

    // Determine active sort config
    const activeSortConfig = sortConfig || internalSortConfig;

    // Handle sort click
    const handleHeaderClick = (key) => {
        if (onSort) {
            // Controlled mode: notify parent
            let direction = 'asc';
            if (activeSortConfig.key === key && activeSortConfig.direction === 'asc') {
                direction = 'desc';
            }
            onSort(key, direction);
        } else {
            // Uncontrolled mode: local state (legacy behavior)
            let direction = 'asc';
            if (internalSortConfig.key === key && internalSortConfig.direction === 'asc') {
                direction = 'desc';
            }
            setInternalSortConfig({ key, direction });
        }
    };

    // Handle resize start
    const handleMouseDown = (columnIndex, e) => {
        e.preventDefault();
        setResizing({
            columnIndex,
            startX: e.clientX,
            startWidth: columnWidths[columnIndex] || 150
        });
    };

    // Handle resize drag
    useEffect(() => {
        const handleMouseMove = (e) => {
            if (!resizing) return;

            const diff = e.clientX - resizing.startX;
            const newWidth = Math.max(50, resizing.startWidth + diff); // Min width 50px

            setColumnWidths(prev => ({
                ...prev,
                [resizing.columnIndex]: newWidth
            }));
        };

        const handleMouseUp = () => {
            setResizing(null);
        };

        if (resizing) {
            document.addEventListener('mousemove', handleMouseMove);
            document.addEventListener('mouseup', handleMouseUp);

            return () => {
                document.removeEventListener('mousemove', handleMouseMove);
                document.removeEventListener('mouseup', handleMouseUp);
            };
        }
    }, [resizing]);

    // Sort data locally ONLY if not controlled
    // If controlled, we assume 'data' prop is already sorted by parent
    const displayData = React.useMemo(() => {
        if (sortConfig) return data; // Parent handles sorting

        if (!internalSortConfig.key) return data;

        return [...data].sort((a, b) => {
            let aVal = a[internalSortConfig.key];
            let bVal = b[internalSortConfig.key];

            // Normalize nulls
            if (aVal === null || aVal === undefined) aVal = '';
            if (bVal === null || bVal === undefined) bVal = '';

            // Clean strings (remove $, %, ,) for reliable number detection
            const clean = (val) => {
                if (typeof val !== 'string') return val;
                return val.replace(/[$,%]/g, '').replace(/,/g, '');
            };

            const aNum = parseFloat(clean(aVal));
            const bNum = parseFloat(clean(bVal));

            const isANum = !isNaN(aNum) && String(aVal).trim() !== '';
            const isBNum = !isNaN(bNum) && String(bVal).trim() !== '';

            if (isANum && isBNum) {
                return internalSortConfig.direction === 'asc' ? aNum - bNum : bNum - aNum;
            }

            const aStr = String(aVal).toLowerCase();
            const bStr = String(bVal).toLowerCase();

            if (aStr < bStr) return internalSortConfig.direction === 'asc' ? -1 : 1;
            if (aStr > bStr) return internalSortConfig.direction === 'asc' ? 1 : -1;
            return 0;
        });
    }, [data, sortConfig, internalSortConfig]);

    // Handle row click
    const handleRowClick = (row) => {
        if (onRowClick) {
            onRowClick(row);
        }
    };

    if (!data || data.length === 0) {
        return (
            <div className="excel-table-empty">
                <p>暂无数据</p>
            </div>
        );
    }

    return (
        <div className="excel-table-container">
            <table className="excel-table" ref={tableRef}>
                <thead>
                    <tr>
                        {columns.map((column, index) => (
                            <th
                                key={index}
                                style={{
                                    width: `${columnWidths[index] || 150}px`,
                                    minWidth: '50px',
                                    position: 'relative',
                                    cursor: 'pointer'
                                }}
                                onClick={() => handleHeaderClick(column)}
                            >
                                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                                    <span className="column-header-text">{column}</span>
                                    <span style={{ marginLeft: '4px', display: 'flex', alignItems: 'center' }}>
                                        {activeSortConfig.key === column ? (
                                            activeSortConfig.direction === 'asc' ?
                                                <ArrowUp size={14} color="#666" /> :
                                                <ArrowDown size={14} color="#666" />
                                        ) : (
                                            <ArrowUpDown size={14} color="#ccc" style={{ opacity: 0.5 }} />
                                        )}
                                    </span>
                                </div>

                                <div
                                    className="resize-handle"
                                    onMouseDown={(e) => handleMouseDown(index, e)}
                                    onClick={(e) => e.stopPropagation()}
                                />
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {displayData.map((row, rowIndex) => {
                        // Get custom style if provided
                        const customStyle = getRowStyle ? getRowStyle(row, rowIndex) : {};

                        return (
                            <tr
                                key={rowIndex}
                                onClick={() => handleRowClick(row)}
                                className={onRowClick ? 'clickable-row' : ''}
                                style={customStyle}
                            >
                                {columns.map((column, colIndex) => {
                                    const value = row[column];

                                    // Custom Rendering Logic
                                    const cleanNumeric = (val) => {
                                        if (val === null || val === undefined) return NaN;
                                        if (typeof val === 'number') return val;
                                        // Remove any character that isn't a digit, dot, or minus sign for numeric extraction
                                        const clean = String(val).replace(/[^\d.-]/g, '');
                                        return parseFloat(clean);
                                    };

                                    // 1. Initial string representation cleanup
                                    let content = '-';
                                    if (value !== null && value !== undefined) {
                                        // Globally strip common currency symbols and commas for display
                                        content = String(value).replace(/[$,€£¥₩\u20BD\u20B9\u20A9]/g, '').replace(/,/g, '').trim();
                                    }

                                    let cellStyle = {};

                                    if (value !== null && value !== undefined) {
                                        const num = cleanNumeric(value);
                                        const isNumber = !isNaN(num);

                                        // Common money/metric formatting (2 decimal places, no currency symbols)
                                        const moneyColumns = [
                                            'cost', 'conv_value', 'cpa', 'cpa_before_7d_average',
                                            'cost_conv', 'roas', 'roas_before_7d_average',
                                            'avg_cpc', 'price', 'budget', 'avg_cpm'
                                        ];

                                        if (moneyColumns.includes(column) || column.toLowerCase().includes('price') || column.toLowerCase().includes('cost')) {
                                            if (isNumber) {
                                                content = num.toFixed(2);
                                            }
                                        }

                                        // Percentages
                                        else if (column.includes('cvr') || column.includes('ctr') || column.includes('percent')) {
                                            content = isNumber ? `${num.toFixed(2)}%` : content;
                                        }

                                        // ROAS Comparison (Rise = Green/Good)
                                        else if (column === 'roas_compare') {
                                            if (Math.abs(num) < 0.001) {
                                                content = <span style={{ color: '#ccc' }}>0.00</span>;
                                            } else {
                                                const color = num > 0 ? 'green' : 'red';
                                                const arrow = num > 0 ? '▲' : '▼';
                                                content = (
                                                    <span style={{ color, fontWeight: 'bold' }}>
                                                        {arrow} {Math.abs(num).toFixed(2)}
                                                    </span>
                                                );
                                            }
                                        }

                                        // CPA Comparison (Rise = Red/Bad)
                                        else if (column === 'cpa_compare') {
                                            if (Math.abs(num) < 0.001) {
                                                content = <span style={{ color: '#ccc' }}>0.00</span>;
                                            } else {
                                                const color = num < 0 ? 'green' : 'red'; // Drop is Good
                                                const arrow = num > 0 ? '▲' : '▼';
                                                content = (
                                                    <span style={{ color, fontWeight: 'bold' }}>
                                                        {arrow} {Math.abs(num).toFixed(2)}
                                                    </span>
                                                );
                                            }
                                        }
                                    }

                                    return (
                                        <td
                                            key={colIndex}
                                            style={{
                                                width: `${columnWidths[colIndex] || 150}px`,
                                                minWidth: '50px',
                                                ...cellStyle
                                            }}
                                        >
                                            {content}
                                        </td>
                                    );
                                })}
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </div>
    );
};

export default ResizableTable;
