import React, { useState, useEffect } from 'react';
import { Edit, ChevronDown, Save, X } from 'lucide-react';

export default function PerAgentRuleEditor({ tableId, tableLabel, isSelected }) {
    const [isEditing, setIsEditing] = useState(false);
    const [ruleText, setRuleText] = useState("");
    const [originalRule, setOriginalRule] = useState("");
    const [saveStatus, setSaveStatus] = useState(null);

    // Load saved rule on mount
    useEffect(() => {
        const loadRule = async () => {
            try {
                const response = await fetch(`http://localhost:8000/api/agent-rules/${tableId}`);
                const data = await response.json();
                if (data.rule_prompt) {
                    setRuleText(data.rule_prompt);
                    setOriginalRule(data.rule_prompt);
                }
            } catch (e) {
                console.log(`No saved rule for ${tableId}`);
            }
        };
        loadRule();
    }, [tableId]);

    const handleSave = async () => {
        setSaveStatus('saving');
        try {
            const response = await fetch('http://localhost:8000/api/agent-rules', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    table_name: tableId,
                    rule_prompt: ruleText
                })
            });
            if (response.ok) {
                setSaveStatus('saved');
                setOriginalRule(ruleText);
                setTimeout(() => {
                    setSaveStatus(null);
                    setIsEditing(false);
                }, 1500);
            } else {
                setSaveStatus('error');
            }
        } catch (e) {
            setSaveStatus('error');
        }
    };

    const handleCancel = () => {
        setRuleText(originalRule);
        setIsEditing(false);
    };

    const hasCustomRule = originalRule && originalRule.trim().length > 0;

    return (
        <div style={{ marginLeft: '4px' }}>
            {/* Edit Icon */}
            <button
                onClick={(e) => {
                    e.stopPropagation();
                    setIsEditing(!isEditing);
                }}
                style={{
                    background: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    padding: '4px',
                    borderRadius: '4px',
                    color: hasCustomRule ? '#3b82f6' : '#94a3b8',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '2px',
                    fontSize: '0.7rem'
                }}
                title={hasCustomRule ? `已配置规则: ${originalRule.substring(0, 50)}...` : '点击添加自定义规则'}
            >
                <Edit size={12} />
                {hasCustomRule && <span style={{ fontSize: '0.65rem' }}>⚙️</span>}
            </button>

            {/* Expanded Editor */}
            {isEditing && (
                <div
                    onClick={(e) => e.stopPropagation()}
                    style={{
                        position: 'absolute',
                        left: '100%',
                        top: 0,
                        width: '280px',
                        backgroundColor: 'white',
                        borderRadius: '8px',
                        boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
                        border: '1px solid #e2e8f0',
                        padding: '12px',
                        zIndex: 100,
                        marginLeft: '8px'
                    }}
                >
                    <div style={{ fontSize: '0.75rem', fontWeight: 600, color: '#475569', marginBottom: '8px' }}>
                        {tableLabel} - 自定义规则
                    </div>
                    <textarea
                        value={ruleText}
                        onChange={(e) => setRuleText(e.target.value)}
                        placeholder="例如：忽略消耗低于 $50 的项目"
                        style={{
                            width: '100%',
                            minHeight: '60px',
                            padding: '8px',
                            borderRadius: '6px',
                            border: '1px solid #e2e8f0',
                            fontSize: '0.8rem',
                            resize: 'vertical',
                            fontFamily: 'inherit'
                        }}
                    />
                    <div style={{ display: 'flex', gap: '6px', marginTop: '8px' }}>
                        <button
                            onClick={handleCancel}
                            style={{
                                flex: 1,
                                padding: '6px',
                                borderRadius: '6px',
                                border: '1px solid #e2e8f0',
                                backgroundColor: 'white',
                                color: '#64748b',
                                fontSize: '0.75rem',
                                cursor: 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                gap: '4px'
                            }}
                        >
                            <X size={12} /> 取消
                        </button>
                        <button
                            onClick={handleSave}
                            disabled={saveStatus === 'saving'}
                            style={{
                                flex: 1,
                                padding: '6px',
                                borderRadius: '6px',
                                border: 'none',
                                backgroundColor: '#3b82f6',
                                color: 'white',
                                fontSize: '0.75rem',
                                cursor: 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                gap: '4px'
                            }}
                        >
                            <Save size={12} />
                            {saveStatus === 'saving' ? '保存中...' : '保存'}
                        </button>
                    </div>
                    {saveStatus === 'saved' && (
                        <div style={{ marginTop: '6px', fontSize: '0.7rem', color: '#22c55e', textAlign: 'center' }}>
                            ✓ 规则已保存
                        </div>
                    )}
                    {saveStatus === 'error' && (
                        <div style={{ marginTop: '6px', fontSize: '0.7rem', color: '#ef4444', textAlign: 'center' }}>
                            ✗ 保存失败
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
