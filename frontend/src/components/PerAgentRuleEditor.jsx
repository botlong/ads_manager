import React, { useState, useEffect } from 'react';
import { Edit, X, Zap, Database } from 'lucide-react';
import { API_BASE_URL } from '../config';

export default function PerAgentRuleEditor({ tableId, tableLabel, isSelected, onTempRuleChange }) {
    const [isEditing, setIsEditing] = useState(false);
    const [ruleText, setRuleText] = useState("");
    const [originalRule, setOriginalRule] = useState("");
    const [tempRule, setTempRule] = useState("");
    const [saveStatus, setSaveStatus] = useState(null);
    const [defaultPrompt, setDefaultPrompt] = useState("");

    // Load saved rule and default prompt on mount
    useEffect(() => {
        const loadData = async () => {
            try {
                // Load default prompt first
                const promptResponse = await fetch(`${API_BASE_URL}/api/agent-prompts/${tableId}`);
                const promptData = await promptResponse.json();
                const defaultText = promptData.default_prompt || "";
                setDefaultPrompt(defaultText);

                // Then check for custom rule
                const ruleResponse = await fetch(`${API_BASE_URL}/api/agent-rules/${tableId}`);
                const ruleData = await ruleResponse.json();

                if (ruleData.rule_prompt) {
                    // Has saved custom rule
                    setRuleText(ruleData.rule_prompt);
                    setOriginalRule(ruleData.rule_prompt);
                } else {
                    // No custom rule, use default prompt
                    setRuleText(defaultText);
                    setOriginalRule(defaultText);
                }
            } catch (e) {
                console.log(`Error loading data for ${tableId}:`, e);
            }
        };
        loadData();
    }, [tableId]);

    const handleApplyOnce = () => {
        if (!ruleText.trim()) return;
        setTempRule(ruleText);
        setSaveStatus('applied');
        if (onTempRuleChange) {
            onTempRuleChange(tableId, ruleText);
        }
        setTimeout(() => {
            setSaveStatus(null);
            setIsEditing(false);
        }, 1500);
    };

    const handleApplyPermanently = async () => {
        if (!ruleText.trim()) return;
        setSaveStatus('saving');
        try {
            const response = await fetch(`${API_BASE_URL}/api/agent-rules`, {
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
                setTempRule("");
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

    const handleReset = () => {
        setRuleText(defaultPrompt);
    };

    const hasCustomRule = originalRule && originalRule !== defaultPrompt;
    const hasTempRule = tempRule && tempRule.trim().length > 0;
    const isModified = ruleText !== originalRule;

    return (
        <div style={{ marginLeft: '4px', display: 'inline-flex', alignItems: 'center', position: 'relative' }}>
            {/* Edit Button - right next to agent label */}
            <button
                onClick={(e) => {
                    e.stopPropagation();
                    setIsEditing(!isEditing);
                }}
                style={{
                    background: 'none',
                    border: 'none',
                    cursor: 'pointer',
                    padding: '2px 4px',
                    borderRadius: '4px',
                    color: hasTempRule ? '#f59e0b' : (hasCustomRule ? '#3b82f6' : '#94a3b8'),
                    display: 'flex',
                    alignItems: 'center',
                    gap: '2px',
                    fontSize: '0.7rem'
                }}
                title={hasTempRule ? '临时规则生效中' : (hasCustomRule ? '已自定义' : '编辑提示词')}
            >
                <Edit size={12} />
                {hasTempRule && <span style={{ fontSize: '0.6rem' }}>🎯</span>}
                {hasCustomRule && !hasTempRule && <span style={{ fontSize: '0.6rem' }}>✏️</span>}
            </button>

            {isEditing && (
                <div
                    onClick={(e) => e.stopPropagation()}
                    style={{
                        position: 'fixed',
                        top: '50%',
                        left: '50%',
                        transform: 'translate(-50%, -50%)',
                        width: '450px',
                        backgroundColor: 'white',
                        borderRadius: '12px',
                        boxShadow: '0 20px 60px rgba(0,0,0,0.3)',
                        border: '1px solid #e2e8f0',
                        padding: '16px',
                        zIndex: 10000
                    }}
                >
                    {/* Header */}
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
                        <div style={{ fontSize: '0.82rem', fontWeight: 700, color: '#1e293b' }}>
                            ⚙️ {tableLabel} 提示词
                        </div>
                        <button
                            onClick={handleReset}
                            style={{
                                fontSize: '0.7rem',
                                color: '#64748b',
                                background: 'none',
                                border: '1px solid #e2e8f0',
                                borderRadius: '4px',
                                padding: '2px 8px',
                                cursor: 'pointer'
                            }}
                        >
                            🔄 重置为默认
                        </button>
                    </div>

                    {/* Textarea with default prompt pre-filled */}
                    <textarea
                        value={ruleText}
                        onChange={(e) => setRuleText(e.target.value)}
                        style={{
                            width: '100%',
                            minHeight: '180px',
                            padding: '10px',
                            borderRadius: '8px',
                            border: '1px solid #e2e8f0',
                            fontSize: '0.78rem',
                            resize: 'vertical',
                            fontFamily: 'inherit',
                            lineHeight: '1.5',
                            backgroundColor: 'white'
                        }}
                    />

                    {/* Modified indicator */}
                    {isModified && (
                        <div style={{ fontSize: '0.7rem', color: '#f59e0b', marginTop: '6px' }}>
                            ⚠️ 已修改，需要应用才能生效
                        </div>
                    )}

                    {/* Action Buttons */}
                    <div style={{ display: 'flex', gap: '8px', marginTop: '12px' }}>
                        <button
                            onClick={handleCancel}
                            style={{
                                padding: '8px 14px',
                                borderRadius: '8px',
                                border: '1px solid #e2e8f0',
                                backgroundColor: 'white',
                                color: '#64748b',
                                fontSize: '0.78rem',
                                cursor: 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                gap: '4px'
                            }}
                        >
                            <X size={14} /> 取消
                        </button>
                        <button
                            onClick={handleApplyOnce}
                            disabled={!ruleText.trim()}
                            style={{
                                flex: 1,
                                padding: '8px 12px',
                                borderRadius: '8px',
                                border: '1px solid #f59e0b',
                                backgroundColor: 'white',
                                color: '#f59e0b',
                                fontSize: '0.78rem',
                                fontWeight: 600,
                                cursor: ruleText.trim() ? 'pointer' : 'not-allowed',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                gap: '5px',
                                opacity: ruleText.trim() ? 1 : 0.5
                            }}
                            title="仅本次生效"
                        >
                            <Zap size={14} /> 应用一次
                        </button>
                        <button
                            onClick={handleApplyPermanently}
                            disabled={saveStatus === 'saving' || !ruleText.trim()}
                            style={{
                                flex: 1,
                                padding: '8px 12px',
                                borderRadius: '8px',
                                border: 'none',
                                backgroundColor: '#3b82f6',
                                color: 'white',
                                fontSize: '0.78rem',
                                fontWeight: 600,
                                cursor: (saveStatus === 'saving' || !ruleText.trim()) ? 'not-allowed' : 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'center',
                                gap: '5px',
                                opacity: ruleText.trim() ? 1 : 0.5
                            }}
                            title="永久保存"
                        >
                            <Database size={14} /> {saveStatus === 'saving' ? '保存中...' : '永久应用'}
                        </button>
                    </div>

                    {/* Status */}
                    {saveStatus && (
                        <div style={{
                            marginTop: '10px',
                            fontSize: '0.75rem',
                            textAlign: 'center',
                            fontWeight: 600,
                            padding: '6px',
                            borderRadius: '6px',
                            backgroundColor: saveStatus === 'error' ? '#fef2f2' : (saveStatus === 'applied' ? '#fffbeb' : '#f0fdf4'),
                            color: saveStatus === 'error' ? '#ef4444' : (saveStatus === 'applied' ? '#f59e0b' : '#22c55e')
                        }}>
                            {saveStatus === 'saving' && '⏳ 保存中...'}
                            {saveStatus === 'saved' && '✓ 已永久保存'}
                            {saveStatus === 'applied' && '🎯 已应用 (本次生效)'}
                            {saveStatus === 'error' && '✗ 保存失败'}
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
