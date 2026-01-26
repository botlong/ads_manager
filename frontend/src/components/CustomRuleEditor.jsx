import React from 'react';
import { ChevronDown } from 'lucide-react';

export default function CustomRuleEditor({
    customRuleText,
    setCustomRuleText,
    showRuleEditor,
    setShowRuleEditor,
    applyRuleOnce,
    applyRulePermanently,
    ruleSaveStatus
}) {
    return (
        <div style={{ borderTop: '1px solid #e2e8f0', padding: '12px' }}>
            <div
                onClick={() => setShowRuleEditor(!showRuleEditor)}
                style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    cursor: 'pointer',
                    padding: '8px',
                    borderRadius: '8px',
                    backgroundColor: '#f8fafc'
                }}
            >
                <span style={{ fontSize: '11px', fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase' }}>
                    ⚙️ 自定义规则
                </span>
                <ChevronDown
                    size={14}
                    style={{
                        transform: showRuleEditor ? 'rotate(180deg)' : 'rotate(0deg)',
                        transition: 'transform 0.2s'
                    }}
                />
            </div>

            {showRuleEditor && (
                <div style={{ marginTop: '10px' }}>
                    <textarea
                        value={customRuleText}
                        onChange={(e) => setCustomRuleText(e.target.value)}
                        placeholder="输入自定义规则，例如：&#10;• ROAS 阈值改为 30%&#10;• 忽略消耗低于 $50 的搜索词&#10;• 只分析 PMax 渠道"
                        style={{
                            width: '100%',
                            minHeight: '70px',
                            padding: '10px',
                            borderRadius: '8px',
                            border: '1px solid #e2e8f0',
                            fontSize: '0.85rem',
                            resize: 'vertical',
                            fontFamily: 'inherit',
                            lineHeight: '1.5'
                        }}
                    />
                    <div style={{ display: 'flex', gap: '8px', marginTop: '10px' }}>
                        <button
                            onClick={applyRuleOnce}
                            style={{
                                flex: 1,
                                padding: '8px 12px',
                                borderRadius: '8px',
                                border: '1px solid #3b82f6',
                                backgroundColor: 'white',
                                color: '#3b82f6',
                                fontSize: '0.8rem',
                                fontWeight: 600,
                                cursor: 'pointer',
                                transition: 'all 0.2s'
                            }}
                            onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#eff6ff'}
                            onMouseOut={(e) => e.currentTarget.style.backgroundColor = 'white'}
                        >
                            🎯 应用一次
                        </button>
                        <button
                            onClick={applyRulePermanently}
                            style={{
                                flex: 1,
                                padding: '8px 12px',
                                borderRadius: '8px',
                                border: 'none',
                                backgroundColor: '#3b82f6',
                                color: 'white',
                                fontSize: '0.8rem',
                                fontWeight: 600,
                                cursor: 'pointer',
                                transition: 'all 0.2s'
                            }}
                            onMouseOver={(e) => e.currentTarget.style.backgroundColor = '#2563eb'}
                            onMouseOut={(e) => e.currentTarget.style.backgroundColor = '#3b82f6'}
                        >
                            💾 永久应用
                        </button>
                    </div>
                    {ruleSaveStatus && (
                        <div style={{
                            marginTop: '8px',
                            fontSize: '0.75rem',
                            color: ruleSaveStatus === 'error' ? '#ef4444' : '#22c55e',
                            textAlign: 'center',
                            fontWeight: 600
                        }}>
                            {ruleSaveStatus === 'saving' && '⏳ 保存中...'}
                            {ruleSaveStatus === 'saved' && '✓ 规则已永久保存'}
                            {ruleSaveStatus === 'applied' && '✓ 规则已应用 (本次生效)'}
                            {ruleSaveStatus === 'error' && '✗ 保存失败，请重试'}
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
