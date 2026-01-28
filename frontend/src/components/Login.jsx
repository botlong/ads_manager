import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { Lock, User, Zap } from 'lucide-react';

const Login = () => {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [error, setError] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const { login } = useAuth();
    const navigate = useNavigate();

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError('');
        setIsLoading(true);

        const result = await login(username, password);
        
        if (result.success) {
            navigate('/');
        } else {
            setError(result.message);
        }
        setIsLoading(false);
    };

    return (
        <div style={{
            height: '100vh',
            width: '100vw',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: 'linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%)'
        }}>
            <div style={{
                width: '100%',
                maxWidth: '400px',
                background: 'white',
                borderRadius: '16px',
                boxShadow: '0 20px 50px rgba(0,0,0,0.1)',
                padding: '40px',
                display: 'flex',
                flexDirection: 'column',
                gap: '20px',
                boxSizing: 'border-box' // Ensure padding doesn't expand width
            }}>
                <div style={{ textAlign: 'center', marginBottom: '10px' }}>
                    <div style={{ 
                        display: 'inline-flex', 
                        alignItems: 'center', 
                        justifyContent: 'center',
                        width: '60px', 
                        height: '60px', 
                        borderRadius: '20px', 
                        background: '#6366f1',
                        color: 'white',
                        marginBottom: '15px',
                        boxShadow: '0 10px 20px rgba(99, 102, 241, 0.3)'
                    }}>
                        <Zap size={32} fill="currentColor" />
                    </div>
                    <h1 style={{ fontSize: '24px', fontWeight: '700', color: '#1e293b', margin: 0 }}>AdsManager</h1>
                    <p style={{ color: '#64748b', fontSize: '14px', marginTop: '5px' }}>Sign in to access your dashboard</p>
                </div>

                {error && (
                    <div style={{
                        padding: '12px',
                        borderRadius: '8px',
                        background: '#fef2f2',
                        border: '1px solid #fecaca',
                        color: '#ef4444',
                        fontSize: '13px',
                        textAlign: 'center'
                    }}>
                        {error}
                    </div>
                )}

                <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '15px' }}>
                    <div style={{ position: 'relative' }}>
                        <User size={18} color="#94a3b8" style={{ position: 'absolute', left: '12px', top: '50%', transform: 'translateY(-50%)' }} />
                        <input
                            type="text"
                            placeholder="Username"
                            value={username}
                            onChange={(e) => setUsername(e.target.value)}
                            style={{
                                width: '100%',
                                padding: '12px 12px 12px 40px',
                                borderRadius: '10px',
                                border: '1px solid #e2e8f0',
                                outline: 'none',
                                fontSize: '14px',
                                transition: 'border 0.2s',
                                boxSizing: 'border-box' // Fix overflow issue
                            }}
                            onFocus={(e) => e.target.style.borderColor = '#6366f1'}
                            onBlur={(e) => e.target.style.borderColor = '#e2e8f0'}
                            required
                        />
                    </div>
                    
                    <div style={{ position: 'relative' }}>
                        <Lock size={18} color="#94a3b8" style={{ position: 'absolute', left: '12px', top: '50%', transform: 'translateY(-50%)' }} />
                        <input
                            type="password"
                            placeholder="Password"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                            style={{
                                width: '100%',
                                padding: '12px 12px 12px 40px',
                                borderRadius: '10px',
                                border: '1px solid #e2e8f0',
                                outline: 'none',
                                fontSize: '14px',
                                transition: 'border 0.2s',
                                boxSizing: 'border-box' // Fix overflow issue
                            }}
                            onFocus={(e) => e.target.style.borderColor = '#6366f1'}
                            onBlur={(e) => e.target.style.borderColor = '#e2e8f0'}
                            required
                        />
                    </div>

                    <button 
                        type="submit" 
                        disabled={isLoading}
                        style={{
                            marginTop: '10px',
                            padding: '14px',
                            borderRadius: '10px',
                            background: '#6366f1',
                            color: 'white',
                            border: 'none',
                            fontWeight: '600',
                            fontSize: '15px',
                            cursor: isLoading ? 'not-allowed' : 'pointer',
                            opacity: isLoading ? 0.7 : 1,
                            transition: 'background 0.2s',
                            boxShadow: '0 4px 12px rgba(99, 102, 241, 0.25)'
                        }}
                    >
                        {isLoading ? 'Signing in...' : 'Sign In'}
                    </button>
                </form>
            </div>
        </div>
    );
};

export default Login;
