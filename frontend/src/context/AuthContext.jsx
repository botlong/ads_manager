import React, { createContext, useState, useContext, useEffect } from 'react';
import { API_BASE_URL } from '../config';

const AuthContext = createContext(null);

export const AuthProvider = ({ children }) => {
    const [user, setUser] = useState(null);
    const [token, setToken] = useState(localStorage.getItem('auth_token'));
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const storedToken = localStorage.getItem('auth_token');
        const storedUser = localStorage.getItem('auth_user');
        
        if (storedToken && storedUser) {
            setToken(storedToken);
            setUser(JSON.parse(storedUser));
        }
        setLoading(false);
    }, []);

    const login = async (username, password) => {
        try {
            const response = await fetch(`${API_BASE_URL}/api/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password })
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Login failed');
            }

            const data = await response.json();
            const userData = { username: data.username, role: data.role };
            
            setToken(data.access_token);
            setUser(userData);
            
            localStorage.setItem('auth_token', data.access_token);
            localStorage.setItem('auth_user', JSON.stringify(userData));
            
            return { success: true };
        } catch (error) {
            return { success: false, message: error.message };
        }
    };

    const logout = () => {
        // Optional: Call backend to invalidate token
        if (token) {
            fetch(`${API_BASE_URL}/api/logout`, {
                method: 'POST',
                headers: { 
                    'Authorization': `Bearer ${token}`
                }
            }).catch(err => console.error("Logout error", err));
        }

        setToken(null);
        setUser(null);
        localStorage.removeItem('auth_token');
        localStorage.removeItem('auth_user');
    };

    // Helper to add auth header to fetch requests
    const authFetch = async (url, options = {}) => {
        const headers = options.headers || {};
        headers['Authorization'] = `Bearer ${token}`;
        
        const response = await fetch(url, { ...options, headers });
        
        if (response.status === 401) {
            logout(); // Auto logout on 401
            throw new Error("Session expired");
        }
        
        return response;
    };

    return (
        <AuthContext.Provider value={{ user, token, login, logout, loading, authFetch }}>
            {children}
        </AuthContext.Provider>
    );
};

export const useAuth = () => useContext(AuthContext);
