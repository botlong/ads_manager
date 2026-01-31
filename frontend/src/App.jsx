import React from 'react';
import { BrowserRouter, Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { AuthProvider, useAuth } from './context/AuthContext';
import Dashboard from './components/Dashboard';
import CampaignDetail from './components/CampaignDetail';
import SeoAnalysis from './components/SeoAnalysis';
import AgentChat from './components/AgentChat';
import Login from './components/Login';
import './App.css';

// Protected Route Wrapper
const ProtectedRoute = ({ children }) => {
    const { token, loading } = useAuth();
    const location = useLocation();

    if (loading) {
        return <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>Loading...</div>;
    }

    if (!token) {
        return <Navigate to="/login" state={{ from: location }} replace />;
    }

    return children;
};

// Layout for authenticated pages (includes AgentChat)
const AuthenticatedLayout = ({ children }) => {
    return (
        <div className="app-container">
            {children}
            <AgentChat />
        </div>
    );
};

function AppRoutes() {
    return (
        <Routes>
            <Route path="/login" element={<Login />} />

            <Route path="/" element={
                <ProtectedRoute>
                    <AuthenticatedLayout>
                        <Dashboard />
                    </AuthenticatedLayout>
                </ProtectedRoute>
            } />

            <Route path="/campaign/:campaignName" element={
                <ProtectedRoute>
                    <AuthenticatedLayout>
                        <CampaignDetail />
                    </AuthenticatedLayout>
                </ProtectedRoute>
            } />

            <Route path="/seo-analysis" element={
                <ProtectedRoute>
                    <AuthenticatedLayout>
                        <SeoAnalysis />
                    </AuthenticatedLayout>
                </ProtectedRoute>
            } />
        </Routes>
    );
}

function App() {
    return (
        <AuthProvider>
            <BrowserRouter>
                <AppRoutes />
            </BrowserRouter>
        </AuthProvider>
    );
}

export default App;
