// Configuration for API endpoints

// Automatically determine the API base URL
const getApiBaseUrl = () => {
    // 1. If VITE_API_BASE_URL is set in environment variables (e.g. build time), use it.
    if (import.meta.env.VITE_API_BASE_URL) {
        return import.meta.env.VITE_API_BASE_URL;
    }

    // 2. Automatic detection:
    // Assume the backend is running on the SAME machine (IP/Domain) as the frontend,
    // but on port 8000.
    // This works for:
    // - Localhost: frontend localhost:5173 -> backend localhost:8000
    // - Remote Server: frontend 192.168.1.100:5173 -> backend 192.168.1.100:8000
    
    // Note: window.location is only available in the browser
    if (typeof window !== 'undefined') {
        const { protocol, hostname } = window.location;
        return `${protocol}//${hostname}:8000`;
    }

    // Fallback if window is undefined (e.g. SSR, though not applicable here usually)
    return "http://localhost:8000";
};

export const API_BASE_URL = getApiBaseUrl();
