import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Dashboard from './components/Dashboard'
import CampaignDetail from './components/CampaignDetail'
import AgentChat from './components/AgentChat'
import './App.css'

function App() {
  return (
    <BrowserRouter>
      <div className="app-container">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/campaign/:campaignName" element={<CampaignDetail />} />
        </Routes>
        <AgentChat />
      </div>
    </BrowserRouter>
  )
}

export default App
