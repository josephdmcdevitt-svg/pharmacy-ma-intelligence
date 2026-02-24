import React, { useState, useEffect } from 'react'
import api from '../api'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import { Building2, Store, MapPin, Bell, Play, RefreshCw } from 'lucide-react'

export default function Dashboard() {
  const [stats, setStats] = useState(null)
  const [pipelineStatus, setPipelineStatus] = useState(null)
  const [loading, setLoading] = useState(true)
  const [triggering, setTriggering] = useState(false)

  useEffect(() => {
    Promise.all([
      api.get('/dashboard/stats'),
      api.get('/pipeline/status'),
    ]).then(([statsRes, statusRes]) => {
      setStats(statsRes.data)
      setPipelineStatus(statusRes.data)
    }).catch(console.error)
    .finally(() => setLoading(false))
  }, [])

  const triggerPipeline = async () => {
    setTriggering(true)
    try {
      await api.post('/pipeline/trigger')
      setPipelineStatus({ status: 'running' })
    } catch (e) {
      console.error(e)
    }
    setTriggering(false)
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-accent"></div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-bold text-gray-900">Dashboard</h2>
        <button
          onClick={triggerPipeline}
          disabled={triggering || pipelineStatus?.status === 'running'}
          className="flex items-center gap-2 px-4 py-2 bg-accent text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
        >
          {pipelineStatus?.status === 'running' ? (
            <><RefreshCw className="w-4 h-4 animate-spin" /> Pipeline Running...</>
          ) : (
            <><Play className="w-4 h-4" /> Run Pipeline</>
          )}
        </button>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard icon={Building2} label="Total Pharmacies" value={stats?.total_pharmacies || 0} color="blue" />
        <StatCard icon={Store} label="Independent" value={stats?.independent_count || 0} color="emerald" />
        <StatCard icon={Building2} label="Chain" value={stats?.chain_count || 0} color="gray" />
        <StatCard icon={MapPin} label="States Covered" value={stats?.states_covered || 0} color="purple" />
      </div>

      {/* Pipeline Status */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Pipeline Status</h3>
        {pipelineStatus?.status === 'never_run' ? (
          <p className="text-sm text-gray-500">
            No pipeline runs yet. Click "Run Pipeline" to download and process pharmacy data from the NPI registry.
            The first run downloads ~700MB and takes 30-60 minutes.
          </p>
        ) : (
          <div className="text-sm text-gray-600 space-y-1">
            <p>Status: <span className={`font-medium ${pipelineStatus?.status === 'completed' ? 'text-emerald-600' : pipelineStatus?.status === 'running' ? 'text-blue-600' : 'text-red-600'}`}>{pipelineStatus?.status}</span></p>
            {pipelineStatus?.started_at && <p>Started: {new Date(pipelineStatus.started_at).toLocaleString()}</p>}
            {pipelineStatus?.completed_at && <p>Completed: {new Date(pipelineStatus.completed_at).toLocaleString()}</p>}
            {pipelineStatus?.records_processed > 0 && <p>Records Processed: {pipelineStatus.records_processed.toLocaleString()}</p>}
          </div>
        )}
      </div>

      {/* Top States Chart */}
      {stats?.top_states?.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">Pharmacies by State (Top 10)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={stats.top_states}>
              <XAxis dataKey="state" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip formatter={(value) => value.toLocaleString()} />
              <Bar dataKey="count" fill="#2563eb" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Recent Changes */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-2 flex items-center gap-1.5">
          <Bell className="w-4 h-4" /> Recent Changes
        </h3>
        <p className="text-sm text-gray-500">
          {stats?.recent_changes > 0
            ? `${stats.recent_changes.toLocaleString()} changes detected. View the Changes tab for details.`
            : 'No changes detected yet. Run the pipeline to start tracking changes.'}
        </p>
      </div>
    </div>
  )
}

function StatCard({ icon: Icon, label, value, color }) {
  const colors = {
    blue: 'bg-blue-50 text-blue-600',
    emerald: 'bg-emerald-50 text-emerald-600',
    gray: 'bg-gray-100 text-gray-600',
    purple: 'bg-purple-50 text-purple-600',
  }
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="flex items-center gap-3">
        <div className={`p-2 rounded-lg ${colors[color]}`}>
          <Icon className="w-5 h-5" />
        </div>
        <div>
          <p className="text-2xl font-bold text-gray-900">{typeof value === 'number' ? value.toLocaleString() : value}</p>
          <p className="text-xs text-gray-500">{label}</p>
        </div>
      </div>
    </div>
  )
}
