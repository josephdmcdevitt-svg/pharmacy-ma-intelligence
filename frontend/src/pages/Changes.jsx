import React, { useState, useEffect } from 'react'
import api from '../api'
import { Bell, ChevronLeft, ChevronRight } from 'lucide-react'

export default function Changes() {
  const [data, setData] = useState({ data: [], total: 0, page: 1, total_pages: 0 })
  const [filter, setFilter] = useState('')
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    const params = { page, per_page: 50 }
    if (filter) params.change_type = filter
    api.get('/changes', { params })
      .then(res => setData(res.data))
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [page, filter])

  const typeColors = {
    new: 'bg-emerald-100 text-emerald-700',
    updated: 'bg-blue-100 text-blue-700',
    deactivated: 'bg-red-100 text-red-700',
  }

  return (
    <div className="p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-bold text-gray-900 flex items-center gap-2">
          <Bell className="w-5 h-5" /> Change Tracking
        </h2>
        <div className="flex items-center gap-2">
          {['', 'new', 'updated', 'deactivated'].map(type => (
            <button
              key={type}
              onClick={() => { setFilter(type); setPage(1) }}
              className={`px-3 py-1.5 text-xs rounded-md border transition-colors ${
                filter === type
                  ? 'bg-accent text-white border-accent'
                  : 'bg-white text-gray-600 border-gray-200 hover:bg-gray-50'
              }`}
            >
              {type || 'All'}
            </button>
          ))}
        </div>
      </div>

      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50">
            <tr className="text-left text-xs text-gray-500 uppercase">
              <th className="px-4 py-2.5">Type</th>
              <th className="px-4 py-2.5">NPI</th>
              <th className="px-4 py-2.5">Organization</th>
              <th className="px-4 py-2.5">Field</th>
              <th className="px-4 py-2.5">Old Value</th>
              <th className="px-4 py-2.5">New Value</th>
              <th className="px-4 py-2.5">Detected</th>
            </tr>
          </thead>
          <tbody>
            {data.data.map((c, i) => (
              <tr key={c.id} className={`border-b border-gray-100 ${i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}>
                <td className="px-4 py-2.5">
                  <span className={`px-1.5 py-0.5 rounded text-xs ${typeColors[c.change_type] || 'bg-gray-100 text-gray-600'}`}>
                    {c.change_type}
                  </span>
                </td>
                <td className="px-4 py-2.5 font-mono text-xs text-gray-500">{c.npi}</td>
                <td className="px-4 py-2.5 text-gray-900 font-medium max-w-xs truncate">{c.organization_name}</td>
                <td className="px-4 py-2.5 text-gray-600">{c.field_changed}</td>
                <td className="px-4 py-2.5 text-red-600 text-xs max-w-xs truncate">{c.old_value || '—'}</td>
                <td className="px-4 py-2.5 text-emerald-600 text-xs max-w-xs truncate">{c.new_value || '—'}</td>
                <td className="px-4 py-2.5 text-gray-400 text-xs">{c.detected_at ? new Date(c.detected_at).toLocaleString() : '—'}</td>
              </tr>
            ))}
            {data.data.length === 0 && !loading && (
              <tr><td colSpan={7} className="px-4 py-12 text-center text-gray-400">
                No changes detected yet. Run the pipeline to start tracking.
              </td></tr>
            )}
          </tbody>
        </table>

        {data.total_pages > 1 && (
          <div className="p-3 border-t border-gray-200 flex items-center justify-between">
            <span className="text-xs text-gray-500">Page {data.page} of {data.total_pages} ({data.total} total)</span>
            <div className="flex items-center gap-1">
              <button disabled={page <= 1} onClick={() => setPage(p => p - 1)}
                className="p-1.5 rounded hover:bg-gray-100 disabled:opacity-30">
                <ChevronLeft className="w-4 h-4" />
              </button>
              <button disabled={page >= data.total_pages} onClick={() => setPage(p => p + 1)}
                className="p-1.5 rounded hover:bg-gray-100 disabled:opacity-30">
                <ChevronRight className="w-4 h-4" />
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
