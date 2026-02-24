import React, { useState, useEffect, useCallback } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import api from '../api'
import { Search, Download, ChevronLeft, ChevronRight, Filter, X } from 'lucide-react'

export default function Directory() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()

  const [data, setData] = useState({ data: [], total: 0, page: 1, total_pages: 0 })
  const [states, setStates] = useState([])
  const [loading, setLoading] = useState(false)
  const [filters, setFilters] = useState({
    search: searchParams.get('search') || '',
    state: searchParams.get('state') || '',
    city: searchParams.get('city') || '',
    zip: searchParams.get('zip') || '',
    is_independent: searchParams.get('is_independent') !== 'false',
    page: parseInt(searchParams.get('page') || '1'),
  })
  const [showFilters, setShowFilters] = useState(true)

  useEffect(() => {
    api.get('/pharmacies/states').then(r => setStates(r.data)).catch(() => {})
  }, [])

  const fetchData = useCallback(async () => {
    setLoading(true)
    try {
      const params = {}
      if (filters.search) params.search = filters.search
      if (filters.state) params.state = filters.state
      if (filters.city) params.city = filters.city
      if (filters.zip) params.zip = filters.zip
      if (filters.is_independent !== null) params.is_independent = filters.is_independent
      params.page = filters.page
      params.per_page = 50

      const res = await api.get('/pharmacies', { params })
      setData(res.data)
    } catch (e) {
      console.error(e)
    }
    setLoading(false)
  }, [filters])

  useEffect(() => { fetchData() }, [fetchData])

  const updateFilter = (key, value) => {
    setFilters(prev => ({ ...prev, [key]: value, page: 1 }))
  }

  const exportCSV = () => {
    const params = new URLSearchParams()
    if (filters.state) params.set('state', filters.state)
    if (filters.is_independent !== null) params.set('is_independent', filters.is_independent)
    if (filters.search) params.set('search', filters.search)
    window.open(`/api/exports/csv?${params.toString()}`, '_blank')
  }

  return (
    <div className="flex h-screen">
      {/* Filter Panel */}
      {showFilters && (
        <div className="w-64 bg-white border-r border-gray-200 p-4 overflow-y-auto flex-shrink-0">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-1.5">
              <Filter className="w-4 h-4" /> Filters
            </h3>
            <button onClick={() => setShowFilters(false)} className="text-gray-400 hover:text-gray-600">
              <X className="w-4 h-4" />
            </button>
          </div>

          <div className="space-y-4">
            <div>
              <label className="block text-xs text-gray-500 mb-1">Search</label>
              <div className="relative">
                <Search className="absolute left-2.5 top-2.5 w-3.5 h-3.5 text-gray-400" />
                <input
                  type="text" value={filters.search}
                  onChange={e => updateFilter('search', e.target.value)}
                  placeholder="Name, city, NPI..."
                  className="w-full pl-8 pr-3 py-2 text-sm border border-gray-200 rounded focus:outline-none focus:border-accent"
                />
              </div>
            </div>

            <div>
              <label className="block text-xs text-gray-500 mb-1">State</label>
              <select value={filters.state} onChange={e => updateFilter('state', e.target.value)}
                className="w-full px-3 py-2 text-sm border border-gray-200 rounded focus:outline-none focus:border-accent">
                <option value="">All States</option>
                {states.map(s => (
                  <option key={s.state} value={s.state}>{s.state} ({s.count.toLocaleString()})</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs text-gray-500 mb-1">City</label>
              <input type="text" value={filters.city} onChange={e => updateFilter('city', e.target.value)}
                placeholder="City name" className="w-full px-3 py-2 text-sm border border-gray-200 rounded focus:outline-none focus:border-accent" />
            </div>

            <div>
              <label className="block text-xs text-gray-500 mb-1">ZIP Code</label>
              <input type="text" value={filters.zip} onChange={e => updateFilter('zip', e.target.value)}
                placeholder="ZIP or prefix" className="w-full px-3 py-2 text-sm border border-gray-200 rounded focus:outline-none focus:border-accent" />
            </div>

            <div>
              <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
                <input type="checkbox" checked={filters.is_independent}
                  onChange={e => updateFilter('is_independent', e.target.checked)}
                  className="rounded border-gray-300" />
                Independent only
              </label>
            </div>
          </div>
        </div>
      )}

      {/* Main Table */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <div className="p-4 border-b border-gray-200 bg-white flex items-center justify-between">
          <div className="flex items-center gap-3">
            {!showFilters && (
              <button onClick={() => setShowFilters(true)} className="text-gray-500 hover:text-gray-700">
                <Filter className="w-4 h-4" />
              </button>
            )}
            <span className="text-sm text-gray-600">
              {data.total.toLocaleString()} results
              {filters.is_independent && ' (independent)'}
            </span>
          </div>
          <button onClick={exportCSV}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-gray-600 border border-gray-200 rounded hover:bg-gray-50">
            <Download className="w-3.5 h-3.5" /> Export CSV
          </button>
        </div>

        <div className="flex-1 overflow-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-gray-50 z-10">
              <tr className="text-left text-xs text-gray-500 uppercase">
                <th className="px-4 py-2.5">Name</th>
                <th className="px-4 py-2.5">City</th>
                <th className="px-4 py-2.5">State</th>
                <th className="px-4 py-2.5">ZIP</th>
                <th className="px-4 py-2.5">Phone</th>
                <th className="px-4 py-2.5">Type</th>
                <th className="px-4 py-2.5">NPI</th>
                <th className="px-4 py-2.5 text-right">Medicare Claims</th>
              </tr>
            </thead>
            <tbody>
              {data.data.map((p, i) => (
                <tr key={p.id}
                  onClick={() => navigate(`/pharmacy/${p.id}`)}
                  className={`cursor-pointer border-b border-gray-100 hover:bg-blue-50/50 transition-colors ${i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}>
                  <td className="px-4 py-2.5 font-medium text-gray-900 max-w-xs truncate">{p.organization_name}</td>
                  <td className="px-4 py-2.5 text-gray-600">{p.city}</td>
                  <td className="px-4 py-2.5 text-gray-600">{p.state}</td>
                  <td className="px-4 py-2.5 text-gray-600 font-mono text-xs">{p.zip}</td>
                  <td className="px-4 py-2.5 text-gray-600 font-mono text-xs">{p.phone}</td>
                  <td className="px-4 py-2.5">
                    <span className={`px-1.5 py-0.5 rounded text-xs ${p.is_independent ? 'bg-emerald-100 text-emerald-700' : 'bg-gray-100 text-gray-500'}`}>
                      {p.is_independent ? 'Independent' : 'Chain'}
                    </span>
                  </td>
                  <td className="px-4 py-2.5 text-gray-400 font-mono text-xs">{p.npi}</td>
                  <td className="px-4 py-2.5 text-right text-gray-600">{p.medicare_claims_count?.toLocaleString() || '—'}</td>
                </tr>
              ))}
              {data.data.length === 0 && !loading && (
                <tr><td colSpan={8} className="px-4 py-12 text-center text-gray-400">No pharmacies found. Try adjusting filters or run the pipeline first.</td></tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {data.total_pages > 1 && (
          <div className="p-3 border-t border-gray-200 bg-white flex items-center justify-between">
            <span className="text-xs text-gray-500">
              Page {data.page} of {data.total_pages}
            </span>
            <div className="flex items-center gap-1">
              <button disabled={data.page <= 1}
                onClick={() => setFilters(f => ({ ...f, page: f.page - 1 }))}
                className="p-1.5 rounded hover:bg-gray-100 disabled:opacity-30">
                <ChevronLeft className="w-4 h-4" />
              </button>
              <button disabled={data.page >= data.total_pages}
                onClick={() => setFilters(f => ({ ...f, page: f.page + 1 }))}
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
