import React, { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import api from '../api'
import { ArrowLeft, Building2, MapPin, Phone, User, DollarSign } from 'lucide-react'

export default function PharmacyDetail() {
  const { id } = useParams()
  const navigate = useNavigate()
  const [pharmacy, setPharmacy] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    api.get(`/pharmacies/${id}`)
      .then(res => setPharmacy(res.data))
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [id])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-accent"></div>
      </div>
    )
  }

  if (!pharmacy) {
    return <div className="p-6 text-gray-500">Pharmacy not found.</div>
  }

  return (
    <div className="p-6 max-w-4xl">
      <button onClick={() => navigate(-1)} className="flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-700 mb-4">
        <ArrowLeft className="w-4 h-4" /> Back
      </button>

      <div className="space-y-6">
        {/* Header */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-start justify-between">
            <div>
              <h1 className="text-xl font-bold text-gray-900">{pharmacy.organization_name}</h1>
              {pharmacy.dba_name && <p className="text-sm text-gray-500 mt-0.5">DBA: {pharmacy.dba_name}</p>}
              <p className="text-xs text-gray-400 mt-1 font-mono">NPI: {pharmacy.npi}</p>
            </div>
            <span className={`px-2.5 py-1 rounded text-xs font-medium ${pharmacy.is_independent ? 'bg-emerald-100 text-emerald-700' : 'bg-gray-100 text-gray-600'}`}>
              {pharmacy.is_independent ? 'Independent' : `Chain — ${pharmacy.chain_parent || 'Unknown'}`}
            </span>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Location */}
          <div className="bg-white rounded-lg border border-gray-200 p-5">
            <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-1.5 mb-3">
              <MapPin className="w-4 h-4" /> Location
            </h3>
            <div className="space-y-2 text-sm text-gray-600">
              {pharmacy.address_line1 && <p>{pharmacy.address_line1}</p>}
              {pharmacy.address_line2 && <p>{pharmacy.address_line2}</p>}
              <p>{pharmacy.city}, {pharmacy.state} {pharmacy.zip}</p>
              {pharmacy.county && <p>County: {pharmacy.county}</p>}
            </div>
          </div>

          {/* Contact */}
          <div className="bg-white rounded-lg border border-gray-200 p-5">
            <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-1.5 mb-3">
              <Phone className="w-4 h-4" /> Contact
            </h3>
            <div className="space-y-2 text-sm text-gray-600">
              {pharmacy.phone && <p>Phone: {pharmacy.phone}</p>}
              {pharmacy.fax && <p>Fax: {pharmacy.fax}</p>}
            </div>
          </div>

          {/* Ownership */}
          <div className="bg-white rounded-lg border border-gray-200 p-5">
            <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-1.5 mb-3">
              <User className="w-4 h-4" /> Ownership Signals
            </h3>
            <div className="space-y-2 text-sm text-gray-600">
              {pharmacy.authorized_official_name && <p>Official: {pharmacy.authorized_official_name}</p>}
              {pharmacy.authorized_official_title && <p>Title: {pharmacy.authorized_official_title}</p>}
              {pharmacy.authorized_official_phone && <p>Phone: {pharmacy.authorized_official_phone}</p>}
              {pharmacy.ownership_type && <p>Entity Type: {pharmacy.ownership_type}</p>}
            </div>
          </div>

          {/* Medicare */}
          <div className="bg-white rounded-lg border border-gray-200 p-5">
            <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-1.5 mb-3">
              <DollarSign className="w-4 h-4" /> Medicare Data
            </h3>
            <div className="space-y-2 text-sm text-gray-600">
              <p>Claims: {pharmacy.medicare_claims_count?.toLocaleString() || 'N/A'}</p>
              <p>Beneficiaries: {pharmacy.medicare_beneficiary_count?.toLocaleString() || 'N/A'}</p>
              <p>Total Cost: {pharmacy.medicare_total_cost ? `$${pharmacy.medicare_total_cost.toLocaleString()}` : 'N/A'}</p>
            </div>
          </div>
        </div>

        {/* Metadata */}
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">Record Info</h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm text-gray-600">
            <div><span className="text-gray-400">Taxonomy:</span> {pharmacy.taxonomy_code}</div>
            <div><span className="text-gray-400">First Seen:</span> {pharmacy.first_seen ? new Date(pharmacy.first_seen).toLocaleDateString() : 'N/A'}</div>
            <div><span className="text-gray-400">Last Refreshed:</span> {pharmacy.last_refreshed ? new Date(pharmacy.last_refreshed).toLocaleDateString() : 'N/A'}</div>
            <div><span className="text-gray-400">Institutional:</span> {pharmacy.is_institutional ? 'Yes' : 'No'}</div>
          </div>
        </div>
      </div>
    </div>
  )
}
