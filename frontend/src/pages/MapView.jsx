import React, { useState, useEffect } from 'react'
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet'
import api from '../api'

export default function MapView() {
  const [pharmacies, setPharmacies] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    // Load pharmacies with coordinates
    api.get('/pharmacies', { params: { per_page: 200, is_independent: true } })
      .then(res => setPharmacies(res.data.data))
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  return (
    <div className="h-screen flex flex-col">
      <div className="p-4 bg-white border-b border-gray-200">
        <h2 className="text-lg font-bold text-gray-900">Map View</h2>
        <p className="text-xs text-gray-500 mt-0.5">
          {pharmacies.length} pharmacies loaded. Map shows pharmacies with available coordinates.
        </p>
      </div>

      <div className="flex-1">
        <MapContainer
          center={[39.8283, -98.5795]}
          zoom={4}
          style={{ height: '100%', width: '100%' }}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {pharmacies
            .filter(p => p.latitude && p.longitude)
            .map(p => (
              <Marker key={p.id} position={[p.latitude, p.longitude]}>
                <Popup>
                  <strong>{p.organization_name}</strong><br />
                  {p.city}, {p.state} {p.zip}<br />
                  <span className="text-xs">{p.npi}</span>
                </Popup>
              </Marker>
            ))}
        </MapContainer>
      </div>

      {loading && (
        <div className="absolute inset-0 bg-white/50 flex items-center justify-center z-[1000]">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-accent"></div>
        </div>
      )}
    </div>
  )
}
