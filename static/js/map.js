// Ensure Leaflet is loaded before using it
if (typeof L === 'undefined') {
  console.error('Leaflet is not loaded. Make sure to include Leaflet before this script.');
}

/* global L, turf */

// Global constants
const MCLENNAN_COUNTY_BOUNDS = L.latLngBounds(
  L.latLng(31.3501, -97.4585), // Southwest corner
  L.latLng(31.7935, -96.8636)  // Northeast corner
);
const DEFAULT_WACO_BOUNDARY = 'city_limits';
const ALL_TIME_START_DATE = new Date(2020, 0, 1);
const PROGRESS_UPDATE_INTERVAL = 60000; // Update progress every minute
const PROGRESS_DATA_UPDATE_INTERVAL = 300000; // Update progress data every 5 minutes
const FEEDBACK_DURATION = 5000; // Default duration for feedback messages

// Determine the WebSocket protocol based on the current page protocol
const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';

// Construct the WebSocket URL using the current hostname and port
const wsBaseUrl = `${wsProtocol}//${window.location.host}`;

let liveDataSocket = null;
let metricsSocket = null;

// Global variables
let map = null;
let wacoLimitsLayer = null;
let progressLayer = null;
let historicalDataLayer = null;
let liveRoutePolyline = null; // Changed from const to let
let liveMarker = null;
let playbackPolyline = null;
let playbackMarker = null;
let wacoStreetsLayer = null;
let drawnItems = null;
let playbackSpeed = 1;
let isPlaying = false;
let currentCoordIndex = 0;
let playbackAnimation = null;
let isProcessing = false;
let searchMarker = null;
let liveDataFetchController = null;
let isCenteringOnLiveMarker = false;
let centeringInterval;
const processingQueue = [];
let wacoOnlyMode = true; // only allow panning in/around waco


async function clearAllBrowserStorage() {
  console.log('Clearing all browser storage...');

  // Clear localStorage
  localStorage.clear();
  console.log('localStorage cleared');

  // Clear sessionStorage
  sessionStorage.clear();
  console.log('sessionStorage cleared');

  // Clear cookies
  const cookies = document.cookie.split(";");
  for (let i = 0; i < cookies.length; i++) {
      const cookie = cookies[i];
      const eqPos = cookie.indexOf("=");
      const name = eqPos > -1 ? cookie.substr(0, eqPos) : cookie;
      document.cookie = name + "=;expires=Thu, 01 Jan 1970 00:00:00 GMT;path=/";
  }
  console.log('Cookies cleared');

  // Clear cache storage
  if ('caches' in window) {
      try {
          const cacheNames = await caches.keys();
          await Promise.all(cacheNames.map(name => caches.delete(name)));
          console.log('Cache storage cleared');
      } catch (error) {
          console.error('Error clearing cache storage:', error);
      }
  }

  // Clear IndexedDB
  const databases = await indexedDB.databases();
  databases.forEach(db => {
      indexedDB.deleteDatabase(db.name);
  });
  console.log('IndexedDB cleared');

  // Clear service worker caches
  if ('serviceWorker' in navigator) {
      const registrations = await navigator.serviceWorker.getRegistrations();
      for(let registration of registrations) {
          await registration.unregister();
      }
      console.log('Service worker caches cleared');
  }
}

// Function to force a hard reload of the page
function forceReload() {
  clearAllBrowserStorage().then(() => {
      console.log('All browser storage cleared. Reloading page...');
      window.location.reload(true);
  });
}

// Call clearAllBrowserStorage when the page is about to unload
window.addEventListener('beforeunload', clearAllBrowserStorage);

// Call clearAllBrowserStorage when the page loads to ensure a fresh start
window.addEventListener('load', clearAllBrowserStorage);

window.addEventListener('beforeunload', () => {
  if (centeringInterval) {
    clearInterval(centeringInterval);
  }
});

function setupWebSocketConnections() {
  const connectWebSocket = (url, handlers) => {
    const socket = new WebSocket(url);
    socket.onmessage = handlers.onmessage;
    socket.onerror = handlers.onerror;
    socket.onclose = () => {
      console.log('WebSocket connection closed. Reconnecting...');
      setTimeout(() => connectWebSocket(url, handlers), 5000);
    };
    return socket;
  };

  liveDataSocket = connectWebSocket(`${wsBaseUrl}/ws/live_route`, {
    onmessage: (event) => {
      const liveData = JSON.parse(event.data);
      updateLiveRouteOnMap(liveData);
    },
    onerror: (error) => {
      console.error('WebSocket error:', error);
      showFeedback('Error in WebSocket connection', 'error');
    }
  });

  metricsSocket = connectWebSocket(`${wsBaseUrl}/ws/trip_metrics`, {
    onmessage: (event) => {
      const metrics = JSON.parse(event.data);
      updateMetrics(metrics);
    },
    onerror: (error) => {
      console.error('WebSocket error:', error);
      showFeedback('Error in WebSocket connection', 'error');
    }
  });

  // Fetch initial live route data
  fetchInitialLiveRouteData();
}

async function fetchInitialLiveRouteData() {
  try {
    const response = await fetch('/api/live_route_data');
    if (!response.ok) {
      if (response.status === 404) {
        // No active trip, use last known position
        const lastPosition = JSON.parse(localStorage.getItem('lastKnownPosition'));
        if (lastPosition) {
          updateLiveRouteOnMap({ 
            features: [{ 
              geometry: { 
                type: "LineString",
                coordinates: [lastPosition] 
              } 
            }] 
          });
        }
        return;
      }
      throw new Error(`Failed to fetch initial live route data: ${response.status}`);
    }
    const initialData = await response.json();
    if (initialData && initialData.features && initialData.features.length > 0) {
      updateLiveRouteOnMap(initialData);
      // Store the last position
      const coordinates = initialData.features[0].geometry.coordinates;
      if (coordinates && coordinates.length > 0) {
        localStorage.setItem('lastKnownPosition', JSON.stringify(coordinates[coordinates.length - 1]));
      }
    } else {
      console.log('No live route data available');
    }
  } catch (error) {
    console.error('Error fetching initial live route data:', error);
    showFeedback('Error fetching initial live route data', 'error');
  }
}
function updateLiveRouteOnMap(liveData) {
  if (liveData && liveData.features && liveData.features.length > 0) {
    const coordinates = liveData.features[0].geometry.coordinates;
    if (coordinates.length > 0) {
      // Convert coordinates to LatLng objects
      const latLngs = coordinates.map(coord => L.latLng(coord[1], coord[0]));

      if (!liveRoutePolyline) {
        liveRoutePolyline = L.polyline(latLngs, {
          color: '#ADD8E6',
          weight: 3,
          opacity: 0.7,
          pane: 'liveRoutePane'
        }).addTo(map);
      } else {
        const currentLatLngs = liveRoutePolyline.getLatLngs();
        const newPoints = latLngs.slice(currentLatLngs.length);

        if (newPoints.length > 0) {
          newPoints.forEach((point, index) => {
            setTimeout(() => {
              currentLatLngs.push(point);
              liveRoutePolyline.setLatLngs(currentLatLngs);

              // Update marker position for the last point
              if (index === newPoints.length - 1) {
                if (liveMarker) {
                  liveMarker.setLatLng(point);
                } else {
                  liveMarker = L.marker(point, { 
                    icon: RED_BLINKING_MARKER_ICON,
                    pane: 'liveRoutePane'
                  }).addTo(map);
                }

                // Only update the map view if centering is enabled
                if (isCenteringOnLiveMarker) {
                  updateMapCenter();
                }
              }
            }, index * 50); // Adjust the delay (50ms) to control the animation speed
          });
        }
      }
    }
  }
}

function updateMetrics(metrics) {
  if (!metrics) {
    console.warn('No metrics data received');
    return;
  }

  const updateElement = (id, value, unit = '') => {
    const element = document.getElementById(id);
    if (element && value !== undefined) {
      element.textContent = `${value}${unit}`;
    }
  };

  updateElement('totalDistance', metrics.total_distance?.toFixed(2), ' miles');
  updateElement('totalTime', metrics.total_time);
  updateElement('maxSpeed', metrics.max_speed?.toFixed(2), ' mph');
  updateElement('startTime', new Date(metrics.start_time).toLocaleString());
  updateElement('endTime', new Date(metrics.end_time).toLocaleString());
}

setupWebSocketConnections();

// Custom marker icons
const BLUE_BLINKING_MARKER_ICON = L.divIcon({
  className: 'blinking-marker',
  iconSize: [20, 20],
  html: '<div style="background-color: blue; width: 100%; height: 100%; border-radius: 50%;"></div>'
});

const RED_BLINKING_MARKER_ICON = L.divIcon({
  className: 'blinking-marker animate__animated animate__bounce',
  iconSize: [20, 20],
  html: '<div style="background-color: red; width: 100%; height: 100%; border-radius: 50%;"></div>'
});

const RED_MARKER_ICON = L.divIcon({
  className: 'custom-marker animate__animated animate__bounceInDown',
  iconSize: [30, 30],
  html: '<div style="background-color: red; width: 100%; height: 100%; border-radius: 50%;"></div>'
});

// Initialize the application
document.addEventListener('DOMContentLoaded', async () => {
  try {
      showFeedback('Initializing application...', 'info');

      // Initialize the map
      await initMap();

      // Set initial state for checkboxes
      document.getElementById('filterWaco').checked = true;
      document.getElementById('wacoLimitsCheckbox').checked = true;

      // Initialize layers and controls
      await Promise.all([
          initWacoLimitsLayer(),
          initProgressLayer(),
          initWacoStreetsLayer(),
          loadHistoricalData().catch(error => {
              console.error('Historical data loading failed:', error);
              showFeedback('Error loading historical data. Some features may be unavailable.', 'error');
          }),
      ]);

      // Finalize map initialization
      finalizeMapInitialization();

      // Ensure Waco limits layer is visible
      updateWacoLimitsLayerVisibility();

      // Set up data polling and updates
      setInterval(updateProgress, PROGRESS_UPDATE_INTERVAL);
      setInterval(loadProgressData, PROGRESS_DATA_UPDATE_INTERVAL);

      // Set up event listeners
      setupEventListeners();

      // Set up WebSocket connections after map initialization
      setupWebSocketConnections();

      // Initial display of historical data with Waco filter applied
      displayHistoricalData(true);

      showFeedback('Application initialized successfully', 'success');
  } catch (error) {
      handleError(error, 'initializing application');
  } finally {
      hideLoading();
  }
});

// Initialize the Leaflet map
function initMap() {
  return new Promise((resolve, reject) => {
    try {
      if (map) {
        map.remove();
      }
      map = L.map('map', {
        maxBounds: wacoOnlyMode ? MCLENNAN_COUNTY_BOUNDS.pad(0.1) : null,
        minZoom: wacoOnlyMode ? 10 : null,
        closePopupOnClick: false,
        popupOptions: {
          className: 'custom-popup-wrapper'
        }
      });

      // Set a default view
      map.setView([31.5497, -97.1467], 10); // Coordinates for Waco, TX with a zoom level of 10

      L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        maxZoom: 19
      }).addTo(map);

      // Create map panes with correct z-index order
      map.createPane('wacoLimitsPane').style.zIndex = 300;
      map.createPane('progressPane').style.zIndex = 410;
      map.createPane('historicalDataPane').style.zIndex = 430;
      map.createPane('wacoStreetsPane').style.zIndex = 440;
      map.createPane('liveRoutePane').style.zIndex = 450;

      // Set a flag to indicate that the map is ready to be centered
      map.initialized = true;

      // Initialize drawing tools
      drawnItems = new L.FeatureGroup();
      map.addLayer(drawnItems);

      const drawControl = new L.Control.Draw({
        draw: {
          polyline: false,
          polygon: true,
          circle: false,
          rectangle: false,
          marker: false,
          circlemarker: false
        },
        edit: {
          featureGroup: drawnItems
        }
      });
      map.addControl(drawControl);

      // Event listeners for drawing tools
      map.on(L.Draw.Event.CREATED, (e) => {
        drawnItems.addLayer(e.layer);
        filterHistoricalDataByPolygon(e.layer);
      });

      map.on(L.Draw.Event.EDITED, (e) => {
        e.layers.eachLayer(filterHistoricalDataByPolygon);
      });

      map.on(L.Draw.Event.DELETED, displayHistoricalData);

      // Add progress controls
      const progressControl = L.control({ position: 'bottomleft' });
      progressControl.onAdd = () => {
        const div = L.DomUtil.create('div', 'progress-control');
        div.innerHTML = '<div id="progress-bar-container"><div id="progress-bar"></div></div><div id="progress-text"></div>';
        return div;
      };
      progressControl.addTo(map);

      if (wacoOnlyMode) {
        map.on('drag', function() {
          map.panInsideBounds(MCLENNAN_COUNTY_BOUNDS, { animate: false });
        });
      }

      // Ensure the map is properly loaded before resolving the promise
      map.whenReady(() => {
        showFeedback('Map initialized successfully', 'success');
        resolve(map);
      });
    } catch (error) {
      console.error('Error initializing map:', error);
      showFeedback('Error initializing map. Please try again.', 'error');
      reject(error);
    }
  });
}
// Call this function after all layers are initialized
function finalizeMapInitialization() {
  if (map) {
    map.fitBounds(MCLENNAN_COUNTY_BOUNDS);
    map.setMinZoom(map.getZoom());  // Set the minimum zoom to the current zoom level
    map.initialized = true;
  }
}

// Initialize the Waco city limits layer
async function initWacoLimitsLayer() {
  try {
    const wacoBoundary = document.getElementById('wacoBoundarySelect').value;

    // Wait for map to be initialized
    await map; // This will wait for the Promise returned by initMap() to resolve

    if (wacoLimitsLayer && map.hasLayer(wacoLimitsLayer)) {
      map.removeLayer(wacoLimitsLayer);
    }

    const geoJSONData = await fetchGeoJSON(`/static/boundaries/${wacoBoundary}.geojson`);
    wacoLimitsLayer = L.geoJSON(geoJSONData, {
      style: {
        color: 'red',
        weight: 2,
        fillColor: 'orange',
        fillOpacity: 0.03
      },
      pane: 'wacoLimitsPane',
      interactive: false  // Make the layer non-interactive
    });
    updateWacoLimitsLayerVisibility();
  } catch (error) {
    console.error('Error initializing Waco limits layer:', error);
    showFeedback('Error loading Waco limits. Some features may be unavailable.', 'error');
  }
}

// Update the visibility of the Waco limits layer based on checkbox state
function updateWacoLimitsLayerVisibility() {
  const showWacoLimits = document.getElementById('wacoLimitsCheckbox').checked;

  // Wait for map to be initialized
  if (map) {
    if (showWacoLimits) {
      if (!wacoLimitsLayer) {
        // Initialize the layer if it doesn't exist
        initWacoLimitsLayer().then(() => {
          if (map && !map.hasLayer(wacoLimitsLayer)) {
            wacoLimitsLayer.addTo(map);
          }
        });
      } else if (!map.hasLayer(wacoLimitsLayer)) {
        wacoLimitsLayer.addTo(map);
      }
    } else if (map.hasLayer(wacoLimitsLayer)) {
      map.removeLayer(wacoLimitsLayer);
    }
  }
}

// Initialize the progress layer
async function initProgressLayer() {
  try {
    // Wait for map to be initialized
    await map;

    const data = await fetchGeoJSON(`/progress_geojson?wacoBoundary=${DEFAULT_WACO_BOUNDARY}`);
    progressLayer = L.geoJSON(data, {
      style: (feature) => ({
        color: feature.properties.traveled ? '#00ff00' : '#ff0000',
        weight: 3,
        opacity: 0.7
      }),
      pane: 'progressPane'
    });
    updateProgressLayerVisibility();
  } catch (error) {
    console.error('Error initializing progress layer:', error);
    showFeedback('Error loading progress data. Some features may be unavailable.', 'error');
  }
}

// Update the visibility of the progress layer based on checkbox state
function updateProgressLayerVisibility() {
  const showProgressLayer = document.getElementById('progressLayerCheckbox').checked;

  // Wait for map to be initialized
  if (map) {
    if (showProgressLayer) {
      if (!progressLayer) {
        // Initialize the layer if it doesn't exist
        initProgressLayer().then(() => {
          if (map && !map.hasLayer(progressLayer)) {
            progressLayer.addTo(map);
          }
        });
      } else if (!map.hasLayer(progressLayer)) {
        progressLayer.addTo(map);
      }
    } else if (map.hasLayer(progressLayer)) {
      map.removeLayer(progressLayer);
    }
  }
}

// Initialize the Waco streets layer
async function initWacoStreetsLayer() {
  try {
    const wacoBoundary = document.getElementById('wacoBoundarySelect').value;
    const streetsFilter = document.getElementById('streets-select').value;

    // Wait for map to be initialized
    await map;

    const data = await fetchGeoJSON(`/waco_streets?wacoBoundary=${wacoBoundary}&filter=${streetsFilter}`);
    if (wacoStreetsLayer && map) {
      map.removeLayer(wacoStreetsLayer);
    }
    wacoStreetsLayer = L.geoJSON(data, {
      style: {
        color: '#808080',
        weight: 1,
        opacity: 0.7
      },
      pane: 'wacoStreetsPane',
      onEachFeature: (feature, layer) => {
        if (feature.properties && feature.properties.name) {
          layer.bindPopup(feature.properties.name);
        }

        layer.on('mouseover', function() {
          this.setStyle({
            color: '#FFFF00',
            weight: 5
          });
        });

        layer.on('mouseout', function() {
          this.setStyle({
            color: '#808080',
            weight: 1
          });
        });

        layer.on('click', function() {
          this.openPopup();
        });
      }
    });
    updateWacoStreetsLayerVisibility();
    showFeedback('Waco streets displayed', 'success');
  } catch (error) {
    console.error('Error initializing Waco streets layer:', error);
    showFeedback('Error loading Waco streets. Some features may be unavailable.', 'error');
  }
}

// Load historical data from the server
async function loadHistoricalData() {
  try {
    const response = await fetch('/api/load_historical_data');
    const data = await response.json();
    if (data.historical_geojson_features) {
      historicalDataLayer = L.geoJSON(data.historical_geojson_features, {
        style: {
          color: '#0000FF',
          weight: 3,
          opacity: 0.7
        },
        onEachFeature: addRoutePopup,
        pane: 'historicalDataPane'
      });
      updateHistoricalDataLayerVisibility();
      animateStatUpdate('totalHistoricalDistance', `${calculateTotalDistance(data.historical_geojson_features).toFixed(2)} miles`);
      showFeedback('Historical data loaded successfully', 'success');
    }
  } catch (error) {
    handleError(error, 'loading historical data');
  }
}

async function fetchHistoricalData(startDate = null, endDate = null) {
  try {
    const filterWaco = document.getElementById('filterWaco').checked;
    const wacoBoundary = document.getElementById('wacoBoundarySelect').value;
    const startDateParam = startDate || document.getElementById('startDate').value;
    const endDateParam = endDate || document.getElementById('endDate').value;

    const response = await fetch(
      `/filtered_historical_data?startDate=${startDateParam}&endDate=${endDateParam}` +
      `&filterWaco=${filterWaco}&wacoBoundary=${wacoBoundary}`
    );
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error fetching historical data:', error);
    showFeedback('Error fetching historical data. Please try again.', 'error');
    return { type: "FeatureCollection", features: [] };  // Return empty GeoJSON
  }
}

// Update the visibility of the Waco streets layer based on checkbox state
function updateWacoStreetsLayerVisibility() {
  const showWacoStreets = document.getElementById('wacoStreetsCheckbox').checked;

  // Wait for map to be initialized
  if (map && showWacoStreets && !map.hasLayer(wacoStreetsLayer)) {
    wacoStreetsLayer.addTo(map);
  } else if (map && !showWacoStreets && map.hasLayer(wacoStreetsLayer)) {
    map.removeLayer(wacoStreetsLayer);
  }
}

// Load live route data from the server
async function loadLiveRouteData() {
  try {
    liveDataSocket.onmessage = function (event) {
      const data = JSON.parse(event.data);
      
      if (data?.features?.[0]?.geometry?.coordinates?.length > 0) {
        const coordinates = data.features[0].geometry.coordinates.filter(coord => 
          Array.isArray(coord) && coord.length >= 2 && 
          !isNaN(coord[0]) && !isNaN(coord[1])
        );
        let liveRouteLayer; 
        if (coordinates.length > 0) {
          // Update or create the polyline layer on the map with new coordinates
          if (liveRouteLayer) {
            liveRouteLayer.setLatLngs(coordinates);  // Update existing polyline
          } else {
            liveRouteLayer = L.polyline(coordinates, {
              color: 'red',
              weight: 5,
              opacity: 0.7,
              pane: 'liveRoutePane'
            }).addTo(map);
          }
          
          // Update the live marker position with the last coordinate
          const lastCoord = coordinates[coordinates.length - 1];
          if (liveMarker) {
            liveMarker.setLatLng(lastCoord);  // Update marker position
          } else {
            liveMarker = L.marker(lastCoord, { icon: RED_BLINKING_MARKER_ICON }).addTo(map);
          }
          
          // Adjust the map bounds to fit the live route
          map.fitBounds(liveRouteLayer.getBounds());
          showFeedback('Live route data loaded successfully', 'success');
        }
      }
    };

    liveDataSocket.onerror = function (error) {
      handleError(error, 'loading live route data');
    };

    liveDataSocket.onclose = function () {
      console.log("WebSocket connection closed");
    };
  } catch (error) {
    handleError(error, 'loading live route data');
  }
}

function toggleCenterOnLiveMarker() {
  isCenteringOnLiveMarker = !isCenteringOnLiveMarker;
  
  const centerLiveMarkerBtn = document.getElementById('centerLiveMarkerBtn');
  
  if (isCenteringOnLiveMarker) {
    // Start centering
    centeringInterval = setInterval(updateMapCenter, 1000); // Update every second
    showFeedback('Started centering on live marker', 'info');
    centerLiveMarkerBtn.textContent = 'Stop Centering';
    updateMapCenter(); // Center immediately
  } else {
    // Stop centering
    clearInterval(centeringInterval);
    showFeedback('Stopped centering on live marker', 'info');
    centerLiveMarkerBtn.textContent = 'Center on Live Marker';
  }
}

function toggleWacoOnlyMode() {
  wacoOnlyMode = !wacoOnlyMode;
  initMap().then(() => {
    if (wacoOnlyMode) {
      map.setMaxBounds(MCLENNAN_COUNTY_BOUNDS.pad(0.1));
      map.setMinZoom(10);
      map.fitBounds(MCLENNAN_COUNTY_BOUNDS);
    } else {
      map.setMaxBounds(null);
      map.setMinZoom(null);
      // Set a default view for non-Waco-only mode
      map.setView([31.5497, -97.1467], 10); // Coordinates for Waco, TX with a zoom level of 10
    }
    
    // Ensure the map is properly loaded before proceeding
    map.whenReady(() => {
      // Reinitialize layers
      Promise.all([
        initWacoLimitsLayer(),
        loadProgressData(),
        loadWacoStreets(),
        displayHistoricalData(true)
      ]).then(() => {
        showFeedback(`Waco-only mode ${wacoOnlyMode ? 'enabled' : 'disabled'}`, 'info');
      }).catch(error => {
        console.error('Error reinitializing layers:', error);
        showFeedback('Error updating map layers. Please try again.', 'error');
      });
    });
  }).catch(error => {
    console.error('Error initializing map:', error);
    showFeedback('Error initializing map. Please try again.', 'error');
  });
}
function updateMapCenter() {
  if (isCenteringOnLiveMarker && liveMarker && map) {
    map.panTo(liveMarker.getLatLng(), { animate: true });
  }
}

// Clear the live route from the map
function clearLiveRoute() {
  if (liveRoutePolyline) {
    map.removeLayer(liveRoutePolyline);
    liveRoutePolyline = null;
  }

  // Send request to clear live route data on the server
  fetch('/clear_live_route', { method: 'POST' })
    .then(response => response.json())
    .then(data => {
      console.log(data.message);
      showFeedback('Live route cleared', 'info');
    })
    .catch(error => {
      console.error('Error clearing live route:', error);
      showFeedback('Error clearing live route', 'error');
    });
}



// Update live data on the map
function updateLiveData(liveData) {
  removeLayer(liveMarker);
  if (liveData && typeof liveData.latitude === 'number' && typeof liveData.longitude === 'number') {
    const latLng = [liveData.latitude, liveData.longitude];
    liveMarker = L.marker(latLng, { icon: BLUE_BLINKING_MARKER_ICON }).addTo(map);
    
    if (!liveRoutePolyline) {
      liveRoutePolyline = L.polyline([], { color: 'red', weight: 3 }).addTo(map);
    }
    liveRoutePolyline.addLatLng(latLng);
  } else {
    console.warn('Invalid live data received:', liveData);
    showFeedback('Invalid live data received', 'warning');
  }
}


// Update the animateStatUpdates function as well
function animateStatUpdates(metrics) {
  if (!metrics || Object.keys(metrics).length === 0) return;

  const updateQueue = [];

  Object.entries(metrics).forEach(([imei, deviceMetrics]) => {
    if (deviceMetrics) {
      updateQueue.push(() => {
        if (deviceMetrics.total_distance !== undefined) {
          animateStatUpdate('totalDistance', `${deviceMetrics.total_distance.toFixed(2)} miles`);
        }
        if (deviceMetrics.total_time !== undefined) {
          animateStatUpdate('totalTime', deviceMetrics.total_time);
        }
        if (deviceMetrics.max_speed !== undefined) {
          animateStatUpdate('maxSpeed', `${deviceMetrics.max_speed.toFixed(2)} mph`);
        }
        if (deviceMetrics.start_time) {
          animateStatUpdate('startTime', new Date(deviceMetrics.start_time).toLocaleString());
        }
        if (deviceMetrics.end_time) {
          animateStatUpdate('endTime', new Date(deviceMetrics.end_time).toLocaleString());
        }
      });
    }
  });

  // Execute updates sequentially
  updateQueue.reduce((promise, update) => promise.then(update), Promise.resolve());
}
// Update the progress bar and text
async function updateProgress() {
  try {
    const data = await fetchJSON('/progress');
    const progressBar = document.getElementById('progress-bar');
    const progressText = document.getElementById('progress-text');

    if (progressBar && progressText && data.coverage_percentage !== undefined) {
      const newWidth = `${data.coverage_percentage}%`;
      if (progressBar.style.width !== newWidth) {
        progressBar.style.width = newWidth;
        progressBar.classList.add('progress-bar-update');
        setTimeout(() => {
          progressBar.classList.remove('progress-bar-update');
        }, 1000);
      }
      
      // Update the progress text
      progressText.textContent = `${data.coverage_percentage.toFixed(2)}% Complete`;
      
      // Update the stat cards
      updateStatCard('lengthCoverage', `${data.coverage_percentage.toFixed(2)}% of Waco Streets Traveled`);
      updateStatCard('streetCount', `${data.total_streets}`);
      updateStatCard('streetsDriven', data.traveled_streets);
      updateStatCard('streetsRemaining', data.total_streets - data.traveled_streets);
      updateStatCard('percentageDriven', `${(data.traveled_streets / data.total_streets * 100).toFixed(2)}%`);
    } else {
      console.warn("Progress data is incomplete or DOM elements not found");
    }
  } catch (error) {
    console.error('Error fetching progress:', error);
  }
}

function updateStatCard(id, value) {
  const element = document.getElementById(id);
  if (element) {
    element.textContent = value;
  }
}

// Load progress data and update the progress layer
async function loadProgressData() {
  const wacoBoundary = document.getElementById('wacoBoundarySelect').value;

  try {
    await waitForMap();
    const data = await fetchGeoJSON(`/progress_geojson?wacoBoundary=${wacoBoundary}`);
    
    const newProgressLayer = L.vectorGrid.slicer(data, {
      maxZoom: 22,  // Increase this value
      tolerance: 5, // Adjust this value
      rendererFactory: L.canvas.tile,
      vectorTileLayerStyles: {
        sliced: (properties) => ({
          color: properties.traveled ? '#00ff00' : '#ff0000',
          weight: 3,
          opacity: 0.4
        })
      },
      interactive: true
    });

    if (progressLayer && map.hasLayer(progressLayer)) {
      map.removeLayer(progressLayer);
    }

    progressLayer = newProgressLayer;
    updateProgressLayerVisibility();

    showFeedback('Progress data loaded successfully', 'success');
    return progressLayer;
  } catch (error) {
    console.error('Error loading progress data:', error);
    showFeedback('Error loading progress data. Please try again.', 'error');
    return null;
  }
}

// Load Waco streets data and update the Waco streets layer
async function loadWacoStreets() {
  const wacoBoundary = document.getElementById('wacoBoundarySelect').value;
  const streetsFilter = document.getElementById('streets-select').value;

  try {
    await waitForMap();
    const data = await fetchGeoJSON(`/waco_streets?wacoBoundary=${wacoBoundary}&filter=${streetsFilter}`);
    
    if (wacoStreetsLayer && map.hasLayer(wacoStreetsLayer)) {
      map.removeLayer(wacoStreetsLayer);
    }

    wacoStreetsLayer = L.geoJSON(data, {
      style: {
        color: '#808080',
        weight: 1,
        opacity: 0.6
      },
      pane: 'wacoStreetsPane',
      onEachFeature: (feature, layer) => {
        if (feature.properties && feature.properties.name) {
          layer.bindPopup(feature.properties.name);
        }

        layer.on('mouseover', function() {
          this.setStyle({
            color: '#FFFF00',
            weight: 5
          });
        });

        layer.on('mouseout', function() {
          this.setStyle({
            color: '#808080',
            weight: 1
          });
        });

        layer.on('click', function() {
          this.openPopup();
        });
      }
    });

    updateWacoStreetsLayerVisibility();
    showFeedback('Waco streets displayed', 'success');
  } catch (error) {
    console.error('Error initializing Waco streets layer:', error);
    showFeedback('Error loading Waco streets. Some features may be unavailable.', 'error');
  }
}

// Add a popup to a route feature
function addRoutePopup(feature, layer) {
  console.log('Adding popup for feature:', feature);

  const timestamp = feature.properties.timestamp;
  console.log('Raw timestamp:', timestamp);

  let formattedDate = 'N/A';
  let formattedTime = 'N/A';

  if (timestamp) {
    try {
      const date = new Date(timestamp);
      console.log('Parsed date:', date);

      if (!isNaN(date.getTime())) {
        formattedDate = date.toLocaleDateString();
        formattedTime = date.toLocaleTimeString();
        console.log('Formatted date/time:', formattedDate, formattedTime);
      } else {
        console.error('Invalid date:', timestamp);
      }
    } catch (error) {
      console.error('Error parsing date:', error);
    }
  } else {
    console.warn('No timestamp provided for feature');
  }

  const distance = calculateTotalDistance([feature]);
  console.log('Calculated distance:', distance);

  const popupContent = L.DomUtil.create('div', 'custom-popup');
  
  const dateElement = L.DomUtil.create('p', 'popup-date', popupContent);
  dateElement.textContent = `Date: ${formattedDate}`;
  
  const timeElement = L.DomUtil.create('p', 'popup-time', popupContent);
  timeElement.textContent = `Time: ${formattedTime}`;
  
  const distanceElement = L.DomUtil.create('p', 'popup-distance', popupContent);
  distanceElement.textContent = `Distance: ${distance.toFixed(2)} miles`;

  const playbackButton = L.DomUtil.create('button', 'popup-button animate__animated animate__pulse', popupContent);
  playbackButton.textContent = 'Play Route';
  L.DomEvent.on(playbackButton, 'click', () => {
    console.log('Playback button clicked');
    if (feature.geometry.type === 'LineString' && feature.geometry.coordinates.length > 1) {
      startPlayback(feature.geometry.coordinates);
    } else if (feature.geometry.type === 'MultiLineString') {
      const validSegments = feature.geometry.coordinates.filter(segment => segment.length > 1);
      validSegments.forEach(startPlayback);
    }
  });

  console.log('Popup content:', popupContent.innerHTML);

  layer.bindPopup(popupContent);
}

// Calculate the total distance of a set of features
function calculateTotalDistance(features) {
  return features.reduce((total, feature) => {
    const coords = feature.geometry.coordinates;
    if (!coords || coords.length < 2) {
      console.error('Invalid coordinates:', coords);
      return total;
    }
    return total + coords.reduce((routeTotal, coord, index) => {
      if (index === 0) return routeTotal;
      const prevLatLng = L.latLng(coords[index - 1][1], coords[index - 1][0]);
      const currLatLng = L.latLng(coord[1], coord[0]);
      return routeTotal + prevLatLng.distanceTo(currLatLng) * 0.000621371; // Convert meters to miles
    }, 0);
  }, 0);
}

// Start route playback
function startPlayback(coordinates) {
  stopPlayback();
  currentCoordIndex = 0;
  playbackPolyline = L.polyline([], {
    color: 'yellow',
    weight: 4
  }).addTo(map);

  playbackMarker = createAnimatedMarker(L.latLng(coordinates[0][1], coordinates[0][0]), {
    icon: RED_BLINKING_MARKER_ICON
  }).addTo(map);

  isPlaying = true;
  document.getElementById('playPauseBtn').textContent = 'Pause';
  playbackAnimation = setInterval(() => {
    if (isPlaying && currentCoordIndex < coordinates.length) {
      const latLng = L.latLng(coordinates[currentCoordIndex][1], coordinates[currentCoordIndex][0]);
      playbackMarker.setLatLng(latLng);
      playbackPolyline.addLatLng(latLng);
      currentCoordIndex++;
    } else if (currentCoordIndex >= coordinates.length) {
      stopPlayback();
    }
  }, 100 / playbackSpeed);
}

// Toggle route playback
function togglePlayPause() {
  isPlaying = !isPlaying;
  document.getElementById('playPauseBtn').textContent = isPlaying ? 'Pause' : 'Play';
  showFeedback(isPlaying ? 'Playback resumed' : 'Playback paused', 'info');
}

// Stop route playback
function stopPlayback() {
  isPlaying = false;
  document.getElementById('playPauseBtn').textContent = 'Play';
  currentCoordIndex = 0;
  clearInterval(playbackAnimation);
  removeLayer(playbackPolyline);
  removeLayer(playbackMarker);
}

// Adjust route playback speed
function adjustPlaybackSpeed() {
  playbackSpeed = parseFloat(document.getElementById('playbackSpeed').value);
  document.getElementById('speedValue').textContent = playbackSpeed.toFixed(1) + 'x';
  if (playbackAnimation) {
    clearInterval(playbackAnimation);
    startPlayback(playbackPolyline.getLatLngs());
  }
}

// Filter historical data by a drawn polygon
function filterHistoricalDataByPolygon(polygon) {
  if (!historicalDataLayer) return;

  const filteredFeatures = historicalDataLayer.toGeoJSON().features.filter(feature => {
    if (feature.geometry.type === 'LineString') {
      return turf.booleanCrosses(polygon.toGeoJSON(), feature) ||
          turf.booleanWithin(feature, polygon.toGeoJSON());
    } else if (feature.geometry.type === 'MultiLineString') {
      return feature.geometry.coordinates.some(segment =>
          turf.booleanCrosses(polygon.toGeoJSON(), turf.lineString(segment)) ||
          turf.booleanWithin(turf.lineString(segment), polygon.toGeoJSON())
      );
    }
    return false;
  });

  updateMapWithHistoricalData({
    type: 'FeatureCollection',
    features: filteredFeatures
  });
}

// Clear all drawn shapes from the map
function clearDrawnShapes() {
  drawnItems.clearLayers();
  displayHistoricalData();
}

// Filter routes by a predefined time period
function filterRoutesBy(period) {
  const now = new Date();
  let startDate, endDate;

  switch (period) {
    case 'today':
      startDate = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      endDate = now;
      break;
    case 'yesterday':
      startDate = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
      endDate = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      break;
    case 'lastWeek':
      startDate = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 7);
      endDate = now;
      break;
    case 'lastMonth':
      startDate = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate());
      endDate = now;
      break;
    case 'lastYear':
      startDate = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
      endDate = now;
      break;
    case 'allTime':
      startDate = ALL_TIME_START_DATE;
      endDate = now;
      break;
    default:
      console.error('Invalid period:', period);
      return;
  }

  document.getElementById('startDate').value = startDate.toISOString().slice(0, 10);
  document.getElementById('endDate').value = endDate.toISOString().slice(0, 10);
  displayHistoricalData();
}

// Update the map with historical data
function updateMapWithHistoricalData(data, fitBounds = false) {
  removeLayer(historicalDataLayer); // Remove previous layer if any

  if (!data || !data.features || data.features.length === 0) {
    showFeedback('No historical data available for the selected period.', 'info');
    return;
  }

  try {
    // Use standard Leaflet GeoJSON layer
    historicalDataLayer = L.geoJSON(data, {
      style: {
        color: '#0000FF', // Blue color
        weight: 3,
        opacity: 0.7
      },
      onEachFeature: (feature, layer) => {
        layer.on('click', (e) => {
          L.popup()
            .setLatLng(e.latlng)
            .setContent(createPopupContent(feature))
            .openOn(map);
        });
      }
    }).addTo(map);

    // Only fit bounds if explicitly requested
    if (fitBounds && data.features.length > 0) {
      const bounds = L.latLngBounds(
        data.features.flatMap(f => 
          f.geometry.coordinates.map(coord => L.latLng(coord[1], coord[0]))
        )
      );
      if (bounds.isValid()) {
        map.fitBounds(bounds);
      } else {
        console.warn('Invalid bounds for historical data');
      }
    }

    // Calculate total distance if needed
    const totalDistance = calculateTotalDistance(data.features);
    document.getElementById('totalHistoricalDistance').textContent = `${totalDistance.toFixed(2)} miles`;

    showFeedback(`Displayed ${data.features.length} historical features`, 'success');
  } catch (error) {
    console.error('Error updating map with historical data:', error);
    showFeedback('Error displaying historical data on the map. Please try again.', 'error');
  }
}

function createPopupContent(feature) {
  const timestamp = feature.properties.timestamp;
  let formattedDate = 'N/A';
  let formattedTime = 'N/A';

  if (timestamp) {
    try {
      const date = new Date(timestamp);
      if (!isNaN(date.getTime())) {
        formattedDate = date.toLocaleDateString();
        formattedTime = date.toLocaleTimeString();
      }
    } catch (error) {
      console.error('Error parsing date:', error);
    }
  }

  const distance = calculateTotalDistance([feature]);

  const popupContent = document.createElement('div');
  popupContent.innerHTML = `
    Date: ${formattedDate}<br>
    Time: ${formattedTime}<br>
    Distance: ${distance.toFixed(2)} miles<br>
    <button class="playback-btn">Play Route</button>
  `;
  
  popupContent.querySelector('.playback-btn').addEventListener('click', () => {
    if (feature.geometry.type === 'LineString' && feature.geometry.coordinates.length > 1) {
      startPlayback(feature.geometry.coordinates);
    } else if (feature.geometry.type === 'MultiLineString') {
      const validSegments = feature.geometry.coordinates.filter(segment => segment.length > 1);
      validSegments.forEach(startPlayback);
    }
  });

  return popupContent;
}
// Display historical data based on filter settings
async function displayHistoricalData(fitBounds = false) {
  if (isProcessing) {
    showFeedback('A task is already in progress. Please wait.', 'warning');
    return;
  }

  isProcessing = true;
  disableFilterControls();
  showLoading('Loading historical data...');

  try {
    await waitForMap();
    const currentCenter = map.getCenter();
    const currentZoom = map.getZoom();
    const filterWaco = document.getElementById('filterWaco').checked;
    const wacoBoundary = document.getElementById('wacoBoundarySelect').value;
    const data = await fetchHistoricalData(null, null, filterWaco, wacoBoundary);
    
    if (data && data.features) {
      updateMapWithHistoricalData(data, fitBounds);
    } else {
      showFeedback('No historical data available for the selected period.', 'info');
    }

    // Reload progress layer with the new boundary
    const progressLayer = await loadProgressData();
    if (progressLayer) {
      showFeedback('Progress data loaded successfully', 'success');
    }

    // Reload Waco streets layer with the new boundary and filter
    await loadWacoStreets();

    // Restore the previous view instead of fitting to bounds
    map.setView(currentCenter, currentZoom);

  } catch (error) {
    console.error('Error displaying historical data:', error);
    showFeedback(`Error loading historical data: ${error.message}. Please try again.`, 'error');
  } finally {
    isProcessing = false;
    enableFilterControls();
    hideLoading();
    checkQueuedTasks();
  }
}
function waitForMap() {
  return new Promise((resolve) => {
    if (map && map.initialized) {
      resolve();
    } else {
      const checkInterval = setInterval(() => {
        if (map && map.initialized) {
          clearInterval(checkInterval);
          resolve();
        }
      }, 100);
    }
  });
}

// Disable filter controls
function disableFilterControls() {
  ['#time-filters button', '#applyFilterBtn', '#filterWaco', '#startDate', '#endDate', '#wacoBoundarySelect']
    .forEach(selector => {
      document.querySelectorAll(selector).forEach(el => el.disabled = true);
    });
}

// Enable filter controls
function enableFilterControls() {
  ['#time-filters button', '#applyFilterBtn', '#filterWaco', '#startDate', '#endDate', '#wacoBoundarySelect']
    .forEach(selector => {
      document.querySelectorAll(selector).forEach(el => el.disabled = false);
    });
}
// Update the visibility of the historical data layer based on checkbox state
function updateHistoricalDataLayerVisibility() {
  const showHistoricalData = document.getElementById('historicalDataCheckbox').checked;

  // Wait for map to be initialized
  if (map && showHistoricalData && historicalDataLayer && !map.hasLayer(historicalDataLayer)) {
    historicalDataLayer.addTo(map);
  } else if (map && !showHistoricalData && historicalDataLayer && map.hasLayer(historicalDataLayer)) {
    map.removeLayer(historicalDataLayer);
  }
}

// Set up event listeners for all controls
function setupEventListeners() {
  // Filter Controls
  const startDateEl = document.getElementById('startDate');
  const endDateEl = document.getElementById('endDate');
  const wacoBoundarySelectEl = document.getElementById('wacoBoundarySelect');
  const filterWacoEl = document.getElementById('filterWaco');
  const applyFilterBtnEl = document.getElementById('applyFilterBtn');

  if (startDateEl) startDateEl.addEventListener('change', () => displayHistoricalData(false));
  if (endDateEl) endDateEl.addEventListener('change', () => displayHistoricalData(false));
  if (filterWacoEl) filterWacoEl.addEventListener('change', () => displayHistoricalData(false));
  if (applyFilterBtnEl) applyFilterBtnEl.addEventListener('click', () => displayHistoricalData(false));

  // Waco Boundary Select Event Listener
  if (wacoBoundarySelectEl) {
    wacoBoundarySelectEl.addEventListener('change', async () => {
      await initWacoLimitsLayer();
      await loadProgressData();
      await loadWacoStreets();
      displayHistoricalData(true); // Fit bounds when changing boundary
    });
  }

  const timeFiltersEl = document.getElementById('time-filters');
  if (timeFiltersEl) {
    timeFiltersEl.querySelectorAll('button').forEach(button => {
      button.addEventListener('click', () => {
        filterRoutesBy(button.dataset.filter);
        displayHistoricalData(true); // Fit bounds when changing time filter
      });
    });
  }
  const centerLiveMarkerBtn = document.getElementById('centerLiveMarkerBtn');
  if (centerLiveMarkerBtn) {
    centerLiveMarkerBtn.addEventListener('click', toggleCenterOnLiveMarker);
  }
  const toggleWacoOnlyModeBtn = document.getElementById('toggleWacoOnlyModeBtn');
if (toggleWacoOnlyModeBtn) {
  toggleWacoOnlyModeBtn.addEventListener('click', toggleWacoOnlyMode);
}



  // Layer Control Checkboxes
  const historicalDataCheckboxEl = document.getElementById('historicalDataCheckbox');
  const wacoStreetsCheckboxEl = document.getElementById('wacoStreetsCheckbox');
  const wacoLimitsCheckboxEl = document.getElementById('wacoLimitsCheckbox');
  const progressLayerCheckboxEl = document.getElementById('progressLayerCheckbox');

  if (historicalDataCheckboxEl) historicalDataCheckboxEl.addEventListener('change', updateHistoricalDataLayerVisibility);
  if (wacoStreetsCheckboxEl) wacoStreetsCheckboxEl.addEventListener('change', updateWacoStreetsLayerVisibility);
  if (wacoLimitsCheckboxEl) wacoLimitsCheckboxEl.addEventListener('change', updateWacoLimitsLayerVisibility);
  if (progressLayerCheckboxEl) progressLayerCheckboxEl.addEventListener('change', updateProgressLayerVisibility);

  // Initialize the date range slider
  initializeDateRangeSlider();

  // Update the "Check for New Driving Data" button event listener
  const updateDataBtn = document.getElementById('updateDataBtn');
  if (updateDataBtn) {
    updateDataBtn.addEventListener('click', handleBackgroundTask(async () => {
      try {
        const dateRange = getSelectedDateRange();
        const response = await fetch('/update_historical_data', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(dateRange),
        });
        const data = await response.json();
        if (response.ok) {
          showFeedback(data.message, 'success');
          await Promise.all([
            displayHistoricalData(true), // Fit bounds after updating data
            updateProgress()
          ]);
        } else {
          throw new Error(data.error);
        }
      } catch (error) {
        throw new Error('Error updating historical data: ' + error.message);
      }
    }, 'Checking for new driving data...'));
  }

  // Clear Route Button
  const clearRouteBtn = document.getElementById('clearRouteBtn');
  if (clearRouteBtn) {
    clearRouteBtn.addEventListener('click', handleBackgroundTask(() => {
      clearLiveRoute();
      showFeedback('Live route cleared', 'info');
    }, 'Clearing live route...'));
  }

  // Playback Controls
  const playPauseBtn = document.getElementById('playPauseBtn');
  if (playPauseBtn) {
    playPauseBtn.addEventListener('click', togglePlayPause);
  }

  const stopBtn = document.getElementById('stopBtn');
  if (stopBtn) {
    stopBtn.addEventListener('click', () => {
      stopPlayback();
      showFeedback('Playback stopped', 'info');
    });
  }

  const playbackSpeedInput = document.getElementById('playbackSpeed');
  if (playbackSpeedInput) {
    playbackSpeedInput.addEventListener('input', adjustPlaybackSpeed);
  }

  // Search Controls
  const searchInput = document.getElementById('searchInput');
  const searchBtn = document.getElementById('searchBtn');
  if (searchBtn && searchInput) {
    searchBtn.addEventListener('click', handleBackgroundTask(async () => {
      const query = searchInput.value;
      if (!query) {
        showFeedback('Please enter a location to search for.', 'warning');
        return;
      }

      try {
        const data = await fetchJSON(`/search_location?query=${query}`);
        if (data.error) {
          throw new Error(data.error);
        } else {
          const { latitude, longitude, address } = data;
          map.setView([latitude, longitude], 13);
          removeLayer(searchMarker);
          searchMarker = createAnimatedMarker([latitude, longitude], { icon: RED_MARKER_ICON })
            .addTo(map)
            .bindPopup(`<b>${address}</b>`)
            .openPopup();

          showFeedback(`Found location: ${address}`, 'success');

          setTimeout(() => removeLayer(searchMarker), 10000);
        }
      } catch (error) {
        throw new Error('Error searching for location: ' + error.message);
      }
    }, 'Searching for location...'));

    searchInput.addEventListener('input', debounce(async () => {
      const query = searchInput.value;
      const suggestionsContainer = document.getElementById('searchSuggestions');
      suggestionsContainer.innerHTML = '';

      if (query.length < 3) {
        return;
      }

      try {
        const suggestions = await fetchJSON(`/search_suggestions?query=${query}`);
        if (suggestions.length > 0) {
          suggestions.forEach(suggestion => {
            const suggestionElement = document.createElement('div');
            suggestionElement.textContent = suggestion.address; // Safe insertion
            suggestionElement.classList.add('animate__animated', 'animate__fadeIn');
            suggestionElement.addEventListener('click', () => {
              searchInput.value = suggestion.address;
              suggestionsContainer.innerHTML = '';
            });
            suggestionsContainer.appendChild(suggestionElement);
          });
        }
      } catch (error) {
        console.error('Error fetching search suggestions:', error);
      }
    }, 300));

    searchInput.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        document.getElementById('searchSuggestions').innerHTML = '';
      }
    });

    searchBtn.addEventListener('click', () => {
      document.getElementById('searchSuggestions').innerHTML = '';
    });
  }

  // Toggle Map Controls Button
  const toggleMapControlsBtn = document.getElementById('toggleMapControlsBtn');
  const mapControls = document.getElementById('map-controls');

  if (toggleMapControlsBtn && mapControls) {
    toggleMapControlsBtn.addEventListener('click', () => {
      mapControls.classList.toggle('show'); // Toggle the 'show' class to control visibility
    });
  }


  // Clear Drawn Shapes Button
  const clearDrawnShapesBtn = document.getElementById('clearDrawnShapesBtn');
  if (clearDrawnShapesBtn) {
    clearDrawnShapesBtn.addEventListener('click', handleBackgroundTask(() => {
      clearDrawnShapes();
      displayHistoricalData(true); // Fit bounds after clearing shapes
    }, 'Clearing drawn shapes...'));
  }

  // Reset Progress Button
  const resetProgressBtn = document.getElementById('resetProgressBtn');
  if (resetProgressBtn) {
    resetProgressBtn.addEventListener('click', handleBackgroundTask(async () => {
      try {
        const response = await fetch('/reset_progress', { method: 'POST' });
        const data = await response.json();
        if (response.ok) {
          showFeedback(data.message, 'success');
          await Promise.all([
            updateProgress(),
            loadWacoStreets(),
            loadProgressData()
          ]);
          displayHistoricalData(true); // Fit bounds after resetting progress
        } else {
          throw new Error(data.error);
        }
      } catch (error) {
        throw new Error('Error resetting progress: ' + error.message);
      }
    }, 'Resetting progress...'));
  }

  // Streets Filter Select
  const streetsSelect = document.getElementById('streets-select');
  if (streetsSelect) {
    streetsSelect.addEventListener('change', () => {
      loadWacoStreets();
    });
  }

  // Logout Button
  const logoutBtn = document.getElementById('logoutBtn');
  if (logoutBtn) {
    logoutBtn.addEventListener('click', () => {
      window.location.href = '/logout';
    });
  }
}

// Utility Functions

// Debounce function for search suggestions
function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

// Show loading overlay with a message
function showLoading(message = 'Loading...') {
  const loadingOverlay = document.getElementById('loadingOverlay');
  if (loadingOverlay) {
    loadingOverlay.querySelector('.loading-text').textContent = message;
    loadingOverlay.style.display = 'flex';
  }
}

// Hide loading overlay
function hideLoading() {
  const loadingOverlay = document.getElementById('loadingOverlay');
  if (loadingOverlay) {
    loadingOverlay.style.display = 'none';
  }
}

// Create an animated marker
function createAnimatedMarker(latLng, options = {}) {
  return L.marker(latLng, { icon: BLUE_BLINKING_MARKER_ICON, ...options });
}

// Animate the update of a statistic element
function animateStatUpdate(elementId, newValue) {
  const element = document.getElementById(elementId);
  if (element) {
    element.textContent = newValue;
  }
}


function initializeDateRangeSlider() {
  const slider = document.getElementById('dateRangeSlider');
  const valueDisplay = document.getElementById('dateRangeValue');
  
  slider.addEventListener('input', function() {
    const days = this.value;
    valueDisplay.textContent = days === '365' ? 'All Time' : `${days} days`;
  });
}

function getSelectedDateRange() {
  const days = document.getElementById('dateRangeSlider').value;
  const endDate = new Date();
  let startDate;
  
  if (days === '365') {
    startDate = new Date(2020, 0, 1); // ALL_TIME_START_DATE
  } else {
    startDate = new Date(endDate);
    startDate.setDate(startDate.getDate() - parseInt(days) + 1);
  }
  
  return {
    startDate: formatDate(startDate),
    endDate: formatDate(endDate)
  };
}

// Fetch JSON data from a given URL
async function fetchJSON(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  return response.json();
}

// Fetch GeoJSON data from a given URL
function fetchGeoJSON(url) {
  return fetchJSON(url);
}
// Display a feedback message
function showFeedback(message, type = 'info', duration = FEEDBACK_DURATION) {
  const notificationList = document.getElementById('notification-list');
  const notificationCount = document.getElementById('notification-count');

  const listItem = document.createElement('li');
  listItem.className = `feedback ${type}`;
  listItem.textContent = `${type.toUpperCase()}: ${message}`;
  notificationList.appendChild(listItem);

  // Update notification count
  const currentCount = parseInt(notificationCount.textContent, 10);
  notificationCount.textContent = currentCount + 1;

  console.log(`${type.toUpperCase()}: ${message}`); // Log all feedback messages
}

// Toggle notification panel visibility
document.getElementById('notification-icon').addEventListener('click', () => {
  const panel = document.getElementById('notification-panel');
  panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
});

function handleError(error, context) {
  console.error(`Error in ${context}:`, error);
  showFeedback(`An error occurred while ${context}. Please try again.`, 'error');
}

// Handle a background task with UI locking and feedback
function handleBackgroundTask(taskFunction, feedbackMessage) {
  return async function(...args) {
    if (isProcessing) {
      processingQueue.push(() => handleBackgroundTask(taskFunction, feedbackMessage)(...args));
      showFeedback('Task queued. Please wait for the current task to finish.', 'info');
      return;
    }
    isProcessing = true;
    disableUI();
    showLoading(feedbackMessage);
    try {
      await taskFunction(...args);
    } catch (error) {
      console.error('Task failed:', error);
      showFeedback(`Error: ${error.message}`, 'error');
    } finally {
      isProcessing = false;
      enableUI();
      hideLoading();
      checkQueuedTasks();
    }
  };
}

// Disable UI elements
function disableUI() {
  document.body.classList.add('processing');
  document.querySelectorAll('button, input, select').forEach(el => el.disabled = true);
}

// Enable UI elements
function enableUI() {
  document.body.classList.remove('processing');
  document.querySelectorAll('button, input, select').forEach(el => el.disabled = false);
}

// Check and process queued background tasks
function checkQueuedTasks() {
  if (processingQueue.length > 0 && !isProcessing) {
    processingQueue.shift()();
  }
}

// Helper function to remove a layer from the map
function removeLayer(layer) {
  if (layer && map && map.hasLayer(layer)) { // Check if map is defined
    map.removeLayer(layer);
  }
}

// Helper function to introduce a delay
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Helper functions for date manipulation
function today() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate());
}

function yesterday() {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
}

function daysAgoDaysAgo(daysAgo) {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate() - daysAgo);
}

function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

// Expose functions for debugging (optional)
window.mapApp = {
  map,
  initMap,
  loadHistoricalData,
  loadLiveRouteData,
  updateProgress,
  loadProgressData,
  displayHistoricalData,
  clearLiveRoute,
  filterRoutesBy,
  clearDrawnShapes,
  togglePlayPause,
  stopPlayback,
  adjustPlaybackSpeed,
  showFeedback,
  showLoading,
  hideLoading
};