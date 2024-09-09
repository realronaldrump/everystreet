// Global constants
const MCLENNAN_COUNTY_BOUNDS = L.latLngBounds(
  L.latLng(31.3501, -97.4585), // Southwest corner
  L.latLng(31.7935, -96.8636)  // Northeast corner
);
const DEFAULT_WACO_BOUNDARY = 'less_goofy';
const ALL_TIME_START_DATE = new Date(2020, 0, 1);
const PROGRESS_UPDATE_INTERVAL = 60000; // Update progress every minute
const PROGRESS_DATA_UPDATE_INTERVAL = 300000; // Update progress data every 5 minutes
const LIVE_DATA_AND_METRICS_UPDATE_INTERVAL = 3000; // Update live data and metrics every 3 seconds
const FEEDBACK_DURATION = 5000; // Default duration for feedback messages
const LIVE_DATA_TIMEOUT = 5000; // Timeout for live data requests (in milliseconds)

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

// Global variables
let map = null;
let wacoLimitsLayer = null;
let progressLayer = null;
let historicalDataLayer = null;
let liveRoutePolyline = null;
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
let liveDataFetchController = null; // AbortController for live data requests
const processingQueue = [];

// Initialize the application
document.addEventListener('DOMContentLoaded', async () => {
  try {
    showFeedback('Initializing application...', 'info');

    // Initialize the map
    map = await initMap();

    // Initialize layers and controls
    await Promise.all([
      initWacoLimitsLayer(),
      initProgressLayer(),
      initWacoStreetsLayer(),
      loadHistoricalData().catch(error => {
        console.error('Historical data loading failed:', error);
        showFeedback('Error loading historical data. Some features may be unavailable.', 'error');
      }),
      loadLiveRouteData().catch(error => {
        console.error('Live route data loading failed:', error);
        showFeedback('Error loading live route data. Some features may be unavailable.', 'error');
      })
    ]);

    // Set up data polling and updates
    setInterval(updateLiveDataAndMetrics, LIVE_DATA_AND_METRICS_UPDATE_INTERVAL);
    setInterval(updateProgress, PROGRESS_UPDATE_INTERVAL);
    setInterval(loadProgressData, PROGRESS_DATA_UPDATE_INTERVAL);

    // Set up event listeners
    setupEventListeners();

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
      map = L.map('map').fitBounds(MCLENNAN_COUNTY_BOUNDS);

      L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        maxZoom: 19
      }).addTo(map);

      // Create map panes with correct z-index order
      map.createPane('wacoLimitsPane').style.zIndex = 400;
      map.createPane('progressPane').style.zIndex = 410;
      map.createPane('historicalDataPane').style.zIndex = 430;
      map.createPane('wacoStreetsPane').style.zIndex = 440;

      // Add progress controls
      const progressControl = L.control({ position: 'bottomleft' });
      progressControl.onAdd = () => {
        const div = L.DomUtil.create('div', 'progress-control');
        div.innerHTML = '<div id="progress-bar-container"><div id="progress-bar"></div></div><div id="progress-text"></div>';
        return div;
      };
      progressControl.addTo(map);

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

      showFeedback('Map initialized successfully', 'success');
      resolve(map);
    } catch (error) {
      reject(error);
    }
  });
}

// Initialize the Waco city limits layer
async function initWacoLimitsLayer() {
  try {
    const wacoBoundary = document.getElementById('wacoBoundarySelect').value;

    await map;

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
      pane: 'wacoLimitsPane'
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

  if (map && showWacoLimits && !map.hasLayer(wacoLimitsLayer)) {
    wacoLimitsLayer.addTo(map);
  } else if (map && !showWacoLimits && map.hasLayer(wacoLimitsLayer)) {
    map.removeLayer(wacoLimitsLayer);
  }
}

// Initialize the progress layer
async function initProgressLayer() {
  try {
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

  if (map && showProgressLayer && !map.hasLayer(progressLayer)) {
    progressLayer.addTo(map);
  } else if (map && !showProgressLayer && map.hasLayer(progressLayer)) {
    map.removeLayer(progressLayer);
  }
}

// Initialize the Waco streets layer
async function initWacoStreetsLayer() {
  try {
    const wacoBoundary = document.getElementById('wacoBoundarySelect').value;
    const streetsFilter = document.getElementById('streets-select').value;

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
      `/historical_data?startDate=${startDateParam}&endDate=${endDateParam}` +
      `&filterWaco=${filterWaco}&wacoBoundary=${wacoBoundary}`
    );

    if (!response.ok) {
      if (response.status === 404) {
        console.warn("No historical data found for the specified date range.");
        return { type: "FeatureCollection", features: [] };
      }
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();

    if (!data.type || data.type !== 'FeatureCollection') {
      console.warn('Historical data missing "FeatureCollection" type. Correcting...');
      data.type = 'FeatureCollection';
    }

    data.features = data.features.filter(feature => 
      feature.geometry && 
      feature.geometry.type === 'LineString' && 
      Array.isArray(feature.geometry.coordinates) &&
      feature.geometry.coordinates.length > 1
    );

    return data;
  } catch (error) {
    handleError(error, 'fetching historical data');
    return { type: "FeatureCollection", features: [] };
  }
}

// Update the visibility of the Waco streets layer based on checkbox state
function updateWacoStreetsLayerVisibility() {
  const showWacoStreets = document.getElementById('wacoStreetsCheckbox').checked;

  if (map && showWacoStreets && !map.hasLayer(wacoStreetsLayer)) {
    wacoStreetsLayer.addTo(map);
  } else if (map && !showWacoStreets && map.hasLayer(wacoStreetsLayer)) {
    map.removeLayer(wacoStreetsLayer);
  }
}

// Load live route data from the server
async function loadLiveRouteData() {
  try {
    const response = await fetch('/api/live_route_data');
    const data = await response.json();
    if (data?.features?.[0]?.geometry?.coordinates?.length > 0) {
      const coordinates = data.features[0].geometry.coordinates.filter(coord => 
        Array.isArray(coord) && coord.length >= 2 && 
        !isNaN(coord[0]) && !isNaN(coord[1])
      );
      
      if (coordinates.length > 0) {
        liveRoutePolyline = L.polyline(coordinates, {
          color: 'red',
          weight: 5,
          opacity: 0.7,
          pane: 'liveRoutePane'
        }).addTo(map);
        
        const lastCoord = coordinates[coordinates.length - 1];
        liveMarker = L.marker(lastCoord, {icon: RED_BLINKING_MARKER_ICON}).addTo(map);
        
        map.fitBounds(liveRoutePolyline.getBounds());
        showFeedback('Live route data loaded successfully', 'success');
      }
    }
  } catch (error) {
    handleError(error, 'loading live route data');
  }
}

// Clear the live route from the map
function clearLiveRoute() {
  removeLayer(liveRoutePolyline);
  removeLayer(liveMarker);
}

// Update live data on the map
function updateLiveData(liveData) {
  removeLayer(liveMarker);
  if (liveData && typeof liveData.latitude === 'number' && typeof liveData.longitude === 'number') {
    const latLng = [liveData.latitude, liveData.longitude];
    liveMarker = createAnimatedMarker(latLng, { icon: BLUE_BLINKING_MARKER_ICON }).addTo(map);
  } else {
    console.warn('Invalid live data received:', liveData);
    showFeedback('Invalid live data received', 'warning');
  }
}

// Update live data and metrics
async function updateLiveDataAndMetrics() {
  if (liveDataFetchController) {
    liveDataFetchController.abort();
  }

  liveDataFetchController = new AbortController();
  const signal = liveDataFetchController.signal;

  try {
    const liveDataPromise = fetchJSON('/live_data', { signal });
    const metricsPromise = fetchJSON('/trip_metrics');

    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Live data request timed out')), LIVE_DATA_TIMEOUT);
    });

    const [liveData, metrics] = await Promise.race([
      Promise.all([liveDataPromise, metricsPromise]),
      timeoutPromise
    ]);

    console.log('Live data:', liveData);
    console.log('Metrics:', metrics);

    if (liveData && !liveData.error) {
      Object.entries(liveData).forEach(([imei, data]) => {
        updateLiveData(imei, data);
        
        // Update UI elements for live data
        document.getElementById('lastUpdated').textContent = new Date(data.timestamp * 1000).toLocaleString();
        document.getElementById('speed').textContent = `${data.speed.toFixed(2)} mph`;
        document.getElementById('location').textContent = `${data.latitude.toFixed(6)}, ${data.longitude.toFixed(6)}`;
      });
    } else {
      console.error('Error fetching live data:', liveData?.error || 'No data received');
      showFeedback('Device offline or no live data available', 'warning');
    }

    // Update trip metrics
    if (metrics && Object.keys(metrics).length > 0) {
      Object.entries(metrics).forEach(([imei, deviceMetrics]) => {
        document.getElementById('totalDistance').textContent = `${deviceMetrics.total_distance.toFixed(2)} miles`;
        document.getElementById('totalTime').textContent = deviceMetrics.total_time;
        document.getElementById('maxSpeed').textContent = `${deviceMetrics.max_speed.toFixed(2)} mph`;
        document.getElementById('startTime').textContent = new Date(deviceMetrics.start_time).toLocaleString();
        document.getElementById('endTime').textContent = new Date(deviceMetrics.end_time).toLocaleString();
      });
    } else {
      console.warn('No metrics data received');
    }

    // Animate the updates
    animateStatUpdates(metrics);

  } catch (error) {
    if (error.name === 'AbortError') {
      console.log('Live data request aborted');
    } else {
      console.error('Error updating live data and metrics:', error);
      showFeedback('Error updating live data and metrics', 'error');
    }
  } finally {
    liveDataFetchController = null;
  }
}

// Animate updates to the statistics panel
function animateStatUpdates(metrics) {
  ['totalDistance', 'totalTime', 'maxSpeed', 'startTime', 'endTime'].forEach(id => {
    animateStatUpdate(id, metrics[id.toLowerCase()]);
  });
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
      
      progressText.textContent = ''; // Clear existing content
      
      const lengthCoverage = document.createElement('p');
      lengthCoverage.innerHTML = '<strong>Length Coverage:</strong> ';
      lengthCoverage.appendChild(document.createTextNode(`${data.coverage_percentage.toFixed(2)}% of Waco Streets Traveled`));
      
      const streetCount = document.createElement('p');
      streetCount.innerHTML = '<strong>Street Count:</strong> ';
      streetCount.appendChild(document.createTextNode(`${data.traveled_streets} / ${data.total_streets} (${(data.traveled_streets / data.total_streets * 100).toFixed(2)}%)`));
      
      progressText.appendChild(lengthCoverage);
      progressText.appendChild(streetCount);
    } else {
      console.warn("Progress data is incomplete or DOM elements not found");
    }
  } catch (error) {
    console.error('Error fetching progress:', error);
  }
}

// Load progress data and update the progress layer
async function loadProgressData() {
  try {
    const wacoBoundary = document.getElementById('wacoBoundarySelect').value;

    await map;

    const data = await fetchGeoJSON(`/progress_geojson?wacoBoundary=${wacoBoundary}`);
    if (progressLayer && map) {
      map.removeLayer(progressLayer);
    }
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
    console.error('Error loading progress data:', error);
    showFeedback('Error loading progress data. Please try again.', 'error');
  }
}

// Load Waco streets data and update the Waco streets layer
async function loadWacoStreets() {
  await initWacoStreetsLayer();
  try {
    const wacoBoundary = document.getElementById('wacoBoundarySelect').value;
    const streetsFilter = document.getElementById('streets-select').value;

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
      pane: 'wacoStreetsPane'
    });
    updateWacoStreetsLayerVisibility();
    showFeedback('Waco streets displayed', 'success');
  } catch (error) {
    console.error('Error loading Waco streets:', error);
    showFeedback('Error loading Waco streets', 'error');
  }
}

// Add a popup to a route feature
function addRoutePopup(feature, layer) {
  const timestamp = feature.properties.timestamp;
  let formattedDate = 'N/A';
  let formattedTime = 'N/A';

  if (timestamp) {
      try {
          const date = new Date(timestamp);
          if (!isNaN(date.getTime())) {
              formattedDate = date.toLocaleDateString();
              formattedTime = date.toLocaleTimeString();
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

  const playbackButton = document.createElement('button');
  playbackButton.textContent = 'Play Route';
  playbackButton.classList.add('animate__animated', 'animate__pulse');
  playbackButton.addEventListener('click', () => {
      if (feature.geometry.type === 'LineString' && feature.geometry.coordinates.length > 1) {
          startPlayback(feature.geometry.coordinates);
      } else if (feature.geometry.type === 'MultiLineString') {
          const validSegments = feature.geometry.coordinates.filter(segment => segment.length > 1);
          validSegments.forEach(startPlayback);
      }
  });

  const popupContent = document.createElement('div');
  popupContent.innerHTML = `Date: ${formattedDate}<br>Time: ${formattedTime}<br>Distance: ${distance.toFixed(2)} miles`;
  popupContent.appendChild(playbackButton);

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
  animateElement(document.getElementById('speedValue'), 'animate__rubberBand');
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

// Export historical data to a GPX file
async function exportToGPX() {
  const startDate = document.getElementById('startDate').value;
  const endDate = document.getElementById('endDate').value;
  const filterWaco = document.getElementById('filterWaco').checked;
  const wacoBoundary = document.getElementById('wacoBoundarySelect').value;

  await handleLongRunningTask('/export_gpx', {
    startDate,
    endDate,
    filterWaco,
    wacoBoundary
  });
}

// Update the map with historical data
function updateMapWithHistoricalData(data) {
  removeLayer(historicalDataLayer);

  historicalDataLayer = L.geoJSON(data, {
    style: {
      color: '#0000FF',
      weight: 3,
      opacity: 0.7
    },
    onEachFeature: (feature, layer) => {
      addRoutePopup(feature, layer);
      layer.on('mouseover', function() {
        this.setStyle({
          color: '#FF0000',
          weight: 5
        });
      });
      layer.on('mouseout', function() {
        this.setStyle({
          color: '#0000FF',
          weight: 3
        });
      });
    },
    pane: 'historicalDataPane'
  });

  if (map && !map.hasLayer(historicalDataLayer)) {
    historicalDataLayer.addTo(map);
  }

  const totalDistance = calculateTotalDistance(data.features);
  animateStatUpdate('totalHistoricalDistance', `${totalDistance.toFixed(2)} miles`);

  showFeedback(`Displayed ${data.features.length} historical features`, 'success');

  if (data.features.length > 0 && historicalDataLayer) {
    const bounds = historicalDataLayer.getBounds();
    if (bounds.isValid()) {
      map.fitBounds(bounds);
    } else {
      console.warn('Invalid bounds for historical data');
    }
  }
  
  // Animate the historical data layer
  if (historicalDataLayer) {
    historicalDataLayer.eachLayer(function (layer) {
      if (layer.getElement()) {
        layer.getElement().classList.add('animate__animated', 'animate__fadeIn');
      }
    });
  }
}

// Display historical data based on filter settings
async function displayHistoricalData() {
  if (isProcessing) {
    showFeedback('A task is already in progress. Please wait.', 'warning');
    return;
  }

  isProcessing = true;
  disableFilterControls();
  showLoading('Loading historical data...');

  try {
    const data = await fetchHistoricalData();
    updateMapWithHistoricalData(data);

    // Reload progress layer with the new boundary
    await loadProgressData();

    // Reload Waco streets layer with the new boundary and filter
    await loadWacoStreets();

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

  if (startDateEl) startDateEl.addEventListener('change', displayHistoricalData);
  if (endDateEl) endDateEl.addEventListener('change', displayHistoricalData);
  if (filterWacoEl) filterWacoEl.addEventListener('change', displayHistoricalData);
  if (applyFilterBtnEl) applyFilterBtnEl.addEventListener('click', displayHistoricalData);

  // Waco Boundary Select Event Listener
  if (wacoBoundarySelectEl) {
    wacoBoundarySelectEl.addEventListener('change', async () => {
      await initWacoLimitsLayer();
      await loadProgressData();
      await loadWacoStreets();
    });
  }

  const timeFiltersEl = document.getElementById('time-filters');
  if (timeFiltersEl) {
    timeFiltersEl.querySelectorAll('button').forEach(button => {
      button.addEventListener('click', () => filterRoutesBy(button.dataset.filter));
    });
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

  // Data Update Button
  const updateDataBtn = document.getElementById('updateDataBtn');
  if (updateDataBtn) {
    updateDataBtn.addEventListener('click', updateHistoricalData);
  }

  // Clear Route Button
  const clearRouteBtn = document.getElementById('clearRouteBtn');
  if (clearRouteBtn) {
    clearRouteBtn.addEventListener('click', () => {
      clearLiveRoute();
      showFeedback('Live route cleared', 'info');
    });
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
            suggestionElement.textContent = suggestion.address;
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
      mapControls.classList.toggle('show');
    });
  }

  // Export to GPX Button
  const exportToGPXBtn = document.getElementById('exportToGPXBtn');
  if (exportToGPXBtn) {
    exportToGPXBtn.addEventListener('click', exportToGPX);
  }

  // Clear Drawn Shapes Button
  const clearDrawnShapesBtn = document.getElementById('clearDrawnShapesBtn');
  if (clearDrawnShapesBtn) {
    clearDrawnShapesBtn.addEventListener('click', clearDrawnShapes);
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
    animateElement(element, 'animate__flipInX');
    element.textContent = newValue;
  }
}

// Animate an element with a given animation class
function animateElement(element, animationClass) {
  element.classList.add('animate__animated', animationClass);
  setTimeout(() => {
    element.classList.remove('animate__animated', animationClass);
  }, 1000);
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
  if (layer && map && map.hasLayer(layer)) {
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

function daysAgo(daysAgo) {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate() - daysAgo);
}

function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

// New function to handle long-running tasks
async function handleLongRunningTask(endpoint, params = {}) {
  showLoading('Starting task...');
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(params),
    });
    const data = await response.json();
    if (response.ok) {
      pollTaskStatus(data.task_id);
    } else {
      throw new Error(data.error || 'An error occurred');
    }
  } catch (error) {
    showFeedback(error.message, 'error');
    hideLoading();
  }
}

// Function to poll task status
async function pollTaskStatus(taskId) {
  try {
    const response = await fetch(`/task_status/${taskId}`);
    const data = await response.json();
    
    if (data.state === 'PENDING' || data.state === 'PROGRESS') {
      showLoading(`Task in progress: ${data.status || 'Processing...'}`);
      setTimeout(() => pollTaskStatus(taskId), 1000); // Poll every second
    } else if (data.state === 'SUCCESS') {
      showFeedback('Task completed successfully!', 'success');
      hideLoading();
      if (data.result) {
        handleTaskResult(data.result);
      }
    } else {
      throw new Error(data.status || 'Task failed');
    }
  } catch (error) {
    showFeedback(error.message, 'error');
    hideLoading();
  }
}

// Function to handle task result
function handleTaskResult(result) {
  if (result.type === 'historical_data_update') {
    displayHistoricalData();
  } else if (result.type === 'gpx_export') {
    // Trigger download of GPX file
    const blob = new Blob([result.data], { type: 'application/gpx+xml' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.style.display = 'none';
    a.href = url;
    a.download = 'export.gpx';
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
  }
}

// Update historical data
async function updateHistoricalData() {
  await handleLongRunningTask('/update_historical_data');
}

// Expose functions for debugging (optional)
window.mapApp = {
  map,
  initMap,
  loadHistoricalData,
  loadLiveRouteData,
  updateLiveDataAndMetrics,
  updateProgress,
  loadProgressData,
  displayHistoricalData,
  clearLiveRoute,
  exportToGPX,
  filterRoutesBy,
  clearDrawnShapes,
  togglePlayPause,
  stopPlayback,
  adjustPlaybackSpeed,
  showFeedback,
  showLoading,
  hideLoading,
  updateHistoricalData
};