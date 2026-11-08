import fs from 'fs';
import https from 'https';
import { execSync } from 'child_process';

const DATA_ONLY = process.argv.includes('--data-only') || process.env.DATA_ONLY === '1';

const SIZE = 520;
const CX = SIZE / 2;
const CY = SIZE / 2;
const RADIUS = 238;
const FRAMES = 120;
const FRAME_DELAY = 50; // ms — 120 frames × 50ms = 6s per rotation (50% slower)
const WORLD_GEOJSON_URL = 'https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson';

function getRepositoryOwner() {
  if (process.env.GITHUB_REPOSITORY) {
    return process.env.GITHUB_REPOSITORY.split('/')[0];
  }
  try {
    const remote = execSync('git remote get-url origin', { encoding: 'utf8' }).trim();
    const match = remote.match(/github\.com(?::[0-9]+)?[:/]([^/]+)/);
    if (match) return match[1].toLowerCase();
  } catch (err) {}
  if (process.env.GITHUB_ACTOR) return process.env.GITHUB_ACTOR;
  throw new Error('Could not determine repository owner. Set GITHUB_ACTOR or ensure the script is run in a GitHub repo with remote origin.');
}

const USER = getRepositoryOwner();

function fetchJSONOnce(url, headers = {}) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          const error = new Error(`GET ${url} returned ${res.statusCode}`);
          error.status = res.statusCode;
          const retryAfter = res.headers['retry-after'];
          const resetHeader = res.headers['x-ratelimit-reset'];
          if (retryAfter) error.retryAfterMs = Number(retryAfter) * 1000;
          else if (resetHeader) error.retryAfterMs = Math.max(0, Number(resetHeader) * 1000 - Date.now());
          reject(error);
          return;
        }
        try { resolve(JSON.parse(data)); }
        catch (error) { reject(error); }
      });
    }).on('error', reject);
  });
}

async function fetchJSON(url, headers = {}) {
  let lastError;
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      return await fetchJSONOnce(url, headers);
    } catch (error) {
      lastError = error;
      const isRateLimited = error.status === 403 || error.status === 429;
      if (attempt < 4) {
        const waitMs = isRateLimited && error.retryAfterMs
          ? error.retryAfterMs + 500
          : 1000 * (attempt + 1);
        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }
    }
  }
  throw lastError;
}

function githubHeaders() {
  return {
    Accept: 'application/vnd.github+json',
    'User-Agent': 'github-contrib-globe-badge',
    ...(process.env.GITHUB_TOKEN ? { Authorization: `Bearer ${process.env.GITHUB_TOKEN}` } : {}),
  };
}

// Throttles calls to a shared budget within a rolling time window.
class RateLimiter {
  constructor(maxCalls, windowMs) {
    this.maxCalls = maxCalls;
    this.windowMs = windowMs;
    this.timestamps = [];
  }

  async wait() {
    const now = Date.now();
    this.timestamps = this.timestamps.filter((t) => now - t < this.windowMs);
    if (this.timestamps.length >= this.maxCalls) {
      const waitMs = this.windowMs - (now - this.timestamps[0]) + 250;
      await new Promise((resolve) => setTimeout(resolve, waitMs));
      return this.wait();
    }
    this.timestamps.push(Date.now());
    return undefined;
  }
}

// Keep a shared conservative budget for GitHub Search API requests.
// Commit search and pull-request search both use the Search API, so sharing
// the limiter avoids exhausting the search budget when both are used.
const githubSearchLimiter = new RateLimiter(25, 60_000);

async function searchCommits(query) {
  await githubSearchLimiter.wait();
  return fetchJSON(
    `https://api.github.com/search/commits?q=${query}&per_page=100`,
    { ...githubHeaders(), Accept: 'application/vnd.github+json' },
  );
}

async function searchPullRequests(query) {
  await githubSearchLimiter.wait();
  return fetchJSON(
    `https://api.github.com/search/issues?q=${query}&per_page=100`,
    { ...githubHeaders(), Accept: 'application/vnd.github+json' },
  );
}

async function fetchAllPages(url, headers = {}) {
  const items = [];
  for (let page = 1; page <= 10; page += 1) {
    const separator = url.includes('?') ? '&' : '?';
    const data = await fetchJSON(`${url}${separator}per_page=100&page=${page}`, headers);
    if (!Array.isArray(data) || data.length === 0) break;
    items.push(...data);
    if (data.length < 100) break;
  }
  return items;
}

// Commit search is public-only, so private repositories never appear there.
// When the token can read them, list their commits through the REST API and
// let the caller merge them into the same owner totals.
async function getPrivateCommitItems(user, sinceDate, untilDate) {
  const headers = githubHeaders();
  // Only named private repositories are exposed on the public globe.
  const allowlist = (process.env.PRIVATE_REPOS || '')
    .split(',')
    .map((name) => name.trim().toLowerCase())
    .filter(Boolean);
  if (!allowlist.length) return [];
  const repos = await fetchAllPages(
    'https://api.github.com/user/repos?affiliation=owner,collaborator,organization_member&visibility=private&sort=updated',
    headers,
  );
  const items = [];
  for (const repo of repos) {
    if (!repo?.private || !repo.full_name) continue;
    if (!allowlist.includes(repo.full_name.toLowerCase())) continue;
    let commits;
    try {
      commits = await fetchAllPages(
        `https://api.github.com/repos/${repo.full_name}/commits`
          + `?since=${sinceDate}T00:00:00Z&until=${untilDate}T23:59:59Z`,
        headers,
      );
    } catch (error) {
      console.warn(`Could not list commits for ${repo.full_name}: ${error.message}`);
      continue;
    }
    const self = user.toLowerCase();
    for (const commit of commits) {
      const login = commit?.author?.login?.toLowerCase();
      const email = commit?.commit?.author?.email?.toLowerCase() || '';
      if (!commit?.sha || (login !== self && !email.includes(self))) continue;
      items.push({ repository: { full_name: repo.full_name }, sha: commit.sha });
    }
  }
  return items;
}

const COMMIT_CACHE_PATH = 'commit-cache.json';
// A cached commit->origin mapping is trusted for this long before being
// re-resolved, since a repository that is private today (and thus invisible
// to the search API) can turn public later and turn out to be the true,
// older origin of a commit we already attributed elsewhere.
const ORIGIN_CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000;

function loadCommitCache() {
  try {
    return JSON.parse(fs.readFileSync(COMMIT_CACHE_PATH, 'utf8'));
  } catch {
    return {};
  }
}

function addDays(dateStr, days) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function midpointDate(fromDate, toDate) {
  const from = new Date(`${fromDate}T00:00:00Z`).getTime();
  const to = new Date(`${toDate}T00:00:00Z`).getTime();
  return new Date(from + Math.floor((to - from) / 2)).toISOString().slice(0, 10);
}

// GitHub's search API refuses to paginate past 1000 results for a single
// query, so a query matching more than that would silently drop the rest.
// Splitting the author-date range in half and recursing (only when needed)
// keeps each query's total_count under the cap while still covering the
// account's entire history.
async function fetchAllCommitItems(baseQuery, fromDate, toDate) {
  const items = [];
  async function fetchRange(from, to) {
    const q = encodeURIComponent(`${baseQuery} author-date:${from}..${to}`);
    const first = await searchCommits(`${q}&page=1`);
    const totalCount = first.total_count || 0;
    if (totalCount === 0) return;

    if (totalCount > 1000 && from !== to) {
      const mid = midpointDate(from, to);
      await fetchRange(from, mid);
      await fetchRange(addDays(mid, 1), to);
      return;
    }

    items.push(...(first.items || []));
    if (totalCount > 1000) {
      console.warn(`${totalCount} commits on ${from} exceed the search API's 1000-result cap; some may be missed.`);
    }
    const pages = Math.min(10, Math.ceil(totalCount / 100));
    for (let page = 2; page <= pages; page += 1) {
      const data = await searchCommits(`${q}&page=${page}`);
      items.push(...(data.items || []));
    }
  }
  await fetchRange(fromDate, toDate);
  return items;
}

// GitHub's search API refuses to paginate past 1000 results for a single
// query. Split the merged-date range recursively when necessary so that
// all merged pull requests can be collected.
async function fetchAllPullRequestItems(baseQuery, fromDate, toDate) {
  const items = [];
  async function fetchRange(from, to) {
    const q = encodeURIComponent(`${baseQuery} merged:${from}..${to}`);
    const first = await searchPullRequests(`${q}&page=1`);
    const totalCount = first.total_count || 0;
    if (totalCount === 0) return;

    if (totalCount > 1000 && from !== to) {
      const mid = midpointDate(from, to);
      await fetchRange(from, mid);
      await fetchRange(addDays(mid, 1), to);
      return;
    }

    items.push(...(first.items || []));
    if (totalCount > 1000) {
      console.warn(
        `${totalCount} pull requests on ${from} exceed the search API's ` +
        '1000-result cap; some may be missed.',
      );
    }
    const pages = Math.min(10, Math.ceil(totalCount / 100));
    for (let page = 2; page <= pages; page += 1) {
      const data = await searchPullRequests(`${q}&page=${page}`);
      items.push(...(data.items || []));
    }
  }
  await fetchRange(fromDate, toDate);
  return items;
}

async function getContributions(user, commitCache) {
  // Cache for repository metadata (repos API — 5000 req/hour limit).
  // Failures are NOT cached, so a transient error can be retried on a later
  // reference to the same repo instead of permanently poisoning the result.
  const repoCache = new Map();
  async function getRepositoryInfo(fullName) {
    if (repoCache.has(fullName)) return repoCache.get(fullName);
    const repo = await fetchJSON(`https://api.github.com/repos/${fullName}`, githubHeaders());
    repoCache.set(fullName, repo);
    return repo;
  }

  // Resolve the oldest repository that shares history with `fullName`.
  // A repo duplicating another repo's commits (same SHAs) is treated as a
  // copy of the original regardless of whether GitHub marks it as a fork —
  // plenty of repos are seeded from a full clone of another repo's history
  // without going through GitHub's fork feature. All candidates sharing a
  // SHA are found via the commits search API and the oldest (by creation
  // date) is kept as the origin. Results are cached per repository (not per
  // commit), so this search runs at most once per distinct repository
  // instead of once per commit.
  const originCache = new Map();
  async function resolveOriginRepo(fullName, sha) {
    if (originCache.has(fullName)) return originCache.get(fullName);

    const candidates = new Set([fullName]);
    try {
      const data = await searchCommits(`sha%3A${encodeURIComponent(sha)}+is:public`);
      for (const item of data.items || []) {
        if (item.repository?.full_name) candidates.add(item.repository.full_name);
      }
    } catch (error) {
      console.warn(`Could not search repositories for commit ${sha.slice(0, 8)}: ${error.message}`);
    }

    // Expand fork networks too — a candidate that's a GitHub fork points at its ultimate origin.
    for (const name of [...candidates]) {
      try {
        const info = await getRepositoryInfo(name);
        if (info?.fork && info.source?.full_name) candidates.add(info.source.full_name);
      } catch (error) {
        console.warn(`Could not fetch repository ${name}: ${error.message}`);
      }
    }

    // Pick the oldest candidate by creation date; unresolved repos are ignored.
    let oldest = null;
    let oldestDate = null;
    for (const name of candidates) {
      let info;
      try {
        info = await getRepositoryInfo(name);
      } catch (error) {
        console.warn(`Could not fetch repository ${name}: ${error.message}`);
        continue;
      }
      if (!info?.created_at) continue;
      if (oldest === null || info.created_at < oldestDate) {
        oldest = name;
        oldestDate = info.created_at;
      }
    }

    const resolved = oldest || fullName;
    originCache.set(fullName, resolved);
    return resolved;
  }

  // Search no further back than the account's own creation date — commits
  // authored before the account existed cannot belong to it. Falls back to
  // the previous hardcoded date if the account lookup fails for any reason.
  const accountCreatedAt = await fetchJSON(`https://api.github.com/users/${encodeURIComponent(user)}`, githubHeaders())
    .then((info) => info.created_at)
    .catch((error) => {
      console.warn(`Could not fetch account creation date for ${user}: ${error.message}`);
      return null;
    });
  const sinceDate = accountCreatedAt ? accountCreatedAt.slice(0, 10) : '2023-01-01';
  const untilDate = new Date().toISOString().slice(0, 10);

  const counts = new Map();
  const repoCommits = new Map();
  const seenShas = new Set();
  const seenPullRequests = new Set();
  let totalCommits = 0;
  let totalPullRequests = 0;
  const now = Date.now();

  // Count every public commit the user authored, in their own repos as well
  // as external ones. The globe is a map of where the user's work lives, so
  // excluding own repos would render it empty for anyone who mostly ships in
  // their own projects.
  const baseQuery = `author:${user} is:public`;
  const items = await fetchAllCommitItems(baseQuery, sinceDate, untilDate);
  const self = user.toLowerCase();
  for (const item of items) {
    const fullName = item.repository?.full_name;
    const sha = item.sha;
    if (!fullName || !sha) continue;
    if (seenShas.has(sha)) continue;
    seenShas.add(sha);

    const repoOwner = fullName.split('/')[0].toLowerCase();
    let originRepo;
    if (repoOwner === self) {
      // Own repo: attribute to itself. Origin resolution would walk a fork
      // back to its upstream and credit the upstream owner instead.
      originRepo = fullName;
    } else {
      const cached = commitCache[sha];
      if (cached && now - cached.resolvedAt < ORIGIN_CACHE_TTL_MS) {
        originRepo = cached.origin;
      } else {
        originRepo = await resolveOriginRepo(fullName, sha);
        commitCache[sha] = { origin: originRepo, resolvedAt: now };
      }
      if (originRepo.split('/')[0].toLowerCase() === self) continue;
    }

    const owner = originRepo.split('/')[0];
    if (!counts.has(owner)) counts.set(owner, { commits: 0, pullRequests: 0, repositories: new Set() });
    const ownerStats = counts.get(owner);
    ownerStats.commits += 1;
    ownerStats.repositories.add(originRepo);
    repoCommits.set(originRepo, (repoCommits.get(originRepo) || 0) + 1);
    totalCommits += 1;
  }

  // Private repositories are invisible to the public search above; include
  // them when the token has access so the globe reflects all shipped work.
  try {
    const privateItems = await getPrivateCommitItems(user, sinceDate, untilDate);
    for (const item of privateItems) {
      const fullName = item.repository?.full_name;
      const sha = item.sha;
      if (!fullName || !sha || seenShas.has(sha)) continue;
      seenShas.add(sha);
      const owner = fullName.split('/')[0];
      if (!counts.has(owner)) counts.set(owner, { commits: 0, pullRequests: 0, repositories: new Set() });
      const ownerStats = counts.get(owner);
      ownerStats.commits += 1;
      ownerStats.repositories.add(fullName);
      repoCommits.set(fullName, (repoCommits.get(fullName) || 0) + 1);
      totalCommits += 1;
    }
  } catch (error) {
    console.warn(`Could not include private repositories: ${error.message}`);
  }

  // Find pull requests authored by the user that were merged, in any public
  // repository (own or external).
  const pullRequestQuery = `author:${user} is:public is:merged type:pr`;
  const pullRequests = await fetchAllPullRequestItems(pullRequestQuery, sinceDate, untilDate);
  for (const pullRequest of pullRequests) {
    const repoUrl = pullRequest.repository_url;
    let fullName = null;
    if (repoUrl) {
      const parts = repoUrl.split('/');
      if (parts.length >= 2) {
        fullName = parts.slice(4).join('/');
      }
    }
    const number = pullRequest.number;
    if (!fullName || !number) continue;
    const pullRequestId = `${fullName}#${number}`;
    if (seenPullRequests.has(pullRequestId)) continue;
    seenPullRequests.add(pullRequestId);
    const owner = fullName.split('/')[0];
    if (!counts.has(owner)) counts.set(owner, { commits: 0, pullRequests: 0, repositories: new Set() });
    const ownerStats = counts.get(owner);
    ownerStats.pullRequests += 1;
    ownerStats.repositories.add(fullName);
    totalPullRequests += 1;
  }

  return { counts, repoCommits, totalCommits, totalPullRequests };
}

// Fallback location for the profile owner when GitHub has no location set.
const SELF_LOCATION = process.env.SELF_LOCATION || 'Soroti, Uganda';

async function getOwnerLocation(owner) {
  const user = await fetchJSON(`https://api.github.com/users/${encodeURIComponent(owner)}`, githubHeaders());
  if (user.location) return user.location;
  if (owner.toLowerCase() === USER.toLowerCase()) return SELF_LOCATION;
  return null;
}

async function geocode(location) {
  if (!location) return null;
  const result = await fetchJSON(
    `https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(location)}`,
    { 'User-Agent': 'github-contrib-globe-badge/1.0' },
  );
  if (!result[0]) return null;
  return [+result[0].lat, +result[0].lon];
}

function pointInRing(lon, lat, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i];
    const [xj, yj] = ring[j];
    const intersects = ((yi > lat) !== (yj > lat))
      && (lon < ((xj - xi) * (lat - yi)) / (yj - yi) + xi);
    if (intersects) inside = !inside;
  }
  return inside;
}

function pointInGeometry(lon, lat, geometry) {
  if (!geometry) return false;
  if (geometry.type === 'Polygon') {
    return pointInRing(lon, lat, geometry.coordinates[0])
      && !geometry.coordinates.slice(1).some((ring) => pointInRing(lon, lat, ring));
  }
  if (geometry.type === 'MultiPolygon') {
    return geometry.coordinates.some((polygon) => (
      pointInRing(lon, lat, polygon[0])
      && !polygon.slice(1).some((ring) => pointInRing(lon, lat, ring))
    ));
  }
  return false;
}

function landPolygons(geojson) {
  const polygons = [];
  for (const feature of geojson.features || []) {
    const geometry = feature.geometry;
    const groups = geometry?.type === 'Polygon'
      ? [geometry.coordinates]
      : geometry?.type === 'MultiPolygon' ? geometry.coordinates : [];
    for (const polygon of groups) {
      const ring = polygon[0];
      const lons = ring.map(([lon]) => lon);
      const lats = ring.map(([, lat]) => lat);
      polygons.push({
        polygon,
        minLon: Math.min(...lons),
        maxLon: Math.max(...lons),
        minLat: Math.min(...lats),
        maxLat: Math.max(...lats),
      });
    }
  }
  return polygons;
}

// Orthographic projection — centerLonDeg rotates the globe for animation.
function project(lat, lon, centerLonDeg = 0) {
  const latRad = (lat * Math.PI) / 180;
  const lonRad = (lon * Math.PI) / 180;
  const centerLon = (centerLonDeg * Math.PI) / 180;
  const centerLat = (12 * Math.PI) / 180;
  const cosLat = Math.cos(latRad);
  const x = Math.cos(latRad) * Math.sin(lonRad - centerLon);
  const y = Math.cos(centerLat) * Math.sin(latRad) - Math.sin(centerLat) * cosLat * Math.cos(lonRad - centerLon);
  const z = Math.sin(centerLat) * Math.sin(latRad) + Math.cos(centerLat) * cosLat * Math.cos(lonRad - centerLon);
  if (z < 0) return null;
  return [CX + x * RADIUS, CY - y * RADIUS, z];
}

// Precompute which grid points are land — independent of rotation angle.
function computeLandGrid(geojson) {
  const polygons = landPolygons(geojson);
  const grid = [];
  for (let lat = -84; lat <= 84; lat += 2.8) {
    for (let lon = -180; lon < 180; lon += 2.8) {
      const land = polygons.some(({ polygon, minLon, maxLon, minLat, maxLat }) => (
        lon >= minLon && lon <= maxLon && lat >= minLat && lat <= maxLat
        && pointInRing(lon, lat, polygon[0])
        && !polygon.slice(1).some((ring) => pointInRing(lon, lat, ring))
      ));
      if (land) grid.push([lat, lon]);
    }
  }
  return grid;
}

function roundRect(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

function renderFrame(ctx, centerLonDeg, landGrid, markers, totalCommits, totalPullRequests) {
  // Background
  ctx.fillStyle = '#fff';
  ctx.fillRect(0, 0, SIZE, SIZE);

  // Shadow
  ctx.fillStyle = 'rgba(156, 163, 175, 0.2)';
  ctx.beginPath();
  ctx.arc(CX + 4, CY + 8, RADIUS, 0, Math.PI * 2);
  ctx.fill();

  // Ocean
  const grad = ctx.createRadialGradient(
    CX - RADIUS * 0.08, CY - RADIUS * 0.15, 0,
    CX, CY, RADIUS,
  );
  grad.addColorStop(0, '#fff');
  grad.addColorStop(0.86, '#f3f4f6');
  grad.addColorStop(1, '#d1d5db');
  ctx.fillStyle = grad;
  ctx.beginPath();
  ctx.arc(CX, CY, RADIUS, 0, Math.PI * 2);
  ctx.fill();
  ctx.strokeStyle = '#e5e7eb';
  ctx.lineWidth = 3;
  ctx.stroke();

  // Land dots (clipped to globe)
  ctx.save();
  ctx.beginPath();
  ctx.arc(CX, CY, RADIUS, 0, Math.PI * 2);
  ctx.clip();
  ctx.fillStyle = '#343a40';
  ctx.globalAlpha = 0.9;
  for (const [lat, lon] of landGrid) {
    const p = project(lat, lon, centerLonDeg);
    if (!p) continue;
    ctx.beginPath();
    ctx.arc(p[0], p[1], 1.15, 0, Math.PI * 2);
    ctx.fill();
  }
  ctx.globalAlpha = 1;
  ctx.restore();

  // Markers — fade near the globe edge for smooth rotation
  for (const marker of markers) {
    const p = project(marker.location[0], marker.location[1], centerLonDeg);
    if (!p) continue;
    const [x, y, z] = p;
    const opacity = Math.min(1, z * 2.5);
    if (opacity <= 0) continue;
    const activityCount = marker.commits + marker.pullRequests;
    const totalActivities = totalCommits + totalPullRequests;
    const percentage = totalActivities ? Math.round((activityCount / totalActivities) * 100) : 0;

    ctx.globalAlpha = opacity;

    // Marker dot
    ctx.fillStyle = '#34d399';
    ctx.strokeStyle = '#fff';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(x, y, 6, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();

    // Label box
    const boxWidth = 118;
    const boxHeight = 48;
    const boxX = Math.max(8, Math.min(SIZE - boxWidth - 8, x - boxWidth / 2));
    const boxY = Math.max(8, y - 48);

    ctx.fillStyle = 'rgba(23, 23, 23, 0.94)';
    roundRect(ctx, boxX, boxY, boxWidth, boxHeight, 7);
    ctx.fill();

    ctx.fillStyle = '#fff';
    ctx.font = 'bold 14px monospace';
    ctx.textBaseline = 'middle';
    ctx.textAlign = 'left';
    ctx.fillText(`${marker.commits} commits`, boxX + 9, boxY + 16);
    ctx.fillText(`${marker.pullRequests} PRs`, boxX + 9, boxY + 34);
    ctx.fillStyle = '#34d399';
    ctx.font = '10px monospace';
    ctx.fillText(`↑ ${percentage}%`, boxX + 77, boxY + 34);

    ctx.globalAlpha = 1;
  }
}

async function main() {
  const commitCache = loadCommitCache();
  const { counts, repoCommits, totalCommits, totalPullRequests } = await getContributions(USER, commitCache);
  // Persist right away so already-resolved commits are saved even if a
  // later step (geocoding, GIF rendering) fails on this run.
  fs.writeFileSync(COMMIT_CACHE_PATH, JSON.stringify(commitCache, null, 2));
  const rankedOwners = [...counts.entries()].sort((a, b) => b[1].commits - a[1].commits);
  console.log(`Found ${totalCommits} commits and ${totalPullRequests} merged PRs across ${counts.size} owners`);
  // Walk the full ranking (not just the top 8) so an owner with no usable
  // location — e.g. a bot or org account — doesn't consume one of the 8
  // marker slots and hide a lower-ranked but geocodable contributor.
  const markers = [];
  for (const [owner, stats] of rankedOwners) {
    if (markers.length >= 1000) break;
    const location = await getOwnerLocation(owner).then(geocode);
    console.log(`Located ${owner}: ${location ? location.join(', ') : 'unknown'}`);
    if (location) markers.push({
      owner,
      commits: stats.commits,
      pullRequests: stats.pullRequests,
      location,
      repositories: [...stats.repositories]
        .map((fullName) => ({
          name: fullName.split('/').slice(1).join('/'),
          url: `https://github.com/${fullName}`,
          commits: repoCommits.get(fullName) || 0,
        }))
        .sort((a, b) => b.commits - a.commits || a.name.localeCompare(b.name)),
    });
  }
  const peak = Math.max(1, ...markers.map((m) => m.commits + m.pullRequests));
  for (const marker of markers) {
    const share = (marker.commits + marker.pullRequests) / peak;
    marker.size = Math.round((0.03 + share * 0.055) * 1000) / 1000;
  }
  fs.writeFileSync('data.json', JSON.stringify({
    user: USER,
    generatedAt: new Date().toISOString(),
    totalCommits,
    totalPullRequests,
    markers,
  }, null, 2));
  console.log(`data.json written – ${markers.length} markers, ${totalCommits} commits, ${totalPullRequests} merged PRs`);

  if (DATA_ONLY) {
    console.log('--data-only: skipping badge.gif render');
    return;
  }

  const geojson = await fetchJSON(WORLD_GEOJSON_URL, { 'User-Agent': 'github-contrib-globe-badge/1.0' });
  const { createCanvas } = await import('canvas');
  const { default: GIFEncoder } = await import('gif-encoder-2');

  // Render animated GIF — globe rotates one full turn seamlessly.
  const landGrid = computeLandGrid(geojson);
  const canvas = createCanvas(SIZE, SIZE);
  const ctx = canvas.getContext('2d');
  const encoder = new GIFEncoder(SIZE, SIZE);
  encoder.setDelay(FRAME_DELAY);
  encoder.setRepeat(0); // loop forever
  encoder.setQuality(10);
  encoder.start();
  for (let i = 0; i < FRAMES; i += 1) {
    const angle = (360 / FRAMES) * i;
    renderFrame(ctx, angle, landGrid, markers, totalCommits, totalPullRequests);
    encoder.addFrame(ctx);
  }
  encoder.finish();
  fs.writeFileSync('badge.gif', Buffer.from(encoder.out.getData()));
  console.log(`✅ badge.gif written – ${markers.length} locations, ${totalCommits} commits, ${totalPullRequests} merged PRs, ${FRAMES} frames`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
