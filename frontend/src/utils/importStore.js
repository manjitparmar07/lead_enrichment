/**
 * importStore.js
 * --------------
 * Persists active import jobs in localStorage so they survive navigation.
 * The backend asyncio.create_task runs forever — this just keeps the frontend
 * tracking them regardless of which page the user is on.
 */

const KEY = 'wb_active_imports'

function _read() {
  try { return JSON.parse(localStorage.getItem(KEY) || '{}') } catch { return {} }
}

function _write(jobs) {
  localStorage.setItem(KEY, JSON.stringify(jobs))
}

/** Save a newly started job */
export function saveJob(jobId, filename, total = 0) {
  const jobs = _read()
  jobs[jobId] = { jobId, filename, total, status: 'running', pct: 0, processed: 0, startedAt: Date.now() }
  _write(jobs)
}

/** Merge a status update into a stored job */
export function updateJob(jobId, patch) {
  const jobs = _read()
  if (jobs[jobId]) { jobs[jobId] = { ...jobs[jobId], ...patch }; _write(jobs) }
}

/** Remove a job from the store */
export function removeJob(jobId) {
  const jobs = _read(); delete jobs[jobId]; _write(jobs)
}

/** All jobs in the store */
export function getAllJobs() {
  return Object.values(_read())
}

/** Only running/pending jobs */
export function getActiveJobs() {
  return getAllJobs().filter(j => j.status === 'running' || j.status === 'pending')
}
