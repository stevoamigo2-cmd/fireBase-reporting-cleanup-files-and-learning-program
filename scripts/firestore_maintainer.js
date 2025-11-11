// scripts/firestore_maintainer.js
import admin from "firebase-admin";
import { getDistance } from "geolib";


const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
admin.initializeApp({
credential: admin.credential.cert(serviceAccount)
});
const db = admin.firestore();


const MOBILE_LIFETIME_HOURS = 8;
const OTHER_LIFETIME_HOURS = 4;
const HOTSPOT_RADIUS_M = 200;
const HOTSPOT_DAYS = 7;
const HOTSPOT_THRESHOLD = 3;
const BATCH_SIZE = 400; // keep below 500


function toMillis(ts) {
if (!ts) return 0;
if (typeof ts === 'number') return ts; // epoch ms
const d = new Date(ts);
return isNaN(d.getTime()) ? 0 : d.getTime();
}


async function main() {
const now = Date.now();
const sevenDaysAgo = now - HOTSPOT_DAYS * 24 * 60 * 60 * 1000;


// Fetch relevant docs — for hotpot detection we only need recent mobile_camera docs.
// But we also need to consider fixed/other for expiry. We'll fetch everything but ignore removed.
const snapshot = await db.collection("cameras").get();
const docs = snapshot.docs.map(d => ({ id: d.id, ref: d.ref, data: d.data() }));


const toDelete = [];
const updates = [];


// Pre-filter mobile docs within HOTSPOT_DAYS (for faster hotspot grouping)
const recentMobile = docs.filter(d => d.data.type === 'mobile_camera' && !d.data.removed)
.filter(d => toMillis(d.data.timestamp) >= sevenDaysAgo);


// === 1. Expiry cleanup & ensure expiresAt is set ===
for (const { id, ref, data } of docs) {
if (data.removed) continue; // ignore explicitly removed


const type = data.type || 'other';


if (type === 'mobile_camera' || type === 'other') {
// compute expiresAt if not set
const ts = toMillis(data.timestamp) || now;
const lifetime = type === 'mobile_camera' ? MOBILE_LIFETIME_HOURS : OTHER_LIFETIME_HOURS;
const computedExpires = ts + lifetime * 60 * 60 * 1000;


const expiresAtMs = data.expiresAt ? toMillis(data.expiresAt) : 0;
if (!expiresAtMs) {
// set expiresAt on the doc
updates.push({ ref, change: { expiresAt: new Date(computedExpires).toISOString() } });
}


// check expiration
const finalExpires = expiresAtMs || computedExpires;
if (finalExpires < now && type !== 'fixed_camera') {
toDelete.push(ref);
continue;
}
}
}


// === 2. Hotspot detection & reports counting ===
// For each recent mobile doc, count nearby recent mobile reports inside HOTSPOT_RADIUS_M.
for (const doc of recentMobile) {
const baseLat = doc.data.lat;
const baseLon = doc.data.lon;


// find nearby from recentMobile set (only within HOTSPOT_DAYS window)
const nearby = recentMobile.filter(d2 => {
// include self
const ts = toMillis(d2.data.timestamp);
if (ts < sevenDaysAgo) return false;
if (typeof d2.data.lat !== 'number' || typeof d2.data.lon !== 'number') return false;
});
