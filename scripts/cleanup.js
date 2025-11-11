// cleanup.js - GitHub Actions worker to maintain cameras + hotspots
const admin = require('firebase-admin');
const geolib = require('geolib');

// CONFIGURATION - tune these values
const MOBILE_EXPIRE_HOURS = 10;        // mobile reports expire after 10h (unless hotspot / preserved)
const OTHER_EXPIRE_HOURS  = 10;        // other hazards expire after 10h
const FIXED_REMOVE_THRESHOLD = -3;     // fixed camera removal requires count <= -3
const HOTSPOT_RADIUS_M = 200;          // radius to consider same hotspot
const HOTSPOT_WINDOW_DAYS = 10;         // lookback window for hotspot detection
const HOTSPOT_THRESHOLD = 3;           // reports needed in window to mark hotspot
const PRESERVE_HOTSPOT_DAYS = 10;       // when hotspot detected, preserve camera docs for this many days

// Helper ms
const MS = {await db.collection('cameras').doc(id).update({ removed: true, removedByWorker: true, lastSeen: now });
  HOUR: 1000 * 60 * 60,
  DAY: 1000 * 60 * 60 * 24
};

async function main() {
  if (!process.env.FIREBASE_SERVICE_ACCOUNT) {
    throw new Error('Missing FIREBASE_SERVICE_ACCOUNT env secret (set in GitHub repository secrets).');
  }

  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });

  const db = admin.firestore();
  const now = Date.now();

  console.log('Starting Firestore camera cleanup...', new Date(now).toISOString());

  // Fetch all camera docs (small/medium dataset assumed)
  const camsSnap = await db.collection('cameras').get();
  console.log(`Found ${camsSnap.size} camera docs`);

  const cams = camsSnap.docs.map(d => ({ id: d.id, data: d.data() }));

  // Precompute window cutoff
  const hotspotWindowCutoff = now - HOTSPOT_WINDOW_DAYS * MS.DAY;

  // 1) Per-doc maintenance: add missing fields, enforce thresholds, expire/delete by expiresAt or count
  for (const cam of cams) {
    const d = cam.data || {};
    const id = cam.id;
    const type = (d.type || 'mobile_camera');

    // ensure timestamp exists
    if (!d.timestamp) {
      await db.collection('cameras').doc(id).update({ timestamp: now });
      d.timestamp = now;
      console.log(`[${id}] Missing timestamp set to now`);
    }

    // Ensure numeric count
    const count = (typeof d.count === 'number') ? d.count : Number(d.count || 0);

    // Ensure confidence exists
    if (d.confidence === undefined || d.confidence === null) {
      let conf = (type === 'fixed_camera') ? 100 : 70;
      await db.collection('cameras').doc(id).update({ confidence: conf });
      d.confidence = conf;
      console.log(`[${id}] Set default confidence=${conf}`);
    }

    // Fixed cameras: ensure hotspot flag and do not auto-delete (mark removed only by threshold)
    if (type === 'fixed_camera') {
      if (!d.hotspot) {
        await db.collection('cameras').doc(id).update({ hotspot: true });
        d.hotspot = true;
        console.log(`[${id}] Marked fixed camera hotspot=true`);
      }

      if (count <= FIXED_REMOVE_THRESHOLD && !d.removed) {
        await db.collection('cameras').doc(id).update({
          removed: true,
          removedByWorker: true,
          lastSeen: now
        });
        console.log(`[${id}] Fixed camera marked removed (count=${count} <= ${FIXED_REMOVE_THRESHOLD})`);
      }
      continue; // skip deletion for fixed cameras
    }

    // For mobile_camera and other:
    const lifetimeHours = (type === 'other') ? OTHER_EXPIRE_HOURS : MOBILE_EXPIRE_HOURS;

    // If expiresAt missing, add it using timestamp + lifetime
    if (!d.expiresAt) {
      const expiresAt = (Number(d.timestamp) || now) + lifetimeHours * MS.HOUR;
      await db.collection('cameras').doc(id).update({ expiresAt: expiresAt });
      d.expiresAt = expiresAt;
      console.log(`[${id}] Added expiresAt=${new Date(expiresAt).toISOString()}`);
    }

    // Update lastSeen if missing
    if (!d.lastSeen) {
      await db.collection('cameras').doc(id).update({ lastSeen: d.timestamp || now });
      d.lastSeen = d.timestamp || now;
    }

    // ===== PATCH: preserve docs for hotspot window, hide from app on expiry =====
    const ts = Number(d.timestamp) || 0;
    const windowCutoff = hotspotWindowCutoff;

    // 1) If count <= 0 and not a hotspot: delete only when older than hotspot window; otherwise mark removed=true so app hides it
    if (!d.hotspot && count <= 0) {
      if (ts < windowCutoff) {
        await db.collection('cameras').doc(id).delete();
        console.log(`[${id}] Deleted because count <= 0 and not hotspot (older than hotspot window)`);
        continue;
      } else {
        if (!d.removed) {
          await db.collection('cameras').doc(id).update({ removed: true, removedByWorker: true, lastSeen: now });

          console.log(`[${id}] Marked removed=true because count <= 0 (kept for hotspot window)`);
        }
        continue;
      }
    }

    // 2) If expiresAt reached: hide from app (removed=true) but keep until hotspot window cutoff; if hotspot, extend preservation
    if (d.expiresAt && Number(d.expiresAt) < now) {
      if (!d.hotspot) {
        if (ts < windowCutoff) {
          await db.collection('cameras').doc(id).delete();
          console.log(`[${id}] Deleted (expired and older than hotspot window)`);
          continue;
        } else {
          if (!d.removed) {
            await db.collection('cameras').doc(id).update({ removed: true, removedByWorker: true, lastSeen: now });

            console.log(`[${id}] Marked removed=true (expired) — kept for hotspot window`);
          }
          continue;
        }
      } else {
        // hotspot: extend preservation
        const preserveUntil = now + PRESERVE_HOTSPOT_DAYS * MS.DAY;
        await db.collection('cameras').doc(id).update({ expiresAt: preserveUntil, lastSeen: now });
        console.log(`[${id}] Hotspot preserved until ${new Date(preserveUntil).toISOString()}`);
        continue;
      }
    }
    // ===== end patch =====

    // otherwise leave the doc in place so it can participate in hotspot detection
  } // end per-cam pass

  // 1b) OPTIONAL: include recent raw reports from 'camera_reports' collection (append-only) so hotspot detection can use all submissions
  // If you don't use camera_reports, this block is harmless (it will skip if collection empty).
  let reportDocs = [];
  try {
    const reportsSnap = await db.collection('camera_reports')
      .where('timestamp', '>=', hotspotWindowCutoff)
      .get();
    reportDocs = reportsSnap.docs.map(d => ({ id: `r_${d.id}`, data: d.data() }));
    if (reportDocs.length) console.log(`Included ${reportDocs.length} recent raw reports for hotspot detection`);
  } catch (err) {
    // ignore if camera_reports doesn't exist or query fails
  }

  // 2) Hotspot detection pass (group nearby recent mobile reports)
  // Build source list: recent mobile docs from 'cameras' + optional recent 'camera_reports'
  const mobileDocs = cams
    .filter(c => {
      const t = c.data && c.data.type ? c.data.type : 'mobile_camera';
      return (t === 'mobile_camera') && (c.data && c.data.timestamp && c.data.timestamp >= hotspotWindowCutoff);
    })
    .concat(reportDocs.filter(r => {
      const t = r.data && r.data.type ? r.data.type : 'mobile_camera';
      return t === 'mobile_camera' && r.data && r.data.timestamp && r.data.timestamp >= hotspotWindowCutoff;
    }));

  console.log(`Checking hotspots among ${mobileDocs.length} recent mobile docs/reports`);

  // naive O(n^2) cluster check — fine for small/medium dataset
  for (const base of mobileDocs) {
    const b = base.data;
    if (!b || !b.lat || !b.lon || !b.timestamp) continue;
    // skip if already hotspot
    if (b.hotspot) continue;

    // find nearby reports in window (search both camera docs and raw reports)
    const nearby = mobileDocs.filter(o => {
      const od = o.data;
      if (!od || !od.lat || !od.lon || !od.timestamp) return false;
      const dist = geolib.getDistance({ latitude: b.lat, longitude: b.lon }, { latitude: od.lat, longitude: od.lon });
      return dist <= HOTSPOT_RADIUS_M;
    });

    if (nearby.length >= HOTSPOT_THRESHOLD) {
      // mark cluster docs (only those backed by 'cameras' collection) as hotspot and increase expiresAt/confidence
      for (const n of nearby) {
        const nid = n.id;
        const nd = n.data || {};
        // Only update 'cameras' docs (skip pure 'camera_reports' entries prefixed with r_)
        if (nid && nid.startsWith('r_')) continue;

        const newConf = Math.min(100, (nd.confidence || 70) + 20);
        const preserveUntil = Date.now() + (PRESERVE_HOTSPOT_DAYS * MS.DAY);

        await db.collection('cameras').doc(nid).update({
          hotspot: true,
          confidence: newConf,
          expiresAt: preserveUntil,
          lastSeen: Math.max(nd.lastSeen || 0, nd.timestamp || 0)
        });

        console.log(`[${nid}] Promoted to hotspot (cluster size=${nearby.length})`);
      }

      // upsert a summary doc into 'camera_hotspots' collection keyed by rounded coords
      const keyLat = Number(b.lat).toFixed(4);
      const keyLon = Number(b.lon).toFixed(4);
      const hotspotId = `hs_${keyLat}_${keyLon}`;
      const recurringCount = nearby.length;
      const hotspotDoc = {
        lat: Number(b.lat),
        lon: Number(b.lon),
        type: 'mobile_camera',
        report_count: recurringCount,
        lastReported: Math.max(...nearby.map(n => n.data.timestamp || 0)),
        confidence: Math.min(1, recurringCount / HOTSPOT_THRESHOLD),
        updatedAt: Date.now()
      };
      await db.collection('camera_hotspots').doc(hotspotId).set(hotspotDoc, { merge: true });
      console.log(`[HOTSPOT] Updated hotspot doc ${hotspotId}`);
    }
  }

  console.log('Cleanup + hotspot worker finished.');
  process.exit(0);
}

main().catch(err => {
  console.error('Worker failure', err);
  process.exit(1);
});
