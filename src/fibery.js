import { http } from './http.js';

const FIBERY_HOST = process.env.FIBERY_HOST; // e.g., yourcompany.fibery.io
const FIBERY_SPACE = process.env.FIBERY_SPACE; // e.g., Planning Center Sync
const FIBERY_TOKEN = process.env.FIBERY_TOKEN;
const FIBERY_API = `https://${FIBERY_HOST}/api/commands`;

function hdr() {
  return { 'Authorization': `Token ${FIBERY_TOKEN}`, 'Content-Type': 'application/json' };
}

// Utilities to build fully-qualified field names
const F = {
  People: (f) => `${FIBERY_SPACE}/People/${f}`,
  Household: (f) => `${FIBERY_SPACE}/Household/${f}`,
};

export async function fiberyQueryPeopleSince(tsISO) {
  const body = [{
    command: 'fibery.entity/query',
    args: { query: {
      from: `${FIBERY_SPACE}/People`,
      select: ['fibery/id', F.People('Name'), F.People('Person ID'), F.People('Household'), 'fibery/modificationDate'],
      filter: tsISO ? { 'fibery/modificationDate': { '>': tsISO } } : undefined,
      limit: 1000,
    }}
  }];
  const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(body) });
  const [out] = await res.json();
  return out.result || [];
}

export async function fiberyQueryHouseholdsSince(tsISO) {
  const body = [{
    command: 'fibery.entity/query',
    args: { query: {
      from: `${FIBERY_SPACE}/Household`,
      select: ['fibery/id', F.Household('Name'), F.Household('Household ID'), F.Household('Members'), 'fibery/modificationDate'],
      filter: tsISO ? { 'fibery/modificationDate': { '>': tsISO } } : undefined,
      limit: 1000,
    }}
  }];
  const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(body) });
  const [out] = await res.json();
  return out.result || [];
}

export async function fiberyUpsertHouseholds(items) {
  if (!items?.length) return [];
  const ids = items.map(h => h.householdId).filter(Boolean);
  const find = [{
    command: 'fibery.entity/query',
    args: { query: {
      from: `${FIBERY_SPACE}/Household`,
      select: ['fibery/id', F.Household('Household ID')],
      where: ['in', F.Household('Household ID'), ['const', ids]],
      limit: ids.length
    }}
  }];
  const found = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(find) }).then(r => r.json());
  const index = new Map((found?.[0]?.result || []).map(r => [r[F.Household('Household ID')], r['fibery/id']]));

  const cmds = items.map(h => {
    const existing = index.get(h.householdId);
    return existing ? {
      command: 'fibery.entity/update',
      args: { entity: { 'fibery/id': existing }, patch: { [F.Household('Name')]: h.name } }
    } : {
      command: 'fibery.entity/create',
      args: { type: `${FIBERY_SPACE}/Household`, entity: { [F.Household('Name')]: h.name, [F.Household('Household ID')]: h.householdId } }
    };
  });
  const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(cmds) });
  return await res.json();
}

export async function fiberyUpsertPeople(items, { householdIndexById } = {}) {
  if (!items?.length) return [];
  const ids = items.map(p => p.personId).filter(Boolean);
  const find = [{
    command: 'fibery.entity/query',
    args: { query: {
      from: `${FIBERY_SPACE}/People`,
      select: ['fibery/id', F.People('Person ID')],
      where: ['in', F.People('Person ID'), ['const', ids]],
      limit: ids.length
    }}
  }];
  const found = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(find) }).then(r => r.json());
  const index = new Map((found?.[0]?.result || []).map(r => [r[F.People('Person ID')], r['fibery/id']]));

  const cmds = items.map(p => {
    const existing = index.get(p.personId);
    const rel = p.householdId && householdIndexById?.get(p.householdId)
      ? { 'fibery/id': householdIndexById.get(p.householdId) }
      : null;
    return existing ? {
      command: 'fibery.entity/update',
      args: { entity: { 'fibery/id': existing }, patch: { [F.People('Name')]: p.name, [F.People('Household')]: rel } }
    } : {
      command: 'fibery.entity/create',
      args: { type: `${FIBERY_SPACE}/People`, entity: { [F.People('Name')]: p.name, [F.People('Person ID')]: p.personId, [F.People('Household')]: rel } }
    };
  });
  const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(cmds) });
  return await res.json();
}
