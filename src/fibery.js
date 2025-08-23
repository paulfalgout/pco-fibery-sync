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

// Test Fibery connection and schema
export async function fiberyTestConnection() {
  try {
    console.log('Testing Fibery connection...');
    console.log(`Host: ${FIBERY_HOST}`);
    console.log(`Space: ${FIBERY_SPACE}`);
    console.log(`Token: ${FIBERY_TOKEN ? FIBERY_TOKEN.substring(0, 10) + '...' : 'MISSING'}`);
    
    // Test with a simple query to check if the space and databases exist
    const body = [{
      command: 'fibery.entity/query',
      args: {
        'q/from': `${FIBERY_SPACE}/People`,
        'q/select': ['fibery/id'],
        'q/limit': 1
      }
    }];
    
    const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(body) });
    const result = await res.json();
    console.log('✅ Fibery connection successful. Response:', JSON.stringify(result, null, 2));
    
    return result;
  } catch (error) {
    console.error('❌ Fibery connection test failed:', error);
    throw error;
  }
}

export async function fiberyQueryPeopleSince(tsISO) {
  const body = [{
    command: 'fibery.entity/query',
    args: {
      'q/from': `${FIBERY_SPACE}/People`,
      'q/select': ['fibery/id', F.People('Name'), F.People('Person ID'), F.People('Household'), 'fibery/modification-date'],
      'q/where': tsISO ? ['>', 'fibery/modification-date', tsISO] : null,
      'q/limit': 1000
    }
  }];
  const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(body) });
  const [out] = await res.json();
  return out.result || [];
}

export async function fiberyQueryHouseholdsSince(tsISO) {
  const body = [{
    command: 'fibery.entity/query',
    args: {
      'q/from': `${FIBERY_SPACE}/Household`,
      'q/select': ['fibery/id', F.Household('Name'), F.Household('Household ID'), F.Household('Members'), 'fibery/modification-date'],
      'q/where': tsISO ? ['>', 'fibery/modification-date', tsISO] : null,
      'q/limit': 1000
    }
  }];
  const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(body) });
  const [out] = await res.json();
  return out.result || [];
}

export async function fiberyUpsertHouseholds(items) {
  if (!items?.length) {
    console.log('No households to upsert');
    return [];
  }
  
  console.log(`Upserting ${items.length} households to Fibery`);
  
  const ids = items.map(h => h.householdId).filter(Boolean);
  if (ids.length === 0) {
    console.log('No household IDs found, creating all as new');
    // Create all as new entities
    const cmds = items.map(h => ({
      command: 'fibery.entity/create',
      args: { 
        type: `${FIBERY_SPACE}/Household`, 
        entity: { 
          [F.Household('Name')]: h.name || 'Unnamed Household',
          [F.Household('Household ID')]: h.householdId || ''
        } 
      }
    }));
    const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(cmds) });
    return await res.json();
  }
  
  // Find existing households
  const find = [{
    command: 'fibery.entity/query',
    args: {
      'q/from': `${FIBERY_SPACE}/Household`,
      'q/select': ['fibery/id', F.Household('Household ID')],
      'q/where': ['in', F.Household('Household ID'), ids],
      'q/limit': ids.length
    }
  }];
  const foundRes = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(find) });
  const found = await foundRes.json();
  console.log('Fibery query response:', JSON.stringify(found, null, 2));
  
  const resultData = found?.[0]?.result || [];
  if (!Array.isArray(resultData)) {
    console.warn('Fibery query result is not an array:', resultData);
    return [];
  }
  
  const index = new Map(resultData.map(r => [r[F.Household('Household ID')], r['fibery/id']]));

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
    args: {
      'q/from': `${FIBERY_SPACE}/People`,
      'q/select': ['fibery/id', F.People('Person ID')],
      'q/where': ['in', F.People('Person ID'), ids],
      'q/limit': ids.length
    }
  }];
  const foundRes = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(find) });
  const found = await foundRes.json();
  console.log('Fibery people query response:', JSON.stringify(found, null, 2));
  
  const resultData = found?.[0]?.result || [];
  if (!Array.isArray(resultData)) {
    console.warn('Fibery people query result is not an array:', resultData);
    return [];
  }
  
  const index = new Map(resultData.map(r => [r[F.People('Person ID')], r['fibery/id']]));

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
