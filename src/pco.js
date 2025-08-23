import { http } from './http.js';

const BASE = 'https://api.planningcenteronline.com/people/v2';
const auth = 'Basic ' + Buffer.from(`${process.env.PCO_APP_ID}:${process.env.PCO_SECRET}`).toString('base64');

const H = () => ({ 'Authorization': auth, 'Accept': 'application/json', 'Content-Type': 'application/json' });

// Test PCO connection
export async function pcoTestConnection() {
  try {
    console.log('Testing PCO connection...');
    console.log(`App ID: ${process.env.PCO_APP_ID ? process.env.PCO_APP_ID.substring(0, 10) + '...' : 'MISSING'}`);
    
    const url = `${BASE}/people?per_page=1`;
    const res = await http(url, { headers: H() });
    const result = await res.json();
    
    console.log('PCO connection successful. Sample person:', JSON.stringify(result.data?.[0], null, 2));
    return result;
  } catch (error) {
    console.error('PCO connection test failed:', error);
    throw error;
  }
}

async function listAll(url) {
  let results = [];
  let next = url;
  while (next) {
    const res = await http(next, { headers: H() });
    const json = await res.json();
    results = results.concat(json.data || []);
    next = json.links?.next || null; // PCO sends absolute next link
  }
  return results;
}

export async function pcoPeopleSince(tsISO) {
  const url = new URL(`${BASE}/people`);
  if (tsISO) url.searchParams.set('where[updated_at][gte]', tsISO);
  url.searchParams.set('order', 'updated_at');
  url.searchParams.set('per_page', '100');
  return await listAll(url.href);
}

export async function pcoHouseholdsSince(tsISO) {
  const url = new URL(`${BASE}/households`);
  if (tsISO) url.searchParams.set('where[updated_at][gte]', tsISO);
  url.searchParams.set('order', 'updated_at');
  url.searchParams.set('per_page', '100');
  return await listAll(url.href);
}

// -- Upserts: confirm exact JSON:API types/attributes in your PCO account before enabling --

export async function pcoUpsertHousehold({ householdId, name }) {
  if (householdId) {
    // PATCH existing
    const url = `${BASE}/households/${householdId}`;
    const payload = { data: { type: 'Household', id: householdId, attributes: { name } } };
    const res = await http(url, { method: 'PATCH', headers: H(), body: JSON.stringify(payload) });
    return await res.json();
  } else {
    // POST new
    const url = `${BASE}/households`;
    const payload = { data: { type: 'Household', attributes: { name } } };
    const res = await http(url, { method: 'POST', headers: H(), body: JSON.stringify(payload) });
    return await res.json();
  }
}

export async function pcoUpsertPerson({ personId, name, householdId, firstName, lastName }) {
  // Parse name into first/last if not provided separately
  const nameParts = name ? name.split(' ') : [];
  const first = firstName || nameParts[0] || '';
  const last = lastName || nameParts.slice(1).join(' ') || '';
  
  const attributes = {
    first_name: first,
    last_name: last
  };
  
  const payload = personId ?
    { data: { type: 'Person', id: personId, attributes } } :
    { data: { type: 'Person', attributes } };

  // Relationship: depends on PCO People↔Household shape in your org. If `relationships` is supported:
  if (householdId) {
    payload.data.relationships = {
      households: { data: [{ type: 'Household', id: String(householdId) }] }
    };
  }

  const url = personId ? `${BASE}/people/${personId}` : `${BASE}/people`;
  const method = personId ? 'PATCH' : 'POST';
  const res = await http(url, { method, headers: H(), body: JSON.stringify(payload) });
  return await res.json();
}
