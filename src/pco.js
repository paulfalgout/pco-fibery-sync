import { http } from './http.js';

const BASE = 'https://api.planningcenteronline.com/people/v2';
const auth = 'Basic ' + Buffer.from(`${process.env.PCO_APP_ID}:${process.env.PCO_SECRET}`).toString('base64');

const H = () => ({ 'Authorization': auth, 'Accept': 'application/json', 'Content-Type': 'application/json' });

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

export async function pcoUpsertPerson({ personId, name, householdId }) {
  const payload = personId ?
    { data: { type: 'Person', id: personId, attributes: { name } } } :
    { data: { type: 'Person', attributes: { name } } };

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
