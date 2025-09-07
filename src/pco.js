import { http } from './http.js';
import { DataConverter } from './fibery.js';

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
  let allIncluded = [];
  let next = url;
  while (next) {
    const res = await http(next, { headers: H() });
    const json = await res.json();
    results = results.concat(json.data || []);
    if (json.included) allIncluded = allIncluded.concat(json.included);
    next = json.links?.next || null; // PCO sends absolute next link
  }
  return { data: results, included: allIncluded };
}

async function listAllSimple(url) {
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
  
  // Include emails and phone_numbers to get primary contact info
  url.searchParams.set('include', 'emails,phone_numbers');
  
  // Include all the fields we need for bidirectional sync
  const includeFields = [
    'first_name',
    'last_name', 
    'name',
    'status',
    'birthdate',
    'child',
    'given_name',
    'grade',
    'middle_name',
    'nickname',
    'inactivated_at',
    'membership',
    'directory_status',
    'updated_at',
    'created_at'
  ];
  
  // Note: PCO API uses include parameter for relationships and fields parameter for attributes
  // We'll let the API return all attributes by default to ensure we don't miss any fields
  // url.searchParams.set('fields[person]', includeFields.join(','));
  
  return await listAll(url.href);
}

// Helper function to extract primary email from PCO person data
export function extractPrimaryEmail(pcoPersonData, included = []) {
  if (!pcoPersonData.relationships?.emails?.data?.length) return null;
  
  // Find all emails for this person in the included data
  const personEmails = pcoPersonData.relationships.emails.data
    .map(emailRef => included.find(item => item.type === 'Email' && item.id === emailRef.id))
    .filter(Boolean);
  
  if (!personEmails.length) return null;
  
  // Look for primary email first, then fall back to first email
  const primaryEmail = personEmails.find(email => email.attributes?.primary === true);
  const emailToUse = primaryEmail || personEmails[0];
  
  return emailToUse?.attributes?.address || null;
}

// Helper function to extract primary phone from PCO person data
export function extractPrimaryPhone(pcoPersonData, included = []) {
  if (!pcoPersonData.relationships?.phone_numbers?.data?.length) return null;
  
  // Find all phone numbers for this person in the included data
  const personPhones = pcoPersonData.relationships.phone_numbers.data
    .map(phoneRef => included.find(item => item.type === 'PhoneNumber' && item.id === phoneRef.id))
    .filter(Boolean);
  
  if (!personPhones.length) return null;
  
  // Look for primary phone first, then fall back to first phone
  const primaryPhone = personPhones.find(phone => phone.attributes?.primary === true);
  const phoneToUse = primaryPhone || personPhones[0];
  
  return phoneToUse?.attributes?.number || null;
}

export async function pcoHouseholdsSince(tsISO) {
  const url = new URL(`${BASE}/households`);
  if (tsISO) url.searchParams.set('where[updated_at][gte]', tsISO);
  url.searchParams.set('order', 'updated_at');
  url.searchParams.set('per_page', '100');
  return await listAllSimple(url.href);
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

export async function pcoUpsertPerson({ 
  personId, 
  name, 
  householdId, 
  firstName, 
  lastName,
  status,
  birthdate,
  child,
  givenName,
  grade,
  middleName,
  nickname,
  inactivatedAt,
  membership,
  directoryStatus
}) {
  // Parse name into first/last if not provided separately
  const nameParts = name ? name.split(' ') : [];
  const first = firstName || nameParts[0] || '';
  const last = lastName || nameParts.slice(1).join(' ') || '';
  
  const attributes = {
    first_name: DataConverter.fiberyToPcoText(first),
    last_name: DataConverter.fiberyToPcoText(last)
  };
  
  // Add all the new bidirectional fields with proper conversion
  if (status !== undefined) attributes.status = DataConverter.fiberyToPcoText(status);
  if (birthdate !== undefined) attributes.birthdate = DataConverter.fiberyToPcoDate(birthdate);
  if (child !== undefined) attributes.child = DataConverter.fiberyToPcoBoolean(child);
  if (givenName !== undefined) attributes.given_name = DataConverter.fiberyToPcoText(givenName);
  if (grade !== undefined) attributes.grade = DataConverter.fiberyToPcoInteger(grade);
  if (middleName !== undefined) attributes.middle_name = DataConverter.fiberyToPcoText(middleName);
  if (nickname !== undefined) attributes.nickname = DataConverter.fiberyToPcoText(nickname);
  if (inactivatedAt !== undefined) attributes.inactivated_at = DataConverter.fiberyToPcoDateTime(inactivatedAt);
  if (membership !== undefined) attributes.membership = DataConverter.fiberyToPcoText(membership);
  if (directoryStatus !== undefined) attributes.directory_status = DataConverter.fiberyToPcoText(directoryStatus);
  
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
