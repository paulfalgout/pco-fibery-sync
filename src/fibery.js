import { http } from './http.js';

const FIBERY_HOST = process.env.FIBERY_HOST; // e.g., yourcompany.fibery.io
const FIBERY_SPACE = process.env.FIBERY_SPACE; // e.g., Planning Center Sync
const FIBERY_TOKEN = process.env.FIBERY_TOKEN;
const FIBERY_API = `https://${FIBERY_HOST}/api/commands`;

function hdr() {
  return { 'Authorization': `Token ${FIBERY_TOKEN}`, 'Content-Type': 'application/json' };
}

// PCO API headers for bidirectional sync
function pcoHeaders() {
  return {
    'Authorization': `Bearer ${process.env.PCO_SECRET}`,
    'Content-Type': 'application/json'
  };
}

// Data conversion utilities for PCO <-> Fibery sync
export const DataConverter = {
  // Convert PCO date (YYYY-MM-DD) to Fibery date format
  pcoToFiberyDate: (pcoDate) => {
    if (!pcoDate) return null;
    // PCO dates are already in YYYY-MM-DD format which Fibery accepts
    return pcoDate;
  },
  
  // Convert Fibery date to PCO date format (YYYY-MM-DD)
  fiberyToPcoDate: (fiberyDate) => {
    if (!fiberyDate) return null;
    // If it's already a string in YYYY-MM-DD format, return as-is
    if (typeof fiberyDate === 'string' && fiberyDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
      return fiberyDate;
    }
    // If it's a Date object or other format, convert to YYYY-MM-DD
    const date = new Date(fiberyDate);
    if (isNaN(date.getTime())) return null;
    return date.toISOString().split('T')[0];
  },
  
  // Convert PCO datetime (ISO 8601) to Fibery datetime format
  pcoToFiberyDateTime: (pcoDateTime) => {
    if (!pcoDateTime) return null;
    // PCO sends ISO 8601 format which Fibery accepts
    return pcoDateTime;
  },
  
  // Convert Fibery datetime to PCO datetime format (ISO 8601)
  fiberyToPcoDateTime: (fiberyDateTime) => {
    if (!fiberyDateTime) return null;
    // If it's already ISO 8601 format, return as-is
    if (typeof fiberyDateTime === 'string' && fiberyDateTime.includes('T')) {
      return fiberyDateTime;
    }
    // Convert to ISO 8601 format
    const date = new Date(fiberyDateTime);
    if (isNaN(date.getTime())) return null;
    return date.toISOString();
  },
  
  // Convert PCO boolean to Fibery boolean
  pcoToFiberyBoolean: (pcoBoolean) => {
    if (pcoBoolean === null || pcoBoolean === undefined) return null;
    return Boolean(pcoBoolean);
  },
  
  // Convert Fibery boolean to PCO boolean
  fiberyToPcoBoolean: (fiberyBoolean) => {
    if (fiberyBoolean === null || fiberyBoolean === undefined) return null;
    return Boolean(fiberyBoolean);
  },
  
  // Convert PCO text to Fibery text (handle nulls)
  pcoToFiberyText: (pcoText) => {
    if (pcoText === null || pcoText === undefined || pcoText === '') return null;
    return String(pcoText);
  },
  
  // Convert Fibery text to PCO text (handle nulls)
  fiberyToPcoText: (fiberyText) => {
    if (fiberyText === null || fiberyText === undefined || fiberyText === '') return null;
    return String(fiberyText);
  },
  
  // Convert PCO integer to Fibery integer
  pcoToFiberyInteger: (pcoInteger) => {
    if (pcoInteger === null || pcoInteger === undefined) return null;
    const num = parseInt(pcoInteger, 10);
    return isNaN(num) ? null : num;
  },
  
  // Convert Fibery integer to PCO integer
  fiberyToPcoInteger: (fiberyInteger) => {
    if (fiberyInteger === null || fiberyInteger === undefined) return null;
    const num = parseInt(fiberyInteger, 10);
    return isNaN(num) ? null : num;
  },
  
  // Handle null/empty values
  sanitizeValue: (value) => {
    if (value === null || value === undefined || value === '') return null;
    return value;
  },
  
  // Create a person mapping object from PCO data
  mapPcoPersonToFibery: (pcoPersonData) => {
    const attrs = pcoPersonData.attributes || {};
    return {
      personId: pcoPersonData.id,
      name: [attrs.first_name, attrs.last_name].filter(Boolean).join(' ') || attrs.name || 'Unnamed Person',
      firstName: DataConverter.pcoToFiberyText(attrs.first_name),
      lastName: DataConverter.pcoToFiberyText(attrs.last_name),
      status: DataConverter.pcoToFiberyText(attrs.status),
      birthdate: DataConverter.pcoToFiberyDate(attrs.birthdate),
      child: DataConverter.pcoToFiberyBoolean(attrs.child),
      givenName: DataConverter.pcoToFiberyText(attrs.given_name),
      grade: DataConverter.pcoToFiberyInteger(attrs.grade),
      middleName: DataConverter.pcoToFiberyText(attrs.middle_name),
      nickname: DataConverter.pcoToFiberyText(attrs.nickname),
      inactivatedAt: DataConverter.pcoToFiberyDateTime(attrs.inactivated_at),
      membership: DataConverter.pcoToFiberyText(attrs.membership),
      directoryStatus: DataConverter.pcoToFiberyText(attrs.directory_status),
      householdId: pcoPersonData.relationships?.households?.data?.[0]?.id || null,
    };
  },
  
  // Create a person mapping object from Fibery data
  mapFiberyPersonToPco: (fiberyPersonData, fiberySpace) => {
    const household = fiberyPersonData[`${fiberySpace}/People/Household`];
    const householdId = household ? household[`${fiberySpace}/Household/Household ID`] : null;
    
    return {
      personId: fiberyPersonData[`${fiberySpace}/People/Person ID`],
      name: fiberyPersonData[`${fiberySpace}/People/Name`],
      firstName: fiberyPersonData[`${fiberySpace}/People/First Name`],
      lastName: fiberyPersonData[`${fiberySpace}/People/Last Name`],
      status: fiberyPersonData[`${fiberySpace}/People/Status`],
      birthdate: fiberyPersonData[`${fiberySpace}/People/Birthdate`],
      child: fiberyPersonData[`${fiberySpace}/People/Child`],
      givenName: fiberyPersonData[`${fiberySpace}/People/Given Name`],
      grade: fiberyPersonData[`${fiberySpace}/People/Grade`],
      middleName: fiberyPersonData[`${fiberySpace}/People/Middle Name`],
      nickname: fiberyPersonData[`${fiberySpace}/People/Nickname`],
      inactivatedAt: fiberyPersonData[`${fiberySpace}/People/Inactivated At`],
      membership: fiberyPersonData[`${fiberySpace}/People/Membership`],
      directoryStatus: fiberyPersonData[`${fiberySpace}/People/Directory Status`],
      householdId: householdId,
    };
  }
};

// Utilities to build fully-qualified field names
const F = {
  People: (f) => {
    // Handle shared fields at space level
    if (f === 'Name') return `${FIBERY_SPACE}/Name`;
    // Handle database-specific fields
    if (f === 'Person ID') return `${FIBERY_SPACE}/Person ID`;
    if (f === 'Household') return `${FIBERY_SPACE}/Household`;
    // New bidirectional fields
    if (f === 'First Name') return `${FIBERY_SPACE}/First Name`;
    if (f === 'Last Name') return `${FIBERY_SPACE}/Last Name`;
    if (f === 'Status') return `${FIBERY_SPACE}/Status`;
    if (f === 'Birthdate') return `${FIBERY_SPACE}/Birthdate`;
    if (f === 'Child') return `${FIBERY_SPACE}/Child`;
    if (f === 'Given Name') return `${FIBERY_SPACE}/Given Name`;
    if (f === 'Grade') return `${FIBERY_SPACE}/Grade`;
    if (f === 'Middle Name') return `${FIBERY_SPACE}/Middle Name`;
    if (f === 'Nickname') return `${FIBERY_SPACE}/Nickname`;
    if (f === 'Inactivated At') return `${FIBERY_SPACE}/Inactivated At`;
    if (f === 'Membership') return `${FIBERY_SPACE}/Membership`;
    if (f === 'Directory Status') return `${FIBERY_SPACE}/Directory Status`;
    // Default fallback for any other fields
    return `${FIBERY_SPACE}/${f}`;
  },
  Household: (f) => {
    // Handle shared fields at space level  
    if (f === 'Name') return `${FIBERY_SPACE}/Name`;
    // Handle database-specific fields
    if (f === 'Household ID') return `${FIBERY_SPACE}/Household ID`;
    if (f === 'Members') return `${FIBERY_SPACE}/Members`;
    // Default fallback for any other fields
    return `${FIBERY_SPACE}/${f}`;
  },
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
        query: {
          'q/from': `${FIBERY_SPACE}/People`,
          'q/select': ['fibery/id'],
          'q/limit': 1
        }
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
  try {
    const queryArgs = {
      query: {
        'q/from': `${FIBERY_SPACE}/People`,
        'q/select': [
          'fibery/id', 
          F.People('Name'), 
          F.People('Person ID'), 
          F.People('First Name'),
          F.People('Last Name'),
          F.People('Status'),
          F.People('Birthdate'),
          F.People('Child'),
          F.People('Given Name'),
          F.People('Grade'),
          F.People('Middle Name'),
          F.People('Nickname'),
          F.People('Inactivated At'),
          F.People('Membership'),
          F.People('Directory Status'),
          {
            [F.People('Household')]: [
              'fibery/id',
              F.Household('Household ID'),
              F.Household('Name')
            ]
          },
          'fibery/modification-date'
        ],
        'q/limit': 1000
      }
    };

    // Add where clause with proper parameter formatting if timestamp provided
    if (tsISO) {
      queryArgs.query['q/where'] = ['>', ['fibery/modification-date'], '$timestamp'];
      queryArgs.params = { '$timestamp': tsISO };
    }

    const body = [{
      command: 'fibery.entity/query',
      args: queryArgs
    }];
    
    const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(body) });
    const result = await res.json();
    console.log('Fibery people query response:', JSON.stringify(result, null, 2));
    
    const [out] = result;
    const data = out?.result || [];
    
    if (!Array.isArray(data)) {
      console.warn('Fibery people query did not return an array:', data);
      return [];
    }
    
    return data;
  } catch (error) {
    console.error('Error querying Fibery people:', error);
    return [];
  }
}

export async function fiberyQueryHouseholdsSince(tsISO) {
  try {
    const queryArgs = {
      query: {
        'q/from': `${FIBERY_SPACE}/Household`,
        'q/select': ['fibery/id', F.Household('Name'), F.Household('Household ID'), 'fibery/modification-date'],
        'q/limit': 1000
      }
    };

    // Add where clause with proper parameter formatting if timestamp provided
    if (tsISO) {
      queryArgs.query['q/where'] = ['>', ['fibery/modification-date'], '$timestamp'];
      queryArgs.params = { '$timestamp': tsISO };
    }

    const body = [{
      command: 'fibery.entity/query',
      args: queryArgs
    }];
    
    const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(body) });
    const result = await res.json();
    console.log('Fibery households query response:', JSON.stringify(result, null, 2));
    
    const [out] = result;
    const data = out?.result || [];
    
    if (!Array.isArray(data)) {
      console.warn('Fibery households query did not return an array:', data);
      return [];
    }
    
    return data;
  } catch (error) {
    console.error('Error querying Fibery households:', error);
    return [];
  }
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
      query: {
        'q/from': `${FIBERY_SPACE}/Household`,
        'q/select': ['fibery/id', F.Household('Household ID')],
        'q/where': ['in', [F.Household('Household ID')], '$ids'],
        'q/limit': ids.length
      },
      params: { '$ids': ids }
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
  
  // First, get existing records with ALL their current data
  const find = [{
    command: 'fibery.entity/query',
    args: {
      query: {
        'q/from': `${FIBERY_SPACE}/People`,
        'q/select': [
          'fibery/id', 
          F.People('Person ID'),
          F.People('Name'),
          F.People('First Name'),
          F.People('Last Name'), 
          F.People('Status'),
          F.People('Birthdate'),
          F.People('Child'),
          F.People('Given Name'),
          F.People('Grade'),
          F.People('Middle Name'),
          F.People('Nickname'),
          F.People('Inactivated At'),
          F.People('Membership'),
          F.People('Directory Status'),
          {
            [F.People('Household')]: [
              'fibery/id',
              F.Household('Household ID'),
              F.Household('Name')
            ]
          }
        ],
        'q/where': ['in', [F.People('Person ID')], '$ids'],
        'q/limit': ids.length
      },
      params: { '$ids': ids }
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
  
  const index = new Map(resultData.map(r => [r[F.People('Person ID')], r]));

  console.log(`🔍 Found ${index.size} existing Fibery records to update`);
  console.log(`🔍 Processing ${items.length} PCO records`);
  
  // Define newly-mapped fields that should always be updated on existing records
  const newlyMappedFields = [
    F.People('First Name'),
    F.People('Last Name'), 
    F.People('Status'),
    F.People('Birthdate'),
    F.People('Child'),
    F.People('Given Name'),
    F.People('Grade'),
    F.People('Middle Name'),
    F.People('Nickname'),
    F.People('Inactivated At'),
    F.People('Membership'),
    F.People('Directory Status')
  ];
  
  let updateCount = 0;
  let createCount = 0;
  let skipCount = 0;

  const cmds = [];
  let loggedSample = false; // Only log detailed comparison for first few records
  
  for (const p of items) {
    const existing = index.get(p.personId);
    const rel = p.householdId && householdIndexById?.get(p.householdId)
      ? { 'fibery/id': householdIndexById.get(p.householdId) }
      : null;
    
    const pcoEntityData = {
      [F.People('Name')]: p.name,
      [F.People('Person ID')]: p.personId,
      [F.People('First Name')]: p.firstName,
      [F.People('Last Name')]: p.lastName,
      [F.People('Status')]: p.status,
      [F.People('Birthdate')]: p.birthdate,
      [F.People('Child')]: p.child,
      [F.People('Given Name')]: p.givenName,
      [F.People('Grade')]: p.grade,
      [F.People('Middle Name')]: p.middleName,
      [F.People('Nickname')]: p.nickname,
      [F.People('Inactivated At')]: p.inactivatedAt,
      [F.People('Membership')]: p.membership,
      [F.People('Directory Status')]: p.directoryStatus,
      [F.People('Household')]: rel
    };
    
    if (existing) {
      // For updates: Create differential patch with only changed/new fields
      const fieldsToUpdate = {};
      
      // Check each PCO field for changes or if it's newly mapped
      for (const [fieldName, newValue] of Object.entries(pcoEntityData)) {
        const currentValue = existing[fieldName];
        const isNewlyMapped = newlyMappedFields.includes(fieldName);
        const hasChanged = currentValue !== newValue;
        
        // Log detailed comparison for first few records to debug
        if (!loggedSample && existing && (hasChanged || isNewlyMapped)) {
          console.log(`🔍 Field comparison for ${p.name} (${p.personId}):`);
          console.log(`  Field: ${fieldName}`);
          console.log(`  Current (Fibery): ${JSON.stringify(currentValue)} (type: ${typeof currentValue})`);
          console.log(`  New (PCO): ${JSON.stringify(newValue)} (type: ${typeof newValue})`);
          console.log(`  Has changed: ${hasChanged}, Is newly mapped: ${isNewlyMapped}`);
        }
        
        // Update if: value changed OR it's a newly mapped field OR current is null/undefined
        if (hasChanged || isNewlyMapped || currentValue === null || currentValue === undefined) {
          fieldsToUpdate[fieldName] = newValue;
        }
      }
      
      if (!loggedSample && existing && Object.keys(fieldsToUpdate).length > 1) {
        loggedSample = true; // Only log once
      }
      
      // Always ensure Person ID is maintained
      fieldsToUpdate[F.People('Person ID')] = p.personId;
      
      // Remove null values from patch (except Person ID) to avoid Fibery update issues
      Object.keys(fieldsToUpdate).forEach(key => {
        if (fieldsToUpdate[key] === null && key !== F.People('Person ID')) {
          delete fieldsToUpdate[key];
        }
      });
      
      // Only create update command if there are actual changes
      if (Object.keys(fieldsToUpdate).length > 1) { // > 1 because Person ID is always included
        cmds.push({
          command: 'fibery.entity/update',
          args: { 
            type: `${FIBERY_SPACE}/People`,
            entity: { 
              'fibery/id': existing['fibery/id'],
              ...fieldsToUpdate  // ✅ Include fields directly in entity, not separate patch
            }
          }
        });
        updateCount++;
      } else {
        skipCount++;
      }
    } else {
      // For creates: use complete PCO data
      cmds.push({
        command: 'fibery.entity/create',
        args: { 
          type: `${FIBERY_SPACE}/People`, 
          entity: { 
            ...pcoEntityData,
            [F.People('Person ID')]: p.personId
          }
        }
      });
      createCount++;
    }
  }
  
  console.log(`🔍 Will execute ${updateCount} updates, ${createCount} creates, ${skipCount} skipped (no changes)`);
  
  // Log a sample update command to see what we're sending
  const sampleUpdate = cmds.find(cmd => cmd.command === 'fibery.entity/update');
  if (sampleUpdate) {
    console.log('🔍 Sample update patch:', JSON.stringify(sampleUpdate.args.patch, null, 2));
    console.log('🔍 Full update command:', JSON.stringify(sampleUpdate, null, 2));
  }
  
  // Log the complete payload being sent to Fibery
  console.log('🔍 Complete Fibery update payload:', JSON.stringify(cmds.slice(0, 3), null, 2)); // Log first 3 commands
  
  const res = await http(FIBERY_API, { method: 'POST', headers: hdr(), body: JSON.stringify(cmds) });
  return await res.json();
}

// PCO Update Functions for Bidirectional Sync (Fibery as Source of Truth)

export async function pcoUpdatePerson(personId, updates) {
  const PCO_API = `https://api.planningcenteronline.com/people/v2/people/${personId}`;
  
  const body = {
    data: {
      type: 'Person',
      id: personId,
      attributes: updates
    }
  };
  
  console.log(`🔄 Updating PCO person ${personId} with:`, JSON.stringify(updates, null, 2));
  
  const res = await http(PCO_API, {
    method: 'PATCH',
    headers: pcoHeaders(),
    body: JSON.stringify(body)
  });
  
  if (!res.ok) {
    const error = await res.text();
    console.error(`❌ PCO update failed for person ${personId}:`, error);
    throw new Error(`PCO update failed: ${error}`);
  }
  
  return await res.json();
}

export async function fiberyToPcoSync(fiberyPeople) {
  if (!fiberyPeople?.length) {
    console.log('No Fibery people to sync to PCO');
    return [];
  }
  
  console.log(`🔄 Syncing ${fiberyPeople.length} Fibery changes to PCO`);
  
  const results = [];
  
  for (const fiberyPerson of fiberyPeople) {
    try {
      const personId = fiberyPerson[F.People('Person ID')];
      if (!personId) {
        console.warn('Skipping Fibery person without Person ID:', fiberyPerson);
        continue;
      }
      
      // Map Fibery data to PCO format
      const mapped = DataConverter.mapFiberyPersonToPco(fiberyPerson, FIBERY_SPACE);
      
      // Build PCO update payload
      const pcoUpdates = {
        first_name: mapped.firstName,
        last_name: mapped.lastName,
        status: mapped.status,
        birthdate: DataConverter.fiberyToPcoDate(mapped.birthdate),
        child: mapped.child,
        given_name: mapped.givenName,
        grade: mapped.grade,
        middle_name: mapped.middleName,
        nickname: mapped.nickname,
        membership: mapped.membership,
        directory_status: mapped.directoryStatus
        // Note: inactivated_at is typically read-only in PCO
      };
      
      // Remove null values
      Object.keys(pcoUpdates).forEach(key => {
        if (pcoUpdates[key] === null || pcoUpdates[key] === undefined) {
          delete pcoUpdates[key];
        }
      });
      
      // Only update if there are actual changes
      if (Object.keys(pcoUpdates).length > 0) {
        const result = await pcoUpdatePerson(personId, pcoUpdates);
        results.push(result);
        console.log(`✅ Updated PCO person ${personId}`);
      } else {
        console.log(`⏭️ No changes needed for PCO person ${personId}`);
      }
      
    } catch (error) {
      console.error(`❌ Failed to sync Fibery person to PCO:`, error);
      results.push({ error: error.message, person: fiberyPerson });
    }
  }
  
  return results;
}
