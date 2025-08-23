import { getCursor, setCursor } from './state.js';
import { pcoPeopleSince, pcoHouseholdsSince, pcoUpsertHousehold, pcoUpsertPerson, pcoTestConnection } from './pco.js';
import { fiberyQueryPeopleSince, fiberyQueryHouseholdsSince, fiberyUpsertHouseholds, fiberyUpsertPeople, fiberyTestConnection } from './fibery.js';

const MAX_PER_RUN = 500; // guardrail

export const handler = async () => {
  const started = Date.now();
  const nowISO = new Date().toISOString();
  
  console.log('Starting PCO ↔ Fibery sync...');
  
  // Test connections first
  try {
    await Promise.all([
      fiberyTestConnection(),
      pcoTestConnection()
    ]);
    console.log('✅ Both API connections successful');
  } catch (error) {
    console.error('❌ API connection test failed, aborting sync');
    throw error;
  }

  const [pcoLast, fiberyLast] = await Promise.all([
    getCursor('pcoLastSync'),
    getCursor('fiberyLastSync'),
  ]);

  console.log(`Previous cursors - PCO: ${pcoLast || 'NONE (first run)'}, Fibery: ${fiberyLast || 'NONE (first run)'}`);

  // === PCO -> Fibery ===
  // For first run, pass null to get ALL records (no timestamp filter)
  const [pcoPeep, pcoHh] = await Promise.all([
    pcoPeopleSince(pcoLast), // null on first run = full sync
    pcoHouseholdsSince(pcoLast), // null on first run = full sync
  ]);

  // Upsert Households first
  console.log(`Processing ${pcoHh.length} PCO households`);
  const hhMapped = pcoHh.slice(0, MAX_PER_RUN).map(h => ({
    householdId: h.id,
    name: h.attributes?.name || h.attributes?.label || 'Household'
  }));
  console.log('Mapped households:', JSON.stringify(hhMapped.slice(0, 3), null, 2));
  
  console.log('Calling fiberyUpsertHouseholds...');
  const hhUpserts = await fiberyUpsertHouseholds(hhMapped);

  // Build index HouseholdID -> fibery/id from Fibery response
  const hhIndex = new Map();
  for (const r of hhUpserts) {
    const id = r?.result?.[`${process.env.FIBERY_SPACE}/Household/Household ID`];
    const fid = r?.result?.['fibery/id'];
    if (id && fid) hhIndex.set(String(id), fid);
  }

  const peepMapped = pcoPeep.slice(0, MAX_PER_RUN).map(p => ({
    personId: p.id,
    name: [p.attributes?.first_name, p.attributes?.last_name].filter(Boolean).join(' ') || p.attributes?.name,
    householdId: p.relationships?.households?.data?.[0]?.id || null,
  }));
  await fiberyUpsertPeople(peepMapped, { householdIndexById: hhIndex });

  // === Fibery -> PCO ===
  console.log('Querying Fibery for changes...');
  // For first run, pass null to get ALL records (no timestamp filter)
  const [fibPeople, fibHh] = await Promise.all([
    fiberyQueryPeopleSince(fiberyLast), // null on first run = full sync
    fiberyQueryHouseholdsSince(fiberyLast), // null on first run = full sync
  ]);

  console.log('Fibery query results:', {
    peopleType: typeof fibPeople,
    peopleIsArray: Array.isArray(fibPeople),
    peopleLength: Array.isArray(fibPeople) ? fibPeople.length : 'N/A',
    householdsType: typeof fibHh,
    householdsIsArray: Array.isArray(fibHh),
    householdsLength: Array.isArray(fibHh) ? fibHh.length : 'N/A'
  });

  // Ensure we have arrays
  const safeFibHh = Array.isArray(fibHh) ? fibHh : [];
  const safeFibPeople = Array.isArray(fibPeople) ? fibPeople : [];

  // Skip Fibery → PCO sync for now to avoid validation errors
  // This sync is primarily PCO → Fibery (church data into Fibery)
  console.log('Skipping Fibery → PCO sync (primarily one-way: PCO → Fibery)');
  
  /*
  for (const h of safeFibHh.slice(0, MAX_PER_RUN)) {
    await pcoUpsertHousehold({
      householdId: h[`${process.env.FIBERY_SPACE}/Household/Household ID`],
      name: h[`${process.env.FIBERY_SPACE}/Household/Name`]
    });
  }

  for (const p of safeFibPeople.slice(0, MAX_PER_RUN)) {
    const rel = p[`${process.env.FIBERY_SPACE}/People/Household`];
    await pcoUpsertPerson({
      personId: p[`${process.env.FIBERY_SPACE}/People/Person ID`],
      name: p[`${process.env.FIBERY_SPACE}/People/Name`],
      householdId: rel?.[`${process.env.FIBERY_SPACE}/Household/Household ID`] || null,
    });
  }
  */

  // Update cursors only if we got here without throwing
  await Promise.all([
    setCursor('pcoLastSync', nowISO),
    setCursor('fiberyLastSync', nowISO),
  ]);

  console.log(JSON.stringify({
    pcoPulled: { people: pcoPeep.length, households: pcoHh.length },
    fiberyPulled: { people: safeFibPeople.length, households: safeFibHh.length },
    ms: Date.now() - started,
  }));

  return { ok: true };
};
