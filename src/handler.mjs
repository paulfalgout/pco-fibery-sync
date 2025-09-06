import { getCursor, setCursor } from './state.js';
import { pcoPeopleSince, pcoHouseholdsSince, pcoUpsertHousehold, pcoUpsertPerson, pcoTestConnection } from './pco.js';
import { fiberyQueryPeopleSince, fiberyQueryHouseholdsSince, fiberyUpsertHouseholds, fiberyUpsertPeople, fiberyTestConnection, DataConverter, fiberyToPcoSync } from './fibery.js';

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

  // === FORCE FULL RE-SYNC FOR NEW FIELDS ===
  // Temporarily override cursors to force full sync of all records
  // This will update all existing records with the new bidirectional fields
  // Comment out these lines after the full re-sync is complete
  const forceFullSync = false; // ✅ Set to false after successful sync
  const effectivePcoLast = forceFullSync ? null : pcoLast;
  const effectiveFiberyLast = forceFullSync ? null : fiberyLast;
  
  if (forceFullSync) {
    console.log('🔄 FORCING FULL RE-SYNC to update existing records with new fields...');
    console.log('💡 After this sync completes successfully, set forceFullSync = false to resume normal incremental sync');
  }

  // === PCO -> Fibery ===
  // For first run, pass null to get ALL records (no timestamp filter)
  const [pcoPeep, pcoHh] = await Promise.all([
    pcoPeopleSince(effectivePcoLast), // Use effective cursor (null for full sync)
    pcoHouseholdsSince(effectivePcoLast), // Use effective cursor (null for full sync)
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

  const peepMapped = pcoPeep.slice(0, MAX_PER_RUN).map(p => DataConverter.mapPcoPersonToFibery(p));
  
  console.log(`🔍 Processing ${peepMapped.length} people from PCO. First few Person IDs:`, 
    peepMapped.slice(0, 5).map(p => `${p.personId}:${p.name}`));
  
  console.log('Calling fiberyUpsertPeople...');
  const peopleUpsertResult = await fiberyUpsertPeople(peepMapped, { householdIndexById: hhIndex });
  console.log('fiberyUpsertPeople response:', JSON.stringify(peopleUpsertResult, null, 2));

  // === Fibery -> PCO ===
  console.log('Querying Fibery for changes...');
  // For first run, pass null to get ALL records (no timestamp filter)
  const [fibPeople, fibHh] = await Promise.all([
    fiberyQueryPeopleSince(effectiveFiberyLast), // Use effective cursor (null for full sync)
    fiberyQueryHouseholdsSince(effectiveFiberyLast), // Use effective cursor (null for full sync)
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

  // === Fibery → PCO Sync (Fibery as Source of Truth) ===
  if (safeFibPeople.length > 0) {
    console.log('🔄 Starting Fibery → PCO sync (Fibery is source of truth)');
    try {
      const fiberyToPcoResults = await fiberyToPcoSync(safeFibPeople.slice(0, MAX_PER_RUN));
      console.log(`✅ Fibery → PCO sync completed: ${fiberyToPcoResults.length} updates processed`);
    } catch (error) {
      console.error('❌ Fibery → PCO sync failed:', error);
    }
  } else {
    console.log('No Fibery changes to sync to PCO');
  }

  // Note: Household sync to PCO is typically not needed as PCO manages households
  // but could be added here if required

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
