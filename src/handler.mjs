import { getCursor, setCursor } from './state.js';
import { pcoPeopleSince, pcoHouseholdsSince, pcoUpsertHousehold, pcoUpsertPerson } from './pco.js';
import { fiberyQueryPeopleSince, fiberyQueryHouseholdsSince, fiberyUpsertHouseholds, fiberyUpsertPeople } from './fibery.js';

const MAX_PER_RUN = 500; // guardrail

export const handler = async () => {
  const started = Date.now();
  const nowISO = new Date().toISOString();

  const [pcoLast, fiberyLast] = await Promise.all([
    getCursor('pcoLastSync'),
    getCursor('fiberyLastSync'),
  ]);

  // === PCO -> Fibery ===
  const [pcoPeep, pcoHh] = await Promise.all([
    pcoPeopleSince(pcoLast || new Date(0).toISOString()),
    pcoHouseholdsSince(pcoLast || new Date(0).toISOString()),
  ]);

  // Upsert Households first
  const hhMapped = pcoHh.slice(0, MAX_PER_RUN).map(h => ({
    householdId: h.id,
    name: h.attributes?.name || h.attributes?.label || 'Household'
  }));
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
  const [fibPeople, fibHh] = await Promise.all([
    fiberyQueryPeopleSince(fiberyLast),
    fiberyQueryHouseholdsSince(fiberyLast),
  ]);

  for (const h of fibHh.slice(0, MAX_PER_RUN)) {
    await pcoUpsertHousehold({
      householdId: h[`${process.env.FIBERY_SPACE}/Household/Household ID`],
      name: h[`${process.env.FIBERY_SPACE}/Household/Name`]
    });
  }

  for (const p of fibPeople.slice(0, MAX_PER_RUN)) {
    const rel = p[`${process.env.FIBERY_SPACE}/People/Household`];
    await pcoUpsertPerson({
      personId: p[`${process.env.FIBERY_SPACE}/People/Person ID`],
      name: p[`${process.env.FIBERY_SPACE}/People/Name`],
      householdId: rel?.[`${process.env.FIBERY_SPACE}/Household/Household ID`] || null,
    });
  }

  // Update cursors only if we got here without throwing
  await Promise.all([
    setCursor('pcoLastSync', nowISO),
    setCursor('fiberyLastSync', nowISO),
  ]);

  console.log(JSON.stringify({
    pcoPulled: { people: pcoPeep.length, households: pcoHh.length },
    fiberyPulled: { people: fibPeople.length, households: fibHh.length },
    ms: Date.now() - started,
  }));

  return { ok: true };
};
