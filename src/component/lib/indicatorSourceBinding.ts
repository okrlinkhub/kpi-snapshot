import type { Doc, Id } from '../_generated/dataModel.js'
import type { MutationCtx, QueryCtx } from '../_generated/server.js'
import { listDirectFormulaDependencies } from '../../shared/derivedFormula.js'

type Ctx = QueryCtx | MutationCtx
type DataSourceDoc = Doc<'dataSources'>

export type IndicatorSourceBindingStatus = 'single_source' | 'multi_source' | 'missing_source'

export type IndicatorSourceBinding = {
  status: IndicatorSourceBindingStatus
  sourceKeys: string[]
  sourceLabels: string[]
  lockedSourceKey: string | null
  lockedDataSourceId: Id<'dataSources'> | null
  isEligible: boolean
  reason: 'multi_source' | 'missing_source' | null
}

type BaseReference =
  | { indicatorId: Id<'indicators'>, indicatorSlug?: string }
  | { indicatorSlug: string, indicatorId?: Id<'indicators'> }

type DerivedReference =
  | { indicatorId: Id<'derivedIndicators'>, indicatorSlug?: string }
  | { indicatorSlug: string, indicatorId?: Id<'derivedIndicators'> }

function buildLatestRowsBySlug<T extends { slug: string, version: number }> (rows: T[]) {
  const bySlug = new Map<string, T>()

  for (const row of rows) {
    const current = bySlug.get(row.slug)
    if (!current || row.version > current.version) {
      bySlug.set(row.slug, row)
    }
  }

  return bySlug
}

function buildSourceBinding (args: {
  sourceKeys: string[]
  dataSourcesByKey: Map<string, DataSourceDoc>
}): IndicatorSourceBinding {
  const sourceKeys = [...new Set(args.sourceKeys)].sort((left, right) => left.localeCompare(right))
  const sourceLabels = sourceKeys.map((sourceKey) => {
    const dataSource = args.dataSourcesByKey.get(sourceKey)
    return dataSource?.label || sourceKey
  })

  if (sourceKeys.length === 0) {
    return {
      status: 'missing_source',
      sourceKeys: [],
      sourceLabels: [],
      lockedSourceKey: null,
      lockedDataSourceId: null,
      isEligible: false,
      reason: 'missing_source',
    }
  }

  if (sourceKeys.length > 1) {
    return {
      status: 'multi_source',
      sourceKeys,
      sourceLabels,
      lockedSourceKey: null,
      lockedDataSourceId: null,
      isEligible: false,
      reason: 'multi_source',
    }
  }

  const lockedSourceKey = sourceKeys[0]
  const lockedDataSource = args.dataSourcesByKey.get(lockedSourceKey) ?? null

  return {
    status: 'single_source',
    sourceKeys,
    sourceLabels,
    lockedSourceKey,
    lockedDataSourceId: lockedDataSource?._id ?? null,
    isEligible: true,
    reason: null,
  }
}

export async function createProfileIndicatorSourceResolver (
  ctx: Ctx,
  profileId: Id<'snapshotProfiles'>
) {
  const [allDefinitions, allDataSources, allIndicators, allDerivedIndicators] = await Promise.all([
    ctx.db
      .query('calculationDefinitions')
      .withIndex('by_profile', (q) => q.eq('profileId', profileId))
      .collect(),
    ctx.db.query('dataSources').collect(),
    ctx.db
      .query('indicators')
      .withIndex('by_profile', (q) => q.eq('profileId', profileId))
      .collect(),
    ctx.db
      .query('derivedIndicators')
      .withIndex('by_profile', (q) => q.eq('profileId', profileId))
      .collect(),
  ])

  const dataSources = allDataSources.filter((row) => !row.archivedAt)
  const dataSourcesById = new Map(dataSources.map((row) => [String(row._id), row] as const))
  const dataSourcesByKey = new Map(dataSources.map((row) => [row.sourceKey, row] as const))
  const indicatorsBySlug = buildLatestRowsBySlug(allIndicators)
  const derivedIndicatorsBySlug = buildLatestRowsBySlug(allDerivedIndicators)
  const indicatorsById = new Map(
    [...indicatorsBySlug.values()].map((indicator) => [String(indicator._id), indicator] as const)
  )
  const derivedIndicatorsById = new Map(
    [...derivedIndicatorsBySlug.values()].map((indicator) => [String(indicator._id), indicator] as const)
  )
  const definitionsByIndicatorId = new Map<string, Array<Doc<'calculationDefinitions'>>>()

  for (const definition of allDefinitions) {
    const key = String(definition.indicatorId)
    const current = definitionsByIndicatorId.get(key) ?? []
    current.push(definition)
    definitionsByIndicatorId.set(key, current)
  }

  const baseBindingCache = new Map<string, IndicatorSourceBinding>()
  const derivedBindingCache = new Map<string, IndicatorSourceBinding>()

  function resolveBaseIndicatorByReference (reference: BaseReference) {
    if (reference.indicatorId) {
      return indicatorsById.get(String(reference.indicatorId)) ?? null
    }

    return reference.indicatorSlug
      ? indicatorsBySlug.get(reference.indicatorSlug) ?? null
      : null
  }

  function resolveDerivedIndicatorByReference (reference: DerivedReference) {
    if (reference.indicatorId) {
      return derivedIndicatorsById.get(String(reference.indicatorId)) ?? null
    }

    return reference.indicatorSlug
      ? derivedIndicatorsBySlug.get(reference.indicatorSlug) ?? null
      : null
  }

  function resolveBaseBinding (reference: BaseReference): IndicatorSourceBinding {
    const indicator = resolveBaseIndicatorByReference(reference)
    if (!indicator) {
      return buildSourceBinding({
        sourceKeys: [],
        dataSourcesByKey,
      })
    }

    const cacheKey = String(indicator._id)
    const cached = baseBindingCache.get(cacheKey)
    if (cached) {
      return cached
    }

    const sourceKeys = (definitionsByIndicatorId.get(cacheKey) ?? [])
      .filter((definition) => definition.enabled)
      .map((definition) => dataSourcesById.get(String(definition.dataSourceId))?.sourceKey ?? null)
      .filter((sourceKey): sourceKey is string => Boolean(sourceKey))

    const binding = buildSourceBinding({
      sourceKeys,
      dataSourcesByKey,
    })

    baseBindingCache.set(cacheKey, binding)
    return binding
  }

  function resolveDerivedBinding (
    reference: DerivedReference,
    activeDerivedSlugs: Set<string> = new Set()
  ): IndicatorSourceBinding {
    const indicator = resolveDerivedIndicatorByReference(reference)
    if (!indicator) {
      return buildSourceBinding({
        sourceKeys: [],
        dataSourcesByKey,
      })
    }

    const cacheKey = String(indicator._id)
    const cached = derivedBindingCache.get(cacheKey)
    if (cached) {
      return cached
    }

    if (indicator.lockedSourceKey && !activeDerivedSlugs.has(indicator.slug)) {
      const lockedBinding = buildSourceBinding({
        sourceKeys: [indicator.lockedSourceKey],
        dataSourcesByKey,
      })
      if (lockedBinding.isEligible) {
        derivedBindingCache.set(cacheKey, lockedBinding)
      }
    }

    if (activeDerivedSlugs.has(indicator.slug)) {
      return buildSourceBinding({
        sourceKeys: [],
        dataSourcesByKey,
      })
    }

    activeDerivedSlugs.add(indicator.slug)

    const dependencyBindings = listDirectFormulaDependencies(indicator.formula).map((dependency) => (
      dependency.indicatorKind === 'base'
        ? resolveBaseBinding({ indicatorSlug: dependency.indicatorSlug })
        : resolveDerivedBinding({ indicatorSlug: dependency.indicatorSlug }, activeDerivedSlugs)
    ))

    activeDerivedSlugs.delete(indicator.slug)

    const sourceKeys = dependencyBindings.flatMap((binding) => binding.sourceKeys)
    const binding = buildSourceBinding({
      sourceKeys,
      dataSourcesByKey,
    })

    derivedBindingCache.set(cacheKey, binding)
    return binding
  }

  return {
    dataSourcesByKey,
    resolveBaseBinding,
    resolveDerivedBinding,
    resolveMemberBinding (member: { indicatorKind: 'base' | 'derived', indicatorSlug: string }) {
      return member.indicatorKind === 'base'
        ? resolveBaseBinding({ indicatorSlug: member.indicatorSlug })
        : resolveDerivedBinding({ indicatorSlug: member.indicatorSlug })
    },
  }
}
