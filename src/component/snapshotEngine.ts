import { v } from 'convex/values'
import { internal } from './_generated/api.js'
import { mutation, query } from './_generated/server.js'
import type { Doc, Id } from './_generated/dataModel.js'

const operationValidator = v.union(
  v.literal('sum'),
  v.literal('count'),
  v.literal('avg'),
  v.literal('min'),
  v.literal('max'),
  v.literal('distinct_count')
)

const sourceKindValidator = v.union(
  v.literal('component_table'),
  v.literal('external_reader'),
  v.literal('materialized_rows')
)

const schedulePresetValidator = v.optional(v.union(
  v.literal('manual'),
  v.literal('daily'),
  v.literal('weekly_monday'),
  v.literal('monthly_first_day')
))

const derivedFormulaKindValidator = v.union(
  v.literal('ratio'),
  v.literal('difference'),
  v.literal('sum')
)

const derivedOperandRoleValidator = v.union(
  v.literal('numerator'),
  v.literal('denominator'),
  v.literal('term')
)

const fieldCatalogItemValidator = v.object({
  key: v.string(),
  label: v.string(),
  valueType: v.string(),
  filterable: v.optional(v.boolean()),
})

const materializedRowInputValidator = v.object({
  rowKey: v.string(),
  occurredAt: v.number(),
  rowData: v.any(),
  sourceRecordId: v.optional(v.string()),
  sourceEntityType: v.optional(v.string()),
})

const sourceRowInputValidator = v.object({
  occurredAt: v.number(),
  rowData: v.any(),
})

const sourcePayloadValidator = v.object({
  sourceKey: v.string(),
  rows: v.array(sourceRowInputValidator),
})

const exportFieldFilterValidator = v.object({
  fieldKey: v.string(),
  values: v.array(v.string()),
})

const exportFiltersValidator = v.object({
  startDate: v.optional(v.number()),
  endDate: v.optional(v.number()),
  dateFieldKey: v.optional(v.string()),
  fieldFilters: v.optional(v.array(exportFieldFilterValidator)),
})

const derivedFormulaValidator = v.object({
  kind: derivedFormulaKindValidator,
  operands: v.array(
    v.object({
      indicatorSlug: v.string(),
      role: v.optional(derivedOperandRoleValidator),
      weight: v.optional(v.number()),
    })
  ),
})

type Operation = 'sum' | 'count' | 'avg' | 'min' | 'max' | 'distinct_count'
type DerivedFormulaKind = 'ratio' | 'difference' | 'sum'

type FilterRule = {
  field: string
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'in'
  value: unknown
}

type ExportFilters = {
  startDate?: number
  endDate?: number
  dateFieldKey?: string
  fieldFilters?: Array<{
    fieldKey: string
    values: string[]
  }>
}

type SourceRowInput = {
  occurredAt: number
  rowData: Record<string, unknown>
}

type MaterializedRowInput = {
  rowKey: string
  occurredAt: number
  rowData: Record<string, unknown>
  sourceRecordId?: string
  sourceEntityType?: string
}

type FieldCatalogItem = {
  key: string
  label: string
  valueType: string
  filterable?: boolean
}

type CalculationResult = {
  inputCount: number
  rawResult: number | null
  normalizedResult: number | null
  warningMessage?: string
  filteredRows: Array<SourceRowInput>
}

type DerivedComputationInput = {
  snapshotValueId: Id<'snapshotValues'>
  indicatorSlug: string
  value: number
  sourceExportIds: Array<Id<'analyticsExports'>>
}

type DataSourceDoc = Doc<'dataSources'>

function normalizeIndicatorLabelSnapshot (label: string) {
  return label.replace(/\s+/g, ' ').trim()
}

function getNestedValue (rowData: unknown, fieldPath?: string): unknown {
  if (!fieldPath) return rowData
  if (rowData == null || typeof rowData !== 'object') return undefined
  const chunks = fieldPath.split('.')
  let cursor: unknown = rowData
  for (const chunk of chunks) {
    if (cursor == null || typeof cursor !== 'object') return undefined
    cursor = (cursor as Record<string, unknown>)[chunk]
  }
  return cursor
}

function toFiniteNumber (value: unknown) {
  if (typeof value !== 'number' || Number.isNaN(value) || !Number.isFinite(value)) {
    return null
  }
  return value
}

function applyFilterRule (rowData: unknown, rule: FilterRule) {
  const left = getNestedValue(rowData, rule.field)
  const right = rule.value

  switch (rule.op) {
    case 'eq':
      return left === right
    case 'neq':
      return left !== right
    case 'gt':
      return typeof left === 'number' && typeof right === 'number' && left > right
    case 'gte':
      return typeof left === 'number' && typeof right === 'number' && left >= right
    case 'lt':
      return typeof left === 'number' && typeof right === 'number' && left < right
    case 'lte':
      return typeof left === 'number' && typeof right === 'number' && left <= right
    case 'in':
      return Array.isArray(right) ? right.includes(left) : false
    default:
      return false
  }
}

function matchesFilters (rowData: unknown, filters: unknown) {
  if (!Array.isArray(filters) || filters.length === 0) return true
  return (filters as Array<FilterRule>).every((rule) => applyFilterRule(rowData, rule))
}

function computeOperation (
  rows: Array<SourceRowInput>,
  operation: Operation,
  fieldPath?: string
): number | null {
  if (operation === 'count') return rows.length

  const values = rows
    .map((row) => toFiniteNumber(getNestedValue(row.rowData, fieldPath)))
    .filter((value): value is number => value != null)

  if (values.length === 0) {
    return null
  }

  switch (operation) {
    case 'sum':
      return values.reduce((total, value) => total + value, 0)
    case 'avg':
      return values.reduce((total, value) => total + value, 0) / values.length
    case 'min':
      return Math.min(...values)
    case 'max':
      return Math.max(...values)
    case 'distinct_count':
      return new Set(values.map((value) => String(value))).size
    default:
      return null
  }
}

function applyNormalization (value: number | null, normalization: unknown): number | null {
  if (value == null) {
    if (
      normalization &&
      typeof normalization === 'object' &&
      'coalesce' in normalization &&
      typeof (normalization as { coalesce?: unknown }).coalesce === 'number'
    ) {
      return (normalization as { coalesce: number }).coalesce
    }
    return null
  }

  let result = value
  if (normalization && typeof normalization === 'object') {
    const objectNormalization = normalization as {
      scale?: unknown
      round?: unknown
      clamp?: { min?: number, max?: number }
    }
    if (typeof objectNormalization.scale === 'number') {
      result *= objectNormalization.scale
    }
    if (typeof objectNormalization.round === 'number') {
      const digits = Math.max(0, Math.trunc(objectNormalization.round))
      result = Number(result.toFixed(digits))
    }
    if (objectNormalization.clamp && typeof objectNormalization.clamp === 'object') {
      if (typeof objectNormalization.clamp.min === 'number') {
        result = Math.max(objectNormalization.clamp.min, result)
      }
      if (typeof objectNormalization.clamp.max === 'number') {
        result = Math.min(objectNormalization.clamp.max, result)
      }
    }
  }

  return result
}

function buildRuleHash (definition: Doc<'calculationDefinitions'>) {
  return [
    definition._id,
    definition.ruleVersion,
    definition.operation,
    definition.fieldPath ?? '',
    JSON.stringify(definition.filters ?? null),
    JSON.stringify(definition.normalization ?? null),
  ].join('|')
}

function normalizeSourceRows (rows: Array<SourceRowInput>, snapshotAt: number): Array<SourceRowInput> {
  return rows
    .filter((row) => row.occurredAt <= snapshotAt)
    .map((row) => ({
      occurredAt: row.occurredAt,
      rowData:
        row.rowData && typeof row.rowData === 'object'
          ? row.rowData
          : {},
    }))
}

function normalizeMaterializedRows (
  rows: Array<MaterializedRowInput>,
  snapshotAt?: number
) {
  return rows
    .filter((row) => snapshotAt == null || row.occurredAt <= snapshotAt)
    .map((row) => ({
      rowKey: row.rowKey,
      occurredAt: row.occurredAt,
      sourceRecordId: row.sourceRecordId,
      sourceEntityType: row.sourceEntityType ?? 'record',
      rowData:
        row.rowData && typeof row.rowData === 'object'
          ? row.rowData
          : {},
    }))
}

function escapeCsvValue (value: unknown) {
  if (value == null) {
    return ''
  }
  if (typeof value === 'object') {
    return `"${JSON.stringify(value).replace(/"/g, '""')}"`
  }
  const raw = String(value).replace(/\r?\n/g, ' ')
  if (raw.includes('"') || raw.includes(',') || raw.includes(';')) {
    return `"${raw.replace(/"/g, '""')}"`
  }
  return raw
}

function buildCsv (
  rows: Array<{ occurredAt: number, rowData: Record<string, unknown> }>,
  selectedFieldKeys: string[]
) {
  const headers = selectedFieldKeys.length > 0
    ? ['occurredAt', ...selectedFieldKeys]
    : ['occurredAt', ...new Set(rows.flatMap((row) => Object.keys(row.rowData)))]

  const body = rows.map((row) => headers.map((header) => {
    if (header === 'occurredAt') {
      return escapeCsvValue(row.occurredAt)
    }
    return escapeCsvValue(row.rowData[header])
  }).join(';'))

  return `\uFEFF${headers.join(';')}\n${body.join('\n')}`
}

function runSingleDefinition (
  definition: Doc<'calculationDefinitions'>,
  rows: Array<SourceRowInput>
): CalculationResult {
  const filteredRows = rows.filter((row) => matchesFilters(row.rowData, definition.filters))
  const rawResult = computeOperation(
    filteredRows,
    definition.operation as Operation,
    definition.fieldPath
  )
  const normalizedResult = applyNormalization(rawResult, definition.normalization)

  return {
    inputCount: filteredRows.length,
    rawResult,
    normalizedResult,
    warningMessage:
      definition.groupBy && definition.groupBy.length > 0
        ? 'groupBy presente ma non ancora applicato in questa versione'
        : undefined,
    filteredRows,
  }
}

function buildConfigSnapshot (dataSource: DataSourceDoc) {
  return {
    adapterKey: dataSource.adapterKey,
    label: dataSource.label,
    sourceKey: dataSource.sourceKey,
    entityType: dataSource.entityType,
    selectedFieldKeys: dataSource.selectedFieldKeys,
    dateFieldKey: dataSource.dateFieldKey,
    rowKeyStrategy: dataSource.rowKeyStrategy,
    scopeDefinition: dataSource.scopeDefinition,
    fieldCatalog: dataSource.fieldCatalog,
    metadata: dataSource.metadata,
  }
}

function applyExportFilters (
  rows: Array<{
    occurredAt: number
    rowData: Record<string, unknown>
  }>,
  filters?: ExportFilters
) {
  return rows.filter((row) => {
    if (typeof filters?.startDate === 'number' && row.occurredAt < filters.startDate) {
      return false
    }
    if (typeof filters?.endDate === 'number' && row.occurredAt > filters.endDate) {
      return false
    }
    if (!filters?.fieldFilters || filters.fieldFilters.length === 0) {
      return true
    }
    return filters.fieldFilters.every((fieldFilter) => {
      if (fieldFilter.values.length === 0) {
        return true
      }
      const value = row.rowData[fieldFilter.fieldKey]
      return fieldFilter.values.includes(value == null ? '' : String(value))
    })
  })
}

function isDerivedSnapshotValueId (snapshotValueId: string) {
  return snapshotValueId.startsWith('derived:')
}

function parseDerivedSnapshotValueId (snapshotValueId: string) {
  return snapshotValueId.replace(/^derived:/, '') as Id<'derivedSnapshotValues'>
}

function validateDerivedFormula (formula: {
  kind: DerivedFormulaKind
  operands: Array<{
    indicatorSlug: string
    role?: 'numerator' | 'denominator' | 'term'
    weight?: number
  }>
}) {
  if (formula.operands.length === 0) {
    throw new Error('La formula derivata richiede almeno un operando')
  }
  if (formula.kind === 'ratio') {
    const numeratorCount = formula.operands.filter((operand) => operand.role === 'numerator').length
    const denominatorCount = formula.operands.filter((operand) => operand.role === 'denominator').length
    if (numeratorCount !== 1 || denominatorCount !== 1) {
      throw new Error('La formula ratio richiede un numeratore e un denominatore')
    }
  }
  if (formula.kind === 'difference' && formula.operands.length !== 2) {
    throw new Error('La formula difference richiede esattamente due operandi')
  }
}

function computeDerivedValue (
  formula: {
    kind: DerivedFormulaKind
    operands: Array<{
      indicatorSlug: string
      role?: 'numerator' | 'denominator' | 'term'
      weight?: number
    }>
  },
  valuesByIndicatorSlug: Map<string, DerivedComputationInput>
) {
  const resolvedInputs = formula.operands.map((operand) => ({
    operand,
    input: valuesByIndicatorSlug.get(operand.indicatorSlug),
  }))

  if (resolvedInputs.some((entry) => !entry.input)) {
    return null
  }

  if (formula.kind === 'ratio') {
    const numerator = resolvedInputs.find((entry) => entry.operand.role === 'numerator')?.input
    const denominator = resolvedInputs.find((entry) => entry.operand.role === 'denominator')?.input
    if (!numerator || !denominator || denominator.value === 0) {
      return null
    }
    return {
      value: numerator.value / denominator.value,
      inputs: [numerator, denominator],
    }
  }

  if (formula.kind === 'difference') {
    const left = resolvedInputs[0]?.input
    const right = resolvedInputs[1]?.input
    if (!left || !right) {
      return null
    }
    return {
      value: left.value - right.value,
      inputs: [left, right],
    }
  }

  const inputs = resolvedInputs.map((entry) => ({
    weight: entry.operand.weight ?? 1,
    input: entry.input!,
  }))

  return {
    value: inputs.reduce((total, entry) => total + (entry.input.value * entry.weight), 0),
    inputs: inputs.map((entry) => entry.input),
  }
}

async function getProfileBySlug (ctx: any, slug: string) {
  const profile = await ctx.db
    .query('snapshotProfiles')
    .withIndex('by_slug', (q: any) => q.eq('slug', slug))
    .unique()
  if (!profile) {
    throw new Error(`Snapshot profile '${slug}' non trovato`)
  }
  return profile
}

async function getDataSourceByKey (ctx: any, sourceKey: string) {
  return await ctx.db
    .query('dataSources')
    .withIndex('by_source_key', (q: any) => q.eq('sourceKey', sourceKey))
    .unique()
}

async function getIndicatorByProfileAndSlug (
  ctx: any,
  profileId: Id<'snapshotProfiles'>,
  slug: string
) {
  return await ctx.db
    .query('indicators')
    .withIndex('by_profile_and_slug', (q: any) =>
      q.eq('profileId', profileId).eq('slug', slug)
    )
    .unique()
}

async function getDerivedIndicatorByProfileAndSlug (
  ctx: any,
  profileId: Id<'snapshotProfiles'>,
  slug: string
) {
  return await ctx.db
    .query('derivedIndicators')
    .withIndex('by_profile_and_slug', (q: any) =>
      q.eq('profileId', profileId).eq('slug', slug)
    )
    .unique()
}

async function listMaterializedRowsForDataSource (
  ctx: any,
  dataSourceId: Id<'dataSources'>,
  snapshotAt?: number
) {
  const rows = await ctx.db
    .query('analyticsMaterializedRows')
    .withIndex('by_data_source', (q: any) => q.eq('dataSourceId', dataSourceId))
    .collect()

  return rows
    .filter((row: Doc<'analyticsMaterializedRows'>) => snapshotAt == null || row.occurredAt <= snapshotAt)
    .map((row: Doc<'analyticsMaterializedRows'>) => ({
      rowKey: row.rowKey,
      occurredAt: row.occurredAt,
      sourceRecordId: row.sourceRecordId,
      sourceEntityType: row.sourceEntityType,
      rowData: row.rowData as Record<string, unknown>,
    }))
}

async function createCompletedExport (
  ctx: any,
  args: {
    requestedBy?: string
    name?: string
    dataSource: DataSourceDoc
    rows: Array<{ occurredAt: number, rowData: Record<string, unknown> }>
    filters?: ExportFilters
    usageKind: 'manual' | 'kpi_snapshot_source'
    pinnedByAudit: boolean
    auditProfileSlug?: string
    auditSnapshotId?: Id<'snapshots'>
    auditSnapshotRunId?: Id<'snapshotRuns'>
    clonedFromExportId?: Id<'analyticsExports'>
    regeneratedFromExportId?: Id<'analyticsExports'>
  }
) {
  const now = Date.now()
  const csvContent = buildCsv(args.rows, args.dataSource.selectedFieldKeys)
  const storageId = await ctx.storage.store(
    new Blob([csvContent], { type: 'text/csv;charset=utf-8' })
  )
  const namePart = (args.name ?? args.dataSource.label ?? args.dataSource.sourceKey)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
  const fileName = `analytics-export-${namePart || args.dataSource.sourceKey}-${now}.csv`

  return await ctx.db.insert('analyticsExports', {
    requestedBy: args.requestedBy,
    name: args.name,
    status: 'completed',
    dataSourceId: args.dataSource._id,
    dataSourceKey: args.dataSource.sourceKey,
    configSnapshot: buildConfigSnapshot(args.dataSource),
    filters: args.filters,
    storageId,
    fileName,
    rowCount: args.rows.length,
    usageKind: args.usageKind,
    pinnedByAudit: args.pinnedByAudit,
    auditProfileSlug: args.auditProfileSlug,
    auditSnapshotId: args.auditSnapshotId,
    auditSnapshotRunId: args.auditSnapshotRunId,
    clonedFromExportId: args.clonedFromExportId,
    regeneratedFromExportId: args.regeneratedFromExportId,
    createdAt: now,
    startedAt: now,
    completedAt: now,
    expiresAt: args.pinnedByAudit ? undefined : now + (7 * 24 * 60 * 60 * 1000),
  })
}

async function resolveExportSummaries (
  ctx: any,
  exportIds: Array<Id<'analyticsExports'>>
) {
  const uniqueExportIds = [...new Set(exportIds)]
  const exportRows = await Promise.all(uniqueExportIds.map((exportId) => ctx.db.get(exportId)))
  const filteredRows = exportRows.filter(Boolean) as Array<Doc<'analyticsExports'>>
  const urls = await Promise.all(filteredRows.map(async (exportRow) => ({
    exportId: exportRow._id,
    downloadUrl: exportRow.storageId ? await ctx.storage.getUrl(exportRow.storageId) : null,
  })))
  const urlMap = new Map(urls.map((row) => [row.exportId, row.downloadUrl]))

  return filteredRows.map((exportRow) => ({
    exportId: String(exportRow._id),
    name: exportRow.name,
    fileName: exportRow.fileName,
    createdAt: exportRow.createdAt,
    downloadUrl: urlMap.get(exportRow._id) ?? null,
    usageKind: exportRow.usageKind,
  }))
}

export const createSnapshotProfile = mutation({
  args: {
    slug: v.string(),
    name: v.string(),
    description: v.optional(v.string()),
    isActive: v.optional(v.boolean()),
  },
  returns: v.id('snapshotProfiles'),
  handler: async (ctx, args) => {
    const now = Date.now()
    const existing = await ctx.db
      .query('snapshotProfiles')
      .withIndex('by_slug', (q) => q.eq('slug', args.slug))
      .unique()

    if (existing) {
      await ctx.db.patch(existing._id, {
        name: args.name,
        description: args.description,
        isActive: args.isActive ?? existing.isActive,
        version: existing.version + 1,
        updatedAt: now,
      })
      return existing._id
    }

    return await ctx.db.insert('snapshotProfiles', {
      slug: args.slug,
      name: args.name,
      description: args.description,
      isActive: args.isActive ?? true,
      version: 1,
      createdAt: now,
    })
  },
})

export const listSnapshotProfiles = query({
  args: {},
  returns: v.array(v.any()),
  handler: async (ctx) => {
    return await ctx.db.query('snapshotProfiles').collect()
  },
})

export const listDataSources = query({
  args: {
    includeDisabled: v.optional(v.boolean()),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const rows = await ctx.db.query('dataSources').collect()
    const activeRows = rows.filter((item: Doc<'dataSources'>) => !item.archivedAt)
    const filteredRows = args.includeDisabled
      ? activeRows
      : activeRows.filter((item) => item.enabled)
    return filteredRows.sort((left, right) => (right.updatedAt ?? right.createdAt) - (left.updatedAt ?? left.createdAt))
  },
})

export const upsertDataSource = mutation({
  args: {
    profileSlug: v.string(),
    sourceKey: v.string(),
    label: v.string(),
    adapterKey: v.optional(v.string()),
    sourceKind: sourceKindValidator,
    entityType: v.optional(v.string()),
    scopeDefinition: v.optional(v.any()),
    selectedFieldKeys: v.optional(v.array(v.string())),
    dateFieldKey: v.optional(v.string()),
    rowKeyStrategy: v.optional(v.string()),
    schedulePreset: schedulePresetValidator,
    fieldCatalog: v.optional(v.array(fieldCatalogItemValidator)),
    metadata: v.optional(v.any()),
    enabled: v.optional(v.boolean()),
  },
  returns: v.id('dataSources'),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const now = Date.now()
    const existing = await ctx.db
      .query('dataSources')
      .withIndex('by_profile_and_source_key', (q) =>
        q.eq('profileId', profile._id).eq('sourceKey', args.sourceKey)
      )
      .unique()

    if (existing) {
      await ctx.db.patch(existing._id, {
        label: args.label,
        adapterKey: args.adapterKey,
        sourceKind: args.sourceKind,
        entityType: args.entityType,
        scopeDefinition: args.scopeDefinition,
        selectedFieldKeys: args.selectedFieldKeys ?? existing.selectedFieldKeys,
        dateFieldKey: args.dateFieldKey,
        rowKeyStrategy: args.rowKeyStrategy,
        schedulePreset: args.schedulePreset ?? existing.schedulePreset,
        fieldCatalog: args.fieldCatalog,
        metadata: args.metadata,
        enabled: args.enabled ?? existing.enabled,
        updatedAt: now,
      })
      return existing._id
    }

    return await ctx.db.insert('dataSources', {
      profileId: profile._id,
      sourceKey: args.sourceKey,
      label: args.label,
      adapterKey: args.adapterKey,
      sourceKind: args.sourceKind,
      entityType: args.entityType,
      scopeDefinition: args.scopeDefinition,
      selectedFieldKeys: args.selectedFieldKeys ?? [],
      dateFieldKey: args.dateFieldKey,
      rowKeyStrategy: args.rowKeyStrategy,
      schedulePreset: args.schedulePreset ?? 'manual',
      fieldCatalog: args.fieldCatalog,
      metadata: args.metadata,
      enabled: args.enabled ?? true,
      status: 'idle',
      materializedCount: 0,
      createdAt: now,
    })
  },
})

export const replaceMaterializedRows = mutation({
  args: {
    sourceKey: v.string(),
    rows: v.array(materializedRowInputValidator),
  },
  returns: v.object({
    rowCount: v.number(),
  }),
  handler: async (ctx, args) => {
    const dataSource = await getDataSourceByKey(ctx, args.sourceKey)
    if (!dataSource) {
      throw new Error(`Data source '${args.sourceKey}' non trovata`)
    }

    await ctx.db.patch(dataSource._id, {
      status: 'refreshing',
      lastError: undefined,
      updatedAt: Date.now(),
    })

    const existingRows = await ctx.db
      .query('analyticsMaterializedRows')
      .withIndex('by_data_source', (q) => q.eq('dataSourceId', dataSource._id))
      .collect()

    for (const existingRow of existingRows) {
      await ctx.db.delete(existingRow._id)
    }

    const normalizedRows = normalizeMaterializedRows(args.rows)
    const now = Date.now()
    for (const row of normalizedRows) {
      await ctx.db.insert('analyticsMaterializedRows', {
        dataSourceId: dataSource._id,
        sourceKey: dataSource.sourceKey,
        rowKey: row.rowKey,
        sourceRecordId: row.sourceRecordId,
        sourceEntityType: row.sourceEntityType,
        occurredAt: row.occurredAt,
        rowData: row.rowData,
        updatedAt: now,
      })
    }

    await ctx.db.patch(dataSource._id, {
      status: 'ready',
      materializedCount: normalizedRows.length,
      lastRefreshedAt: now,
      lastError: undefined,
      updatedAt: now,
    })

    return {
      rowCount: normalizedRows.length,
    }
  },
})

export const getDataSourceFilterOptions = query({
  args: {
    sourceKey: v.string(),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const dataSource = await getDataSourceByKey(ctx, args.sourceKey)
    if (!dataSource) {
      throw new Error('Data source non trovata')
    }

    const rows = await listMaterializedRowsForDataSource(ctx, dataSource._id)
    const fieldCatalogMap = new Map<string, FieldCatalogItem>(
      ((dataSource.fieldCatalog ?? []) as Array<FieldCatalogItem>)
        .map((field: FieldCatalogItem) => [field.key, field] as const)
    )
    const selectedFieldKeys = dataSource.selectedFieldKeys.length > 0
      ? dataSource.selectedFieldKeys
      : [...fieldCatalogMap.keys()]

    return selectedFieldKeys
      .map((fieldKey: string) => {
        const fieldMeta = fieldCatalogMap.get(fieldKey)
        if (fieldMeta && fieldMeta.filterable === false) {
          return null
        }
        const rawOptions = rows
          .map((row: { rowData: Record<string, unknown> }) => row.rowData[fieldKey])
          .filter((value: unknown): value is string | number | boolean => value != null && typeof value !== 'object')
          .map((value: string | number | boolean) => String(value))
        const options = (Array.from(new Set(rawOptions)) as string[])
          .sort((left: string, right: string) => left.localeCompare(right))
          .slice(0, 200)

        return {
          fieldKey,
          label: fieldMeta?.label ?? fieldKey,
          valueType: fieldMeta?.valueType ?? 'string',
          options,
        }
      })
      .filter(Boolean)
  },
})

export const requestExport = mutation({
  args: {
    requestedBy: v.optional(v.string()),
    name: v.optional(v.string()),
    dataSourceKey: v.string(),
    filters: exportFiltersValidator,
    clonedFromExportId: v.optional(v.string()),
  },
  returns: v.string(),
  handler: async (ctx, args) => {
    const dataSource = await getDataSourceByKey(ctx, args.dataSourceKey)
    if (!dataSource || !dataSource.enabled) {
      throw new Error('Data source non disponibile')
    }
    if (dataSource.status !== 'ready') {
      throw new Error('Rigenera prima la materializzazione della data source')
    }

    const rows = applyExportFilters(
      (await listMaterializedRowsForDataSource(ctx, dataSource._id)).map((row: {
        occurredAt: number
        rowData: Record<string, unknown>
      }) => ({
        occurredAt: row.occurredAt,
        rowData: row.rowData,
      })),
      args.filters
    )
    const exportId = await createCompletedExport(ctx, {
      requestedBy: args.requestedBy,
      name: args.name,
      dataSource,
      rows,
      filters: args.filters,
      usageKind: 'manual',
      pinnedByAudit: false,
      clonedFromExportId: args.clonedFromExportId
        ? args.clonedFromExportId as Id<'analyticsExports'>
        : undefined,
    })

    return String(exportId)
  },
})

export const regenerateExport = mutation({
  args: {
    exportId: v.string(),
    requestedBy: v.optional(v.string()),
    name: v.optional(v.string()),
  },
  returns: v.string(),
  handler: async (ctx, args) => {
    const exportRow = await ctx.db.get(args.exportId as Id<'analyticsExports'>)
    if (!exportRow) {
      throw new Error('Export non trovato')
    }
    const dataSource = await ctx.db.get(exportRow.dataSourceId)
    if (!dataSource) {
      throw new Error('Data source non trovata')
    }

    const rows = applyExportFilters(
      (await listMaterializedRowsForDataSource(ctx, dataSource._id)).map((row: {
        occurredAt: number
        rowData: Record<string, unknown>
      }) => ({
        occurredAt: row.occurredAt,
        rowData: row.rowData,
      })),
      exportRow.filters as ExportFilters | undefined
    )

    const newExportId = await createCompletedExport(ctx, {
      requestedBy: args.requestedBy ?? exportRow.requestedBy,
      name: args.name ?? exportRow.name,
      dataSource,
      rows,
      filters: exportRow.filters as ExportFilters | undefined,
      usageKind: 'manual',
      pinnedByAudit: false,
      regeneratedFromExportId: exportRow._id,
    })

    return String(newExportId)
  },
})

export const listExports = query({
  args: {
    requestedBy: v.optional(v.string()),
    includePinned: v.optional(v.boolean()),
    limit: v.optional(v.number()),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const limit = Math.max(1, Math.min(args.limit ?? 50, 200))
    const rows = await ctx.db.query('analyticsExports').order('desc').take(limit * 3)
    return rows
      .filter((row) => args.includePinned ? true : !row.pinnedByAudit)
      .filter((row) => args.requestedBy ? row.requestedBy === args.requestedBy : true)
      .slice(0, limit)
  },
})

export const getExportDownloadUrl = query({
  args: {
    exportId: v.string(),
  },
  returns: v.union(v.string(), v.null()),
  handler: async (ctx, args) => {
    const exportRow = await ctx.db.get(args.exportId as Id<'analyticsExports'>)
    if (!exportRow?.storageId) {
      return null
    }
    return await ctx.storage.getUrl(exportRow.storageId)
  },
})

export const deleteExportPermanently = mutation({
  args: {
    exportId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const exportRow = await ctx.db.get(args.exportId as Id<'analyticsExports'>)
    if (!exportRow) {
      throw new Error('Export non trovato')
    }
    if (exportRow.pinnedByAudit) {
      throw new Error('Questo export è usato come audit KPI e non può essere eliminato')
    }
    if (exportRow.storageId) {
      await ctx.storage.delete(exportRow.storageId)
    }
    await ctx.db.delete(exportRow._id)
    return null
  },
})

export const upsertIndicator = mutation({
  args: {
    profileSlug: v.string(),
    slug: v.string(),
    label: v.string(),
    unit: v.optional(v.string()),
    category: v.optional(v.string()),
    description: v.optional(v.string()),
    externalId: v.optional(v.string()),
    enabled: v.optional(v.boolean()),
  },
  returns: v.id('indicators'),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const existing = await getIndicatorByProfileAndSlug(ctx, profile._id, args.slug)
    if (args.externalId) {
      const existingExternal = await ctx.db
        .query('indicators')
        .withIndex('by_external_id', (q) => q.eq('externalId', args.externalId))
        .unique()
      if (existingExternal && existingExternal._id !== existing?._id) {
        throw new Error(`externalId '${args.externalId}' già associato a un altro indicatore`)
      }
    }
    const now = Date.now()
    if (existing) {
      await ctx.db.patch(existing._id, {
        label: args.label,
        unit: args.unit,
        category: args.category,
        description: args.description,
        externalId: args.externalId ?? existing.externalId,
        enabled: args.enabled ?? existing.enabled,
        updatedAt: now,
      })
      return existing._id
    }

    return await ctx.db.insert('indicators', {
      profileId: profile._id,
      slug: args.slug,
      label: args.label,
      unit: args.unit,
      category: args.category,
      description: args.description,
      externalId: args.externalId,
      enabled: args.enabled ?? true,
      createdAt: now,
    })
  },
})

export const getIndicatorBySlug = query({
  args: {
    profileSlug: v.string(),
    slug: v.string(),
  },
  returns: v.union(v.any(), v.null()),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    return await getIndicatorByProfileAndSlug(ctx, profile._id, args.slug)
  },
})

export const getDerivedIndicatorBySlug = query({
  args: {
    profileSlug: v.string(),
    slug: v.string(),
  },
  returns: v.union(v.any(), v.null()),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    return await getDerivedIndicatorByProfileAndSlug(ctx, profile._id, args.slug)
  },
})

export const getIndicatorByExternalId = query({
  args: {
    externalId: v.string(),
  },
  returns: v.union(v.any(), v.null()),
  handler: async (ctx, args) => {
    return await ctx.db
      .query('indicators')
      .withIndex('by_external_id', (q) => q.eq('externalId', args.externalId))
      .unique()
  },
})

export const setIndicatorExternalId = mutation({
  args: {
    indicatorId: v.string(),
    externalId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const indicator = await ctx.db.get(args.indicatorId as Id<'indicators'>)
    if (!indicator) {
      throw new Error('Indicatore non trovato')
    }
    const existing = await ctx.db
      .query('indicators')
      .withIndex('by_external_id', (q) => q.eq('externalId', args.externalId))
      .unique()
    if (existing && existing._id !== indicator._id) {
      throw new Error(`externalId '${args.externalId}' già associato a un altro indicatore`)
    }
    await ctx.db.patch(indicator._id, {
      externalId: args.externalId,
      updatedAt: Date.now(),
    })
    return null
  },
})

export const deleteIndicator = mutation({
  args: {
    profileSlug: v.string(),
    slug: v.string(),
  },
  returns: v.object({
    deleted: v.boolean(),
    deletedDefinitionCount: v.number(),
  }),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const indicator = await getIndicatorByProfileAndSlug(ctx, profile._id, args.slug)
    if (!indicator) {
      throw new Error(`Indicatore '${args.slug}' non trovato`)
    }

    const dependentDerivedIndicators = await ctx.db
      .query('derivedIndicators')
      .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
      .collect()

    const blockingDerivedIndicators = dependentDerivedIndicators.filter((derivedIndicator) =>
      derivedIndicator.formula.operands.some((operand) => operand.indicatorSlug === args.slug)
    )

    if (blockingDerivedIndicators.length > 0) {
      throw new Error(
        `Impossibile eliminare '${args.slug}': e' usato da ${blockingDerivedIndicators
          .map((derivedIndicator) => derivedIndicator.slug)
          .join(', ')}`
      )
    }

    const definitions = await ctx.db
      .query('calculationDefinitions')
      .withIndex('by_profile_and_indicator', (q) =>
        q.eq('profileId', profile._id).eq('indicatorId', indicator._id)
      )
      .collect()

    for (const definition of definitions) {
      await ctx.db.delete(definition._id)
    }

    await ctx.db.delete(indicator._id)

    return {
      deleted: true,
      deletedDefinitionCount: definitions.length,
    }
  },
})

export const upsertCalculationDefinition = mutation({
  args: {
    profileSlug: v.string(),
    indicatorSlug: v.string(),
    sourceKey: v.string(),
    operation: operationValidator,
    fieldPath: v.optional(v.string()),
    filters: v.optional(v.any()),
    groupBy: v.optional(v.array(v.string())),
    normalization: v.optional(v.any()),
    priority: v.optional(v.number()),
    enabled: v.optional(v.boolean()),
    ruleVersion: v.optional(v.number()),
  },
  returns: v.id('calculationDefinitions'),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const indicator = await getIndicatorByProfileAndSlug(ctx, profile._id, args.indicatorSlug)
    if (!indicator) {
      throw new Error(`Indicatore '${args.indicatorSlug}' non trovato`)
    }
    const dataSource = await ctx.db
      .query('dataSources')
      .withIndex('by_profile_and_source_key', (q) =>
        q.eq('profileId', profile._id).eq('sourceKey', args.sourceKey)
      )
      .unique()
    if (!dataSource) {
      throw new Error(`Data source '${args.sourceKey}' non trovata`)
    }

    const existingDefinitions = await ctx.db
      .query('calculationDefinitions')
      .withIndex('by_profile_and_indicator', (q) =>
        q.eq('profileId', profile._id).eq('indicatorId', indicator._id)
      )
      .collect()
    const existing = existingDefinitions.find((definition) => (
      definition.dataSourceId === dataSource._id &&
      definition.operation === args.operation &&
      definition.fieldPath === args.fieldPath
    ))
    const now = Date.now()

    if (existing) {
      await ctx.db.patch(existing._id, {
        filters: args.filters,
        groupBy: args.groupBy,
        normalization: args.normalization,
        priority: args.priority ?? existing.priority,
        enabled: args.enabled ?? existing.enabled,
        ruleVersion: args.ruleVersion ?? existing.ruleVersion + 1,
        updatedAt: now,
      })
      return existing._id
    }

    return await ctx.db.insert('calculationDefinitions', {
      profileId: profile._id,
      indicatorId: indicator._id,
      dataSourceId: dataSource._id,
      operation: args.operation,
      fieldPath: args.fieldPath,
      filters: args.filters,
      groupBy: args.groupBy,
      normalization: args.normalization,
      priority: args.priority ?? 100,
      enabled: args.enabled ?? true,
      ruleVersion: args.ruleVersion ?? 1,
      createdAt: now,
    })
  },
})

export const toggleCalculation = mutation({
  args: {
    definitionId: v.string(),
    enabled: v.boolean(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.db.patch(args.definitionId as Id<'calculationDefinitions'>, {
      enabled: args.enabled,
      updatedAt: Date.now(),
    })
    return null
  },
})

export const replaceProfileDefinitions = mutation({
  args: {
    profileSlug: v.string(),
    definitions: v.array(
      v.object({
        indicatorSlug: v.string(),
        sourceKey: v.string(),
        operation: operationValidator,
        fieldPath: v.optional(v.string()),
        filters: v.optional(v.any()),
        groupBy: v.optional(v.array(v.string())),
        normalization: v.optional(v.any()),
        priority: v.optional(v.number()),
        enabled: v.optional(v.boolean()),
        ruleVersion: v.optional(v.number()),
      })
    ),
  },
  returns: v.object({
    deleted: v.number(),
    created: v.number(),
  }),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const existing = await ctx.db
      .query('calculationDefinitions')
      .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
      .collect()

    for (const definition of existing) {
      await ctx.db.delete(definition._id)
    }

    let created = 0
    for (const definition of args.definitions) {
      const indicator = await getIndicatorByProfileAndSlug(ctx, profile._id, definition.indicatorSlug)
      if (!indicator) {
        throw new Error(`Indicatore '${definition.indicatorSlug}' non trovato`)
      }
      const dataSource = await ctx.db
        .query('dataSources')
        .withIndex('by_profile_and_source_key', (q) =>
          q.eq('profileId', profile._id).eq('sourceKey', definition.sourceKey)
        )
        .unique()
      if (!dataSource) {
        throw new Error(`Data source '${definition.sourceKey}' non trovata`)
      }
      await ctx.db.insert('calculationDefinitions', {
        profileId: profile._id,
        indicatorId: indicator._id,
        dataSourceId: dataSource._id,
        operation: definition.operation,
        fieldPath: definition.fieldPath,
        filters: definition.filters,
        groupBy: definition.groupBy,
        normalization: definition.normalization,
        priority: definition.priority ?? 100,
        enabled: definition.enabled ?? true,
        ruleVersion: definition.ruleVersion ?? 1,
        createdAt: Date.now(),
      })
      created++
    }

    return {
      deleted: existing.length,
      created,
    }
  },
})

export const upsertDerivedIndicator = mutation({
  args: {
    profileSlug: v.string(),
    slug: v.string(),
    label: v.string(),
    unit: v.optional(v.string()),
    description: v.optional(v.string()),
    formula: derivedFormulaValidator,
    enabled: v.optional(v.boolean()),
  },
  returns: v.id('derivedIndicators'),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    validateDerivedFormula(args.formula)
    const existing = await getDerivedIndicatorByProfileAndSlug(ctx, profile._id, args.slug)
    const now = Date.now()

    if (existing) {
      await ctx.db.patch(existing._id, {
        label: args.label,
        unit: args.unit,
        description: args.description,
        formula: args.formula,
        enabled: args.enabled ?? existing.enabled,
        updatedAt: now,
      })
      return existing._id
    }

    return await ctx.db.insert('derivedIndicators', {
      profileId: profile._id,
      slug: args.slug,
      label: args.label,
      unit: args.unit,
      description: args.description,
      formula: args.formula,
      enabled: args.enabled ?? true,
      createdAt: now,
    })
  },
})

export const deleteDerivedIndicator = mutation({
  args: {
    profileSlug: v.string(),
    slug: v.string(),
  },
  returns: v.object({
    deleted: v.boolean(),
  }),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const derivedIndicator = await getDerivedIndicatorByProfileAndSlug(ctx, profile._id, args.slug)
    if (!derivedIndicator) {
      throw new Error(`Indicatore derivato '${args.slug}' non trovato`)
    }

    await ctx.db.delete(derivedIndicator._id)

    return {
      deleted: true,
    }
  },
})

export const listDerivedIndicators = query({
  args: {
    profileSlug: v.string(),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    return await ctx.db
      .query('derivedIndicators')
      .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
      .collect()
  },
})

export const listProfileDefinitions = query({
  args: {
    profileSlug: v.string(),
  },
  returns: v.any(),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const [indicators, dataSources, definitions, derivedIndicators] = await Promise.all([
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('dataSources').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('calculationDefinitions').withIndex('by_profile_and_priority', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('derivedIndicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
    ])

    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]))
    const dataSourcesById = new Map(dataSources.map((dataSource) => [dataSource._id, dataSource]))

    return {
      profile,
      indicators,
      dataSources,
      derivedIndicators,
      definitions: definitions.map((definition) => ({
        ...definition,
        indicatorSlug: indicatorsById.get(definition.indicatorId)?.slug ?? null,
        sourceKey: dataSourcesById.get(definition.dataSourceId)?.sourceKey ?? null,
      })),
    }
  },
})

export const listProfileDataSources = query({
  args: {
    profileSlug: v.string(),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    return await ctx.db
      .query('dataSources')
      .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
      .collect()
      .then((rows) => rows.filter((row) => !row.archivedAt))
  },
})

export const ingestSourceRows = mutation({
  args: {
    profileSlug: v.string(),
    sourceKey: v.string(),
    rows: v.array(sourceRowInputValidator),
  },
  returns: v.object({
    inserted: v.number(),
  }),
  handler: async (_ctx, _args) => {
    throw new Error(
      'ingestSourceRows è stato rimosso in kpi-snapshot@2.0.0. ' +
      'Usa replaceMaterializedRows o un wrapper host che aggiorna le righe materializzate.'
    )
  },
})

export const simulateSnapshot = query({
  args: {
    profileSlug: v.string(),
    snapshotAt: v.optional(v.number()),
    sourcePayloads: v.optional(v.array(sourcePayloadValidator)),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const snapshotAt = args.snapshotAt ?? Date.now()
    const [definitions, indicators, dataSources] = await Promise.all([
      ctx.db.query('calculationDefinitions').withIndex('by_profile_and_priority', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('dataSources').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
    ])

    const enabledDefinitions = definitions.filter((definition) => definition.enabled)
    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]))
    const dataSourcesById = new Map(dataSources.map((source) => [source._id, source]))
    const payloadBySourceKey = new Map(
      (args.sourcePayloads ?? []).map((payload) => [
        payload.sourceKey,
        normalizeSourceRows(payload.rows.map((row) => ({
          occurredAt: row.occurredAt,
          rowData: row.rowData as Record<string, unknown>,
        })), snapshotAt),
      ])
    )

    const rowsByDataSource = new Map<Id<'dataSources'>, Array<SourceRowInput>>()
    for (const source of dataSources) {
      const payloadRows = payloadBySourceKey.get(source.sourceKey)
      if (payloadRows) {
        rowsByDataSource.set(source._id, payloadRows)
        continue
      }
      const materializedRows = await listMaterializedRowsForDataSource(ctx, source._id, snapshotAt)
      rowsByDataSource.set(source._id, materializedRows.map((row: {
        occurredAt: number
        rowData: Record<string, unknown>
      }) => ({
        occurredAt: row.occurredAt,
        rowData: row.rowData,
      })))
    }

    return enabledDefinitions.map((definition) => {
      const rows = rowsByDataSource.get(definition.dataSourceId) ?? []
      const result = runSingleDefinition(definition, rows)
      return {
        definitionId: definition._id,
        indicatorId: definition.indicatorId,
        indicatorSlug: indicatorsById.get(definition.indicatorId)?.slug ?? null,
        sourceKey: dataSourcesById.get(definition.dataSourceId)?.sourceKey ?? null,
        operation: definition.operation,
        fieldPath: definition.fieldPath,
        inputCount: result.inputCount,
        rawResult: result.rawResult,
        normalizedResult: result.normalizedResult,
        warningMessage: result.warningMessage,
      }
    })
  },
})

export const createSnapshotRun = mutation({
  args: {
    profileSlug: v.string(),
    snapshotAt: v.optional(v.number()),
    triggeredBy: v.optional(v.string()),
    note: v.optional(v.string()),
    triggerKind: v.optional(v.union(
      v.literal('manual'),
      v.literal('source_materialization'),
      v.literal('scheduled_materialization')
    )),
    triggerSourceKey: v.optional(v.string()),
  },
  returns: v.object({
    snapshotId: v.string(),
    snapshotRunId: v.string(),
    status: v.union(v.literal('success'), v.literal('error')),
    processedCount: v.number(),
    errorsCount: v.number(),
  }),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const snapshotAt = args.snapshotAt ?? Date.now()
    const now = Date.now()

    const [definitions, indicators, dataSources, derivedIndicators] = await Promise.all([
      ctx.db.query('calculationDefinitions').withIndex('by_profile_and_priority', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('dataSources').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('derivedIndicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
    ])

    const enabledDefinitions = definitions.filter((definition) => definition.enabled)
    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]))
    const dataSourcesById = new Map(dataSources.map((source) => [source._id, source]))

    const snapshotId = await ctx.db.insert('snapshots', {
      profileId: profile._id,
      snapshotAt,
      status: 'running',
      note: args.note,
      triggeredBy: args.triggeredBy,
      triggerKind: args.triggerKind,
      triggerSourceKey: args.triggerSourceKey,
      createdAt: now,
    })
    const snapshotRunId = await ctx.db.insert('snapshotRuns', {
      snapshotId,
      profileId: profile._id,
      startedAt: now,
      status: 'running',
      triggeredBy: args.triggeredBy,
      triggerKind: args.triggerKind,
      triggerSourceKey: args.triggerSourceKey,
      definitionsCount: enabledDefinitions.length,
      processedCount: 0,
    })

    const sourceKeysToFreeze = [...new Set(enabledDefinitions.map((definition) => {
      const source = dataSourcesById.get(definition.dataSourceId)
      return source?.sourceKey
    }).filter(Boolean) as string[])]
    const materializedRowsByDataSource = new Map<Id<'dataSources'>, Array<{
      occurredAt: number
      rowData: Record<string, unknown>
    }>>()

    for (const sourceKey of sourceKeysToFreeze) {
      const dataSource = [...dataSourcesById.values()].find((source) => source.sourceKey === sourceKey)
      if (!dataSource) {
        continue
      }
      const materializedRows = await listMaterializedRowsForDataSource(ctx, dataSource._id, snapshotAt)
      const rowsForSnapshot = materializedRows.map((row: {
        occurredAt: number
        rowData: Record<string, unknown>
      }) => ({
        occurredAt: row.occurredAt,
        rowData: row.rowData,
      }))
      materializedRowsByDataSource.set(dataSource._id, rowsForSnapshot)
    }

    let processedCount = 0
    let errorsCount = 0
    const baseSnapshotValues: Array<Doc<'snapshotValues'> & { indicatorSlug: string }> = []

    for (const definition of enabledDefinitions) {
      const startedAt = Date.now()
      const dataSource = dataSourcesById.get(definition.dataSourceId)
      const dataSourceRows = materializedRowsByDataSource.get(definition.dataSourceId) ?? []

      try {
        const result = runSingleDefinition(definition, dataSourceRows)
        const durationMs = Date.now() - startedAt
        const ruleHash = buildRuleHash(definition)
        const indicator = indicatorsById.get(definition.indicatorId)
        if (!indicator) {
          throw new Error(`Indicatore non trovato per definitionId '${definition._id}'`)
        }
        const sourceExportIds: Array<Id<'analyticsExports'>> = []
        const itemId = await ctx.db.insert('snapshotRunItems', {
          snapshotRunId,
          snapshotId,
          profileId: profile._id,
          definitionId: definition._id,
          indicatorId: definition.indicatorId,
          dataSourceId: definition.dataSourceId,
          status: result.normalizedResult == null ? 'skipped' : 'success',
          inputCount: result.inputCount,
          rawResult: result.rawResult ?? undefined,
          normalizedResult: result.normalizedResult ?? undefined,
          durationMs,
          sourceExportIds,
          warningMessage: result.warningMessage,
          ruleHash,
          createdAt: Date.now(),
        })

        if (result.normalizedResult != null) {
          const snapshotValueId = await ctx.db.insert('snapshotValues', {
            snapshotId,
            snapshotRunId,
            snapshotRunItemId: itemId,
            profileId: profile._id,
            indicatorId: definition.indicatorId,
            indicatorLabelSnapshot: normalizeIndicatorLabelSnapshot(indicator.label),
            value: result.normalizedResult,
            computedAt: Date.now(),
            ruleHash,
            sourceExportIds,
            explainRef: `runItem:${itemId}`,
          })
          const snapshotValue = await ctx.db.get(snapshotValueId)
          if (snapshotValue) {
            baseSnapshotValues.push({
              ...snapshotValue,
              indicatorSlug: indicator.slug,
            })
          }
          await ctx.db.insert('integrationValues', {
            snapshotValueId,
            snapshotId,
            snapshotRunId,
            profileId: profile._id,
            indicatorId: definition.indicatorId,
            value: result.normalizedResult,
            measuredAt: snapshotAt,
            syncStatus: 'pending',
            createdAt: Date.now(),
          })
        }

        await ctx.db.insert('calculationTraces', {
          snapshotRunId,
          snapshotRunItemId: itemId,
          profileId: profile._id,
          definitionId: definition._id,
          queryParams: {
            snapshotAt,
            dataSourceId: definition.dataSourceId,
            sourceKey: dataSource?.sourceKey ?? null,
          },
          resolvedFilters: definition.filters,
          sampleRowsPreview: result.filteredRows.slice(0, 10),
          warnings: result.warningMessage ? [result.warningMessage] : undefined,
          createdAt: Date.now(),
        })
      } catch (error) {
        errorsCount++
        const durationMs = Date.now() - startedAt
        await ctx.db.insert('snapshotRunItems', {
          snapshotRunId,
          snapshotId,
          profileId: profile._id,
          definitionId: definition._id,
          indicatorId: definition.indicatorId,
          dataSourceId: definition.dataSourceId,
          status: 'error',
          inputCount: dataSourceRows.length,
          durationMs,
          sourceExportIds: [],
          errorMessage: error instanceof Error ? error.message : String(error),
          ruleHash: buildRuleHash(definition),
          createdAt: Date.now(),
        })
      }
      processedCount++
    }

    const valuesByIndicatorSlug = new Map<string, DerivedComputationInput>()
    for (const baseValue of baseSnapshotValues) {
      if (valuesByIndicatorSlug.has(baseValue.indicatorSlug)) {
        valuesByIndicatorSlug.delete(baseValue.indicatorSlug)
        continue
      }
      valuesByIndicatorSlug.set(baseValue.indicatorSlug, {
        snapshotValueId: baseValue._id,
        indicatorSlug: baseValue.indicatorSlug,
        value: baseValue.value,
        sourceExportIds: baseValue.sourceExportIds,
      })
    }

    const existingDerivedRows = await ctx.db
      .query('derivedSnapshotValues')
      .withIndex('by_snapshot', (q) => q.eq('snapshotId', snapshotId))
      .collect()
    for (const row of existingDerivedRows) {
      await ctx.db.delete(row._id)
    }

    for (const derivedIndicator of derivedIndicators.filter((row) => row.enabled)) {
      const computed = computeDerivedValue(derivedIndicator.formula as any, valuesByIndicatorSlug)
      if (!computed) {
        continue
      }
      await ctx.db.insert('derivedSnapshotValues', {
        snapshotId,
        snapshotRunId,
        profileId: profile._id,
        derivedIndicatorId: derivedIndicator._id,
        derivedIndicatorSlug: derivedIndicator.slug,
        derivedIndicatorLabelSnapshot: derivedIndicator.label,
        derivedIndicatorUnit: derivedIndicator.unit,
        formulaKind: derivedIndicator.formula.kind,
        value: computed.value,
        computedAt: Date.now(),
        baseSnapshotValueIds: computed.inputs.map((input) => input.snapshotValueId),
        baseIndicatorSlugs: computed.inputs.map((input) => input.indicatorSlug),
        sourceExportIds: [...new Set(computed.inputs.flatMap((input) => input.sourceExportIds))],
        formulaSnapshot: derivedIndicator.formula,
        createdAt: Date.now(),
      })
      processedCount++
    }

    const endStatus: 'success' | 'error' = errorsCount > 0 ? 'error' : 'success'
    await ctx.db.patch(snapshotRunId, {
      finishedAt: Date.now(),
      status: endStatus,
      errorMessage: errorsCount > 0 ? `${errorsCount} regole in errore` : undefined,
      processedCount,
    })
    await ctx.db.patch(snapshotId, {
      finishedAt: Date.now(),
      status: endStatus,
      errorMessage: errorsCount > 0 ? `${errorsCount} regole in errore` : undefined,
    })
    await ctx.scheduler.runAfter(0, internal.exportWorkflows.freezeSnapshotExports, {
      profileSlug: profile.slug,
      snapshotId: String(snapshotId),
      snapshotRunId: String(snapshotRunId),
      triggeredBy: args.triggeredBy,
      sourceKeys: sourceKeysToFreeze,
      snapshotAt,
    })

    return {
      snapshotId: String(snapshotId),
      snapshotRunId: String(snapshotRunId),
      status: endStatus,
      processedCount,
      errorsCount,
    }
  },
})

export const attachSnapshotValueEvidence = mutation({
  args: {
    uploads: v.array(
      v.object({
        snapshotRunItemId: v.string(),
        snapshotValueId: v.string(),
        storageId: v.id('_storage'),
        fileName: v.string(),
        rowCount: v.number(),
        mimeType: v.string(),
        sha256: v.optional(v.string()),
      })
    ),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const generatedAt = Date.now()
    for (const upload of args.uploads) {
      const evidenceRef = String(upload.storageId)
      await ctx.db.patch(upload.snapshotRunItemId as Id<'snapshotRunItems'>, {
        evidenceRef,
        evidenceFileName: upload.fileName,
        evidenceRowCount: upload.rowCount,
        evidenceGeneratedAt: generatedAt,
        evidenceMimeType: upload.mimeType,
        evidenceSha256: upload.sha256,
      })
      await ctx.db.patch(upload.snapshotValueId as Id<'snapshotValues'>, {
        evidenceRef,
        evidenceFileName: upload.fileName,
        evidenceRowCount: upload.rowCount,
        evidenceGeneratedAt: generatedAt,
        evidenceMimeType: upload.mimeType,
        evidenceSha256: upload.sha256,
      })
    }
    return null
  },
})

export const getIntegrationValueByExternalId = query({
  args: {
    externalId: v.string(),
  },
  returns: v.union(v.any(), v.null()),
  handler: async (ctx, args) => {
    return await ctx.db
      .query('integrationValues')
      .withIndex('by_external_id', (q) => q.eq('externalId', args.externalId))
      .unique()
  },
})

export const listIntegrationValuesForSync = query({
  args: {
    profileSlug: v.optional(v.string()),
    limit: v.optional(v.number()),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const limit = Math.max(1, Math.min(args.limit ?? 50, 200))
    const profile = args.profileSlug ? await getProfileBySlug(ctx, args.profileSlug) : null
    const pendingRows = profile
      ? await ctx.db
        .query('integrationValues')
        .withIndex('by_profile_and_sync_status_and_measured_at', (q) =>
          q.eq('profileId', profile._id).eq('syncStatus', 'pending')
        )
        .order('desc')
        .take(limit)
      : await ctx.db
        .query('integrationValues')
        .withIndex('by_sync_status_and_measured_at', (q) =>
          q.eq('syncStatus', 'pending')
        )
        .order('desc')
        .take(limit)

    const pendingWithIndicator = await Promise.all(pendingRows.map(async (integrationValue) => {
      const indicator = await ctx.db.get(integrationValue.indicatorId)
      if (!indicator) {
        return null
      }
      return {
        ...integrationValue,
        indicatorExternalId: indicator.externalId,
        indicatorSlug: indicator.slug,
        profileSlug: profile?.slug ?? null,
      }
    }))

    return pendingWithIndicator.filter((row) => row !== null)
  },
})

export const setIntegrationValueExternalId = mutation({
  args: {
    integrationValueId: v.string(),
    externalId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const integrationValue = await ctx.db.get(args.integrationValueId as Id<'integrationValues'>)
    if (!integrationValue) {
      throw new Error('Valore di integrazione non trovato')
    }
    const existing = await ctx.db
      .query('integrationValues')
      .withIndex('by_external_id', (q) => q.eq('externalId', args.externalId))
      .unique()
    if (existing && existing._id !== integrationValue._id) {
      throw new Error(`externalId '${args.externalId}' già associato a un altro valore di integrazione`)
    }
    await ctx.db.patch(integrationValue._id, {
      externalId: args.externalId,
      syncStatus: 'synced',
      lastSyncedAt: Date.now(),
    })
    return null
  },
})

export const listSnapshots = query({
  args: {
    profileSlug: v.optional(v.string()),
    limit: v.optional(v.number()),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const limit = args.limit ?? 20
    if (!args.profileSlug) {
      return await ctx.db.query('snapshots').order('desc').take(limit)
    }
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    return await ctx.db
      .query('snapshots')
      .withIndex('by_profile_and_snapshot_at', (q) => q.eq('profileId', profile._id))
      .order('desc')
      .take(limit)
  },
})

export const listSnapshotValues = query({
  args: {
    snapshotId: v.string(),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const snapshotId = args.snapshotId as Id<'snapshots'>
    const [values, derivedRows] = await Promise.all([
      ctx.db.query('snapshotValues').withIndex('by_snapshot', (q) => q.eq('snapshotId', snapshotId)).collect(),
      ctx.db.query('derivedSnapshotValues').withIndex('by_snapshot', (q) => q.eq('snapshotId', snapshotId)).collect(),
    ])

    const baseRows = await Promise.all(values.map(async (valueRow) => {
      const indicator = await ctx.db.get(valueRow.indicatorId)
      return {
        ...valueRow,
        indicatorSlug: indicator?.slug ?? null,
        indicatorUnit: indicator?.unit ?? null,
        isDerived: false,
        sourceExportCount: valueRow.sourceExportIds.length,
        sourceExports: await resolveExportSummaries(ctx, valueRow.sourceExportIds),
        derivedFromIndicatorSlugs: [],
        formulaKind: null,
      }
    }))

    const derivedValueRows = await Promise.all(derivedRows.map(async (row) => ({
      _id: `derived:${row._id}`,
      indicatorLabelSnapshot: row.derivedIndicatorLabelSnapshot,
      indicatorUnit: row.derivedIndicatorUnit ?? null,
      indicatorSlug: row.derivedIndicatorSlug,
      value: row.value,
      computedAt: row.computedAt,
      evidenceRef: null,
      evidenceFileName: null,
      evidenceRowCount: row.baseSnapshotValueIds.length,
      evidenceGeneratedAt: null,
      isDerived: true,
      sourceExportCount: row.sourceExportIds.length,
      sourceExports: await resolveExportSummaries(ctx, row.sourceExportIds),
      derivedFromIndicatorSlugs: row.baseIndicatorSlugs,
      formulaKind: row.formulaKind,
    })))

    return [...baseRows, ...derivedValueRows].sort((left, right) => right.computedAt - left.computedAt)
  },
})

export const getSnapshotValueEvidenceDownloadUrl = query({
  args: {
    snapshotValueId: v.string(),
  },
  returns: v.union(v.string(), v.null()),
  handler: async (ctx, args) => {
    if (isDerivedSnapshotValueId(args.snapshotValueId)) {
      const derivedRow = await ctx.db.get(parseDerivedSnapshotValueId(args.snapshotValueId))
      if (!derivedRow || derivedRow.sourceExportIds.length === 0) {
        return null
      }
      const exportRow = await ctx.db.get(derivedRow.sourceExportIds[0])
      if (!exportRow?.storageId) {
        return null
      }
      return await ctx.storage.getUrl(exportRow.storageId)
    }

    const snapshotValue = await ctx.db.get(args.snapshotValueId as Id<'snapshotValues'>)
    if (!snapshotValue || snapshotValue.sourceExportIds.length === 0) {
      return null
    }
    const exportRow = await ctx.db.get(snapshotValue.sourceExportIds[0])
    if (!exportRow?.storageId) {
      return null
    }
    return await ctx.storage.getUrl(exportRow.storageId)
  },
})

export const getSnapshotExplain = query({
  args: {
    snapshotId: v.string(),
  },
  returns: v.union(v.any(), v.null()),
  handler: async (ctx, args) => {
    const snapshotId = args.snapshotId as Id<'snapshots'>
    const snapshot = await ctx.db.get(snapshotId)
    if (!snapshot) {
      return null
    }
    const run = await ctx.db
      .query('snapshotRuns')
      .withIndex('by_snapshot', (q) => q.eq('snapshotId', snapshotId))
      .unique()
    if (!run) {
      return {
        snapshot,
        run: null,
        runItems: [],
        traces: [],
        values: [],
      }
    }
    const [runItems, values, traces, exports] = await Promise.all([
      ctx.db.query('snapshotRunItems').withIndex('by_snapshot_run', (q) => q.eq('snapshotRunId', run._id)).collect(),
      ctx.db.query('snapshotValues').withIndex('by_snapshot', (q) => q.eq('snapshotId', snapshotId)).collect(),
      ctx.db.query('calculationTraces').withIndex('by_snapshot_run', (q) => q.eq('snapshotRunId', run._id)).collect(),
      ctx.db.query('analyticsExports').withIndex('by_audit_snapshot', (q) => q.eq('auditSnapshotId', snapshotId)).collect(),
    ])
    return {
      snapshot,
      run,
      runItems,
      traces,
      values,
      exports,
    }
  },
})

export const listSnapshotRunErrors = query({
  args: {
    snapshotRunId: v.string(),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const items = await ctx.db
      .query('snapshotRunItems')
      .withIndex('by_snapshot_run', (q) => q.eq('snapshotRunId', args.snapshotRunId as Id<'snapshotRuns'>))
      .collect()
    return items.filter((item) => item.status === 'error')
  },
})

export const backfillIndicatorLabelSnapshot = mutation({
  args: {
    profileSlug: v.optional(v.string()),
    dryRun: v.optional(v.boolean()),
  },
  returns: v.object({
    scanned: v.number(),
    updated: v.number(),
    skippedAlreadySet: v.number(),
    skippedMissingIndicator: v.number(),
  }),
  handler: async (ctx, args) => {
    const profile = args.profileSlug ? await getProfileBySlug(ctx, args.profileSlug) : null
    const dryRun = args.dryRun ?? false
    const [snapshotValues, indicators] = await Promise.all([
      ctx.db.query('snapshotValues').collect(),
      ctx.db.query('indicators').collect(),
    ])
    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]))

    let scanned = 0
    let updated = 0
    let skippedAlreadySet = 0
    let skippedMissingIndicator = 0

    for (const row of snapshotValues) {
      if (profile && row.profileId !== profile._id) {
        continue
      }
      scanned++
      if (row.indicatorLabelSnapshot && row.indicatorLabelSnapshot.trim().length > 0) {
        skippedAlreadySet++
        continue
      }

      const indicator = indicatorsById.get(row.indicatorId)
      if (!indicator) {
        skippedMissingIndicator++
        continue
      }

      if (!dryRun) {
        await ctx.db.patch(row._id, {
          indicatorLabelSnapshot: normalizeIndicatorLabelSnapshot(indicator.label),
        })
      }
      updated++
    }

    return {
      scanned,
      updated,
      skippedAlreadySet,
      skippedMissingIndicator,
    }
  },
})
