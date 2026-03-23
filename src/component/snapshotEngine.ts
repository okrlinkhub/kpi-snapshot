import { v } from 'convex/values'
import { internal } from './_generated/api.js'
import { mutation, query } from './_generated/server.js'
import type { Doc, Id } from './_generated/dataModel.js'
import {
  calculationFiltersValidator,
  normalizeCalculationFilters,
  resolveCalculationFilters,
} from './lib/calculationFilters.js'
import type {
  CalculationFieldRule,
  CalculationFilters,
  CalculationRuleNode,
  ResolvedCalculationFilters,
} from './lib/calculationFilters.js'

const operationValidator = v.union(
  v.literal('sum'),
  v.literal('count'),
  v.literal('avg'),
  v.literal('min'),
  v.literal('max'),
  v.literal('distinct_count')
)

const sourceKindValidator = v.literal('materialized_rows')

const schedulePresetValidator = v.union(
  v.literal('manual'),
  v.literal('daily'),
  v.literal('weekly_monday'),
  v.literal('monthly_first_day')
)

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
  sourcePath: v.optional(v.string()),
  sourceTable: v.optional(v.string()),
  referenceTable: v.optional(v.string()),
  isSystem: v.optional(v.boolean()),
  isNullable: v.optional(v.boolean()),
  isArray: v.optional(v.boolean()),
})

const catalogOptionValidator = v.object({
  key: v.string(),
  label: v.string(),
})

const schemaIndexValidator = v.object({
  key: v.string(),
  label: v.string(),
  fields: v.array(v.string()),
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
  sourcePath?: string
  sourceTable?: string
  referenceTable?: string
  isSystem?: boolean
  isNullable?: boolean
  isArray?: boolean
}

type CatalogOption = {
  key: string
  label: string
}

type CalculationResult = {
  inputCount: number
  rawResult: number | null
  normalizedResult: number | null
  warningMessage?: string
  filteredRows: Array<SourceRowInput>
  resolvedFilters: ResolvedCalculationFilters
}

type DerivedComputationInput = {
  snapshotValueId: Id<'snapshotValues'>
  indicatorSlug: string
  value: number
  sourceExportIds: Array<Id<'analyticsExports'>>
}

type DataSourceDoc = Doc<'dataSources'>
type DataSourceSettingDoc = Doc<'dataSourceSettings'>

function isAutomaticSchedulePreset (schedulePreset: DataSourceDoc['schedulePreset']) {
  return schedulePreset !== 'manual'
}

function getSchedulePresetLabel (schedulePreset: DataSourceDoc['schedulePreset']) {
  switch (schedulePreset) {
    case 'daily':
      return 'Ogni giorno'
    case 'weekly_monday':
      return 'Ogni lunedi`'
    case 'monthly_first_day':
      return 'Ogni primo del mese'
    default:
      return 'Manuale'
  }
}

function getLikeForLikeWindowStart (endMs: number) {
  const endDate = new Date(endMs)
  const startDate = new Date(endMs)
  startDate.setUTCFullYear(endDate.getUTCFullYear() - 1)
  return startDate.getTime()
}

function getDefaultAutomaticWindow (dataSource: DataSourceDoc, endMs: number) {
  if (!isAutomaticSchedulePreset(dataSource.schedulePreset)) {
    return null
  }

  return {
    startDate: getLikeForLikeWindowStart(endMs),
    endDate: endMs,
    label: 'Ultimo anno like-for-like',
  }
}

function clampExportFiltersToDataSourceWindow (
  dataSource: DataSourceDoc,
  filters?: ExportFilters
) {
  const requestedEndDate = typeof filters?.endDate === 'number'
    ? filters.endDate
    : Date.now()
  const automaticWindow = getDefaultAutomaticWindow(dataSource, requestedEndDate)
  if (!automaticWindow) {
    return filters
  }

  return {
    ...filters,
    startDate: typeof filters?.startDate === 'number'
      ? Math.max(filters.startDate, automaticWindow.startDate)
      : automaticWindow.startDate,
    endDate: typeof filters?.endDate === 'number'
      ? Math.min(filters.endDate, automaticWindow.endDate)
      : automaticWindow.endDate,
  }
}

function buildDataSourceView (dataSource: DataSourceDoc) {
  return {
    ...dataSource,
    schedulePresetLabel: getSchedulePresetLabel(dataSource.schedulePreset),
    automaticWindow: getDefaultAutomaticWindow(dataSource, Date.now()),
  }
}

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

function resolveOperandValue (rowData: unknown, operand: CalculationFieldRule['rightOperand']) {
  if (operand.kind === 'field') {
    return getNestedValue(rowData, operand.field)
  }

  return operand.value
}

function applyFilterRule (rowData: unknown, rule: CalculationFieldRule) {
  const left = getNestedValue(rowData, rule.field)
  const right = resolveOperandValue(rowData, rule.rightOperand)

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
      return rule.rightOperand.kind === 'literal' && Array.isArray(right) ? right.includes(left) : false
    default:
      return false
  }
}

function matchesFieldRules (rowData: unknown, fieldRules: CalculationFieldRule[]) {
  if (fieldRules.length === 0) return true
  return fieldRules.every((rule) => applyFilterRule(rowData, rule))
}

function matchesFieldRuleNode (rowData: unknown, node: CalculationRuleNode): boolean {
  if (node.type === 'rule') {
    return applyFilterRule(rowData, node.rule)
  }

  if (node.children.length === 0) {
    return true
  }

  if (node.op === 'or') {
    return node.children.some((child) => matchesFieldRuleNode(rowData, child))
  }

  return node.children.every((child) => matchesFieldRuleNode(rowData, child))
}

function filterRowsForDefinition (
  rows: Array<SourceRowInput>,
  filters: CalculationFilters,
  snapshotAt: number
) {
  const resolvedFilters = resolveCalculationFilters(filters, snapshotAt)
  const filteredRows = rows.filter((row) => {
    if (
      resolvedFilters.timeRange &&
      (row.occurredAt < resolvedFilters.timeRange.startMs || row.occurredAt > resolvedFilters.timeRange.endMs)
    ) {
      return false
    }

    if (resolvedFilters.fieldRuleTree.children.length > 0) {
      return matchesFieldRuleNode(row.rowData, resolvedFilters.fieldRuleTree)
    }

    return matchesFieldRules(row.rowData, resolvedFilters.fieldRules)
  })

  return {
    filteredRows,
    resolvedFilters,
  }
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
  const raw = (
    typeof value === 'string'
      ? value
      : typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint'
        ? String(value)
        : JSON.stringify(value)
  ).replace(/\r?\n/g, ' ')
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
  rows: Array<SourceRowInput>,
  snapshotAt: number
): CalculationResult {
  const { filteredRows, resolvedFilters } = filterRowsForDefinition(
    rows,
    normalizeCalculationFilters(definition.filters),
    snapshotAt
  )
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
    resolvedFilters,
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
    schedulePreset: dataSource.schedulePreset,
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
      const normalizedValue = (
        value == null
          ? ''
          : typeof value === 'string'
            ? value
            : typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint'
              ? String(value)
              : JSON.stringify(value)
      )
      return fieldFilter.values.includes(normalizedValue)
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

function normalizeCatalogOptions (options: CatalogOption[]) {
  return options
    .map((option) => ({
      key: option.key.trim(),
      label: option.label.trim() || option.key.trim(),
    }))
    .filter((option) => option.key.length > 0)
}

function normalizeFieldCatalog (fieldCatalog: FieldCatalogItem[]) {
  return fieldCatalog
    .map((field) => ({
      key: field.key.trim(),
      label: field.label.trim() || field.key.trim(),
      valueType: field.valueType.trim(),
      filterable: field.filterable,
    }))
    .filter((field) => field.key.length > 0 && field.valueType.length > 0)
}

function ensureUniqueKeys (label: string, values: string[]) {
  const uniqueValues = new Set(values)
  if (uniqueValues.size !== values.length) {
    throw new Error(`Sono presenti chiavi duplicate in ${label}`)
  }
}

function validateDataSourceSettingInput (args: {
  entityType: string
  label: string
  databaseKey?: string
  tableName?: string
  tableKey?: string
  tableLabel?: string
  allowedScopes: CatalogOption[]
  allowedRowKeyStrategies: CatalogOption[]
  idFieldSuggestions?: CatalogOption[]
  indexSuggestions?: Array<{ key: string, label: string, fields: string[] }>
  defaultScopeKey?: string
  defaultRowKeyStrategy?: string
  defaultDateFieldKey?: string
  defaultSelectedFieldKeys: string[]
  fieldCatalog: FieldCatalogItem[]
}) {
  if (!args.entityType.trim()) {
    throw new Error('entityType obbligatorio')
  }
  if (!args.label.trim()) {
    throw new Error('label obbligatoria')
  }
  if (!(args.databaseKey ?? 'app').trim()) {
    throw new Error('databaseKey obbligatorio')
  }
  if (!(args.tableName ?? args.entityType).trim()) {
    throw new Error('tableName obbligatorio')
  }
  if (!(args.tableKey ?? args.entityType).trim()) {
    throw new Error('tableKey obbligatorio')
  }
  if (!(args.tableLabel ?? args.label).trim()) {
    throw new Error('tableLabel obbligatoria')
  }

  const allowedScopes = normalizeCatalogOptions(args.allowedScopes)
  const allowedRowKeyStrategies = normalizeCatalogOptions(args.allowedRowKeyStrategies)
  const fieldCatalog = normalizeFieldCatalog(args.fieldCatalog)

  if (allowedScopes.length === 0) {
    throw new Error('Configura almeno uno scope consentito')
  }
  if (allowedRowKeyStrategies.length === 0) {
    throw new Error('Configura almeno una row key strategy consentita')
  }
  if (fieldCatalog.length === 0) {
    throw new Error('Configura almeno un campo nel field catalog')
  }
  if (args.defaultSelectedFieldKeys.length === 0) {
    throw new Error('Configura almeno un campo selezionato di default')
  }

  const scopeKeys = allowedScopes.map((option) => option.key)
  const rowKeyKeys = allowedRowKeyStrategies.map((option) => option.key)
  const fieldKeys = fieldCatalog.map((field) => field.key)
  const idFieldKeys = normalizeCatalogOptions(args.idFieldSuggestions ?? []).map((option) => option.key)

  ensureUniqueKeys('allowedScopes', scopeKeys)
  ensureUniqueKeys('allowedRowKeyStrategies', rowKeyKeys)
  ensureUniqueKeys('fieldCatalog', fieldKeys)
  ensureUniqueKeys('defaultSelectedFieldKeys', args.defaultSelectedFieldKeys)
  ensureUniqueKeys('idFieldSuggestions', idFieldKeys)

  if (args.defaultScopeKey && !scopeKeys.includes(args.defaultScopeKey)) {
    throw new Error(`defaultScopeKey non valido: ${args.defaultScopeKey}`)
  }
  if (args.defaultRowKeyStrategy && !rowKeyKeys.includes(args.defaultRowKeyStrategy)) {
    throw new Error(`defaultRowKeyStrategy non valida: ${args.defaultRowKeyStrategy}`)
  }
  if (args.defaultDateFieldKey && !fieldKeys.includes(args.defaultDateFieldKey)) {
    throw new Error(`defaultDateFieldKey non valido: ${args.defaultDateFieldKey}`)
  }
  for (const fieldKey of args.defaultSelectedFieldKeys) {
    if (!fieldKeys.includes(fieldKey)) {
      throw new Error(`Campo di default non valido: ${fieldKey}`)
    }
  }
  for (const fieldKey of idFieldKeys) {
    if (!fieldKeys.includes(fieldKey)) {
      throw new Error(`Campo identificativo non valido: ${fieldKey}`)
    }
  }
}

async function getDataSourceSettingByEntityType (ctx: any, entityType?: string) {
  if (!entityType) {
    return null
  }
  const dataSourceSetting = await ctx.db
    .query('dataSourceSettings')
    .withIndex('by_entity_type', (q: any) => q.eq('entityType', entityType))
    .unique()
  if (!dataSourceSetting || dataSourceSetting.archivedAt) {
    return null
  }
  return dataSourceSetting
}

function validateDataSourceSelection (args: {
  entityType: string
  scopeKind: string
  selectedFieldKeys: string[]
  dateFieldKey?: string
  materializationIndexKey?: string
  rowKeyStrategy?: string
  dataSourceSetting: DataSourceSettingDoc
}) {
  const allowedScopeKeys = new Set(args.dataSourceSetting.allowedScopes.map((scope) => scope.key))
  const allowedRowKeyKeys = new Set(args.dataSourceSetting.allowedRowKeyStrategies.map((rowKey) => rowKey.key))
  const validFieldKeys = new Set(args.dataSourceSetting.fieldCatalog.map((field) => field.key))
  const validMaterializationIndexKeys = new Set(
    (args.dataSourceSetting.indexSuggestions ?? [])
      .filter((index) => index.fields.length === 1 && index.fields[0] === args.dateFieldKey)
      .map((index) => index.key)
  )

  if (!allowedScopeKeys.has(args.scopeKind)) {
    throw new Error(`Scope non supportato per ${args.entityType}: ${args.scopeKind}`)
  }
  if (!args.rowKeyStrategy || !allowedRowKeyKeys.has(args.rowKeyStrategy)) {
    throw new Error(`Row key strategy non valida per ${args.entityType}`)
  }
  if (args.selectedFieldKeys.length === 0) {
    throw new Error('Seleziona almeno un campo per la data source')
  }
  for (const selectedFieldKey of args.selectedFieldKeys) {
    if (!validFieldKeys.has(selectedFieldKey)) {
      throw new Error(`Campo non supportato per ${args.entityType}: ${selectedFieldKey}`)
    }
  }
  if (args.dateFieldKey && !validFieldKeys.has(args.dateFieldKey)) {
    throw new Error(`Campo data non supportato per ${args.entityType}: ${args.dateFieldKey}`)
  }
  if (args.materializationIndexKey && !args.dateFieldKey) {
    throw new Error(`Seleziona un campo data prima di configurare l'indice di materializzazione per ${args.entityType}`)
  }
  if (args.materializationIndexKey && !validMaterializationIndexKeys.has(args.materializationIndexKey)) {
    throw new Error(
      `Indice di materializzazione non valido per ${args.entityType}: ${args.materializationIndexKey}`
    )
  }
}

async function getProfileBySlug (ctx: any, slug: string) {
  const profile = await ctx.db
    .query('snapshotProfiles')
    .withIndex('by_slug', (q: any) => q.eq('slug', slug))
    .unique()
  if (!profile || profile.archivedAt) {
    throw new Error(`Snapshot profile '${slug}' non trovato`)
  }
  return profile
}

async function getDataSourceByKey (ctx: any, sourceKey: string) {
  const dataSource = await ctx.db
    .query('dataSources')
    .withIndex('by_source_key', (q: any) => q.eq('sourceKey', sourceKey))
    .unique()
  if (!dataSource || dataSource.archivedAt) {
    return null
  }
  return dataSource
}

function slugifyProfileName (value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'profilo'
}

async function buildUniqueProfileSlug (ctx: any, value: string, excludeProfileId?: Id<'snapshotProfiles'>) {
  const baseSlug = slugifyProfileName(value)
  let candidate = baseSlug
  let attempt = 1

  while (true) {
    const existing = await ctx.db
      .query('snapshotProfiles')
      .withIndex('by_slug', (q: any) => q.eq('slug', candidate))
      .unique()

    if (!existing || existing._id === excludeProfileId) {
      return candidate
    }

    attempt += 1
    candidate = `${baseSlug}-${attempt}`
  }
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

function buildDefinitionView (
  definition: Doc<'calculationDefinitions'>,
  indicatorsById: Map<Id<'indicators'>, Doc<'indicators'>>,
  dataSourcesById: Map<Id<'dataSources'>, DataSourceDoc>
) {
  const source = dataSourcesById.get(definition.dataSourceId)

  return {
    ...definition,
    indicatorSlug: indicatorsById.get(definition.indicatorId)?.slug ?? null,
    sourceKey: source?.sourceKey ?? null,
    sourceLabel: source?.label ?? null,
    sourceSchedulePreset: source?.schedulePreset ?? null,
    sourceSchedulePresetLabel: source ? getSchedulePresetLabel(source.schedulePreset) : null,
    sourceAutomaticWindow: source ? getDefaultAutomaticWindow(source, Date.now()) : null,
  }
}

async function buildIndicatorView (
  ctx: any,
  profileId: Id<'snapshotProfiles'>,
  indicator: Doc<'indicators'>
) {
  const definitions = await ctx.db
    .query('calculationDefinitions')
    .withIndex('by_profile_and_indicator', (q: any) =>
      q.eq('profileId', profileId).eq('indicatorId', indicator._id)
    )
    .collect()

  const dataSourceIds = [...new Set(
    definitions.map((definition: Doc<'calculationDefinitions'>) => definition.dataSourceId)
  )]
  const dataSources = (await Promise.all(dataSourceIds.map(async (dataSourceId) => {
    const row = await ctx.db.get(dataSourceId)
    return row && !row.archivedAt ? row : null
  }))).filter((row): row is DataSourceDoc => row !== null)
  const dataSourcesById = new Map(dataSources.map((dataSource) => [dataSource._id, dataSource] as const))

  return {
    ...indicator,
    definitions: definitions.map((definition: Doc<'calculationDefinitions'>) => ({
      ...definition,
      sourceKey: dataSourcesById.get(definition.dataSourceId)?.sourceKey ?? null,
      sourceLabel: dataSourcesById.get(definition.dataSourceId)?.label ?? null,
      sourceSchedulePreset: dataSourcesById.get(definition.dataSourceId)?.schedulePreset ?? null,
      sourceSchedulePresetLabel: dataSourcesById.get(definition.dataSourceId)
        ? getSchedulePresetLabel(dataSourcesById.get(definition.dataSourceId)!.schedulePreset)
        : null,
    })),
    sourceSummaries: dataSources.map((dataSource) => ({
      sourceKey: dataSource.sourceKey,
      label: dataSource.label,
      schedulePreset: dataSource.schedulePreset,
      schedulePresetLabel: getSchedulePresetLabel(dataSource.schedulePreset),
      automaticWindow: getDefaultAutomaticWindow(dataSource, Date.now()),
    })),
  }
}

async function listMaterializedRowsForDataSource (
  ctx: any,
  dataSourceId: Id<'dataSources'>,
  snapshotAt?: number
) {
  const rows = snapshotAt == null
    ? await ctx.db
      .query('analyticsMaterializedRows')
      .withIndex('by_data_source', (q: any) => q.eq('dataSourceId', dataSourceId))
      .collect()
    : await ctx.db
      .query('analyticsMaterializedRows')
      .withIndex('by_data_source_and_occurred_at', (q: any) =>
        q.eq('dataSourceId', dataSourceId).lte('occurredAt', snapshotAt)
      )
      .collect()

  return rows
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
    profileId?: Id<'snapshotProfiles'>
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
    profileId: args.profileId,
    exportScope: args.profileId ? 'profile' : 'global',
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
    slug: v.optional(v.string()),
    name: v.string(),
    description: v.optional(v.string()),
    isActive: v.optional(v.boolean()),
  },
  returns: v.id('snapshotProfiles'),
  handler: async (ctx, args) => {
    const now = Date.now()
    const nextSlug = args.slug?.trim()
      ? args.slug.trim()
      : await buildUniqueProfileSlug(ctx, args.name)
    const existing = await ctx.db
      .query('snapshotProfiles')
      .withIndex('by_slug', (q) => q.eq('slug', nextSlug))
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
      slug: nextSlug,
      name: args.name,
      description: args.description,
      isActive: args.isActive ?? true,
      version: 1,
      createdAt: now,
    })
  },
})

export const archiveSnapshotProfile = mutation({
  args: {
    profileSlug: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    await ctx.db.patch(profile._id, {
      archivedAt: Date.now(),
      isActive: false,
      updatedAt: Date.now(),
    })
    return null
  },
})

export const updateSnapshotProfile = mutation({
  args: {
    currentSlug: v.string(),
    slug: v.optional(v.string()),
    name: v.optional(v.string()),
    description: v.optional(v.string()),
    isActive: v.optional(v.boolean()),
  },
  returns: v.id('snapshotProfiles'),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.currentSlug)
    const nextSlug = args.slug ?? profile.slug
    if (nextSlug !== profile.slug) {
      const existing = await ctx.db
        .query('snapshotProfiles')
        .withIndex('by_slug', (q) => q.eq('slug', nextSlug))
        .unique()
      if (existing && existing._id !== profile._id) {
        throw new Error(`Esiste gia' un profilo con slug '${nextSlug}'`)
      }
    }

    await ctx.db.patch(profile._id, {
      slug: nextSlug,
      name: args.name ?? profile.name,
      description: args.description ?? profile.description,
      isActive: args.isActive ?? profile.isActive,
      version: profile.version + 1,
      updatedAt: Date.now(),
    })
    return profile._id
  },
})

export const listProfileMembers = query({
  args: {
    profileSlug: v.string(),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const rows = await ctx.db
      .query('profileMembers')
      .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
      .collect()
    return rows.filter((row) => row.isActive)
  },
})

export const upsertProfileMember = mutation({
  args: {
    profileSlug: v.string(),
    memberKey: v.string(),
    assignedBy: v.optional(v.string()),
    isActive: v.optional(v.boolean()),
  },
  returns: v.id('profileMembers'),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const now = Date.now()
    const existing = await ctx.db
      .query('profileMembers')
      .withIndex('by_profile_and_member_key', (q) =>
        q.eq('profileId', profile._id).eq('memberKey', args.memberKey)
      )
      .unique()

    if (existing) {
      await ctx.db.patch(existing._id, {
        assignedBy: args.assignedBy,
        isActive: args.isActive ?? true,
        updatedAt: now,
      })
      return existing._id
    }

    return await ctx.db.insert('profileMembers', {
      profileId: profile._id,
      memberKey: args.memberKey,
      assignedBy: args.assignedBy,
      isActive: args.isActive ?? true,
      createdAt: now,
      updatedAt: now,
    })
  },
})

export const removeProfileMember = mutation({
  args: {
    profileSlug: v.string(),
    memberKey: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const existing = await ctx.db
      .query('profileMembers')
      .withIndex('by_profile_and_member_key', (q) =>
        q.eq('profileId', profile._id).eq('memberKey', args.memberKey)
      )
      .unique()
    if (!existing) {
      return null
    }
    await ctx.db.patch(existing._id, {
      isActive: false,
      updatedAt: Date.now(),
    })
    return null
  },
})

export const listDataSourceSettings = query({
  args: {
    includeArchived: v.optional(v.boolean()),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const rows = await ctx.db.query('dataSourceSettings').collect()
    return rows
      .filter((row) => args.includeArchived ? true : !row.archivedAt)
      .sort((left, right) => left.label.localeCompare(right.label))
  },
})

export const upsertDataSourceSetting = mutation({
  args: {
    entityType: v.string(),
    label: v.string(),
    adapterKey: v.optional(v.string()),
    sourceKind: sourceKindValidator,
    databaseKey: v.optional(v.string()),
    tableName: v.optional(v.string()),
    tableKey: v.optional(v.string()),
    tableLabel: v.optional(v.string()),
    schemaImportId: v.optional(v.id('schemaImports')),
    allowedScopes: v.array(catalogOptionValidator),
    allowedRowKeyStrategies: v.array(catalogOptionValidator),
    idFieldSuggestions: v.optional(v.array(catalogOptionValidator)),
    indexSuggestions: v.optional(v.array(schemaIndexValidator)),
    defaultScopeKey: v.optional(v.string()),
    defaultRowKeyStrategy: v.optional(v.string()),
    defaultDateFieldKey: v.optional(v.string()),
    defaultSelectedFieldKeys: v.array(v.string()),
    fieldCatalog: v.array(fieldCatalogItemValidator),
    metadata: v.optional(v.any()),
    isActive: v.optional(v.boolean()),
  },
  returns: v.id('dataSourceSettings'),
  handler: async (ctx, args) => {
    validateDataSourceSettingInput(args)

    const now = Date.now()
    const entityType = args.entityType.trim()
    const databaseKey = args.databaseKey?.trim() || 'app'
    const tableName = args.tableName?.trim() || entityType
    const tableKey = args.tableKey?.trim() || entityType
    const tableLabel = args.tableLabel?.trim() || args.label.trim()
    const existing = await ctx.db
      .query('dataSourceSettings')
      .withIndex('by_entity_type', (q) => q.eq('entityType', entityType))
      .unique()

    const normalizedAllowedScopes = normalizeCatalogOptions(args.allowedScopes)
    const normalizedAllowedRowKeyStrategies = normalizeCatalogOptions(args.allowedRowKeyStrategies)
    const normalizedIdFieldSuggestions = normalizeCatalogOptions(args.idFieldSuggestions ?? [])
    const normalizedFieldCatalog = normalizeFieldCatalog(args.fieldCatalog)
    const defaultScopeKey = args.defaultScopeKey ?? normalizedAllowedScopes[0]?.key
    const defaultRowKeyStrategy = args.defaultRowKeyStrategy ?? normalizedAllowedRowKeyStrategies[0]?.key

    if (existing) {
      await ctx.db.patch(existing._id, {
        label: args.label.trim(),
        adapterKey: args.adapterKey?.trim() || undefined,
        sourceKind: args.sourceKind,
        databaseKey,
        tableName,
        tableKey,
        tableLabel,
        schemaImportId: args.schemaImportId,
        allowedScopes: normalizedAllowedScopes,
        allowedRowKeyStrategies: normalizedAllowedRowKeyStrategies,
        idFieldSuggestions: normalizedIdFieldSuggestions,
        indexSuggestions: args.indexSuggestions ?? [],
        defaultScopeKey,
        defaultRowKeyStrategy,
        defaultDateFieldKey: args.defaultDateFieldKey,
        defaultSelectedFieldKeys: args.defaultSelectedFieldKeys,
        fieldCatalog: normalizedFieldCatalog,
        metadata: args.metadata,
        isActive: args.isActive ?? existing.isActive,
        archivedAt: undefined,
        updatedAt: now,
      })
      return existing._id
    }

    return await ctx.db.insert('dataSourceSettings', {
      entityType,
      label: args.label.trim(),
      adapterKey: args.adapterKey?.trim() || undefined,
      sourceKind: args.sourceKind,
      databaseKey,
      tableName,
      tableKey,
      tableLabel,
      schemaImportId: args.schemaImportId,
      allowedScopes: normalizedAllowedScopes,
      allowedRowKeyStrategies: normalizedAllowedRowKeyStrategies,
      idFieldSuggestions: normalizedIdFieldSuggestions,
      indexSuggestions: args.indexSuggestions ?? [],
      defaultScopeKey,
      defaultRowKeyStrategy,
      defaultDateFieldKey: args.defaultDateFieldKey,
      defaultSelectedFieldKeys: args.defaultSelectedFieldKeys,
      fieldCatalog: normalizedFieldCatalog,
      metadata: args.metadata,
      isActive: args.isActive ?? true,
      createdAt: now,
      updatedAt: now,
    })
  },
})

export const archiveDataSourceSetting = mutation({
  args: {
    entityType: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const existing = await ctx.db
      .query('dataSourceSettings')
      .withIndex('by_entity_type', (q) => q.eq('entityType', args.entityType))
      .unique()
    if (!existing) {
      return null
    }
    await ctx.db.patch(existing._id, {
      archivedAt: Date.now(),
      isActive: false,
      updatedAt: Date.now(),
    })
    return null
  },
})

export const listSnapshotProfiles = query({
  args: {
    includeArchived: v.optional(v.boolean()),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const rows = await ctx.db.query('snapshotProfiles').collect()
    return rows
      .filter((row) => args.includeArchived ? true : !row.archivedAt)
      .sort((left, right) => left.name.localeCompare(right.name))
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
    return filteredRows
      .sort((left, right) => (right.updatedAt ?? right.createdAt) - (left.updatedAt ?? left.createdAt))
      .map((row) => buildDataSourceView(row))
  },
})

export const upsertDataSource = mutation({
  args: {
    profileSlug: v.optional(v.string()),
    sourceKey: v.string(),
    label: v.string(),
    adapterKey: v.optional(v.string()),
    sourceKind: v.optional(sourceKindValidator),
    entityType: v.string(),
    scopeDefinition: v.optional(v.any()),
    selectedFieldKeys: v.optional(v.array(v.string())),
    dateFieldKey: v.optional(v.string()),
    materializationIndexKey: v.optional(v.string()),
    rowKeyStrategy: v.optional(v.string()),
    schedulePreset: schedulePresetValidator,
    fieldCatalog: v.optional(v.array(fieldCatalogItemValidator)),
    metadata: v.optional(v.any()),
    enabled: v.optional(v.boolean()),
  },
  returns: v.id('dataSources'),
  handler: async (ctx, args) => {
    const now = Date.now()
    const dataSourceSetting = await getDataSourceSettingByEntityType(ctx, args.entityType)
    if (!dataSourceSetting || !dataSourceSetting.isActive) {
      throw new Error(`Catalogo data source non trovato o inattivo per '${args.entityType}'`)
    }
    const existing = await ctx.db
      .query('dataSources')
      .withIndex('by_source_key', (q) => q.eq('sourceKey', args.sourceKey))
      .unique()

    const nextScopeKind = args.scopeDefinition?.kind
      ?? existing?.scopeDefinition?.kind
      ?? dataSourceSetting.defaultScopeKey
    const nextSelectedFieldKeys = args.selectedFieldKeys
      ?? existing?.selectedFieldKeys
      ?? dataSourceSetting.defaultSelectedFieldKeys
    const nextDateFieldKey = args.dateFieldKey
      ?? existing?.dateFieldKey
      ?? dataSourceSetting.defaultDateFieldKey
    const nextMaterializationIndexKey = args.materializationIndexKey !== undefined
      ? args.materializationIndexKey.trim() || undefined
      : existing?.materializationIndexKey
    const nextRowKeyStrategy = args.rowKeyStrategy
      ?? existing?.rowKeyStrategy
      ?? dataSourceSetting.defaultRowKeyStrategy

    if (!nextScopeKind) {
      throw new Error(`Scope di default mancante per '${args.entityType}'`)
    }

    validateDataSourceSelection({
      entityType: args.entityType,
      scopeKind: nextScopeKind,
      selectedFieldKeys: nextSelectedFieldKeys,
      dateFieldKey: nextDateFieldKey,
      materializationIndexKey: nextMaterializationIndexKey,
      rowKeyStrategy: nextRowKeyStrategy,
      dataSourceSetting,
    })

    const nextPatch = {
      label: args.label,
      adapterKey: dataSourceSetting.adapterKey ?? args.adapterKey,
      sourceKind: 'materialized_rows' as const,
      entityType: args.entityType,
      databaseKey: dataSourceSetting.databaseKey,
      tableName: dataSourceSetting.tableName,
      tableKey: dataSourceSetting.tableKey,
      scopeDefinition: { kind: nextScopeKind },
      selectedFieldKeys: nextSelectedFieldKeys,
      dateFieldKey: nextDateFieldKey,
      materializationIndexKey: nextMaterializationIndexKey,
      rowKeyStrategy: nextRowKeyStrategy,
      schedulePreset: args.schedulePreset,
      fieldCatalog: dataSourceSetting.fieldCatalog,
      metadata: args.metadata,
      enabled: args.enabled ?? existing?.enabled ?? true,
      updatedAt: now,
    }

    if (existing) {
      await ctx.db.patch(existing._id, nextPatch)
      return existing._id
    }

    return await ctx.db.insert('dataSources', {
      sourceKey: args.sourceKey,
      label: nextPatch.label,
      adapterKey: nextPatch.adapterKey,
      sourceKind: nextPatch.sourceKind,
      entityType: nextPatch.entityType,
      databaseKey: nextPatch.databaseKey,
      tableName: nextPatch.tableName,
      tableKey: nextPatch.tableKey,
      scopeDefinition: nextPatch.scopeDefinition,
      selectedFieldKeys: nextPatch.selectedFieldKeys,
      dateFieldKey: nextPatch.dateFieldKey,
      materializationIndexKey: nextPatch.materializationIndexKey,
      rowKeyStrategy: nextPatch.rowKeyStrategy,
      schedulePreset: nextPatch.schedulePreset,
      fieldCatalog: nextPatch.fieldCatalog,
      metadata: nextPatch.metadata,
      enabled: nextPatch.enabled,
      status: 'idle',
      materializedCount: 0,
      createdAt: now,
      updatedAt: now,
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
        const rawOptions: string[] = rows
          .map((row: { rowData: Record<string, unknown> }) => row.rowData[fieldKey])
          .filter((value: unknown): value is string | number | boolean => value != null && typeof value !== 'object')
          .map((value: string | number | boolean) => String(value))
        const options = Array.from(new Set(rawOptions))
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
    profileSlug: v.optional(v.string()),
    requestedBy: v.optional(v.string()),
    name: v.optional(v.string()),
    dataSourceKey: v.string(),
    filters: exportFiltersValidator,
    clonedFromExportId: v.optional(v.string()),
  },
  returns: v.string(),
  handler: async (ctx, args) => {
    const profile = args.profileSlug
      ? await getProfileBySlug(ctx, args.profileSlug)
      : null
    const dataSource = await getDataSourceByKey(ctx, args.dataSourceKey)
    if (!dataSource || !dataSource.enabled) {
      throw new Error('Data source non disponibile')
    }
    if (dataSource.status !== 'ready') {
      throw new Error('Rigenera prima la materializzazione della data source')
    }

    const effectiveFilters = clampExportFiltersToDataSourceWindow(dataSource, args.filters)
    const rows = applyExportFilters(
      (await listMaterializedRowsForDataSource(ctx, dataSource._id)).map((row: {
        occurredAt: number
        rowData: Record<string, unknown>
      }) => ({
        occurredAt: row.occurredAt,
        rowData: row.rowData,
      })),
      effectiveFilters
    )
    const exportId = await createCompletedExport(ctx, {
      profileId: profile?._id,
      requestedBy: args.requestedBy,
      name: args.name,
      dataSource,
      rows,
      filters: effectiveFilters,
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

    const effectiveFilters = clampExportFiltersToDataSourceWindow(
      dataSource,
      exportRow.filters as ExportFilters | undefined
    )
    const rows = applyExportFilters(
      (await listMaterializedRowsForDataSource(ctx, dataSource._id)).map((row: {
        occurredAt: number
        rowData: Record<string, unknown>
      }) => ({
        occurredAt: row.occurredAt,
        rowData: row.rowData,
      })),
      effectiveFilters
    )

    const newExportId = await createCompletedExport(ctx, {
      profileId: exportRow.profileId,
      requestedBy: args.requestedBy ?? exportRow.requestedBy,
      name: args.name ?? exportRow.name,
      dataSource,
      rows,
      filters: effectiveFilters,
      usageKind: 'manual',
      pinnedByAudit: false,
      regeneratedFromExportId: exportRow._id,
    })

    return String(newExportId)
  },
})

export const listExports = query({
  args: {
    profileSlug: v.optional(v.string()),
    includeGlobal: v.optional(v.boolean()),
    requestedBy: v.optional(v.string()),
    includePinned: v.optional(v.boolean()),
    limit: v.optional(v.number()),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const limit = Math.max(1, Math.min(args.limit ?? 50, 200))
    const rows = await ctx.db.query('analyticsExports').order('desc').take(limit * 3)
    const profile = args.profileSlug
      ? await getProfileBySlug(ctx, args.profileSlug)
      : null
    return rows
      .filter((row) => args.includePinned ? true : !row.pinnedByAudit)
      .filter((row) => args.requestedBy ? row.requestedBy === args.requestedBy : true)
      .filter((row) => {
        if (!profile) {
          return true
        }
        if (row.profileId === profile._id) {
          return true
        }
        return args.includeGlobal === false ? false : row.exportScope === 'global'
      })
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
    const indicator = await getIndicatorByProfileAndSlug(ctx, profile._id, args.slug)
    if (!indicator) {
      return null
    }
    return await buildIndicatorView(ctx, profile._id, indicator)
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
    filters: calculationFiltersValidator,
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
    const dataSource = await getDataSourceByKey(ctx, args.sourceKey)
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
    const normalizedFilters = normalizeCalculationFilters(args.filters)

    if (existing) {
      await ctx.db.patch(existing._id, {
        filters: normalizedFilters,
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
      filters: normalizedFilters,
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
        filters: calculationFiltersValidator,
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
      const dataSource = await getDataSourceByKey(ctx, definition.sourceKey)
      if (!dataSource) {
        throw new Error(`Data source '${definition.sourceKey}' non trovata`)
      }
      const normalizedFilters = normalizeCalculationFilters(definition.filters)
      await ctx.db.insert('calculationDefinitions', {
        profileId: profile._id,
        indicatorId: indicator._id,
        dataSourceId: dataSource._id,
        operation: definition.operation,
        fieldPath: definition.fieldPath,
        filters: normalizedFilters,
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
    const [indicators, allDataSources, definitions, derivedIndicators] = await Promise.all([
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('dataSources').collect(),
      ctx.db.query('calculationDefinitions').withIndex('by_profile_and_priority', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('derivedIndicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
    ])
    const dataSources = allDataSources.filter((row) => !row.archivedAt)

    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]))
    const dataSourcesById = new Map(dataSources.map((dataSource) => [dataSource._id, dataSource]))

    return {
      profile,
      indicators: await Promise.all(indicators.map(async (indicator) => (
        await buildIndicatorView(ctx, profile._id, indicator)
      ))),
      dataSources: dataSources.map((dataSource) => buildDataSourceView(dataSource)),
      derivedIndicators,
      definitions: definitions.map((definition) => buildDefinitionView(
        definition,
        indicatorsById,
        dataSourcesById
      )),
    }
  },
})

export const listProfileDataSources = query({
  args: {
    profileSlug: v.string(),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    await getProfileBySlug(ctx, args.profileSlug)
    const rows = await ctx.db.query('dataSources').collect()
    return rows
      .filter((row) => !row.archivedAt)
      .sort((left, right) => left.label.localeCompare(right.label))
      .map((row) => buildDataSourceView(row))
  },
})

export const listScheduledRefreshTargets = query({
  args: {
    schedulePreset: v.union(
      v.literal('daily'),
      v.literal('weekly_monday'),
      v.literal('monthly_first_day')
    ),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const [dataSources, profiles, definitions] = await Promise.all([
      ctx.db.query('dataSources').collect(),
      ctx.db.query('snapshotProfiles').collect(),
      ctx.db.query('calculationDefinitions').collect(),
    ])
    const activeProfiles = profiles.filter((profile) => !profile.archivedAt)
    const activeDataSources = dataSources.filter((dataSource) => (
      !dataSource.archivedAt &&
      dataSource.enabled &&
      dataSource.schedulePreset === args.schedulePreset
    ))

    const profileSlugsById = new Map(activeProfiles.map((profile) => [profile._id, profile.slug] as const))
    const profileSlugsByDataSourceId = new Map<Id<'dataSources'>, string[]>()

    for (const definition of definitions) {
      const profileSlug = profileSlugsById.get(definition.profileId)
      if (!profileSlug) {
        continue
      }
      const currentProfileSlugs = profileSlugsByDataSourceId.get(definition.dataSourceId) ?? []
      if (!currentProfileSlugs.includes(profileSlug)) {
        currentProfileSlugs.push(profileSlug)
      }
      profileSlugsByDataSourceId.set(definition.dataSourceId, currentProfileSlugs)
    }

    return activeDataSources.map((dataSource) => ({
      ...buildDataSourceView(dataSource),
      profileSlugs: profileSlugsByDataSourceId.get(dataSource._id) ?? [],
    }))
  },
})

export const listProfileSlugsBySourceKey = query({
  args: {
    sourceKey: v.string(),
  },
  returns: v.array(v.string()),
  handler: async (ctx, args) => {
    const dataSource = await getDataSourceByKey(ctx, args.sourceKey)
    if (!dataSource || dataSource.archivedAt) {
      return []
    }

    const [profiles, definitions] = await Promise.all([
      ctx.db.query('snapshotProfiles').collect(),
      ctx.db.query('calculationDefinitions').withIndex('by_data_source', (q) => q.eq('dataSourceId', dataSource._id)).collect(),
    ])
    const activeProfilesById = new Map(
      profiles
        .filter((profile) => !profile.archivedAt)
        .map((profile) => [profile._id, profile.slug] as const)
    )

    return [...new Set(definitions
      .map((definition) => activeProfilesById.get(definition.profileId))
      .filter((profileSlug): profileSlug is string => Boolean(profileSlug))
    )]
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
    const [definitions, indicators, allDataSources] = await Promise.all([
      ctx.db.query('calculationDefinitions').withIndex('by_profile_and_priority', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('dataSources').collect(),
    ])
    const dataSources = allDataSources.filter((row) => !row.archivedAt)
    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]))
    const dataSourcesById = new Map(dataSources.map((source) => [source._id, source]))
    const enabledDefinitions = definitions.filter((definition) => definition.enabled)
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
      const result = runSingleDefinition(definition, rows, snapshotAt)
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

    const [definitions, indicators, allDataSources, derivedIndicators] = await Promise.all([
      ctx.db.query('calculationDefinitions').withIndex('by_profile_and_priority', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('dataSources').collect(),
      ctx.db.query('derivedIndicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
    ])
    const dataSources = allDataSources.filter((row) => !row.archivedAt)
    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]))
    const dataSourcesById = new Map(dataSources.map((source) => [source._id, source]))
    const enabledDefinitions = definitions.filter((definition) => {
      if (!definition.enabled) {
        return false
      }
      if (!args.triggerSourceKey) {
        return true
      }
      return dataSourcesById.get(definition.dataSourceId)?.sourceKey === args.triggerSourceKey
    })

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
        const result = runSingleDefinition(definition, dataSourceRows, snapshotAt)
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
          resolvedFilters: result.resolvedFilters,
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

export const getLatestSnapshotValuesForProfile = query({
  args: {
    profileSlug: v.string(),
  },
  returns: v.object({
    snapshotId: v.union(v.string(), v.null()),
    snapshotAt: v.union(v.number(), v.null()),
    values: v.array(v.any()),
  }),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const [indicators, derivedIndicators] = await Promise.all([
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('derivedIndicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
    ])

    const latestBaseRows = (await Promise.all(indicators.map(async (indicator) => {
      const rows = await ctx.db
        .query('snapshotValues')
        .withIndex('by_indicator', (q) => q.eq('indicatorId', indicator._id))
        .order('desc')
        .take(1)
      const valueRow = rows[0]
      if (!valueRow || valueRow.profileId !== profile._id) {
        return null
      }
      return {
        ...valueRow,
        indicatorSlug: indicator.slug,
        indicatorUnit: indicator.unit ?? null,
        isDerived: false,
        sourceExportCount: valueRow.sourceExportIds.length,
        derivedFromIndicatorSlugs: [],
        formulaKind: null,
      }
    }))).filter((row) => row !== null)

    const latestDerivedRows = (await Promise.all(derivedIndicators.map(async (indicator) => {
      const rows = await ctx.db
        .query('derivedSnapshotValues')
        .withIndex('by_derived_indicator', (q) => q.eq('derivedIndicatorId', indicator._id))
        .order('desc')
        .take(1)
      const row = rows[0]
      if (!row || row.profileId !== profile._id) {
        return null
      }
      return {
        _id: `derived:${row._id}`,
        indicatorLabelSnapshot: row.derivedIndicatorLabelSnapshot,
        indicatorUnit: row.derivedIndicatorUnit ?? null,
        indicatorSlug: row.derivedIndicatorSlug,
        value: row.value,
        computedAt: row.computedAt,
        evidenceRowCount: row.baseSnapshotValueIds.length,
        isDerived: true,
        sourceExportCount: row.sourceExportIds.length,
        derivedFromIndicatorSlugs: row.baseIndicatorSlugs,
        formulaKind: row.formulaKind,
        snapshotId: String(row.snapshotId),
      }
    }))).filter((row) => row !== null)

    const values = [...latestBaseRows, ...latestDerivedRows]
      .sort((left, right) => (right.computedAt ?? 0) - (left.computedAt ?? 0))

    return {
      snapshotId: values[0]?.snapshotId ? String(values[0].snapshotId) : null,
      snapshotAt: values[0]?.computedAt ?? null,
      values,
    }
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
