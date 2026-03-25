import { v } from 'convex/values'
import { api, internal } from './_generated/api.js'
import { action, internalAction, internalMutation, internalQuery, mutation, query } from './_generated/server.js'
import type { Doc, Id } from './_generated/dataModel.js'
import type { MutationCtx } from './_generated/server.js'
import {
  calculationFiltersValidator,
  normalizeCalculationFilters,
  resolveCalculationFilters,
} from './lib/calculationFilters.js'
import {
  derivedFormulaValidator,
  getDerivedFormulaKind,
  isExpressionDerivedFormula,
  isLegacyDerivedFormula,
  listDirectFormulaDependencies,
} from '../shared/derivedFormula.js'
import type {
  CalculationFieldRule,
  CalculationFilters,
  CalculationRuleNode,
  ResolvedCalculationFilters,
} from './lib/calculationFilters.js'
import type {
  DerivedExpressionNode,
  DerivedFormula,
  ExpressionDerivedFormula,
  LegacyDerivedFormula,
} from '../shared/derivedFormula.js'

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

const snapshotStatusValidator = v.union(
  v.literal('queued'),
  v.literal('loading'),
  v.literal('processing'),
  v.literal('deriving'),
  v.literal('freezing'),
  v.literal('completed'),
  v.literal('error')
)

const snapshotSourceStatusValidator = v.union(
  v.literal('pending'),
  v.literal('processing'),
  v.literal('completed'),
  v.literal('error')
)

const snapshotItemStatusValidator = v.union(
  v.literal('success'),
  v.literal('skipped'),
  v.literal('error')
)

type Operation = 'sum' | 'count' | 'avg' | 'min' | 'max' | 'distinct_count'
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
  snapshotValueId: Id<'snapshotValues'> | Id<'derivedSnapshotValues'>
  snapshotValueKind: 'base' | 'derived'
  indicatorSlug: string
  indicatorVersion: number
  value: number
  sourceExportIds: Array<Id<'analyticsExports'>>
}

type DataSourceDoc = Doc<'dataSources'>
type DataSourceSettingDoc = Doc<'dataSourceSettings'>

function buildIndicatorsById (indicators: Array<Doc<'indicators'>>) {
  return new Map(indicators.map((indicator) => [indicator._id, indicator] as const))
}

function buildLatestRowsBySlug<T extends { slug: string, version: number }> (rows: T[]) {
  const rowsBySlug = new Map<string, T>()
  for (const row of rows) {
    const existing = rowsBySlug.get(row.slug)
    if (!existing || row.version > existing.version) {
      rowsBySlug.set(row.slug, row)
    }
  }
  return rowsBySlug
}

function listLatestRowsBySlug<T extends { slug: string, version: number }> (rows: T[]) {
  return [...buildLatestRowsBySlug(rows).values()]
}

function buildScopedSlugKey (profileId: Id<'snapshotProfiles'>, slug: string) {
  return `${String(profileId)}:${slug}`
}

function buildLatestRowsByProfileAndSlug<
  T extends { profileId: Id<'snapshotProfiles'>, slug: string, version: number }
> (rows: T[]) {
  const rowsByScopedSlug = new Map<string, T>()
  for (const row of rows) {
    const key = buildScopedSlugKey(row.profileId, row.slug)
    const existing = rowsByScopedSlug.get(key)
    if (!existing || row.version > existing.version) {
      rowsByScopedSlug.set(key, row)
    }
  }
  return rowsByScopedSlug
}

function buildIndicatorsBySlug (indicators: Array<Doc<'indicators'>>) {
  return buildLatestRowsBySlug(indicators)
}

function filterDefinitionsForCurrentIndicators (
  definitions: Array<Doc<'calculationDefinitions'>>,
  currentIndicatorsBySlug: Map<string, Doc<'indicators'>>
) {
  const currentIndicatorIds = new Set(
    [...currentIndicatorsBySlug.values()].map((indicator) => String(indicator._id))
  )
  return definitions.filter((definition) => currentIndicatorIds.has(String(definition.indicatorId)))
}

function isIndicatorEnabled (indicator?: Doc<'indicators'> | null) {
  return indicator?.enabled === true
}

function filterEnabledDefinitions (
  definitions: Array<Doc<'calculationDefinitions'>>,
  indicatorsById: Map<Id<'indicators'>, Doc<'indicators'>>
) {
  return definitions.filter((definition) => (
    definition.enabled &&
    isIndicatorEnabled(indicatorsById.get(definition.indicatorId))
  ))
}

function hasInactiveDerivedOperands (
  formula: Doc<'derivedIndicators'>['formula'],
  indicatorsBySlug: Map<string, Doc<'indicators'>>,
  derivedIndicatorsBySlug: Map<string, Doc<'derivedIndicators'>>
) {
  return listDirectFormulaDependencies(formula).some((dependency) => {
    if (dependency.indicatorKind === 'base') {
      const indicator = indicatorsBySlug.get(dependency.indicatorSlug)
      return indicator != null && !indicator.enabled
    }

    const indicator = derivedIndicatorsBySlug.get(dependency.indicatorSlug)
    return indicator != null && !indicator.enabled
  })
}

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

function normalizeReportUsageCount (value?: number) {
  return Math.max(0, Math.trunc(value ?? 0))
}

function sortIndicatorsByReportUsage<T extends { label: string, reportUsageCount?: number }> (rows: T[]) {
  return [...rows].sort((left, right) => {
    const usageDelta = normalizeReportUsageCount(right.reportUsageCount) - normalizeReportUsageCount(left.reportUsageCount)
    if (usageDelta !== 0) {
      return usageDelta
    }

    return left.label.localeCompare(right.label)
  })
}

function buildReportUsageKey (
  indicatorKind: 'base' | 'derived',
  sourceProfileId: Id<'snapshotProfiles'>,
  indicatorSlug: string
) {
  return `${indicatorKind}:${sourceProfileId}:${indicatorSlug}`
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

function isDerivedSnapshotValueId (snapshotValueId: string) {
  return snapshotValueId.startsWith('derived:')
}

function parseDerivedSnapshotValueId (snapshotValueId: string) {
  return snapshotValueId.replace(/^derived:/, '') as Id<'derivedSnapshotValues'>
}

function buildDerivedComputationKey (
  indicatorKind: 'base' | 'derived',
  indicatorSlug: string
) {
  return `${indicatorKind}:${indicatorSlug}`
}

function dedupeDerivedComputationInputs (inputs: DerivedComputationInput[]) {
  const seen = new Set<string>()
  const deduped: DerivedComputationInput[] = []

  for (const input of inputs) {
    const key = `${input.snapshotValueKind}:${String(input.snapshotValueId)}`
    if (seen.has(key)) {
      continue
    }
    seen.add(key)
    deduped.push(input)
  }

  return deduped
}

function buildExpressionNodeMap (formula: ExpressionDerivedFormula) {
  return new Map(formula.nodes.map((node) => [node.id, node] as const))
}

function visitExpressionNode (
  nodeId: string,
  nodesById: Map<string, DerivedExpressionNode>,
  stateByNodeId: Map<string, 'visiting' | 'visited'>,
  reachableNodeIds: Set<string>
) {
  const currentState = stateByNodeId.get(nodeId)
  if (currentState === 'visiting') {
    throw new Error('La formula derivata contiene un ciclo interno tra nodi dell\'espressione')
  }
  if (currentState === 'visited') {
    reachableNodeIds.add(nodeId)
    return
  }

  const node = nodesById.get(nodeId)
  if (!node) {
    throw new Error(`Nodo formula '${nodeId}' non trovato`)
  }

  stateByNodeId.set(nodeId, 'visiting')

  if (node.type === 'operation') {
    visitExpressionNode(node.leftNodeId, nodesById, stateByNodeId, reachableNodeIds)
    visitExpressionNode(node.rightNodeId, nodesById, stateByNodeId, reachableNodeIds)
  }

  stateByNodeId.set(nodeId, 'visited')
  reachableNodeIds.add(nodeId)
}

function validateDerivedFormula (formula: DerivedFormula) {
  if (isLegacyDerivedFormula(formula)) {
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
    return
  }

  if (formula.nodes.length === 0) {
    throw new Error('La formula expression richiede almeno un nodo')
  }

  const nodesById = buildExpressionNodeMap(formula)
  if (nodesById.size !== formula.nodes.length) {
    throw new Error('La formula expression contiene nodi duplicati')
  }
  if (!nodesById.has(formula.rootNodeId)) {
    throw new Error('Il nodo radice della formula expression non esiste')
  }

  for (const node of formula.nodes) {
    if (!node.id.trim()) {
      throw new Error('Ogni nodo della formula expression deve avere un id')
    }
    if (node.type === 'ref' && !node.indicatorSlug.trim()) {
      throw new Error('Ogni riferimento formula deve avere uno slug indicatore')
    }
    if (node.type === 'constant' && !Number.isFinite(node.value)) {
      throw new Error('Le costanti della formula devono essere numeri finiti')
    }
  }

  const reachableNodeIds = new Set<string>()
  visitExpressionNode(formula.rootNodeId, nodesById, new Map(), reachableNodeIds)

  if (reachableNodeIds.size !== nodesById.size) {
    throw new Error('La formula expression contiene nodi orfani non collegati alla radice')
  }
}

function evaluateExpressionNode (
  nodeId: string,
  nodesById: Map<string, DerivedExpressionNode>,
  valuesByIndicatorSlug: Map<string, DerivedComputationInput>,
  memoizedResults: Map<string, { value: number, inputs: DerivedComputationInput[] }>
): { value: number, inputs: DerivedComputationInput[] } | null {
  const cached = memoizedResults.get(nodeId)
  if (cached) {
    return cached
  }

  const node = nodesById.get(nodeId)
  if (!node) {
    throw new Error(`Nodo formula '${nodeId}' non trovato`)
  }

  if (node.type === 'constant') {
    const result = {
      value: node.value,
      inputs: [] as DerivedComputationInput[],
    }
    memoizedResults.set(nodeId, result)
    return result
  }

  if (node.type === 'ref') {
    const input = valuesByIndicatorSlug.get(buildDerivedComputationKey(node.indicatorKind, node.indicatorSlug))
    if (!input) {
      return null
    }
    const result = {
      value: input.value,
      inputs: [input],
    }
    memoizedResults.set(nodeId, result)
    return result
  }

  const left = evaluateExpressionNode(
    node.leftNodeId,
    nodesById,
    valuesByIndicatorSlug,
    memoizedResults
  )
  const right = evaluateExpressionNode(
    node.rightNodeId,
    nodesById,
    valuesByIndicatorSlug,
    memoizedResults
  )

  if (!left || !right) {
    return null
  }

  let value: number | null = null
  switch (node.op) {
    case 'add':
      value = left.value + right.value
      break
    case 'sub':
      value = left.value - right.value
      break
    case 'mul':
      value = left.value * right.value
      break
    case 'div':
      value = right.value === 0 ? null : left.value / right.value
      break
  }

  if (value === null) {
    return null
  }

  const result = {
    value,
    inputs: dedupeDerivedComputationInputs([...left.inputs, ...right.inputs]),
  }
  memoizedResults.set(nodeId, result)
  return result
}

function computeLegacyDerivedValue (
  formula: LegacyDerivedFormula,
  valuesByIndicatorSlug: Map<string, DerivedComputationInput>
) {
  const resolvedInputs = formula.operands.map((operand) => ({
    operand,
    input: valuesByIndicatorSlug.get(buildDerivedComputationKey('base', operand.indicatorSlug)),
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
      inputs: dedupeDerivedComputationInputs([numerator, denominator]),
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
      inputs: dedupeDerivedComputationInputs([left, right]),
    }
  }

  const inputs = resolvedInputs.map((entry) => ({
    weight: entry.operand.weight ?? 1,
    input: entry.input!,
  }))

  return {
    value: inputs.reduce((total, entry) => total + (entry.input.value * entry.weight), 0),
    inputs: dedupeDerivedComputationInputs(inputs.map((entry) => entry.input)),
  }
}

function computeDerivedValue (
  formula: DerivedFormula,
  valuesByIndicatorSlug: Map<string, DerivedComputationInput>
) {
  if (isLegacyDerivedFormula(formula)) {
    return computeLegacyDerivedValue(formula, valuesByIndicatorSlug)
  }

  return evaluateExpressionNode(
    formula.rootNodeId,
    buildExpressionNodeMap(formula),
    valuesByIndicatorSlug,
    new Map()
  )
}

function buildDerivedDependencyGraph (
  rows: Array<Pick<Doc<'derivedIndicators'>, 'slug' | 'formula'>>
) {
  return new Map(rows.map((row) => [
    row.slug,
    listDirectFormulaDependencies(row.formula)
      .filter((dependency) => dependency.indicatorKind === 'derived')
      .map((dependency) => dependency.indicatorSlug),
  ] as const))
}

function detectDerivedDependencyCycle (
  dependencyGraph: Map<string, string[]>
) {
  const stateBySlug = new Map<string, 'visiting' | 'visited'>()
  const visitStack: string[] = []

  const visit = (slug: string): string[] | null => {
    const currentState = stateBySlug.get(slug)
    if (currentState === 'visited') {
      return null
    }
    if (currentState === 'visiting') {
      const cycleStartIndex = visitStack.indexOf(slug)
      return [...visitStack.slice(cycleStartIndex), slug]
    }

    stateBySlug.set(slug, 'visiting')
    visitStack.push(slug)

    for (const dependencySlug of dependencyGraph.get(slug) ?? []) {
      if (!dependencyGraph.has(dependencySlug)) {
        continue
      }
      const cycle = visit(dependencySlug)
      if (cycle) {
        return cycle
      }
    }

    visitStack.pop()
    stateBySlug.set(slug, 'visited')
    return null
  }

  for (const slug of dependencyGraph.keys()) {
    const cycle = visit(slug)
    if (cycle) {
      return cycle
    }
  }

  return null
}

function topologicallySortDerivedIndicators (
  rows: Array<Doc<'derivedIndicators'>>
) {
  const rowsBySlug = new Map(rows.map((row) => [row.slug, row] as const))
  const dependencyGraph = buildDerivedDependencyGraph(rows)
  const stateBySlug = new Map<string, 'visiting' | 'visited'>()
  const orderedSlugs: string[] = []

  const visit = (slug: string) => {
    const currentState = stateBySlug.get(slug)
    if (currentState === 'visited') {
      return
    }
    if (currentState === 'visiting') {
      throw new Error(`Ciclo di dipendenze rilevato tra indicatori derivati: ${slug}`)
    }

    stateBySlug.set(slug, 'visiting')
    for (const dependencySlug of dependencyGraph.get(slug) ?? []) {
      if (!rowsBySlug.has(dependencySlug)) {
        continue
      }
      visit(dependencySlug)
    }
    stateBySlug.set(slug, 'visited')
    orderedSlugs.push(slug)
  }

  for (const slug of rowsBySlug.keys()) {
    visit(slug)
  }

  return orderedSlugs
    .map((slug) => rowsBySlug.get(slug))
    .filter((row): row is Doc<'derivedIndicators'> => row != null)
}

function listTransitiveDerivedDependencySlugs (
  slug: string,
  dependencyGraph: Map<string, string[]>,
  memoizedResults: Map<string, string[]>
) {
  const cached = memoizedResults.get(slug)
  if (cached) {
    return cached
  }

  const collected = new Set<string>()
  for (const dependencySlug of dependencyGraph.get(slug) ?? []) {
    collected.add(dependencySlug)
    for (const nestedDependency of listTransitiveDerivedDependencySlugs(
      dependencySlug,
      dependencyGraph,
      memoizedResults
    )) {
      collected.add(nestedDependency)
    }
  }

  const result = [...collected].sort((left, right) => left.localeCompare(right))
  memoizedResults.set(slug, result)
  return result
}

function buildDerivedDependencyCache (
  rows: Array<Doc<'derivedIndicators'>>
) {
  const dependencyGraph = buildDerivedDependencyGraph(rows)
  const transitiveMemo = new Map<string, string[]>()

  return new Map(rows.map((row) => {
    const dependencies = listDirectFormulaDependencies(row.formula)
    const directBaseDependencySlugs = [...new Set(
      dependencies
        .filter((dependency) => dependency.indicatorKind === 'base')
        .map((dependency) => dependency.indicatorSlug)
    )].sort((left, right) => left.localeCompare(right))
    const directDerivedDependencySlugs = [...new Set(
      dependencies
        .filter((dependency) => dependency.indicatorKind === 'derived')
        .map((dependency) => dependency.indicatorSlug)
    )].sort((left, right) => left.localeCompare(right))

    return [row.slug, {
      directBaseDependencySlugs,
      directDerivedDependencySlugs,
      transitiveDerivedDependencySlugs: listTransitiveDerivedDependencySlugs(
        row.slug,
        dependencyGraph,
        transitiveMemo
      ),
    }] as const
  }))
}

async function syncDerivedDependencyCaches (
  ctx: MutationCtx,
  profileId: Id<'snapshotProfiles'>
) {
  const rows: Array<Doc<'derivedIndicators'>> = listLatestRowsBySlug(await ctx.db
    .query('derivedIndicators')
    .withIndex('by_profile', (q: any) => q.eq('profileId', profileId))
    .collect())
  const dependencyCache = buildDerivedDependencyCache(rows)

  for (const row of rows) {
    const nextCache = dependencyCache.get(row.slug)
    if (!nextCache) {
      continue
    }

    const hasChanges = (
      JSON.stringify(row.directBaseDependencySlugs ?? []) !== JSON.stringify(nextCache.directBaseDependencySlugs) ||
      JSON.stringify(row.directDerivedDependencySlugs ?? []) !== JSON.stringify(nextCache.directDerivedDependencySlugs) ||
      JSON.stringify(row.transitiveDerivedDependencySlugs ?? []) !== JSON.stringify(nextCache.transitiveDerivedDependencySlugs)
    )

    if (!hasChanges) {
      continue
    }

    await ctx.db.patch(row._id, {
      directBaseDependencySlugs: nextCache.directBaseDependencySlugs,
      directDerivedDependencySlugs: nextCache.directDerivedDependencySlugs,
      transitiveDerivedDependencySlugs: nextCache.transitiveDerivedDependencySlugs,
      updatedAt: Date.now(),
    })
  }
}

async function assertNoDerivedIndicatorCycles (
  ctx: MutationCtx,
  profileId: Id<'snapshotProfiles'>,
  nextIndicator: Pick<Doc<'derivedIndicators'>, 'slug' | 'formula'>
) {
  const existingRows: Array<Doc<'derivedIndicators'>> = listLatestRowsBySlug(await ctx.db
    .query('derivedIndicators')
    .withIndex('by_profile', (q: any) => q.eq('profileId', profileId))
    .collect())
    .filter((row) => row.slug !== nextIndicator.slug)
  const nextRows = [...existingRows, nextIndicator]
  const dependencyGraph = buildDerivedDependencyGraph(nextRows)
  const cycle = detectDerivedDependencyCycle(dependencyGraph)

  if (cycle) {
    throw new Error(`Ciclo rilevato tra indicatori derivati: ${cycle.join(' -> ')}`)
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

function listCompatibleMaterializationIndexes (
  dataSourceSetting: DataSourceSettingDoc,
  dateFieldKey?: string
) {
  if (!dateFieldKey) {
    return []
  }

  return (dataSourceSetting.indexSuggestions ?? [])
    .filter((index) => index.fields[0] === dateFieldKey)
}

function resolveAutomaticMaterializationIndexKey (
  dataSourceSetting: DataSourceSettingDoc,
  dateFieldKey?: string
) {
  return listCompatibleMaterializationIndexes(dataSourceSetting, dateFieldKey)[0]?.key
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
    listCompatibleMaterializationIndexes(args.dataSourceSetting, args.dateFieldKey)
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
  const rows = await ctx.db
    .query('indicators')
    .withIndex('by_profile_and_slug_and_version', (q: any) =>
      q.eq('profileId', profileId).eq('slug', slug)
    )
    .order('desc')
    .take(1)
  return rows[0] ?? null
}

async function getIndicatorByProfileAndSlugAndVersion (
  ctx: any,
  profileId: Id<'snapshotProfiles'>,
  slug: string,
  version: number
) {
  return await ctx.db
    .query('indicators')
    .withIndex('by_profile_and_slug_and_version', (q: any) =>
      q.eq('profileId', profileId).eq('slug', slug).eq('version', version)
    )
    .unique()
}

async function getDerivedIndicatorByProfileAndSlug (
  ctx: any,
  profileId: Id<'snapshotProfiles'>,
  slug: string
) {
  const rows = await ctx.db
    .query('derivedIndicators')
    .withIndex('by_profile_and_slug_and_version', (q: any) =>
      q.eq('profileId', profileId).eq('slug', slug)
    )
    .order('desc')
    .take(1)
  return rows[0] ?? null
}

async function getDerivedIndicatorByProfileAndSlugAndVersion (
  ctx: any,
  profileId: Id<'snapshotProfiles'>,
  slug: string,
  version: number
) {
  return await ctx.db
    .query('derivedIndicators')
    .withIndex('by_profile_and_slug_and_version', (q: any) =>
      q.eq('profileId', profileId).eq('slug', slug).eq('version', version)
    )
    .unique()
}

function buildDefinitionView (
  definition: Doc<'calculationDefinitions'>,
  indicatorsById: Map<Id<'indicators'>, Doc<'indicators'>>,
  dataSourcesById: Map<Id<'dataSources'>, DataSourceDoc>
) {
  const indicator = indicatorsById.get(definition.indicatorId)
  const source = dataSourcesById.get(definition.dataSourceId)

  return {
    ...definition,
    indicatorSlug: indicator?.slug ?? null,
    indicatorVersion: indicator?.version ?? null,
    sourceKey: source?.sourceKey ?? null,
    sourceLabel: source?.label ?? null,
    sourceSchedulePreset: source?.schedulePreset ?? null,
    sourceSchedulePresetLabel: source ? getSchedulePresetLabel(source.schedulePreset) : null,
    sourceAutomaticWindow: source ? getDefaultAutomaticWindow(source, Date.now()) : null,
  }
}

function buildDerivedIndicatorView (derivedIndicator: Doc<'derivedIndicators'>) {
  return {
    ...derivedIndicator,
    reportUsageCount: normalizeReportUsageCount(derivedIndicator.reportUsageCount),
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
    reportUsageCount: normalizeReportUsageCount(indicator.reportUsageCount),
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
    const nextRowKeyStrategy = args.rowKeyStrategy
      ?? existing?.rowKeyStrategy
      ?? dataSourceSetting.defaultRowKeyStrategy
    const nextMaterializationIndexKey = resolveAutomaticMaterializationIndexKey(
      dataSourceSetting,
      nextDateFieldKey
    )

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

export const requestExport = action({
  args: {
    profileSlug: v.optional(v.string()),
    requestedBy: v.optional(v.string()),
    name: v.optional(v.string()),
    dataSourceKey: v.string(),
    filters: exportFiltersValidator,
    clonedFromExportId: v.optional(v.string()),
  },
  returns: v.string(),
  handler: async (ctx, args): Promise<string> => {
    return await ctx.runAction(api.exportWorkflows.requestExport, args)
  },
})

export const regenerateExport = action({
  args: {
    exportId: v.string(),
    requestedBy: v.optional(v.string()),
    name: v.optional(v.string()),
  },
  returns: v.string(),
  handler: async (ctx, args): Promise<string> => {
    return await ctx.runAction(api.exportWorkflows.regenerateExport, args)
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
    version: v.number(),
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
    const [existing, latest] = await Promise.all([
      getIndicatorByProfileAndSlugAndVersion(ctx, profile._id, args.slug, args.version),
      getIndicatorByProfileAndSlug(ctx, profile._id, args.slug),
    ])
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
        version: args.version,
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

    if (latest && args.version <= latest.version) {
      throw new Error(
        `La nuova versione di '${args.slug}' deve essere maggiore della versione corrente (${latest.version})`
      )
    }

    if (latest && normalizeReportUsageCount(latest.reportUsageCount) > 0) {
      await ctx.db.patch(latest._id, {
        reportUsageCount: 0,
        updatedAt: now,
      })
    }

    return await ctx.db.insert('indicators', {
      profileId: profile._id,
      slug: args.slug,
      version: args.version,
      label: args.label,
      unit: args.unit,
      category: args.category,
      description: args.description,
      externalId: args.externalId,
      reportUsageCount: normalizeReportUsageCount(latest?.reportUsageCount),
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
    const derivedIndicator = await getDerivedIndicatorByProfileAndSlug(ctx, profile._id, args.slug)
    if (!derivedIndicator) {
      return null
    }

    return buildDerivedIndicatorView(derivedIndicator)
  },
})

export const getIndicatorByExternalId = query({
  args: {
    externalId: v.string(),
  },
  returns: v.union(v.any(), v.null()),
  handler: async (ctx, args) => {
    const indicator = await ctx.db
      .query('indicators')
      .withIndex('by_external_id', (q) => q.eq('externalId', args.externalId))
      .unique()
    if (!indicator) {
      return null
    }

    return {
      ...indicator,
      reportUsageCount: normalizeReportUsageCount(indicator.reportUsageCount),
    }
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

export const rebuildIndicatorReportUsageCounters = mutation({
  args: {
    profileSlug: v.optional(v.string()),
  },
  returns: v.object({
    updatedBaseIndicatorCount: v.number(),
    updatedDerivedIndicatorCount: v.number(),
    activeReportCount: v.number(),
    activeWidgetCount: v.number(),
  }),
  handler: async (ctx, args) => {
    const profile = args.profileSlug
      ? await getProfileBySlug(ctx, args.profileSlug)
      : null

    const reportRows = profile
      ? await ctx.db
        .query('reports')
        .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
        .collect()
      : await ctx.db
        .query('reports')
        .withIndex('by_is_archived', (q) => q.eq('isArchived', false))
        .collect()

    const activeReports = profile
      ? reportRows.filter((report) => !report.isArchived)
      : reportRows

    const usageCountByIndicator = new Map<string, number>()
    let activeWidgetCount = 0

    for (const report of activeReports) {
      const widgets = await ctx.db
        .query('reportWidgets')
        .withIndex('by_report', (q) => q.eq('reportId', report._id))
        .collect()

      for (const widget of widgets) {
        const key = buildReportUsageKey(
          widget.indicatorKind,
          widget.sourceProfileId,
          widget.indicatorSlug
        )
        usageCountByIndicator.set(key, (usageCountByIndicator.get(key) ?? 0) + 1)
        activeWidgetCount++
      }
    }

    const allBaseIndicators = profile
      ? await ctx.db
        .query('indicators')
        .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
        .collect()
      : await ctx.db.query('indicators').collect()
    const allDerivedIndicators = profile
      ? await ctx.db
        .query('derivedIndicators')
        .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
        .collect()
      : await ctx.db.query('derivedIndicators').collect()
    const latestBaseIndicators = buildLatestRowsByProfileAndSlug(allBaseIndicators)
    const latestDerivedIndicators = buildLatestRowsByProfileAndSlug(allDerivedIndicators)

    const now = Date.now()
    let updatedBaseIndicatorCount = 0
    for (const indicator of allBaseIndicators) {
      const latestIndicator = latestBaseIndicators.get(buildScopedSlugKey(indicator.profileId, indicator.slug))
      const nextCount = latestIndicator?._id === indicator._id
        ? usageCountByIndicator.get(
          buildReportUsageKey('base', indicator.profileId, indicator.slug)
        ) ?? 0
        : 0
      if (normalizeReportUsageCount(indicator.reportUsageCount) !== nextCount) {
        await ctx.db.patch(indicator._id, {
          reportUsageCount: nextCount,
          updatedAt: now,
        })
        updatedBaseIndicatorCount++
      }
    }

    let updatedDerivedIndicatorCount = 0
    for (const derivedIndicator of allDerivedIndicators) {
      const latestIndicator = latestDerivedIndicators.get(
        buildScopedSlugKey(derivedIndicator.profileId, derivedIndicator.slug)
      )
      const nextCount = latestIndicator?._id === derivedIndicator._id
        ? usageCountByIndicator.get(
          buildReportUsageKey('derived', derivedIndicator.profileId, derivedIndicator.slug)
        ) ?? 0
        : 0
      if (normalizeReportUsageCount(derivedIndicator.reportUsageCount) !== nextCount) {
        await ctx.db.patch(derivedIndicator._id, {
          reportUsageCount: nextCount,
          updatedAt: now,
        })
        updatedDerivedIndicatorCount++
      }
    }

    return {
      updatedBaseIndicatorCount,
      updatedDerivedIndicatorCount,
      activeReportCount: activeReports.length,
      activeWidgetCount,
    }
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
    const latestDerivedIndicators = listLatestRowsBySlug(dependentDerivedIndicators)

    const blockingDerivedIndicators = latestDerivedIndicators.filter((derivedIndicator) =>
      listDirectFormulaDependencies(derivedIndicator.formula).some((dependency) => (
        dependency.indicatorKind === 'base' &&
        dependency.indicatorSlug === args.slug
      ))
    )

    if (blockingDerivedIndicators.length > 0) {
      throw new Error(
        `Impossibile eliminare '${args.slug}': e' usato da ${blockingDerivedIndicators
          .map((derivedIndicator) => derivedIndicator.slug)
          .join(', ')}`
      )
    }
    if (normalizeReportUsageCount(indicator.reportUsageCount) > 0) {
      throw new Error(
        `Impossibile eliminare '${args.slug}': e' usato in ${normalizeReportUsageCount(indicator.reportUsageCount)} report`
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
    indicatorVersion: v.number(),
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
    const indicator = await getIndicatorByProfileAndSlugAndVersion(
      ctx,
      profile._id,
      args.indicatorSlug,
      args.indicatorVersion
    )
    if (!indicator) {
      throw new Error(`Indicatore '${args.indicatorSlug}' v${args.indicatorVersion} non trovato`)
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
        indicatorVersion: v.number(),
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
      const indicator = await getIndicatorByProfileAndSlugAndVersion(
        ctx,
        profile._id,
        definition.indicatorSlug,
        definition.indicatorVersion
      )
      if (!indicator) {
        throw new Error(`Indicatore '${definition.indicatorSlug}' v${definition.indicatorVersion} non trovato`)
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
    version: v.number(),
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
    await assertNoDerivedIndicatorCycles(ctx, profile._id, {
      slug: args.slug,
      formula: args.formula,
    })
    const [existing, latest] = await Promise.all([
      getDerivedIndicatorByProfileAndSlugAndVersion(ctx, profile._id, args.slug, args.version),
      getDerivedIndicatorByProfileAndSlug(ctx, profile._id, args.slug),
    ])
    const now = Date.now()
    const directDependencies = listDirectFormulaDependencies(args.formula)
    const directBaseDependencySlugs = [...new Set(
      directDependencies
        .filter((dependency) => dependency.indicatorKind === 'base')
        .map((dependency) => dependency.indicatorSlug)
    )].sort((left, right) => left.localeCompare(right))
    const directDerivedDependencySlugs = [...new Set(
      directDependencies
        .filter((dependency) => dependency.indicatorKind === 'derived')
        .map((dependency) => dependency.indicatorSlug)
    )].sort((left, right) => left.localeCompare(right))

    if (existing) {
      await ctx.db.patch(existing._id, {
        version: args.version,
        label: args.label,
        unit: args.unit,
        description: args.description,
        formula: args.formula,
        directBaseDependencySlugs,
        directDerivedDependencySlugs,
        enabled: args.enabled ?? existing.enabled,
        updatedAt: now,
      })
      await syncDerivedDependencyCaches(ctx, profile._id)
      return existing._id
    }

    if (latest && args.version <= latest.version) {
      throw new Error(
        `La nuova versione di '${args.slug}' deve essere maggiore della versione corrente (${latest.version})`
      )
    }

    if (latest && normalizeReportUsageCount(latest.reportUsageCount) > 0) {
      await ctx.db.patch(latest._id, {
        reportUsageCount: 0,
        updatedAt: now,
      })
    }

    const insertedId = await ctx.db.insert('derivedIndicators', {
      profileId: profile._id,
      slug: args.slug,
      version: args.version,
      label: args.label,
      unit: args.unit,
      description: args.description,
      reportUsageCount: normalizeReportUsageCount(latest?.reportUsageCount),
      formula: args.formula,
      directBaseDependencySlugs,
      directDerivedDependencySlugs,
      enabled: args.enabled ?? true,
      createdAt: now,
    })
    await syncDerivedDependencyCaches(ctx, profile._id)
    return insertedId
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
    if (normalizeReportUsageCount(derivedIndicator.reportUsageCount) > 0) {
      throw new Error(
        `Impossibile eliminare '${args.slug}': e' usato in ${normalizeReportUsageCount(derivedIndicator.reportUsageCount)} report`
      )
    }

    const dependentDerivedIndicators = listLatestRowsBySlug(await ctx.db
      .query('derivedIndicators')
      .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
      .collect())
      .filter((row) => row.slug !== args.slug)
      .filter((row) => listDirectFormulaDependencies(row.formula).some((dependency) => (
        dependency.indicatorKind === 'derived' &&
        dependency.indicatorSlug === args.slug
      )))

    if (dependentDerivedIndicators.length > 0) {
      throw new Error(
        `Impossibile eliminare '${args.slug}': e' usato da ${dependentDerivedIndicators
          .map((row) => row.slug)
          .join(', ')}`
      )
    }

    await ctx.db.delete(derivedIndicator._id)
    await syncDerivedDependencyCaches(ctx, profile._id)

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
    const rows = await ctx.db
      .query('derivedIndicators')
      .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
      .collect()
    return sortIndicatorsByReportUsage(
      listLatestRowsBySlug(rows).map((row) => buildDerivedIndicatorView(row))
    )
  },
})

export const listProfileDefinitions = query({
  args: {
    profileSlug: v.string(),
  },
  returns: v.any(),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const [allIndicators, allDataSources, allDefinitions, allDerivedIndicators] = await Promise.all([
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('dataSources').collect(),
      ctx.db.query('calculationDefinitions').withIndex('by_profile_and_priority', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('derivedIndicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
    ])
    const dataSources = allDataSources.filter((row) => !row.archivedAt)
    const indicators = listLatestRowsBySlug(allIndicators)
    const derivedIndicators = listLatestRowsBySlug(allDerivedIndicators)
    const baseVersionCountBySlug = new Map<string, number>()
    const derivedVersionCountBySlug = new Map<string, number>()

    for (const indicator of allIndicators) {
      baseVersionCountBySlug.set(indicator.slug, (baseVersionCountBySlug.get(indicator.slug) ?? 0) + 1)
    }

    for (const indicator of allDerivedIndicators) {
      derivedVersionCountBySlug.set(indicator.slug, (derivedVersionCountBySlug.get(indicator.slug) ?? 0) + 1)
    }

    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]))
    const dataSourcesById = new Map(dataSources.map((dataSource) => [dataSource._id, dataSource]))
    const currentIndicatorIds = new Set(indicators.map((indicator) => String(indicator._id)))
    const definitions = allDefinitions.filter((definition) => currentIndicatorIds.has(String(definition.indicatorId)))
    const indicatorViews = await Promise.all(indicators.map(async (indicator) => {
      const indicatorView = await buildIndicatorView(ctx, profile._id, indicator)
      return {
        ...indicatorView,
        latestVersion: indicator.version,
        hasOlderVersions: (baseVersionCountBySlug.get(indicator.slug) ?? 0) > 1,
      }
    }))
    const derivedIndicatorViews = sortIndicatorsByReportUsage(
      derivedIndicators.map((derivedIndicator) => ({
        ...buildDerivedIndicatorView(derivedIndicator),
        latestVersion: derivedIndicator.version,
        hasOlderVersions: (derivedVersionCountBySlug.get(derivedIndicator.slug) ?? 0) > 1,
      }))
    )

    return {
      profile,
      indicators: sortIndicatorsByReportUsage(indicatorViews),
      dataSources: dataSources.map((dataSource) => buildDataSourceView(dataSource)),
      derivedIndicators: derivedIndicatorViews,
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
    const [dataSources, profiles, definitions, indicators] = await Promise.all([
      ctx.db.query('dataSources').collect(),
      ctx.db.query('snapshotProfiles').collect(),
      ctx.db.query('calculationDefinitions').collect(),
      ctx.db.query('indicators').collect(),
    ])
    const activeProfiles = profiles.filter((profile) => !profile.archivedAt)
    const activeDataSources = dataSources.filter((dataSource) => (
      !dataSource.archivedAt &&
      dataSource.enabled &&
      dataSource.schedulePreset === args.schedulePreset
    ))

    const currentIndicators = [...buildLatestRowsByProfileAndSlug(indicators).values()]
    const indicatorsById = buildIndicatorsById(currentIndicators)
    const profileSlugsById = new Map(activeProfiles.map((profile) => [profile._id, profile.slug] as const))
    const profileSlugsByDataSourceId = new Map<Id<'dataSources'>, string[]>()

    for (const definition of definitions) {
      if (!definition.enabled || !isIndicatorEnabled(indicatorsById.get(definition.indicatorId))) {
        continue
      }
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

    return activeDataSources
      .map((dataSource) => ({
        ...buildDataSourceView(dataSource),
        profileSlugs: profileSlugsByDataSourceId.get(dataSource._id) ?? [],
      }))
      .filter((dataSource) => dataSource.profileSlugs.length > 0)
  },
})

export const listProfileSlugsBySourceKey = query({
  args: {
    sourceKey: v.string(),
  },
  returns: v.array(v.string()),
  handler: async (ctx, args) => {
    const dataSource = await getDataSourceByKey(ctx, args.sourceKey)
    if (!dataSource || dataSource.archivedAt || !dataSource.enabled) {
      return []
    }

    const [profiles, definitions, indicators] = await Promise.all([
      ctx.db.query('snapshotProfiles').collect(),
      ctx.db.query('calculationDefinitions').withIndex('by_data_source', (q) => q.eq('dataSourceId', dataSource._id)).collect(),
      ctx.db.query('indicators').collect(),
    ])
    const activeProfilesById = new Map(
      profiles
        .filter((profile) => !profile.archivedAt)
        .map((profile) => [profile._id, profile.slug] as const)
    )
    const indicatorsById = buildIndicatorsById([...buildLatestRowsByProfileAndSlug(indicators).values()])

    return [...new Set(definitions
      .filter((definition) => definition.enabled && isIndicatorEnabled(indicatorsById.get(definition.indicatorId)))
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
    const [allDefinitions, allIndicators] = await Promise.all([
      ctx.db.query('calculationDefinitions').withIndex('by_profile_and_priority', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
    ])
    const indicators = listLatestRowsBySlug(allIndicators)
    const currentIndicatorsBySlug = buildIndicatorsBySlug(indicators)
    const indicatorsById = buildIndicatorsById(indicators)
    const definitions = filterDefinitionsForCurrentIndicators(allDefinitions, currentIndicatorsBySlug)
    const enabledDefinitions = filterEnabledDefinitions(definitions, indicatorsById)
    const candidateDataSourceIds = [...new Set(enabledDefinitions.map((definition) => definition.dataSourceId))]
    const dataSources = (await Promise.all(
      candidateDataSourceIds.map(async (dataSourceId) => await ctx.db.get(dataSourceId))
    )).filter((row): row is DataSourceDoc => row !== null && row !== undefined && !row.archivedAt)
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

function buildSnapshotRunResponse (
  snapshotId: Id<'snapshots'>,
  run: Doc<'snapshotRuns'>
) {
  return {
    snapshotId: String(snapshotId),
    snapshotRunId: String(run._id),
    status: run.status,
    processedCount: run.processedCount,
    errorsCount: run.errorCount,
  }
}

export const scheduleSnapshotRunWorkflow = internalMutation({
  args: {
    snapshotRunId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.scheduler.runAfter(0, internal.snapshotEngine.runSnapshotRunWorkflow, {
      snapshotRunId: args.snapshotRunId,
    })
    return null
  },
})

export const getSnapshotRunExecutionContext = internalQuery({
  args: {
    snapshotRunId: v.string(),
  },
  returns: v.union(v.any(), v.null()),
  handler: async (ctx, args) => {
    const snapshotRunId = args.snapshotRunId as Id<'snapshotRuns'>
    const run = await ctx.db.get(snapshotRunId)
    if (!run) {
      return null
    }

    const [snapshot, profile, sources, allDefinitions, allIndicators, allDerivedIndicators] = await Promise.all([
      ctx.db.get(run.snapshotId),
      ctx.db.get(run.profileId),
      ctx.db
        .query('snapshotRunSources')
        .withIndex('by_snapshot_run', (q) => q.eq('snapshotRunId', snapshotRunId))
        .collect(),
      ctx.db
        .query('calculationDefinitions')
        .withIndex('by_profile_and_priority', (q) => q.eq('profileId', run.profileId))
        .collect(),
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', run.profileId)).collect(),
      ctx.db.query('derivedIndicators').withIndex('by_profile', (q) => q.eq('profileId', run.profileId)).collect(),
    ])

    if (!snapshot || !profile) {
      return null
    }

    const dataSourceIds = [...new Set(sources.map((source) => source.dataSourceId))]
    const dataSources = (await Promise.all(
      dataSourceIds.map(async (dataSourceId) => await ctx.db.get(dataSourceId))
    )).filter((row): row is DataSourceDoc => row !== null && row !== undefined && !row.archivedAt)
    const indicators = listLatestRowsBySlug(allIndicators)
    const definitions = filterDefinitionsForCurrentIndicators(allDefinitions, buildIndicatorsBySlug(indicators))
    const derivedIndicators = listLatestRowsBySlug(allDerivedIndicators)

    return {
      run,
      snapshot,
      profile,
      sources,
      definitions,
      indicators,
      derivedIndicators,
      dataSources,
    }
  },
})

export const setSnapshotRunLifecycle = internalMutation({
  args: {
    snapshotRunId: v.string(),
    status: snapshotStatusValidator,
    currentSourceKey: v.optional(v.union(v.string(), v.null())),
    errorMessage: v.optional(v.string()),
    processedCountDelta: v.optional(v.number()),
    errorCountDelta: v.optional(v.number()),
    completedSourceCountDelta: v.optional(v.number()),
    finishedAt: v.optional(v.number()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const run = await ctx.db.get(args.snapshotRunId as Id<'snapshotRuns'>)
    if (!run) {
      throw new Error('Snapshot run non trovato')
    }
    const snapshot = await ctx.db.get(run.snapshotId)
    if (!snapshot) {
      throw new Error('Snapshot non trovato')
    }

    const nextProcessedCount = run.processedCount + (args.processedCountDelta ?? 0)
    const nextErrorCount = run.errorCount + (args.errorCountDelta ?? 0)
    const nextCompletedSourceCount = run.completedSourceCount + (args.completedSourceCountDelta ?? 0)
    const nextErrorMessage = args.errorMessage ?? (nextErrorCount > 0 ? `${nextErrorCount} regole in errore` : undefined)

    await ctx.db.patch(run._id, {
      status: args.status,
      currentSourceKey: args.currentSourceKey === undefined
        ? run.currentSourceKey
        : args.currentSourceKey ?? undefined,
      errorMessage: nextErrorMessage,
      processedCount: nextProcessedCount,
      errorCount: nextErrorCount,
      completedSourceCount: nextCompletedSourceCount,
      finishedAt: args.finishedAt,
    })

    await ctx.db.patch(snapshot._id, {
      status: args.status,
      errorMessage: nextErrorMessage,
      finishedAt: args.finishedAt,
    })

    return null
  },
})

export const setSnapshotRunSourceStatus = internalMutation({
  args: {
    snapshotRunId: v.string(),
    sourceKey: v.string(),
    status: snapshotSourceStatusValidator,
    errorMessage: v.optional(v.string()),
    processedCount: v.optional(v.number()),
    errorCount: v.optional(v.number()),
    startedAt: v.optional(v.number()),
    finishedAt: v.optional(v.number()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const sourceRow = await ctx.db
      .query('snapshotRunSources')
      .withIndex('by_snapshot_run_and_source_key', (q) =>
        q.eq('snapshotRunId', args.snapshotRunId as Id<'snapshotRuns'>).eq('sourceKey', args.sourceKey)
      )
      .unique()

    if (!sourceRow) {
      throw new Error(`Source snapshot '${args.sourceKey}' non trovata`)
    }

    await ctx.db.patch(sourceRow._id, {
      status: args.status,
      errorMessage: args.errorMessage,
      processedCount: args.processedCount ?? sourceRow.processedCount,
      errorCount: args.errorCount ?? sourceRow.errorCount,
      startedAt: args.startedAt ?? sourceRow.startedAt,
      finishedAt: args.finishedAt,
    })

    return null
  },
})

export const recordSnapshotDefinitionResult = internalMutation({
  args: {
    snapshotRunId: v.string(),
    definitionId: v.id('calculationDefinitions'),
    indicatorId: v.id('indicators'),
    indicatorVersion: v.number(),
    dataSourceId: v.id('dataSources'),
    sourceKey: v.optional(v.string()),
    status: snapshotItemStatusValidator,
    inputCount: v.number(),
    rawResult: v.optional(v.number()),
    normalizedResult: v.optional(v.number()),
    durationMs: v.number(),
    sourceExportIds: v.array(v.id('analyticsExports')),
    errorMessage: v.optional(v.string()),
    warningMessage: v.optional(v.string()),
    ruleHash: v.string(),
    resolvedFilters: v.optional(v.any()),
    sampleRowsPreview: v.optional(v.array(v.any())),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const run = await ctx.db.get(args.snapshotRunId as Id<'snapshotRuns'>)
    if (!run) {
      throw new Error('Snapshot run non trovato')
    }
    const snapshot = await ctx.db.get(run.snapshotId)
    if (!snapshot) {
      throw new Error('Snapshot non trovato')
    }

    const itemId = await ctx.db.insert('snapshotRunItems', {
      snapshotRunId: run._id,
      snapshotId: snapshot._id,
      profileId: run.profileId,
      definitionId: args.definitionId,
      indicatorId: args.indicatorId,
      indicatorVersion: args.indicatorVersion,
      dataSourceId: args.dataSourceId,
      status: args.status,
      inputCount: args.inputCount,
      rawResult: args.rawResult,
      normalizedResult: args.normalizedResult,
      durationMs: args.durationMs,
      sourceExportIds: args.sourceExportIds,
      errorMessage: args.errorMessage,
      warningMessage: args.warningMessage,
      ruleHash: args.ruleHash,
      createdAt: Date.now(),
    })

    if (args.status !== 'error' && args.normalizedResult != null) {
      const indicator = await ctx.db.get(args.indicatorId)
      if (!indicator) {
        throw new Error(`Indicatore non trovato '${args.indicatorId}'`)
      }

      const snapshotValueId = await ctx.db.insert('snapshotValues', {
        snapshotId: snapshot._id,
        snapshotRunId: run._id,
        snapshotRunItemId: itemId,
        profileId: run.profileId,
        indicatorId: args.indicatorId,
        indicatorVersion: args.indicatorVersion,
        indicatorLabelSnapshot: normalizeIndicatorLabelSnapshot(indicator.label),
        value: args.normalizedResult,
        snapshotAt: snapshot.snapshotAt,
        computedAt: Date.now(),
        ruleHash: args.ruleHash,
        sourceExportIds: args.sourceExportIds,
        explainRef: `runItem:${itemId}`,
      })

      await ctx.db.insert('integrationValues', {
        snapshotValueId,
        snapshotId: snapshot._id,
        snapshotRunId: run._id,
        profileId: run.profileId,
        indicatorId: args.indicatorId,
        indicatorVersion: args.indicatorVersion,
        indicatorExternalIdSnapshot: indicator.externalId,
        value: args.normalizedResult,
        measuredAt: snapshot.snapshotAt,
        syncStatus: 'pending',
        createdAt: Date.now(),
      })
    }

    if (args.status !== 'error') {
      await ctx.db.insert('calculationTraces', {
        snapshotRunId: run._id,
        snapshotRunItemId: itemId,
        profileId: run.profileId,
        definitionId: args.definitionId,
        queryParams: {
          snapshotAt: snapshot.snapshotAt,
          dataSourceId: args.dataSourceId,
          sourceKey: args.sourceKey ?? null,
        },
        resolvedFilters: args.resolvedFilters,
        sampleRowsPreview: args.sampleRowsPreview,
        warnings: args.warningMessage ? [args.warningMessage] : undefined,
        createdAt: Date.now(),
      })
    }

    return null
  },
})

export const rebuildDerivedSnapshotValues = internalMutation({
  args: {
    snapshotRunId: v.string(),
  },
  returns: v.object({
    processedCount: v.number(),
  }),
  handler: async (ctx, args) => {
    const run = await ctx.db.get(args.snapshotRunId as Id<'snapshotRuns'>)
    if (!run) {
      throw new Error('Snapshot run non trovato')
    }
    const snapshot = await ctx.db.get(run.snapshotId)
    if (!snapshot) {
      throw new Error('Snapshot non trovato')
    }

    const [allDerivedIndicators, baseValues, allIndicators, existingDerivedRows] = await Promise.all([
      ctx.db.query('derivedIndicators').withIndex('by_profile', (q) => q.eq('profileId', run.profileId)).collect(),
      ctx.db.query('snapshotValues').withIndex('by_snapshot', (q) => q.eq('snapshotId', snapshot._id)).collect(),
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', run.profileId)).collect(),
      ctx.db.query('derivedSnapshotValues').withIndex('by_snapshot', (q) => q.eq('snapshotId', snapshot._id)).collect(),
    ])

    for (const row of existingDerivedRows) {
      await ctx.db.delete(row._id)
    }

    const derivedIndicators = listLatestRowsBySlug(allDerivedIndicators)
    const indicators = listLatestRowsBySlug(allIndicators)
    const indicatorsById = buildIndicatorsById(indicators)
    const indicatorsBySlug = buildIndicatorsBySlug(indicators)
    const derivedIndicatorsBySlug = buildLatestRowsBySlug(derivedIndicators)
    const dependencyCycle = detectDerivedDependencyCycle(buildDerivedDependencyGraph(derivedIndicators))
    if (dependencyCycle) {
      throw new Error(`Ciclo rilevato tra indicatori derivati: ${dependencyCycle.join(' -> ')}`)
    }
    const valuesByIndicatorSlug = new Map<string, DerivedComputationInput>()

    for (const baseValue of baseValues) {
      const indicator = indicatorsById.get(baseValue.indicatorId)
      if (!indicator || !indicator.enabled) {
        continue
      }
      const baseKey = buildDerivedComputationKey('base', indicator.slug)
      if (valuesByIndicatorSlug.has(baseKey)) {
        valuesByIndicatorSlug.delete(baseKey)
        continue
      }
      valuesByIndicatorSlug.set(baseKey, {
        snapshotValueId: baseValue._id,
        snapshotValueKind: 'base',
        indicatorSlug: indicator.slug,
        indicatorVersion: baseValue.indicatorVersion,
        value: baseValue.value,
        sourceExportIds: baseValue.sourceExportIds,
      })
    }

    let processedCount = 0
    for (const derivedIndicator of topologicallySortDerivedIndicators(derivedIndicators).filter((row) => (
      row.enabled &&
      !hasInactiveDerivedOperands(row.formula, indicatorsBySlug, derivedIndicatorsBySlug)
    ))) {
      const computed = computeDerivedValue(derivedIndicator.formula, valuesByIndicatorSlug)
      if (!computed) {
        continue
      }
      const baseInputs = computed.inputs.filter((input): input is DerivedComputationInput & {
        snapshotValueKind: 'base'
        snapshotValueId: Id<'snapshotValues'>
      } => input.snapshotValueKind === 'base')
      const derivedInputs = computed.inputs.filter((input): input is DerivedComputationInput & {
        snapshotValueKind: 'derived'
        snapshotValueId: Id<'derivedSnapshotValues'>
      } => input.snapshotValueKind === 'derived')
      const sourceExportIds: Array<Id<'analyticsExports'>> = [...new Set(
        computed.inputs.flatMap((input) => input.sourceExportIds)
      )]
      const insertedId = await ctx.db.insert('derivedSnapshotValues', {
        snapshotId: snapshot._id,
        snapshotRunId: run._id,
        profileId: run.profileId,
        derivedIndicatorId: derivedIndicator._id,
        derivedIndicatorSlug: derivedIndicator.slug,
        derivedIndicatorVersion: derivedIndicator.version,
        derivedIndicatorLabelSnapshot: derivedIndicator.label,
        derivedIndicatorUnit: derivedIndicator.unit,
        formulaKind: getDerivedFormulaKind(derivedIndicator.formula),
        formulaVersion: isExpressionDerivedFormula(derivedIndicator.formula) ? 2 : 1,
        value: computed.value,
        snapshotAt: snapshot.snapshotAt,
        computedAt: Date.now(),
        baseSnapshotValueIds: baseInputs.map((input) => input.snapshotValueId),
        baseIndicatorSlugs: baseInputs.map((input) => input.indicatorSlug),
        baseIndicatorVersions: baseInputs.map((input) => input.indicatorVersion),
        derivedSnapshotValueIds: derivedInputs.map((input) => input.snapshotValueId),
        derivedIndicatorDependencySlugs: derivedInputs.map((input) => input.indicatorSlug),
        derivedIndicatorDependencyVersions: derivedInputs.map((input) => input.indicatorVersion),
        sourceExportIds,
        formulaSnapshot: derivedIndicator.formula,
        createdAt: Date.now(),
      })
      valuesByIndicatorSlug.set(buildDerivedComputationKey('derived', derivedIndicator.slug), {
        snapshotValueId: insertedId,
        snapshotValueKind: 'derived',
        indicatorSlug: derivedIndicator.slug,
        indicatorVersion: derivedIndicator.version,
        value: computed.value,
        sourceExportIds,
      })
      processedCount++
    }

    return { processedCount }
  },
})

export const runSnapshotRunWorkflow = internalAction({
  args: {
    snapshotRunId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const state = await ctx.runQuery(internal.snapshotEngine.getSnapshotRunExecutionContext, {
      snapshotRunId: args.snapshotRunId,
    })

    if (!state) {
      return null
    }

    if (state.run.status === 'completed' || state.run.status === 'error') {
      return null
    }

    const indicatorsById = buildIndicatorsById(state.indicators)
    const dataSourcesById = new Map(
      state.dataSources.map((dataSource: DataSourceDoc) => [String(dataSource._id), dataSource] as const)
    )
    const pendingSource = state.sources
      .find((source) => source.status === 'pending')

    if (pendingSource) {
      const sourceDefinitions = state.definitions
        .filter((definition) => (
          definition.dataSourceId === pendingSource.dataSourceId &&
          definition.enabled &&
          isIndicatorEnabled(indicatorsById.get(definition.indicatorId))
        ))
      const dataSource = dataSourcesById.get(String(pendingSource.dataSourceId))

      if (sourceDefinitions.length === 0) {
        await ctx.runMutation(internal.snapshotEngine.setSnapshotRunSourceStatus, {
          snapshotRunId: args.snapshotRunId,
          sourceKey: pendingSource.sourceKey,
          status: 'completed',
          processedCount: 0,
          errorCount: 0,
          finishedAt: Date.now(),
        })
        await ctx.runMutation(internal.snapshotEngine.setSnapshotRunLifecycle, {
          snapshotRunId: args.snapshotRunId,
          status: 'processing',
          currentSourceKey: null,
          completedSourceCountDelta: 1,
        })
        await ctx.runMutation(internal.snapshotEngine.scheduleSnapshotRunWorkflow, {
          snapshotRunId: args.snapshotRunId,
        })
        return null
      }

      await ctx.runMutation(internal.snapshotEngine.setSnapshotRunLifecycle, {
        snapshotRunId: args.snapshotRunId,
        status: 'loading',
        currentSourceKey: pendingSource.sourceKey,
      })
      await ctx.runMutation(internal.snapshotEngine.setSnapshotRunSourceStatus, {
        snapshotRunId: args.snapshotRunId,
        sourceKey: pendingSource.sourceKey,
        status: 'processing',
        startedAt: Date.now(),
      })

      const sourceRows: Array<SourceRowInput> = []
      let cursorRowKey: string | undefined
      let hasMore = true

      while (hasMore) {
        const batch = await ctx.runQuery(internal.exportWorkflows.listMaterializedRowsBatch, {
          dataSourceId: String(pendingSource.dataSourceId),
          snapshotAt: state.snapshot.snapshotAt,
          cursorRowKey,
          batchSize: 500,
        })
        sourceRows.push(...batch.page.map((row: {
          occurredAt: number
          rowData: Record<string, unknown>
        }) => ({
          occurredAt: row.occurredAt,
          rowData: row.rowData,
        })))
        hasMore = batch.hasMore
        cursorRowKey = batch.nextRowKey ?? undefined
      }

      await ctx.runMutation(internal.snapshotEngine.setSnapshotRunLifecycle, {
        snapshotRunId: args.snapshotRunId,
        status: 'processing',
        currentSourceKey: pendingSource.sourceKey,
      })

      let sourceErrorsCount = 0
      for (const definition of sourceDefinitions) {
        const startedAt = Date.now()
        try {
          const result = runSingleDefinition(definition, sourceRows, state.snapshot.snapshotAt)
          await ctx.runMutation(internal.snapshotEngine.recordSnapshotDefinitionResult, {
            snapshotRunId: args.snapshotRunId,
            definitionId: definition._id,
            indicatorId: definition.indicatorId,
            indicatorVersion: indicatorsById.get(definition.indicatorId)?.version ?? 1,
            dataSourceId: definition.dataSourceId,
            sourceKey: dataSource?.sourceKey,
            status: result.normalizedResult == null ? 'skipped' : 'success',
            inputCount: result.inputCount,
            rawResult: result.rawResult ?? undefined,
            normalizedResult: result.normalizedResult ?? undefined,
            durationMs: Date.now() - startedAt,
            sourceExportIds: [],
            warningMessage: result.warningMessage,
            ruleHash: buildRuleHash(definition),
            resolvedFilters: result.resolvedFilters,
            sampleRowsPreview: result.filteredRows.slice(0, 10),
          })
        } catch (error) {
          sourceErrorsCount++
          await ctx.runMutation(internal.snapshotEngine.recordSnapshotDefinitionResult, {
            snapshotRunId: args.snapshotRunId,
            definitionId: definition._id,
            indicatorId: definition.indicatorId,
            indicatorVersion: indicatorsById.get(definition.indicatorId)?.version ?? 1,
            dataSourceId: definition.dataSourceId,
            sourceKey: dataSource?.sourceKey,
            status: 'error',
            inputCount: sourceRows.length,
            durationMs: Date.now() - startedAt,
            sourceExportIds: [],
            errorMessage: error instanceof Error ? error.message : String(error),
            ruleHash: buildRuleHash(definition),
          })
        }
      }

      await ctx.runMutation(internal.snapshotEngine.setSnapshotRunSourceStatus, {
        snapshotRunId: args.snapshotRunId,
        sourceKey: pendingSource.sourceKey,
        status: sourceErrorsCount > 0 ? 'error' : 'completed',
        errorMessage: sourceErrorsCount > 0 ? `${sourceErrorsCount} regole in errore` : undefined,
        processedCount: sourceDefinitions.length,
        errorCount: sourceErrorsCount,
        finishedAt: Date.now(),
      })
      await ctx.runMutation(internal.snapshotEngine.setSnapshotRunLifecycle, {
        snapshotRunId: args.snapshotRunId,
        status: 'processing',
        currentSourceKey: null,
        processedCountDelta: sourceDefinitions.length,
        errorCountDelta: sourceErrorsCount,
        completedSourceCountDelta: 1,
      })
      await ctx.runMutation(internal.snapshotEngine.scheduleSnapshotRunWorkflow, {
        snapshotRunId: args.snapshotRunId,
      })
      return null
    }

    await ctx.runMutation(internal.snapshotEngine.setSnapshotRunLifecycle, {
      snapshotRunId: args.snapshotRunId,
      status: 'deriving',
      currentSourceKey: null,
    })

    const derivedResult = await ctx.runMutation(internal.snapshotEngine.rebuildDerivedSnapshotValues, {
      snapshotRunId: args.snapshotRunId,
    })

    await ctx.runMutation(internal.snapshotEngine.setSnapshotRunLifecycle, {
      snapshotRunId: args.snapshotRunId,
      status: 'freezing',
      processedCountDelta: derivedResult.processedCount,
      currentSourceKey: null,
    })

    await ctx.runAction(internal.exportWorkflows.freezeSnapshotExports, {
      profileSlug: state.profile.slug,
      snapshotId: String(state.snapshot._id),
      snapshotRunId: args.snapshotRunId,
      triggeredBy: state.run.triggeredBy,
      sourceKeys: state.sources
        .filter((source) => source.processedCount > 0 || source.errorCount > 0)
        .map((source) => source.sourceKey),
      snapshotAt: state.snapshot.snapshotAt,
    })

    const latestState = await ctx.runQuery(internal.snapshotEngine.getSnapshotRunExecutionContext, {
      snapshotRunId: args.snapshotRunId,
    })
    if (!latestState) {
      return null
    }

    await ctx.runMutation(internal.snapshotEngine.setSnapshotRunLifecycle, {
      snapshotRunId: args.snapshotRunId,
      status: latestState.run.errorCount > 0 ? 'error' : 'completed',
      currentSourceKey: null,
      finishedAt: Date.now(),
    })

    return null
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
    status: snapshotStatusValidator,
    processedCount: v.number(),
    errorsCount: v.number(),
  }),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const snapshotAt = args.snapshotAt ?? Date.now()
    const now = Date.now()

    const [allDefinitions, allIndicators] = await Promise.all([
      ctx.db
        .query('calculationDefinitions')
        .withIndex('by_profile_and_priority', (q) => q.eq('profileId', profile._id))
        .collect(),
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
    ])

    const indicators = listLatestRowsBySlug(allIndicators)
    const currentIndicatorsBySlug = buildIndicatorsBySlug(indicators)
    const indicatorsById = buildIndicatorsById(indicators)
    const definitions = filterDefinitionsForCurrentIndicators(allDefinitions, currentIndicatorsBySlug)
    const enabledDefinitions = filterEnabledDefinitions(definitions, indicatorsById)
    const candidateDataSourceIds = [...new Set(enabledDefinitions.map((definition) => definition.dataSourceId))]
    const candidateDataSources = (await Promise.all(
      candidateDataSourceIds.map(async (dataSourceId) => await ctx.db.get(dataSourceId))
    )).filter((row): row is DataSourceDoc => row !== null && row !== undefined && !row.archivedAt)

    const dataSourcesById = new Map(
      candidateDataSources.map((dataSource) => [String(dataSource._id), dataSource] as const)
    )
    const filteredDefinitions = enabledDefinitions.filter((definition) => {
      const dataSource = dataSourcesById.get(String(definition.dataSourceId))
      if (!dataSource) {
        return false
      }
      if (!args.triggerSourceKey) {
        return true
      }
      return dataSource.sourceKey === args.triggerSourceKey
    })

    const dataSourcesForRun = [...new Map(
      filteredDefinitions.map((definition) => {
        const dataSource = dataSourcesById.get(String(definition.dataSourceId))
        return dataSource ? [String(dataSource._id), dataSource] as const : null
      }).filter((entry): entry is readonly [string, DataSourceDoc] => entry !== null)
    ).values()]

    const definitionsCountBySourceKey = new Map<string, number>()
    for (const definition of filteredDefinitions) {
      const dataSource = dataSourcesById.get(String(definition.dataSourceId))
      if (!dataSource) {
        continue
      }
      definitionsCountBySourceKey.set(
        dataSource.sourceKey,
        (definitionsCountBySourceKey.get(dataSource.sourceKey) ?? 0) + 1
      )
    }

    const initialStatus = filteredDefinitions.length > 0 ? 'queued' : 'completed'
    const snapshotId = await ctx.db.insert('snapshots', {
      profileId: profile._id,
      snapshotAt,
      status: initialStatus,
      note: args.note,
      triggeredBy: args.triggeredBy,
      triggerKind: args.triggerKind,
      triggerSourceKey: args.triggerSourceKey,
      createdAt: now,
      finishedAt: filteredDefinitions.length > 0 ? undefined : now,
    })

    const snapshotRunId = await ctx.db.insert('snapshotRuns', {
      snapshotId,
      profileId: profile._id,
      startedAt: now,
      status: initialStatus,
      triggeredBy: args.triggeredBy,
      triggerKind: args.triggerKind,
      triggerSourceKey: args.triggerSourceKey,
      definitionsCount: filteredDefinitions.length,
      processedCount: 0,
      errorCount: 0,
      sourceCount: dataSourcesForRun.length,
      completedSourceCount: 0,
      finishedAt: filteredDefinitions.length > 0 ? undefined : now,
    })

    for (const dataSource of dataSourcesForRun) {
      await ctx.db.insert('snapshotRunSources', {
        snapshotRunId,
        snapshotId,
        profileId: profile._id,
        dataSourceId: dataSource._id,
        sourceKey: dataSource.sourceKey,
        status: 'pending',
        definitionCount: definitionsCountBySourceKey.get(dataSource.sourceKey) ?? 0,
        processedCount: 0,
        errorCount: 0,
        createdAt: now,
      })
    }

    const run = await ctx.db.get(snapshotRunId)
    if (!run) {
      throw new Error('Snapshot run non trovato dopo la creazione')
    }

    if (filteredDefinitions.length === 0) {
      return buildSnapshotRunResponse(snapshotId, run)
    }

    await ctx.scheduler.runAfter(0, internal.snapshotEngine.runSnapshotRunWorkflow, {
      snapshotRunId: String(snapshotRunId),
    })

    return buildSnapshotRunResponse(snapshotId, run)
  },
})

export const getSnapshotRunStatus = query({
  args: {
    snapshotRunId: v.string(),
  },
  returns: v.union(v.any(), v.null()),
  handler: async (ctx, args) => {
    const run = await ctx.db.get(args.snapshotRunId as Id<'snapshotRuns'>)
    if (!run) {
      return null
    }

    const [snapshot, sources] = await Promise.all([
      ctx.db.get(run.snapshotId),
      ctx.db
        .query('snapshotRunSources')
        .withIndex('by_snapshot_run', (q) => q.eq('snapshotRunId', run._id))
        .collect(),
    ])

    return {
      snapshotId: String(run.snapshotId),
      snapshotRunId: String(run._id),
      status: run.status,
      processedCount: run.processedCount,
      errorsCount: run.errorCount,
      definitionsCount: run.definitionsCount,
      sourceCount: run.sourceCount,
      completedSourceCount: run.completedSourceCount,
      currentSourceKey: run.currentSourceKey ?? null,
      errorMessage: run.errorMessage ?? null,
      snapshotStatus: snapshot?.status ?? null,
      snapshotErrorMessage: snapshot?.errorMessage ?? null,
      sources: sources.map((source) => ({
        sourceKey: source.sourceKey,
        status: source.status,
        definitionCount: source.definitionCount,
        processedCount: source.processedCount,
        errorCount: source.errorCount,
        errorMessage: source.errorMessage ?? null,
        startedAt: source.startedAt ?? null,
        finishedAt: source.finishedAt ?? null,
      })),
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
        indicatorExternalId: integrationValue.indicatorExternalIdSnapshot ?? indicator.externalId,
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
        indicatorVersion: valueRow.indicatorVersion,
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
      indicatorVersion: row.derivedIndicatorVersion,
      value: row.value,
      snapshotAt: row.snapshotAt,
      computedAt: row.computedAt,
      evidenceRef: null,
      evidenceFileName: null,
      evidenceRowCount: row.baseSnapshotValueIds.length + row.derivedSnapshotValueIds.length,
      evidenceGeneratedAt: null,
      isDerived: true,
      sourceExportCount: row.sourceExportIds.length,
      sourceExports: await resolveExportSummaries(ctx, row.sourceExportIds),
      derivedFromIndicatorSlugs: [...row.baseIndicatorSlugs, ...row.derivedIndicatorDependencySlugs],
      derivedFromIndicatorVersions: [...row.baseIndicatorVersions, ...row.derivedIndicatorDependencyVersions],
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
    const [allIndicators, allDerivedIndicators] = await Promise.all([
      ctx.db.query('indicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
      ctx.db.query('derivedIndicators').withIndex('by_profile', (q) => q.eq('profileId', profile._id)).collect(),
    ])
    const indicators = listLatestRowsBySlug(allIndicators)
    const derivedIndicators = listLatestRowsBySlug(allDerivedIndicators)
    const indicatorsBySlug = buildIndicatorsBySlug(indicators)
    const derivedIndicatorsBySlug = buildLatestRowsBySlug(derivedIndicators)

    const latestBaseRows = (await Promise.all(indicators.map(async (indicator) => {
      const rows = await ctx.db
        .query('snapshotValues')
        .withIndex('by_indicator_and_snapshot_at', (q) => q.eq('indicatorId', indicator._id))
        .order('desc')
        .take(1)
      const valueRow = rows[0]
      if (!valueRow || valueRow.profileId !== profile._id) {
        return null
      }
      const isStaleInactive = !indicator.enabled
      return {
        ...valueRow,
        snapshotId: String(valueRow.snapshotId),
        snapshotAt: valueRow.snapshotAt,
        indicatorSlug: indicator.slug,
        indicatorVersion: indicator.version,
        indicatorUnit: indicator.unit ?? null,
        value: isStaleInactive ? null : valueRow.value,
        latestRecordedValue: valueRow.value,
        isDerived: false,
        isIndicatorEnabled: indicator.enabled,
        hasInactiveOperands: false,
        isStaleInactive,
        staleReason: isStaleInactive ? 'indicator_disabled' : null,
        sourceExportCount: valueRow.sourceExportIds.length,
        derivedFromIndicatorSlugs: [],
        formulaKind: null,
      }
    }))).filter((row) => row !== null)

    const latestDerivedRows = (await Promise.all(derivedIndicators.map(async (indicator) => {
      const rows = await ctx.db
        .query('derivedSnapshotValues')
        .withIndex('by_derived_indicator_and_snapshot_at', (q) => q.eq('derivedIndicatorId', indicator._id))
        .order('desc')
        .take(1)
      const row = rows[0]
      if (!row || row.profileId !== profile._id) {
        return null
      }
      const hasInactiveOperands = hasInactiveDerivedOperands(
        indicator.formula,
        indicatorsBySlug,
        derivedIndicatorsBySlug
      )
      const isStaleInactive = !indicator.enabled || hasInactiveOperands
      return {
        _id: `derived:${row._id}`,
        indicatorLabelSnapshot: row.derivedIndicatorLabelSnapshot,
        indicatorUnit: row.derivedIndicatorUnit ?? null,
        indicatorSlug: row.derivedIndicatorSlug,
        indicatorVersion: row.derivedIndicatorVersion,
        value: isStaleInactive ? null : row.value,
        latestRecordedValue: row.value,
        computedAt: row.computedAt,
        snapshotAt: row.snapshotAt,
        evidenceRowCount: row.baseSnapshotValueIds.length + row.derivedSnapshotValueIds.length,
        isDerived: true,
        isIndicatorEnabled: indicator.enabled,
        hasInactiveOperands,
        isStaleInactive,
        staleReason: !indicator.enabled
          ? 'indicator_disabled'
          : hasInactiveOperands
            ? 'operand_disabled'
            : null,
        sourceExportCount: row.sourceExportIds.length,
        derivedFromIndicatorSlugs: [...row.baseIndicatorSlugs, ...row.derivedIndicatorDependencySlugs],
        derivedFromIndicatorVersions: [...row.baseIndicatorVersions, ...row.derivedIndicatorDependencyVersions],
        formulaKind: row.formulaKind,
        snapshotId: String(row.snapshotId),
      }
    }))).filter((row) => row !== null)

    const values = [...latestBaseRows, ...latestDerivedRows]
      .sort((left, right) => (
        (right.snapshotAt ?? right.computedAt ?? 0) - (left.snapshotAt ?? left.computedAt ?? 0)
      ))

    return {
      snapshotId: values[0]?.snapshotId ? String(values[0].snapshotId) : null,
      snapshotAt: values[0]?.snapshotAt ?? values[0]?.computedAt ?? null,
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
