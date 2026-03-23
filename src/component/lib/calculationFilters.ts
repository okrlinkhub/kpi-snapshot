import { v } from 'convex/values'

export const fieldRuleOperatorValidator = v.union(
  v.literal('eq'),
  v.literal('neq'),
  v.literal('gt'),
  v.literal('gte'),
  v.literal('lt'),
  v.literal('lte'),
  v.literal('in')
)

export const calculationOperandValidator = v.union(
  v.object({
    kind: v.literal('literal'),
    value: v.any(),
  }),
  v.object({
    kind: v.literal('field'),
    field: v.string(),
  })
)

export const fieldRuleValidator = v.object({
  field: v.string(),
  op: fieldRuleOperatorValidator,
  rightOperand: calculationOperandValidator,
})

export const timeRangeKindValidator = v.union(
  v.literal('last_month'),
  v.literal('last_3_months'),
  v.literal('month_to_date'),
  v.literal('year_to_date')
)

export const calculationTimeRangeValidator = v.object({
  kind: timeRangeKindValidator,
})

export const calculationFiltersValidator = v.object({
  fieldRules: v.array(fieldRuleValidator),
  fieldRuleTree: v.optional(v.any()),
  timeRange: v.optional(calculationTimeRangeValidator),
})

export type FieldRuleOperator =
  | 'eq'
  | 'neq'
  | 'gt'
  | 'gte'
  | 'lt'
  | 'lte'
  | 'in'

export type CalculationOperand =
  | {
    kind: 'literal'
    value: unknown
  }
  | {
    kind: 'field'
    field: string
  }

export type CalculationFieldRule = {
  field: string
  op: FieldRuleOperator
  rightOperand: CalculationOperand
}

export type CalculationRuleGroupOperator = 'and' | 'or'

export type CalculationRuleNode =
  | {
    type: 'rule'
    rule: CalculationFieldRule
  }
  | {
    type: 'group'
    op: CalculationRuleGroupOperator
    children: CalculationRuleNode[]
  }

export type CalculationTimeRangeKind =
  | 'last_month'
  | 'last_3_months'
  | 'month_to_date'
  | 'year_to_date'

export type CalculationTimeRange = {
  kind: CalculationTimeRangeKind
}

export type CalculationFilters = {
  fieldRules: CalculationFieldRule[]
  fieldRuleTree?: {
    type: 'group'
    op: CalculationRuleGroupOperator
    children: CalculationRuleNode[]
  }
  timeRange?: CalculationTimeRange
}

export type ResolvedCalculationTimeRange = {
  kind: CalculationTimeRangeKind
  startMs: number
  endMs: number
}

export type ResolvedCalculationFilters = {
  fieldRules: CalculationFieldRule[]
  fieldRuleTree: {
    type: 'group'
    op: CalculationRuleGroupOperator
    children: CalculationRuleNode[]
  }
  timeRange?: ResolvedCalculationTimeRange
}

function buildLegacyFieldRuleTree (fieldRules: CalculationFieldRule[]) {
  return {
    type: 'group' as const,
    op: 'and' as const,
    children: fieldRules.map((rule) => ({
      type: 'rule' as const,
      rule,
    })),
  }
}

function normalizeFieldRuleTree (
  fieldRuleTree: CalculationFilters['fieldRuleTree'] | undefined,
  fieldRules: CalculationFieldRule[]
) {
  if (
    fieldRuleTree &&
    fieldRuleTree.type === 'group' &&
    Array.isArray(fieldRuleTree.children)
  ) {
    return fieldRuleTree
  }

  return buildLegacyFieldRuleTree(fieldRules)
}

export function normalizeCalculationFilters (filters?: Partial<CalculationFilters> | null): CalculationFilters {
  const normalizedFieldRules = Array.isArray(filters?.fieldRules) ? filters.fieldRules : []

  return {
    fieldRules: normalizedFieldRules,
    fieldRuleTree: normalizeFieldRuleTree(filters?.fieldRuleTree, normalizedFieldRules),
    timeRange: filters?.timeRange ?? undefined,
  }
}

export function resolveCalculationTimeRange (
  timeRange: CalculationTimeRange | undefined,
  snapshotAt: number
): ResolvedCalculationTimeRange | null {
  if (!timeRange) {
    return null
  }

  const snapshotDate = new Date(snapshotAt)
  const snapshotYear = snapshotDate.getUTCFullYear()
  const snapshotMonth = snapshotDate.getUTCMonth()

  switch (timeRange.kind) {
    case 'last_month':
      return {
        kind: timeRange.kind,
        startMs: Date.UTC(snapshotYear, snapshotMonth - 1, 1, 0, 0, 0, 0),
        endMs: Date.UTC(snapshotYear, snapshotMonth, 1, 0, 0, 0, 0) - 1,
      }
    case 'last_3_months':
      return {
        kind: timeRange.kind,
        startMs: Date.UTC(snapshotYear, snapshotMonth - 3, 1, 0, 0, 0, 0),
        endMs: Date.UTC(snapshotYear, snapshotMonth, 1, 0, 0, 0, 0) - 1,
      }
    case 'month_to_date':
      return {
        kind: timeRange.kind,
        startMs: Date.UTC(snapshotYear, snapshotMonth, 1, 0, 0, 0, 0),
        endMs: snapshotAt,
      }
    case 'year_to_date':
      return {
        kind: timeRange.kind,
        startMs: Date.UTC(snapshotYear, 0, 1, 0, 0, 0, 0),
        endMs: snapshotAt,
      }
    default:
      return null
  }
}

export function resolveCalculationFilters (
  filters: CalculationFilters | undefined,
  snapshotAt: number
): ResolvedCalculationFilters {
  const normalizedFilters = normalizeCalculationFilters(filters)
  const resolvedTimeRange = resolveCalculationTimeRange(normalizedFilters.timeRange, snapshotAt)

  return {
    fieldRules: normalizedFilters.fieldRules,
    fieldRuleTree: normalizeFieldRuleTree(normalizedFilters.fieldRuleTree, normalizedFilters.fieldRules),
    timeRange: resolvedTimeRange ?? undefined,
  }
}
