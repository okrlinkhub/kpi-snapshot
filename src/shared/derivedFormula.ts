import { v } from 'convex/values'

export const derivedFormulaKindValidator = v.union(
  v.literal('ratio'),
  v.literal('difference'),
  v.literal('sum')
)

export const derivedOperandRoleValidator = v.union(
  v.literal('numerator'),
  v.literal('denominator'),
  v.literal('term')
)

export const derivedIndicatorRefKindValidator = v.union(
  v.literal('base'),
  v.literal('derived')
)

export const derivedExpressionOperatorValidator = v.union(
  v.literal('add'),
  v.literal('sub'),
  v.literal('mul'),
  v.literal('div')
)

export const legacyDerivedFormulaValidator = v.object({
  formulaVersion: v.optional(v.literal(1)),
  kind: derivedFormulaKindValidator,
  operands: v.array(
    v.object({
      indicatorSlug: v.string(),
      role: v.optional(derivedOperandRoleValidator),
      weight: v.optional(v.number()),
    })
  ),
})

export const derivedExpressionNodeValidator = v.union(
  v.object({
    id: v.string(),
    type: v.literal('ref'),
    indicatorKind: derivedIndicatorRefKindValidator,
    indicatorSlug: v.string(),
  }),
  v.object({
    id: v.string(),
    type: v.literal('constant'),
    value: v.number(),
  }),
  v.object({
    id: v.string(),
    type: v.literal('operation'),
    op: derivedExpressionOperatorValidator,
    leftNodeId: v.string(),
    rightNodeId: v.string(),
  })
)

export const expressionDerivedFormulaValidator = v.object({
  formulaVersion: v.literal(2),
  rootNodeId: v.string(),
  nodes: v.array(derivedExpressionNodeValidator),
})

export const derivedFormulaValidator = v.union(
  legacyDerivedFormulaValidator,
  expressionDerivedFormulaValidator
)

export type DerivedFormulaKind = 'ratio' | 'difference' | 'sum'
export type DerivedOperandRole = 'numerator' | 'denominator' | 'term'
export type DerivedIndicatorRefKind = 'base' | 'derived'
export type DerivedExpressionOperator = 'add' | 'sub' | 'mul' | 'div'

export type LegacyDerivedFormula = {
  formulaVersion?: 1
  kind: DerivedFormulaKind
  operands: Array<{
    indicatorSlug: string
    role?: DerivedOperandRole
    weight?: number
  }>
}

export type DerivedExpressionNode =
  | {
      id: string
      type: 'ref'
      indicatorKind: DerivedIndicatorRefKind
      indicatorSlug: string
    }
  | {
      id: string
      type: 'constant'
      value: number
    }
  | {
      id: string
      type: 'operation'
      op: DerivedExpressionOperator
      leftNodeId: string
      rightNodeId: string
    }

export type ExpressionDerivedFormula = {
  formulaVersion: 2
  rootNodeId: string
  nodes: DerivedExpressionNode[]
}

export type DerivedFormula = LegacyDerivedFormula | ExpressionDerivedFormula

export type DerivedFormulaDependency = {
  indicatorKind: DerivedIndicatorRefKind
  indicatorSlug: string
}

export function isExpressionDerivedFormula (
  formula: DerivedFormula
): formula is ExpressionDerivedFormula {
  return formula.formulaVersion === 2
}

export function isLegacyDerivedFormula (
  formula: DerivedFormula
): formula is LegacyDerivedFormula {
  return !isExpressionDerivedFormula(formula)
}

export function getDerivedFormulaKind (formula: DerivedFormula) {
  if (isExpressionDerivedFormula(formula)) {
    return 'expression' as const
  }
  return formula.kind
}

export function listDirectFormulaDependencies (formula: DerivedFormula): DerivedFormulaDependency[] {
  if (isLegacyDerivedFormula(formula)) {
    return formula.operands.map((operand) => ({
      indicatorKind: 'base' as const,
      indicatorSlug: operand.indicatorSlug,
    }))
  }

  return formula.nodes
    .filter((node): node is Extract<DerivedExpressionNode, { type: 'ref' }> => node.type === 'ref')
    .map((node) => ({
      indicatorKind: node.indicatorKind,
      indicatorSlug: node.indicatorSlug,
    }))
}
