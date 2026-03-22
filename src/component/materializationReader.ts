import { v } from 'convex/values'
import { query } from './_generated/server.js'

function scopeKindToCutoff (scopeKind: 'all' | 'last_3_months') {
  if (scopeKind !== 'last_3_months') {
    return null
  }

  const cutoffDate = new Date()
  cutoffDate.setMonth(cutoffDate.getMonth() - 3)
  return cutoffDate.getTime()
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

export const listMaterializableRows = query({
  args: {
    tableName: v.string(),
    scopeKind: v.union(v.literal('all'), v.literal('last_3_months')),
    dateFieldKey: v.optional(v.string()),
    indexKey: v.optional(v.string()),
  },
  returns: v.array(v.any()),
  handler: async (ctx, args) => {
    const cutoff = scopeKindToCutoff(args.scopeKind)
    const dateFieldKey = args.dateFieldKey || '_creationTime'
    const rows = (
      cutoff != null && args.indexKey
        ? await ctx.db
          .query(args.tableName as never)
          .withIndex(args.indexKey as never, (q) => q.gte(dateFieldKey as never, cutoff as never))
          .collect()
        : await ctx.db.query(args.tableName as never).collect()
    ) as Array<Record<string, unknown> & {
      _id: string
      _creationTime: number
    }>

    if (cutoff == null) {
      return rows
    }

    return rows.filter((row) => {
      const dateValue = getNestedValue(row, dateFieldKey)
      if (typeof dateValue === 'number') {
        return dateValue >= cutoff
      }
      return row._creationTime >= cutoff
    })
  },
})
