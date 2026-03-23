import { v } from 'convex/values'
import { internal } from './_generated/api.js'
import { internalMutation, mutation, query } from './_generated/server.js'
import type { Doc, Id } from './_generated/dataModel.js'

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

const schemaIndexValidator = v.object({
  key: v.string(),
  label: v.string(),
  fields: v.array(v.string()),
})

const schemaTableValidator = v.object({
  tableName: v.string(),
  tableKey: v.string(),
  label: v.string(),
  fields: v.array(fieldCatalogItemValidator),
  indexes: v.array(schemaIndexValidator),
  defaultDateFieldKey: v.optional(v.string()),
  defaultRowKeyFieldKey: v.optional(v.string()),
})

type CatalogOption = {
  key: string
  label: string
}

type SchemaField = {
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

type SchemaIndex = {
  key: string
  label: string
  fields: string[]
}

type SchemaTable = {
  tableName: string
  tableKey: string
  label: string
  fields: SchemaField[]
  indexes: SchemaIndex[]
  defaultDateFieldKey?: string
  defaultRowKeyFieldKey?: string
}

type ResettableTableName =
  | 'materializationJobRows'
  | 'analyticsMaterializedRows'
  | 'analyticsExports'
  | 'materializationJobs'
  | 'calculationDefinitions'
  | 'dataSources'

const RESETTABLE_TABLES: ResettableTableName[] = [
  'materializationJobRows',
  'analyticsMaterializedRows',
  'analyticsExports',
  'materializationJobs',
  'calculationDefinitions',
  'dataSources',
]

function humanizeLabel (value: string) {
  return value
    .replace(/[_-]+/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/^./, (char) => char.toUpperCase())
}

function normalizeCatalogOptions (options: CatalogOption[]) {
  return options
    .map((option) => ({
      key: option.key.trim(),
      label: option.label.trim() || option.key.trim(),
    }))
    .filter((option) => option.key)
}

function normalizeFieldCatalog (fieldCatalog: SchemaField[]) {
  return fieldCatalog
    .map((field) => ({
      key: field.key.trim(),
      label: field.label.trim() || humanizeLabel(field.key),
      valueType: field.valueType.trim(),
      filterable: field.filterable !== false,
      sourcePath: field.sourcePath?.trim() || undefined,
      sourceTable: field.sourceTable?.trim() || undefined,
      referenceTable: field.referenceTable?.trim() || undefined,
      isSystem: field.isSystem === true,
      isNullable: field.isNullable === true,
      isArray: field.isArray === true,
    }))
    .filter((field) => field.key && field.valueType)
}

function ensureUniqueValues (label: string, values: string[]) {
  if (new Set(values).size !== values.length) {
    throw new Error(`${label} contiene valori duplicati`)
  }
}

function normalizeIndexes (indexes: SchemaIndex[]) {
  return indexes
    .map((index) => ({
      key: index.key.trim(),
      label: index.label.trim() || humanizeLabel(index.key),
      fields: index.fields.map((field) => field.trim()).filter(Boolean),
    }))
    .filter((index) => index.key && index.fields.length > 0)
}

function buildScopeOptions (defaultDateFieldKey?: string) {
  const base = [{ key: 'all', label: 'Tutti i record' }]
  if (!defaultDateFieldKey) {
    return base
  }
  return [
    ...base,
    { key: 'last_3_months', label: 'Ultimi 3 mesi' },
  ]
}

function buildIdFieldSuggestions (table: SchemaTable) {
  const candidates = table.fields.filter((field) => (
    field.key === '_id' ||
    field.key === table.defaultRowKeyFieldKey ||
    field.valueType === 'id' ||
    /(?:^|_)(code|slug|email|external.*id|id)$/.test(field.key)
  ))

  const normalized = normalizeCatalogOptions(
    candidates.map((field) => ({
      key: field.key,
      label: field.label,
    }))
  )

  if (normalized.length > 0) {
    return normalized
  }

  return [{ key: '_id', label: 'ID documento' }]
}

function buildDefaultSelectedFieldKeys (table: SchemaTable) {
  const preferred = table.fields
    .filter((field) => field.valueType !== 'object')
    .slice(0, 12)
    .map((field) => field.key)

  if (preferred.length > 0) {
    return preferred
  }

  return table.fields.slice(0, 1).map((field) => field.key)
}

function buildEntityType (databaseKey: string, tableName: string) {
  return `${databaseKey}__${tableName}`
}

async function deleteRowsByIds (ctx: any, ids: Array<Id<any>>) {
  for (const id of ids) {
    await ctx.db.delete(id)
  }
}

async function clearDataSourceSettings (ctx: any) {
  const dataSourceSettings = await ctx.db.query('dataSourceSettings').collect()
  await deleteRowsByIds(
    ctx,
    dataSourceSettings.map((row: Doc<'dataSourceSettings'>) => row._id)
  )
}

async function deleteResettableBatch (
  ctx: any,
  tableName: ResettableTableName,
  batchSize: number
) {
  const rows = await ctx.db.query(tableName as never).take(batchSize)
  await deleteRowsByIds(
    ctx,
    rows.map((row: { _id: Id<any> }) => row._id)
  )
  return {
    deletedCount: rows.length,
    hasMore: rows.length === batchSize,
  }
}

async function regenerateCatalogFromSchemaImportsInternal (ctx: any) {
  const schemaImports = await ctx.db.query('schemaImports').collect()
  await clearDataSourceSettings(ctx)

  let generatedSettingsCount = 0
  const now = Date.now()

  for (const schemaImport of schemaImports as Array<Doc<'schemaImports'>>) {
    for (const table of schemaImport.tables as Array<SchemaTable>) {
      const fieldCatalog = normalizeFieldCatalog(table.fields)
      const idFieldSuggestions = buildIdFieldSuggestions(table)
      const allowedRowKeyStrategies = idFieldSuggestions.length > 0
        ? idFieldSuggestions
        : [{ key: '_id', label: 'ID documento' }]
      const defaultRowKeyStrategy = table.defaultRowKeyFieldKey || allowedRowKeyStrategies[0]?.key || '_id'
      const allowedScopes = buildScopeOptions(table.defaultDateFieldKey)
      const defaultSelectedFieldKeys = buildDefaultSelectedFieldKeys(table)
      const entityType = buildEntityType(schemaImport.databaseKey, table.tableName)

      await ctx.db.insert('dataSourceSettings', {
        entityType,
        label: `${humanizeLabel(schemaImport.databaseKey)} / ${table.label}`,
        adapterKey: `${schemaImport.databaseKey}:${table.tableName}`,
        sourceKind: 'materialized_rows',
        databaseKey: schemaImport.databaseKey,
        tableName: table.tableName,
        tableKey: table.tableKey,
        tableLabel: table.label,
        schemaImportId: schemaImport._id,
        allowedScopes,
        allowedRowKeyStrategies,
        idFieldSuggestions,
        indexSuggestions: normalizeIndexes(table.indexes),
        defaultScopeKey: allowedScopes[0]?.key,
        defaultRowKeyStrategy,
        defaultDateFieldKey: table.defaultDateFieldKey,
        defaultSelectedFieldKeys,
        fieldCatalog,
        metadata: {
          schemaDriven: true,
        },
        isActive: true,
        createdAt: now,
        updatedAt: now,
      })
      generatedSettingsCount++
    }
  }

  return {
    schemaImportCount: schemaImports.length,
    generatedSettingsCount,
  }
}

function validateSchemaTables (tables: SchemaTable[]) {
  if (tables.length === 0) {
    throw new Error('Lo schema non contiene tabelle utilizzabili')
  }

  ensureUniqueValues('tableKey', tables.map((table) => table.tableKey))
  ensureUniqueValues('tableName', tables.map((table) => table.tableName))

  for (const table of tables) {
    if (!table.tableName.trim()) {
      throw new Error('tableName obbligatorio')
    }
    if (!table.tableKey.trim()) {
      throw new Error(`tableKey obbligatorio per ${table.tableName}`)
    }
    if (table.fields.length === 0) {
      throw new Error(`Nessun campo disponibile per ${table.tableName}`)
    }
    ensureUniqueValues(`fields:${table.tableName}`, table.fields.map((field) => field.key))
  }
}

export const listCatalogResetJobs = query({
  args: {},
  returns: v.array(v.any()),
  handler: async (ctx) => {
    const rows = await ctx.db.query('catalogResetJobs').collect()
    return rows.sort((left, right) => (right.createdAt ?? 0) - (left.createdAt ?? 0))
  },
})

export const processCatalogResetBatch = internalMutation({
  args: {
    jobId: v.id('catalogResetJobs'),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId)
    if (!job || job.status === 'completed' || job.status === 'error') {
      return null
    }

    const now = Date.now()
    const batchSize = Math.max(25, Math.min(job.batchSize || 200, 500))
    const currentTableName = RESETTABLE_TABLES[job.currentTableIndex]

    if (!currentTableName) {
      await ctx.db.patch(args.jobId, {
        status: 'completed',
        currentTableName: undefined,
        finishedAt: now,
      })
      return null
    }

    await ctx.db.patch(args.jobId, {
      status: 'running',
      startedAt: job.startedAt ?? now,
      currentTableName,
      errorMessage: undefined,
    })

    try {
      const batchResult = await deleteResettableBatch(ctx, currentTableName, batchSize)
      const deletedCount = job.deletedCount + batchResult.deletedCount

      if (batchResult.hasMore) {
        await ctx.db.patch(args.jobId, {
          deletedCount,
        })
        await ctx.scheduler.runAfter(0, internal.schemaRegistry.processCatalogResetBatch, {
          jobId: args.jobId,
        })
        return null
      }

      const nextTableIndex = job.currentTableIndex + 1
      const nextTableName = RESETTABLE_TABLES[nextTableIndex]

      if (!nextTableName) {
        await ctx.db.patch(args.jobId, {
          status: 'completed',
          deletedCount,
          currentTableIndex: nextTableIndex,
          currentTableName: undefined,
          finishedAt: now,
        })
        return null
      }

      await ctx.db.patch(args.jobId, {
        deletedCount,
        currentTableIndex: nextTableIndex,
        currentTableName: nextTableName,
      })
      await ctx.scheduler.runAfter(0, internal.schemaRegistry.processCatalogResetBatch, {
        jobId: args.jobId,
      })
      return null
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Errore durante il reset catalogo'
      await ctx.db.patch(args.jobId, {
        status: 'error',
        errorMessage,
        finishedAt: now,
      })
      return null
    }
  },
})

export const startCatalogReset = mutation({
  args: {
    requestedBy: v.optional(v.string()),
    batchSize: v.optional(v.number()),
  },
  returns: v.object({
    jobId: v.id('catalogResetJobs'),
  }),
  handler: async (ctx, args) => {
    const activeJobs = await ctx.db
      .query('catalogResetJobs')
      .withIndex('by_status', (q) => q.eq('status', 'queued'))
      .collect()
    const runningJobs = await ctx.db
      .query('catalogResetJobs')
      .withIndex('by_status', (q) => q.eq('status', 'running'))
      .collect()

    if (activeJobs.length > 0 || runningJobs.length > 0) {
      throw new Error('Esiste gia` un reset dati materializzati in corso')
    }

    const now = Date.now()
    const jobId = await ctx.db.insert('catalogResetJobs', {
      status: 'queued',
      requestedBy: args.requestedBy,
      batchSize: Math.max(25, Math.min(args.batchSize ?? 200, 500)),
      currentTableIndex: 0,
      currentTableName: RESETTABLE_TABLES[0],
      deletedCount: 0,
      createdAt: now,
    })

    await ctx.scheduler.runAfter(0, internal.schemaRegistry.processCatalogResetBatch, {
      jobId,
    })

    return { jobId }
  },
})

export const listSchemaImports = query({
  args: {},
  returns: v.array(v.any()),
  handler: async (ctx) => {
    const rows = await ctx.db.query('schemaImports').collect()
    return rows.sort((left, right) => left.databaseKey.localeCompare(right.databaseKey) || left.fileName.localeCompare(right.fileName))
  },
})

export const replaceSchemaImport = mutation({
  args: {
    databaseKey: v.string(),
    fileName: v.string(),
    checksum: v.string(),
    schemaSource: v.string(),
    tables: v.array(schemaTableValidator),
  },
  returns: v.object({
    schemaImportId: v.id('schemaImports'),
  }),
  handler: async (ctx, args) => {
    validateSchemaTables(args.tables as SchemaTable[])

    const now = Date.now()
    const existing = await ctx.db
      .query('schemaImports')
      .withIndex('by_database_key_and_file_name', (q) =>
        q.eq('databaseKey', args.databaseKey).eq('fileName', args.fileName)
      )
      .unique()

    if (existing) {
      await ctx.db.patch(existing._id, {
        checksum: args.checksum,
        schemaSource: args.schemaSource,
        tables: args.tables,
        updatedAt: now,
      })
      return { schemaImportId: existing._id }
    }

    const schemaImportId = await ctx.db.insert('schemaImports', {
      databaseKey: args.databaseKey,
      fileName: args.fileName,
      checksum: args.checksum,
      schemaSource: args.schemaSource,
      tables: args.tables,
      createdAt: now,
      updatedAt: now,
    })

    return { schemaImportId }
  },
})

export const regenerateCatalogFromSchemas = mutation({
  args: {},
  returns: v.object({
    schemaImportCount: v.number(),
    generatedSettingsCount: v.number(),
  }),
  handler: async (ctx) => {
    return await regenerateCatalogFromSchemaImportsInternal(ctx)
  },
})

export const deleteSchemaImport = mutation({
  args: {
    schemaImportId: v.id('schemaImports'),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.db.delete(args.schemaImportId)
    await regenerateCatalogFromSchemaImportsInternal(ctx)
    return null
  },
})

export const regenerateSchemaCatalog = mutation({
  args: {},
  returns: v.object({
    schemaImportCount: v.number(),
    generatedSettingsCount: v.number(),
  }),
  handler: async (ctx) => {
    return await regenerateCatalogFromSchemaImportsInternal(ctx)
  },
})
