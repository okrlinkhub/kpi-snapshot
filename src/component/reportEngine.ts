import { v } from 'convex/values'
import { mutation, query, type MutationCtx, type QueryCtx } from './_generated/server.js'
import type { Doc, Id } from './_generated/dataModel.js'

const reportWidgetKindValidator = v.union(
  v.literal('base'),
  v.literal('derived')
)

const reportSummaryValidator = v.object({
  _id: v.id('reports'),
  _creationTime: v.number(),
  profileId: v.id('snapshotProfiles'),
  profileSlug: v.string(),
  slug: v.string(),
  name: v.string(),
  description: v.optional(v.string()),
  isArchived: v.boolean(),
  createdByKey: v.optional(v.string()),
  updatedByKey: v.optional(v.string()),
  createdAt: v.number(),
  updatedAt: v.number(),
})

const reportWidgetValidator = v.object({
  _id: v.id('reportWidgets'),
  _creationTime: v.number(),
  reportId: v.id('reports'),
  indicatorSlug: v.string(),
  indicatorLabel: v.string(),
  indicatorUnit: v.optional(v.string()),
  indicatorKind: reportWidgetKindValidator,
  order: v.number(),
  createdAt: v.number(),
  updatedAt: v.optional(v.number()),
})

function normalizeOptionalString (value?: string) {
  const normalized = value?.trim()
  return normalized ? normalized : undefined
}

function slugifyReportName (value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'report'
}

async function getProfileBySlug (ctx: QueryCtx | MutationCtx, slug: string) {
  const profile = await ctx.db
    .query('snapshotProfiles')
    .withIndex('by_slug', (q) => q.eq('slug', slug))
    .unique()

  if (!profile || profile.archivedAt) {
    throw new Error(`Snapshot profile '${slug}' non trovato`)
  }

  return profile
}

async function buildUniqueReportSlug (
  ctx: QueryCtx | MutationCtx,
  value: string,
  excludeReportId?: Id<'reports'>
) {
  const baseSlug = slugifyReportName(value)
  let candidate = baseSlug
  let attempt = 1

  while (true) {
    const existing = await ctx.db
      .query('reports')
      .withIndex('by_slug', (q) => q.eq('slug', candidate))
      .unique()

    if (!existing || existing._id === excludeReportId) {
      return candidate
    }

    attempt += 1
    candidate = `${baseSlug}-${attempt}`
  }
}

async function buildReportSummary (
  ctx: QueryCtx | MutationCtx,
  report: Doc<'reports'>
) {
  const profile = await ctx.db.get(report.profileId)
  if (!profile || profile.archivedAt) {
    return null
  }

  return {
    ...report,
    profileSlug: profile.slug,
  }
}

async function getReportOrThrow (ctx: QueryCtx | MutationCtx, reportId: Id<'reports'>) {
  const report = await ctx.db.get(reportId)
  if (!report || report.isArchived) {
    throw new Error('Report non trovato')
  }
  return report
}

async function loadReportDetail (
  ctx: QueryCtx | MutationCtx,
  report: Doc<'reports'>
) {
  if (report.isArchived) {
    return null
  }

  const reportSummary = await buildReportSummary(ctx, report)
  if (!reportSummary) {
    return null
  }

  const widgets = await ctx.db
    .query('reportWidgets')
    .withIndex('by_report_and_order', (q) => q.eq('reportId', report._id))
    .collect()

  return {
    report: reportSummary,
    widgets,
  }
}

export const listReports = query({
  args: {
    profileSlug: v.optional(v.string()),
    includeArchived: v.optional(v.boolean()),
  },
  returns: v.array(reportSummaryValidator),
  handler: async (ctx, args) => {
    const includeArchived = args.includeArchived ?? false
    const profile = args.profileSlug
      ? await getProfileBySlug(ctx, args.profileSlug)
      : null

    const rows = profile
      ? await ctx.db
        .query('reports')
        .withIndex('by_profile', (q) => q.eq('profileId', profile._id))
        .collect()
      : includeArchived
        ? await ctx.db.query('reports').collect()
        : await ctx.db
          .query('reports')
          .withIndex('by_is_archived', (q) => q.eq('isArchived', false))
          .collect()

    const summaries = (await Promise.all(rows.map(async (report) => {
      if (!includeArchived && report.isArchived) {
        return null
      }
      return await buildReportSummary(ctx, report)
    }))).filter((report): report is NonNullable<Awaited<ReturnType<typeof buildReportSummary>>> => Boolean(report))

    return summaries.sort((left, right) => right.updatedAt - left.updatedAt)
  },
})

export const getReport = query({
  args: {
    reportId: v.id('reports'),
  },
  returns: v.union(v.object({
    report: reportSummaryValidator,
    widgets: v.array(reportWidgetValidator),
  }), v.null()),
  handler: async (ctx, args) => {
    const report = await ctx.db.get(args.reportId)
    if (!report) {
      return null
    }

    return await loadReportDetail(ctx, report)
  },
})

export const getReportBySlug = query({
  args: {
    slug: v.string(),
  },
  returns: v.union(v.object({
    report: reportSummaryValidator,
    widgets: v.array(reportWidgetValidator),
  }), v.null()),
  handler: async (ctx, args) => {
    const report = await ctx.db
      .query('reports')
      .withIndex('by_slug', (q) => q.eq('slug', args.slug))
      .unique()

    if (!report) {
      return null
    }

    return await loadReportDetail(ctx, report)
  },
})

export const createReport = mutation({
  args: {
    profileSlug: v.string(),
    name: v.string(),
    description: v.optional(v.string()),
    slug: v.optional(v.string()),
    createdByKey: v.optional(v.string()),
  },
  returns: v.object({
    reportId: v.id('reports'),
    slug: v.string(),
  }),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug)
    const now = Date.now()
    const slug = await buildUniqueReportSlug(
      ctx,
      normalizeOptionalString(args.slug) ?? args.name
    )

    const reportId = await ctx.db.insert('reports', {
      profileId: profile._id,
      slug,
      name: args.name.trim(),
      description: normalizeOptionalString(args.description),
      isArchived: false,
      createdByKey: normalizeOptionalString(args.createdByKey),
      updatedByKey: normalizeOptionalString(args.createdByKey),
      createdAt: now,
      updatedAt: now,
    })

    return {
      reportId,
      slug,
    }
  },
})

export const archiveReport = mutation({
  args: {
    reportId: v.id('reports'),
    updatedByKey: v.optional(v.string()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const report = await getReportOrThrow(ctx, args.reportId)

    await ctx.db.patch(report._id, {
      isArchived: true,
      updatedByKey: normalizeOptionalString(args.updatedByKey),
      updatedAt: Date.now(),
    })

    return null
  },
})

export const updateReportMeta = mutation({
  args: {
    reportId: v.id('reports'),
    name: v.optional(v.string()),
    description: v.optional(v.string()),
    slug: v.optional(v.string()),
    isArchived: v.optional(v.boolean()),
    updatedByKey: v.optional(v.string()),
  },
  returns: v.object({
    reportId: v.id('reports'),
    slug: v.string(),
  }),
  handler: async (ctx, args) => {
    const report = await ctx.db.get(args.reportId)
    if (!report) {
      throw new Error('Report non trovato')
    }

    const nextSlug = args.slug !== undefined
      ? await buildUniqueReportSlug(ctx, args.slug, report._id)
      : report.slug

    await ctx.db.patch(report._id, {
      name: args.name?.trim() || report.name,
      description: args.description !== undefined
        ? normalizeOptionalString(args.description)
        : report.description,
      slug: nextSlug,
      isArchived: args.isArchived ?? report.isArchived,
      updatedByKey: normalizeOptionalString(args.updatedByKey),
      updatedAt: Date.now(),
    })

    return {
      reportId: report._id,
      slug: nextSlug,
    }
  },
})

export const addReportWidget = mutation({
  args: {
    reportId: v.id('reports'),
    indicatorSlug: v.string(),
    indicatorLabel: v.string(),
    indicatorUnit: v.optional(v.string()),
    indicatorKind: reportWidgetKindValidator,
  },
  returns: v.id('reportWidgets'),
  handler: async (ctx, args) => {
    const report = await getReportOrThrow(ctx, args.reportId)

    const existingWidget = await ctx.db
      .query('reportWidgets')
      .withIndex('by_report_and_indicator_kind_and_indicator_slug', (q) =>
        q
          .eq('reportId', report._id)
          .eq('indicatorKind', args.indicatorKind)
          .eq('indicatorSlug', args.indicatorSlug)
      )
      .unique()

    if (existingWidget) {
      await ctx.db.patch(existingWidget._id, {
        indicatorLabel: args.indicatorLabel,
        indicatorUnit: normalizeOptionalString(args.indicatorUnit),
        updatedAt: Date.now(),
      })
      await ctx.db.patch(report._id, {
        updatedAt: Date.now(),
      })
      return existingWidget._id
    }

    const lastWidget = await ctx.db
      .query('reportWidgets')
      .withIndex('by_report_and_order', (q) => q.eq('reportId', report._id))
      .order('desc')
      .take(1)

    const now = Date.now()
    const widgetId = await ctx.db.insert('reportWidgets', {
      reportId: report._id,
      indicatorSlug: args.indicatorSlug,
      indicatorLabel: args.indicatorLabel,
      indicatorUnit: normalizeOptionalString(args.indicatorUnit),
      indicatorKind: args.indicatorKind,
      order: (lastWidget[0]?.order ?? -1) + 1,
      createdAt: now,
      updatedAt: now,
    })

    await ctx.db.patch(report._id, {
      updatedAt: now,
    })

    return widgetId
  },
})

export const removeReportWidget = mutation({
  args: {
    widgetId: v.id('reportWidgets'),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const widget = await ctx.db.get(args.widgetId)
    if (!widget) {
      throw new Error('Widget non trovato')
    }

    const report = await getReportOrThrow(ctx, widget.reportId)
    await ctx.db.delete(widget._id)

    const remainingWidgets = await ctx.db
      .query('reportWidgets')
      .withIndex('by_report_and_order', (q) => q.eq('reportId', report._id))
      .collect()

    const now = Date.now()
    for (const [index, currentWidget] of remainingWidgets.entries()) {
      if (currentWidget.order !== index) {
        await ctx.db.patch(currentWidget._id, {
          order: index,
          updatedAt: now,
        })
      }
    }

    await ctx.db.patch(report._id, {
      updatedAt: now,
    })

    return null
  },
})

export const reorderReportWidgets = mutation({
  args: {
    reportId: v.id('reports'),
    widgetIds: v.array(v.id('reportWidgets')),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const report = await getReportOrThrow(ctx, args.reportId)
    const widgets = await ctx.db
      .query('reportWidgets')
      .withIndex('by_report_and_order', (q) => q.eq('reportId', report._id))
      .collect()

    if (args.widgetIds.length !== widgets.length) {
      throw new Error('Lista widget non valida')
    }

    const widgetMap = new Map(widgets.map((widget) => [widget._id, widget] as const))
    const now = Date.now()
    for (const [index, widgetId] of args.widgetIds.entries()) {
      const widget = widgetMap.get(widgetId)
      if (!widget) {
        throw new Error('Widget non appartenente al report selezionato')
      }
      await ctx.db.patch(widgetId, {
        order: index,
        updatedAt: now,
      })
    }

    await ctx.db.patch(report._id, {
      updatedAt: now,
    })

    return null
  },
})
