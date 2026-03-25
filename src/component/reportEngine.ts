import { v } from 'convex/values'
import { mutation, query, type MutationCtx, type QueryCtx } from './_generated/server.js'
import type { Doc, Id } from './_generated/dataModel.js'
import {
  createReportWidgetArgsValidator,
  storedReportWidgetValidator,
  type CreateChartReportWidgetArgs,
  type CreateReportWidgetArgs,
  type CreateSingleValueReportWidgetArgs,
  type ReportWidgetLayout,
  type ReportWidgetMember,
  type ReportWidgetMemberInput,
} from '../shared/reportWidgets.js'

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

function normalizeReportUsageCount (value?: number) {
  return Math.max(0, Math.trunc(value ?? 0))
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

async function getBaseIndicatorByProfileAndSlug (
  ctx: QueryCtx | MutationCtx,
  profileId: Id<'snapshotProfiles'>,
  indicatorSlug: string
) {
  const rows = await ctx.db
    .query('indicators')
    .withIndex('by_profile_and_slug_and_version', (q) =>
      q.eq('profileId', profileId).eq('slug', indicatorSlug)
    )
    .order('desc')
    .take(1)
  return rows[0] ?? null
}

async function getDerivedIndicatorByProfileAndSlug (
  ctx: QueryCtx | MutationCtx,
  profileId: Id<'snapshotProfiles'>,
  indicatorSlug: string
) {
  const rows = await ctx.db
    .query('derivedIndicators')
    .withIndex('by_profile_and_slug_and_version', (q) =>
      q.eq('profileId', profileId).eq('slug', indicatorSlug)
    )
    .order('desc')
    .take(1)
  return rows[0] ?? null
}

function buildDefaultWidgetLayout (args: CreateReportWidgetArgs): ReportWidgetLayout {
  if (args.widgetType === 'single_value') {
    return {
      width: 'compact',
      height: 'sm',
      emphasis: 'default',
    }
  }

  return {
    width: args.chartKind === 'pie' ? 'wide' : 'full',
    height: args.chartKind === 'pie' ? 'md' : 'lg',
    emphasis: args.chartKind === 'pie' ? 'accent' : 'default',
  }
}

function normalizeReportWidgetLayout (
  args: CreateReportWidgetArgs
) {
  return args.layout ?? buildDefaultWidgetLayout(args)
}

function normalizeReportWidgetTitle (
  args: CreateSingleValueReportWidgetArgs | CreateChartReportWidgetArgs,
  members: ReportWidgetMember[]
) {
  const explicitTitle = normalizeOptionalString(args.title)
  if (explicitTitle) {
    return explicitTitle
  }

  if (args.widgetType === 'single_value') {
    return members[0]?.indicatorLabel ?? 'KPI'
  }

  if (args.chartKind === 'pie') {
    return 'Confronto indicatori'
  }

  if (members.length === 1) {
    return `${members[0].indicatorLabel} nel tempo`
  }

  return 'Trend indicatori'
}

function assertValidTimeRange (limit?: number) {
  if (limit === undefined) {
    return
  }

  if (!Number.isInteger(limit) || limit < 2 || limit > 36) {
    throw new Error('Il time range dei widget trend deve avere un limite intero tra 2 e 36 snapshot')
  }
}

function assertUniqueMembers (members: ReportWidgetMember[]) {
  const keys = new Set<string>()
  for (const member of members) {
    const key = `${member.sourceProfileSlug}:${member.indicatorKind}:${member.indicatorSlug}`
    if (keys.has(key)) {
      throw new Error(`Indicatore duplicato nel widget: ${member.indicatorLabel}`)
    }
    keys.add(key)
  }
}

async function resolveReportWidgetMember (
  ctx: QueryCtx | MutationCtx,
  member: ReportWidgetMemberInput
) {
  const sourceProfile = await getProfileBySlug(ctx, member.sourceProfileSlug)

  if (member.indicatorKind === 'base') {
    const indicator = await getBaseIndicatorByProfileAndSlug(ctx, sourceProfile._id, member.indicatorSlug)
    if (!indicator) {
      throw new Error(`Indicatore '${member.indicatorSlug}' non trovato nel profilo sorgente selezionato`)
    }

    return {
      sourceProfileId: sourceProfile._id,
      sourceProfileSlug: sourceProfile.slug,
      indicatorSlug: indicator.slug,
      indicatorLabel: normalizeOptionalString(member.indicatorLabel) ?? indicator.label,
      indicatorUnit: normalizeOptionalString(member.indicatorUnit) ?? normalizeOptionalString(indicator.unit),
      indicatorKind: 'base' as const,
    }
  }

  const derivedIndicator = await getDerivedIndicatorByProfileAndSlug(ctx, sourceProfile._id, member.indicatorSlug)
  if (!derivedIndicator) {
    throw new Error(`Indicatore derivato '${member.indicatorSlug}' non trovato nel profilo sorgente selezionato`)
  }

  return {
    sourceProfileId: sourceProfile._id,
    sourceProfileSlug: sourceProfile.slug,
    indicatorSlug: derivedIndicator.slug,
    indicatorLabel: normalizeOptionalString(member.indicatorLabel) ?? derivedIndicator.label,
    indicatorUnit: normalizeOptionalString(member.indicatorUnit) ?? normalizeOptionalString(derivedIndicator.unit),
    indicatorKind: 'derived' as const,
  }
}

async function resolveReportWidgetMembers (
  ctx: QueryCtx | MutationCtx,
  members: ReportWidgetMemberInput[]
) {
  const resolvedMembers = await Promise.all(members.map(async (member) => {
    return await resolveReportWidgetMember(ctx, member)
  }))

  assertUniqueMembers(resolvedMembers)
  return resolvedMembers
}

function assertCreateWidgetArgs (
  args: CreateReportWidgetArgs,
  members: ReportWidgetMember[]
) {
  if (args.widgetType === 'single_value') {
    if (members.length !== 1) {
      throw new Error('I widget KPI devono avere esattamente un indicatore')
    }
    return
  }

  if (members.length === 0) {
    throw new Error('Seleziona almeno un indicatore per il grafico')
  }

  if (members.length > 8) {
    throw new Error('Ogni grafico puo` contenere al massimo 8 indicatori')
  }

  if (args.chartKind === 'pie') {
    if (members.length < 2) {
      throw new Error('I grafici a torta richiedono almeno 2 indicatori')
    }
    const profileSlugs = new Set(members.map((member) => member.sourceProfileSlug))
    if (profileSlugs.size > 1) {
      throw new Error('I grafici a torta richiedono indicatori dello stesso profilo')
    }
    if (args.timeRange) {
      throw new Error('I grafici a torta non supportano un time range storico')
    }
    return
  }

  assertValidTimeRange(args.timeRange?.limit)
}

function parseCreateReportWidgetArgs (args: {
  reportId: Id<'reports'>
  widgetType: 'single_value' | 'chart'
  title?: string
  description?: string
  layout?: ReportWidgetLayout
  member?: ReportWidgetMemberInput
  chartKind?: 'line' | 'area' | 'bar' | 'pie'
  timeRange?: { mode: 'latest_n_snapshots', limit: number }
  members?: ReportWidgetMemberInput[]
}): CreateReportWidgetArgs {
  if (args.widgetType === 'single_value') {
    if (!args.member) {
      throw new Error('I widget KPI richiedono un indicatore')
    }

    return {
      reportId: args.reportId,
      widgetType: 'single_value',
      title: args.title,
      description: args.description,
      layout: args.layout,
      member: args.member,
    }
  }

  if (!args.chartKind) {
    throw new Error('Specifica il tipo di grafico da creare')
  }

  return {
    reportId: args.reportId,
    widgetType: 'chart',
    chartKind: args.chartKind,
    title: args.title,
    description: args.description,
    layout: args.layout,
    timeRange: args.timeRange,
    members: args.members ?? [],
  }
}

async function adjustReportWidgetMemberUsageCounter (
  ctx: MutationCtx,
  member: ReportWidgetMember,
  delta: 1 | -1
) {
  if (member.indicatorKind === 'base') {
    const indicator = await getBaseIndicatorByProfileAndSlug(
      ctx,
      member.sourceProfileId as Id<'snapshotProfiles'>,
      member.indicatorSlug
    )
    if (!indicator) {
      return
    }

    await ctx.db.patch(indicator._id, {
      reportUsageCount: Math.max(0, normalizeReportUsageCount(indicator.reportUsageCount) + delta),
      updatedAt: Date.now(),
    })
    return
  }

  const derivedIndicator = await getDerivedIndicatorByProfileAndSlug(
    ctx,
    member.sourceProfileId as Id<'snapshotProfiles'>,
    member.indicatorSlug
  )
  if (!derivedIndicator) {
    return
  }

  await ctx.db.patch(derivedIndicator._id, {
    reportUsageCount: Math.max(0, normalizeReportUsageCount(derivedIndicator.reportUsageCount) + delta),
    updatedAt: Date.now(),
  })
}

async function adjustReportWidgetUsageCounter (
  ctx: MutationCtx,
  widget: Pick<Doc<'reportWidgets'>, 'members'>,
  delta: 1 | -1
) {
  for (const member of widget.members) {
    await adjustReportWidgetMemberUsageCounter(ctx, member as ReportWidgetMember, delta)
  }
}

async function adjustReportWidgetsUsageCounter (
  ctx: MutationCtx,
  reportId: Id<'reports'>,
  delta: 1 | -1
) {
  const widgets = await ctx.db
    .query('reportWidgets')
    .withIndex('by_report', (q) => q.eq('reportId', reportId))
    .collect()

  for (const widget of widgets) {
    await adjustReportWidgetUsageCounter(ctx, widget, delta)
  }
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
    widgets: v.array(storedReportWidgetValidator),
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
    widgets: v.array(storedReportWidgetValidator),
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
    await adjustReportWidgetsUsageCounter(ctx, report._id, -1)

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
    const nextIsArchived = args.isArchived ?? report.isArchived

    if (report.isArchived !== nextIsArchived) {
      await adjustReportWidgetsUsageCounter(ctx, report._id, nextIsArchived ? -1 : 1)
    }

    await ctx.db.patch(report._id, {
      name: args.name?.trim() || report.name,
      description: args.description !== undefined
        ? normalizeOptionalString(args.description)
        : report.description,
      slug: nextSlug,
      isArchived: nextIsArchived,
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
  args: createReportWidgetArgsValidator,
  returns: v.id('reportWidgets'),
  handler: async (ctx, args) => {
    const parsedArgs = parseCreateReportWidgetArgs(args)
    const report = await getReportOrThrow(ctx, parsedArgs.reportId as Id<'reports'>)
    const members = parsedArgs.widgetType === 'single_value'
      ? [await resolveReportWidgetMember(ctx, parsedArgs.member)]
      : await resolveReportWidgetMembers(ctx, parsedArgs.members)

    assertCreateWidgetArgs(parsedArgs, members)

    const lastWidget = await ctx.db
      .query('reportWidgets')
      .withIndex('by_report_and_order', (q) => q.eq('reportId', report._id))
      .order('desc')
      .take(1)

    const now = Date.now()
    const baseDocument = {
      reportId: report._id,
      title: normalizeReportWidgetTitle(parsedArgs, members),
      description: normalizeOptionalString(parsedArgs.description),
      layout: normalizeReportWidgetLayout(parsedArgs),
      members,
      order: (lastWidget[0]?.order ?? -1) + 1,
      createdAt: now,
      updatedAt: now,
    }
    const widgetId = await ctx.db.insert('reportWidgets', parsedArgs.widgetType === 'single_value'
      ? {
          ...baseDocument,
          widgetType: 'single_value',
        }
      : {
          ...baseDocument,
          widgetType: 'chart',
          chartKind: parsedArgs.chartKind,
          timeRange: parsedArgs.timeRange
            ? {
                mode: parsedArgs.timeRange.mode,
                limit: parsedArgs.timeRange.limit,
              }
            : undefined,
        })
    await adjustReportWidgetUsageCounter(ctx, {
      members,
    }, 1)

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
    await adjustReportWidgetUsageCounter(ctx, widget, -1)
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
