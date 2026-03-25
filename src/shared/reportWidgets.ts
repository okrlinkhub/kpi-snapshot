import { v } from 'convex/values'

export const reportWidgetIndicatorKindValidator = v.union(
  v.literal('base'),
  v.literal('derived')
)

export const reportWidgetTypeValidator = v.union(
  v.literal('single_value'),
  v.literal('chart')
)

export const reportWidgetChartKindValidator = v.union(
  v.literal('line'),
  v.literal('area'),
  v.literal('bar'),
  v.literal('pie')
)

export const reportWidgetLayoutWidthValidator = v.union(
  v.literal('compact'),
  v.literal('wide'),
  v.literal('full')
)

export const reportWidgetLayoutHeightValidator = v.union(
  v.literal('sm'),
  v.literal('md'),
  v.literal('lg')
)

export const reportWidgetLayoutEmphasisValidator = v.union(
  v.literal('default'),
  v.literal('accent'),
  v.literal('subtle')
)

export const reportWidgetLayoutValidator = v.object({
  width: reportWidgetLayoutWidthValidator,
  height: reportWidgetLayoutHeightValidator,
  emphasis: reportWidgetLayoutEmphasisValidator,
})

export const reportWidgetTimeRangeValidator = v.object({
  mode: v.literal('latest_n_snapshots'),
  limit: v.number(),
})

export const reportWidgetMemberInputValidator = v.object({
  sourceProfileSlug: v.string(),
  indicatorSlug: v.string(),
  indicatorLabel: v.string(),
  indicatorUnit: v.optional(v.string()),
  indicatorKind: reportWidgetIndicatorKindValidator,
})

export const reportWidgetMemberValidator = v.object({
  sourceProfileId: v.id('snapshotProfiles'),
  sourceProfileSlug: v.string(),
  indicatorSlug: v.string(),
  indicatorLabel: v.string(),
  indicatorUnit: v.optional(v.string()),
  indicatorKind: reportWidgetIndicatorKindValidator,
})

export const reportWidgetCommonFieldsValidator = {
  reportId: v.id('reports'),
  title: v.string(),
  description: v.optional(v.string()),
  layout: v.optional(reportWidgetLayoutValidator),
  order: v.number(),
  createdAt: v.number(),
  updatedAt: v.optional(v.number()),
}

export const storedSingleValueReportWidgetValidator = v.object({
  _id: v.id('reportWidgets'),
  _creationTime: v.number(),
  ...reportWidgetCommonFieldsValidator,
  widgetType: v.literal('single_value'),
  members: v.array(reportWidgetMemberValidator),
})

export const storedChartReportWidgetValidator = v.object({
  _id: v.id('reportWidgets'),
  _creationTime: v.number(),
  ...reportWidgetCommonFieldsValidator,
  widgetType: v.literal('chart'),
  chartKind: reportWidgetChartKindValidator,
  members: v.array(reportWidgetMemberValidator),
  timeRange: v.optional(reportWidgetTimeRangeValidator),
})

export const storedReportWidgetValidator = v.union(
  storedSingleValueReportWidgetValidator,
  storedChartReportWidgetValidator
)

export const transportSingleValueReportWidgetValidator = v.object({
  _id: v.id('reportWidgets'),
  ...reportWidgetCommonFieldsValidator,
  widgetType: v.literal('single_value'),
  members: v.array(reportWidgetMemberValidator),
})

export const transportChartReportWidgetValidator = v.object({
  _id: v.id('reportWidgets'),
  ...reportWidgetCommonFieldsValidator,
  widgetType: v.literal('chart'),
  chartKind: reportWidgetChartKindValidator,
  members: v.array(reportWidgetMemberValidator),
  timeRange: v.optional(reportWidgetTimeRangeValidator),
})

export const transportReportWidgetValidator = v.union(
  transportSingleValueReportWidgetValidator,
  transportChartReportWidgetValidator
)

export const createSingleValueReportWidgetArgsValidator = v.object({
  reportId: v.id('reports'),
  widgetType: v.literal('single_value'),
  title: v.optional(v.string()),
  description: v.optional(v.string()),
  layout: v.optional(reportWidgetLayoutValidator),
  member: reportWidgetMemberInputValidator,
})

export const createChartReportWidgetArgsValidator = v.object({
  reportId: v.id('reports'),
  widgetType: v.literal('chart'),
  chartKind: reportWidgetChartKindValidator,
  title: v.optional(v.string()),
  description: v.optional(v.string()),
  layout: v.optional(reportWidgetLayoutValidator),
  timeRange: v.optional(reportWidgetTimeRangeValidator),
  members: v.array(reportWidgetMemberInputValidator),
})

export const createReportWidgetArgsValidator = v.object({
  reportId: v.id('reports'),
  widgetType: reportWidgetTypeValidator,
  title: v.optional(v.string()),
  description: v.optional(v.string()),
  layout: v.optional(reportWidgetLayoutValidator),
  member: v.optional(reportWidgetMemberInputValidator),
  chartKind: v.optional(reportWidgetChartKindValidator),
  timeRange: v.optional(reportWidgetTimeRangeValidator),
  members: v.optional(v.array(reportWidgetMemberInputValidator)),
})

export type ReportWidgetIndicatorKind = 'base' | 'derived'
export type ReportWidgetType = 'single_value' | 'chart'
export type ReportWidgetChartKind = 'line' | 'area' | 'bar' | 'pie'
export type ReportWidgetLayoutWidth = 'compact' | 'wide' | 'full'
export type ReportWidgetLayoutHeight = 'sm' | 'md' | 'lg'
export type ReportWidgetLayoutEmphasis = 'default' | 'accent' | 'subtle'

export type ReportWidgetLayout = {
  width: ReportWidgetLayoutWidth
  height: ReportWidgetLayoutHeight
  emphasis: ReportWidgetLayoutEmphasis
}

export type ReportWidgetTimeRange = {
  mode: 'latest_n_snapshots'
  limit: number
}

export type ReportWidgetMemberInput = {
  sourceProfileSlug: string
  indicatorSlug: string
  indicatorLabel: string
  indicatorUnit?: string
  indicatorKind: ReportWidgetIndicatorKind
}

export type ReportWidgetMember = ReportWidgetMemberInput & {
  sourceProfileId: string
}

export type ReportWidgetBase = {
  _id: string
  reportId: string
  title: string
  description?: string
  layout?: ReportWidgetLayout
  order: number
  createdAt: number
  updatedAt?: number
}

export type SingleValueReportWidget = ReportWidgetBase & {
  widgetType: 'single_value'
  members: [ReportWidgetMember]
}

export type ChartReportWidget = ReportWidgetBase & {
  widgetType: 'chart'
  chartKind: ReportWidgetChartKind
  members: ReportWidgetMember[]
  timeRange?: ReportWidgetTimeRange
}

export type AnalyticsReportWidget = SingleValueReportWidget | ChartReportWidget

export type CreateSingleValueReportWidgetArgs = {
  reportId: string
  widgetType: 'single_value'
  title?: string
  description?: string
  layout?: ReportWidgetLayout
  member: ReportWidgetMemberInput
}

export type CreateChartReportWidgetArgs = {
  reportId: string
  widgetType: 'chart'
  chartKind: ReportWidgetChartKind
  title?: string
  description?: string
  layout?: ReportWidgetLayout
  timeRange?: ReportWidgetTimeRange
  members: ReportWidgetMemberInput[]
}

export type CreateReportWidgetArgs =
  | CreateSingleValueReportWidgetArgs
  | CreateChartReportWidgetArgs

export function buildReportWidgetMemberKey (member: Pick<ReportWidgetMemberInput, 'sourceProfileSlug' | 'indicatorKind' | 'indicatorSlug'>) {
  return `${member.sourceProfileSlug}:${member.indicatorKind}:${member.indicatorSlug}`
}
