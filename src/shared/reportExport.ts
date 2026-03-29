type Nullable<T> = T | null

export type ReportExportCellValue = string | number | boolean | null

export type ReportExportWidgetMember = {
  sourceProfileSlug: string
  indicatorSlug: string
  indicatorLabel: string
  indicatorUnit?: string | null
  indicatorKind: 'base' | 'derived'
}

export type ReportExportWidget = {
  _id: string
  widgetType: 'single_value' | 'chart'
  title?: string
  description?: string
  chartKind?: 'line' | 'area' | 'bar' | 'pie'
  members: ReportExportWidgetMember[]
}

export type ReportExportReportSummary = {
  _id: string
  profileSlug: string
  slug: string
  name: string
  description?: string
  lockedSourceKey?: string
  lockedSourceLabel?: string | null
  pinnedSnapshotId?: string | null
  pinnedSnapshotAt?: number | null
}

export type ReportExportDetail = {
  report: ReportExportReportSummary
  widgets: ReportExportWidget[]
}

export type ReportExportSingleValueData = {
  widgetId: string
  widgetType: 'single_value'
  title: string
  description: string | null
  member: Nullable<{
    memberKey: string
    sourceProfileSlug: string
    indicatorSlug: string
    indicatorKind: 'base' | 'derived'
    indicatorLabel: string
    indicatorUnit: string | null
    value: number | null
    recordedValue: number | null
    snapshotId: string | null
    snapshotAt: number | null
    computedAt: number | null
    isStaleInactive: boolean
    staleReason: 'indicator_disabled' | 'operand_disabled' | null
  }>
}

export type ReportExportPieData = {
  widgetId: string
  widgetType: 'chart'
  chartKind: 'pie'
  title: string
  description: string | null
  slice: {
    snapshotId: string | null
    snapshotAt: number | null
    items: Array<{
      memberKey: string
      sourceProfileSlug: string
      indicatorSlug: string
      indicatorKind: 'base' | 'derived'
      indicatorLabel: string
      indicatorUnit: string | null
      value: number | null
      recordedValue: number | null
      snapshotId: string | null
      snapshotAt: number | null
      computedAt: number | null
      isStaleInactive: boolean
      staleReason: 'indicator_disabled' | 'operand_disabled' | null
    }>
  }
}

export type ReportExportTimelineData = {
  widgetId: string
  widgetType: 'chart'
  chartKind: 'line' | 'area' | 'bar'
  title: string
  description: string | null
  timeline: {
    series: Array<{
      memberKey: string
      label: string
      unit: string | null
    }>
    points: Array<{
      timestamp: number
      snapshotId: string
      values: Array<{
        memberKey: string
        label: string
        unit: string | null
        value: number | null
        recordedValue: number | null
      }>
    }>
  }
}

export type ReportExportWidgetData =
  | ReportExportSingleValueData
  | ReportExportPieData
  | ReportExportTimelineData

export type ReportExportFlatRow = {
  sectionIndex: number
  sectionTitle: string
  sectionKind: 'single_value' | 'slice' | 'timeline'
  widgetId: string
  widgetType: 'single_value' | 'chart'
  chartKind: 'line' | 'area' | 'bar' | 'pie' | null
  rowIndex: number
  primaryLabel: string
  secondaryLabel: string | null
  profileSlug: string | null
  indicatorKind: 'base' | 'derived' | null
  indicatorLabel: string | null
  unit: string | null
  value: number | null
  recordedValue: number | null
  snapshotId: string | null
  snapshotAt: string | null
  computedAt: string | null
  timestamp: string | null
  status: 'ok' | 'stale' | null
  staleReason: 'indicator_disabled' | 'operand_disabled' | null
}

export type ReportExportSection = {
  widgetId: string
  widgetType: 'single_value' | 'chart'
  chartKind: 'line' | 'area' | 'bar' | 'pie' | null
  title: string
  description: string | null
  kind: 'single_value' | 'slice' | 'timeline'
  columns: string[]
  rows: Array<Record<string, ReportExportCellValue>>
  flatRows: ReportExportFlatRow[]
}

export type ReportExportDocument = {
  reportId: string
  reportSlug: string
  reportName: string
  reportDescription: string | null
  profileSlug: string
  sourceKey: string | null
  sourceLabel: string | null
  snapshotId: string | null
  snapshotAt: string | null
  generatedAt: string
  sections: ReportExportSection[]
  flatRows: ReportExportFlatRow[]
}

export type BuildReportExportDocumentArgs = {
  reportDetail: ReportExportDetail
  widgetData: ReportExportWidgetData[]
  sourceKey?: string | null
  sourceLabel?: string | null
  snapshotId?: string | null
  snapshotAt?: number | null
  generatedAt?: number
}

type CsvOptions = {
  delimiter?: string
}

const flatRowColumns: string[] = [
  'sectionIndex',
  'sectionTitle',
  'sectionKind',
  'widgetId',
  'widgetType',
  'chartKind',
  'rowIndex',
  'primaryLabel',
  'secondaryLabel',
  'profileSlug',
  'indicatorKind',
  'indicatorLabel',
  'unit',
  'value',
  'recordedValue',
  'snapshotId',
  'snapshotAt',
  'computedAt',
  'timestamp',
  'status',
  'staleReason',
]

function toIsoString(value?: number | null) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null
  }

  return new Date(value).toISOString()
}

function escapeCsvValue(value: unknown, delimiter: string) {
  if (value == null) {
    return ''
  }

  if (typeof value === 'object') {
    return `"${JSON.stringify(value).replace(/"/g, '""')}"`
  }

  const raw = (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    typeof value === 'bigint'
      ? String(value)
      : JSON.stringify(value)
  ).replace(/\r?\n/g, ' ')

  if (raw.includes('"') || raw.includes(delimiter) || raw.includes(';')) {
    return `"${raw.replace(/"/g, '""')}"`
  }

  return raw
}

function slugifyName(value?: string, fallback?: string) {
  return (value ?? fallback ?? 'report')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
}

function getWidgetTitle(widget: ReportExportWidget, widgetIndex: number) {
  const normalized = widget.title?.trim()
  if (normalized) {
    return normalized
  }

  return `Widget ${widgetIndex + 1}`
}

function getFlatRowBase(section: ReportExportSection, sectionIndex: number) {
  return {
    sectionIndex,
    sectionTitle: section.title,
    sectionKind: section.kind,
    widgetId: section.widgetId,
    widgetType: section.widgetType,
    chartKind: section.chartKind,
  } as const
}

function buildSingleValueSection(
  widget: ReportExportWidget,
  data: ReportExportSingleValueData,
  sectionIndex: number,
  widgetIndex: number,
): ReportExportSection {
  const title = data.title || getWidgetTitle(widget, widgetIndex)
  const description = data.description ?? widget.description ?? null
  const columns = [
    'memberKey',
    'profileSlug',
    'indicatorSlug',
    'indicatorKind',
    'indicatorLabel',
    'unit',
    'value',
    'recordedValue',
    'snapshotId',
    'snapshotAt',
    'computedAt',
    'status',
    'staleReason',
  ]

  const member = data.member
  const rows = member
    ? [{
        memberKey: member.memberKey,
        profileSlug: member.sourceProfileSlug,
        indicatorSlug: member.indicatorSlug,
        indicatorKind: member.indicatorKind,
        indicatorLabel: member.indicatorLabel,
        unit: member.indicatorUnit,
        value: member.value,
        recordedValue: member.recordedValue,
        snapshotId: member.snapshotId,
        snapshotAt: toIsoString(member.snapshotAt),
        computedAt: toIsoString(member.computedAt),
        status: member.isStaleInactive ? 'stale' : 'ok',
        staleReason: member.staleReason,
      }]
    : []

  const section: ReportExportSection = {
    widgetId: data.widgetId,
    widgetType: data.widgetType,
    chartKind: null,
    title,
    description,
    kind: 'single_value',
    columns,
    rows,
    flatRows: [],
  }

  const base = getFlatRowBase(section, sectionIndex)
  section.flatRows = member
    ? [{
        ...base,
        rowIndex: 0,
        primaryLabel: member.indicatorLabel,
        secondaryLabel: member.indicatorSlug,
        profileSlug: member.sourceProfileSlug,
        indicatorKind: member.indicatorKind,
        indicatorLabel: member.indicatorLabel,
        unit: member.indicatorUnit,
        value: member.value,
        recordedValue: member.recordedValue,
        snapshotId: member.snapshotId,
        snapshotAt: toIsoString(member.snapshotAt),
        computedAt: toIsoString(member.computedAt),
        timestamp: null,
        status: member.isStaleInactive ? 'stale' : 'ok',
        staleReason: member.staleReason,
      }]
    : []

  return section
}

function buildSliceSection(
  widget: ReportExportWidget,
  data: ReportExportPieData,
  sectionIndex: number,
  widgetIndex: number,
): ReportExportSection {
  const title = data.title || getWidgetTitle(widget, widgetIndex)
  const description = data.description ?? widget.description ?? null
  const columns = [
    'memberKey',
    'profileSlug',
    'indicatorSlug',
    'indicatorKind',
    'indicatorLabel',
    'unit',
    'value',
    'recordedValue',
    'snapshotId',
    'snapshotAt',
    'computedAt',
    'status',
    'staleReason',
  ]
  const rows = data.slice.items.map((item) => ({
    memberKey: item.memberKey,
    profileSlug: item.sourceProfileSlug,
    indicatorSlug: item.indicatorSlug,
    indicatorKind: item.indicatorKind,
    indicatorLabel: item.indicatorLabel,
    unit: item.indicatorUnit,
    value: item.value,
    recordedValue: item.recordedValue,
    snapshotId: item.snapshotId,
    snapshotAt: toIsoString(item.snapshotAt),
    computedAt: toIsoString(item.computedAt),
    status: item.isStaleInactive ? 'stale' : 'ok',
    staleReason: item.staleReason,
  }))

  const section: ReportExportSection = {
    widgetId: data.widgetId,
    widgetType: data.widgetType,
    chartKind: data.chartKind,
    title,
    description,
    kind: 'slice',
    columns,
    rows,
    flatRows: [],
  }

  const base = getFlatRowBase(section, sectionIndex)
  section.flatRows = data.slice.items.map((item, rowIndex) => ({
    ...base,
    rowIndex,
    primaryLabel: item.indicatorLabel,
    secondaryLabel: item.indicatorSlug,
    profileSlug: item.sourceProfileSlug,
    indicatorKind: item.indicatorKind,
    indicatorLabel: item.indicatorLabel,
    unit: item.indicatorUnit,
    value: item.value,
    recordedValue: item.recordedValue,
    snapshotId: item.snapshotId,
    snapshotAt: toIsoString(item.snapshotAt),
    computedAt: toIsoString(item.computedAt),
    timestamp: null,
    status: item.isStaleInactive ? 'stale' : 'ok',
    staleReason: item.staleReason,
  }))

  return section
}

function buildTimelineSection(
  widget: ReportExportWidget,
  data: ReportExportTimelineData,
  sectionIndex: number,
  widgetIndex: number,
): ReportExportSection {
  const title = data.title || getWidgetTitle(widget, widgetIndex)
  const description = data.description ?? widget.description ?? null
  const columns = [
    'timestamp',
    'snapshotId',
    'memberKey',
    'seriesLabel',
    'unit',
    'value',
    'recordedValue',
  ]
  const rows = data.timeline.points.flatMap((point) => point.values.map((value) => ({
    timestamp: toIsoString(point.timestamp),
    snapshotId: point.snapshotId,
    memberKey: value.memberKey,
    seriesLabel: value.label,
    unit: value.unit,
    value: value.value,
    recordedValue: value.recordedValue,
  })))

  const section: ReportExportSection = {
    widgetId: data.widgetId,
    widgetType: data.widgetType,
    chartKind: data.chartKind,
    title,
    description,
    kind: 'timeline',
    columns,
    rows,
    flatRows: [],
  }

  const base = getFlatRowBase(section, sectionIndex)
  section.flatRows = data.timeline.points.flatMap((point, pointIndex) => point.values.map((value, valueIndex) => ({
    ...base,
    rowIndex: (pointIndex * Math.max(point.values.length, 1)) + valueIndex,
    primaryLabel: value.label,
    secondaryLabel: value.memberKey,
    profileSlug: null,
    indicatorKind: null,
    indicatorLabel: value.label,
    unit: value.unit,
    value: value.value,
    recordedValue: value.recordedValue,
    snapshotId: point.snapshotId,
    snapshotAt: null,
    computedAt: null,
    timestamp: toIsoString(point.timestamp),
    status: null,
    staleReason: null,
  })))

  return section
}

function buildWidgetSection(
  widget: ReportExportWidget,
  widgetData: ReportExportWidgetData,
  sectionIndex: number,
  widgetIndex: number,
) {
  if (widgetData.widgetType === 'single_value') {
    return buildSingleValueSection(widget, widgetData, sectionIndex, widgetIndex)
  }

  if (widgetData.chartKind === 'pie') {
    return buildSliceSection(widget, widgetData, sectionIndex, widgetIndex)
  }

  return buildTimelineSection(widget, widgetData, sectionIndex, widgetIndex)
}

export function buildReportExportDocument({
  reportDetail,
  widgetData,
  sourceKey,
  sourceLabel,
  snapshotId,
  snapshotAt,
  generatedAt,
}: BuildReportExportDocumentArgs): ReportExportDocument {
  const widgetDataById = new Map(widgetData.map((entry) => [entry.widgetId, entry] as const))
  const sections = reportDetail.widgets
    .map((widget, widgetIndex) => {
      const data = widgetDataById.get(widget._id)
      if (!data) {
        return null
      }

      return buildWidgetSection(widget, data, widgetIndex, widgetIndex)
    })
    .filter((section): section is ReportExportSection => section !== null)

  return {
    reportId: reportDetail.report._id,
    reportSlug: reportDetail.report.slug,
    reportName: reportDetail.report.name,
    reportDescription: reportDetail.report.description ?? null,
    profileSlug: reportDetail.report.profileSlug,
    sourceKey: sourceKey ?? reportDetail.report.lockedSourceKey ?? null,
    sourceLabel: sourceLabel ?? reportDetail.report.lockedSourceLabel ?? null,
    snapshotId: snapshotId ?? reportDetail.report.pinnedSnapshotId ?? null,
    snapshotAt: toIsoString(snapshotAt ?? reportDetail.report.pinnedSnapshotAt ?? null),
    generatedAt: toIsoString(generatedAt ?? Date.now()) ?? new Date().toISOString(),
    sections,
    flatRows: sections.flatMap((section) => section.flatRows),
  }
}

export function buildReportExportCsv(document: ReportExportDocument, options?: CsvOptions) {
  const delimiter = options?.delimiter ?? ';'
  const rows: Array<Record<string, ReportExportCellValue>> = document.flatRows.map((row) => ({
    sectionIndex: row.sectionIndex,
    sectionTitle: row.sectionTitle,
    sectionKind: row.sectionKind,
    widgetId: row.widgetId,
    widgetType: row.widgetType,
    chartKind: row.chartKind,
    rowIndex: row.rowIndex,
    primaryLabel: row.primaryLabel,
    secondaryLabel: row.secondaryLabel,
    profileSlug: row.profileSlug,
    indicatorKind: row.indicatorKind,
    indicatorLabel: row.indicatorLabel,
    unit: row.unit,
    value: row.value,
    recordedValue: row.recordedValue,
    snapshotId: row.snapshotId,
    snapshotAt: row.snapshotAt,
    computedAt: row.computedAt,
    timestamp: row.timestamp,
    status: row.status,
    staleReason: row.staleReason,
  }))

  const header = flatRowColumns.join(delimiter)
  const body = rows.map((row) => flatRowColumns
    .map((column) => escapeCsvValue(row[column], delimiter))
    .join(delimiter))

  return [header, ...body].join('\n')
}

export function buildReportExportFilename(
  document: Pick<ReportExportDocument, 'reportSlug' | 'reportName'>,
  extension: 'csv' | 'pdf',
) {
  const baseName = slugifyName(document.reportSlug, document.reportName)
  return `${baseName}.${extension}`
}

export function getReportExportFlatRowColumns() {
  return [...flatRowColumns]
}
