import { describe, expect, test } from 'vitest'
import {
  buildReportExportCsv,
  buildReportExportDocument,
  buildReportExportFilename,
} from './reportExport.js'

const reportDetail = {
  report: {
    _id: 'report-1',
    profileSlug: 'clinic-ops',
    slug: 'report-direzione',
    name: 'Report Direzione',
    description: 'Cruscotto sintetico settimanale',
    lockedSourceKey: 'tickets',
    lockedSourceLabel: 'Ticketing',
    pinnedSnapshotId: 'snapshot-3',
    pinnedSnapshotAt: Date.UTC(2026, 2, 29, 8, 0, 0),
  },
  widgets: [
    {
      _id: 'widget-1',
      widgetType: 'single_value' as const,
      title: 'Ticket chiusi',
      description: 'Totale snapshot corrente',
      members: [{
        sourceProfileSlug: 'clinic-ops',
        indicatorSlug: 'tickets-closed',
        indicatorLabel: 'Ticket chiusi',
        indicatorUnit: null,
        indicatorKind: 'base' as const,
      }],
    },
    {
      _id: 'widget-2',
      widgetType: 'chart' as const,
      chartKind: 'pie' as const,
      title: 'Ripartizione KPI',
      members: [
        {
          sourceProfileSlug: 'clinic-ops',
          indicatorSlug: 'resolved',
          indicatorLabel: 'Risolti',
          indicatorUnit: '%',
          indicatorKind: 'base' as const,
        },
        {
          sourceProfileSlug: 'clinic-ops',
          indicatorSlug: 'pending',
          indicatorLabel: 'Pendenti',
          indicatorUnit: '%',
          indicatorKind: 'base' as const,
        },
      ],
    },
    {
      _id: 'widget-3',
      widgetType: 'chart' as const,
      chartKind: 'line' as const,
      title: 'Trend SLA',
      members: [{
        sourceProfileSlug: 'clinic-ops',
        indicatorSlug: 'sla-compliance',
        indicatorLabel: 'SLA compliance',
        indicatorUnit: '%',
        indicatorKind: 'derived' as const,
      }],
    },
  ],
}

const widgetData = [
  {
    widgetId: 'widget-1',
    widgetType: 'single_value' as const,
    title: 'Ticket chiusi',
    description: 'Totale snapshot corrente',
    member: {
      memberKey: 'm-1',
      sourceProfileSlug: 'clinic-ops',
      indicatorSlug: 'tickets-closed',
      indicatorKind: 'base' as const,
      indicatorLabel: 'Ticket chiusi',
      indicatorUnit: null,
      value: 42,
      recordedValue: 42,
      snapshotId: 'snapshot-3',
      snapshotAt: Date.UTC(2026, 2, 29, 8, 0, 0),
      computedAt: Date.UTC(2026, 2, 29, 8, 5, 0),
      isStaleInactive: false,
      staleReason: null,
    },
  },
  {
    widgetId: 'widget-2',
    widgetType: 'chart' as const,
    chartKind: 'pie' as const,
    title: 'Ripartizione KPI',
    description: 'Confronto stesso snapshot',
    slice: {
      snapshotId: 'snapshot-3',
      snapshotAt: Date.UTC(2026, 2, 29, 8, 0, 0),
      items: [
        {
          memberKey: 'm-2',
          sourceProfileSlug: 'clinic-ops',
          indicatorSlug: 'resolved',
          indicatorKind: 'base' as const,
          indicatorLabel: 'Risolti',
          indicatorUnit: '%',
          value: 71,
          recordedValue: 71,
          snapshotId: 'snapshot-3',
          snapshotAt: Date.UTC(2026, 2, 29, 8, 0, 0),
          computedAt: Date.UTC(2026, 2, 29, 8, 5, 0),
          isStaleInactive: false,
          staleReason: null,
        },
        {
          memberKey: 'm-3',
          sourceProfileSlug: 'clinic-ops',
          indicatorSlug: 'pending',
          indicatorKind: 'base' as const,
          indicatorLabel: 'Pendenti',
          indicatorUnit: '%',
          value: 29,
          recordedValue: 29,
          snapshotId: 'snapshot-3',
          snapshotAt: Date.UTC(2026, 2, 29, 8, 0, 0),
          computedAt: Date.UTC(2026, 2, 29, 8, 5, 0),
          isStaleInactive: false,
          staleReason: null,
        },
      ],
    },
  },
  {
    widgetId: 'widget-3',
    widgetType: 'chart' as const,
    chartKind: 'line' as const,
    title: 'Trend SLA',
    description: 'Ultimi snapshot',
    timeline: {
      series: [{
        memberKey: 'm-4',
        label: 'SLA compliance',
        unit: '%',
      }],
      points: [
        {
          timestamp: Date.UTC(2026, 1, 28, 8, 0, 0),
          snapshotId: 'snapshot-2',
          values: [{
            memberKey: 'm-4',
            label: 'SLA compliance',
            unit: '%',
            value: 88,
            recordedValue: 88,
          }],
        },
        {
          timestamp: Date.UTC(2026, 2, 29, 8, 0, 0),
          snapshotId: 'snapshot-3',
          values: [{
            memberKey: 'm-4',
            label: 'SLA compliance',
            unit: '%',
            value: 91,
            recordedValue: 91,
          }],
        },
      ],
    },
  },
]

describe('reportExport', () => {
  test('normalizza sezioni e flat rows dal report widget data', () => {
    const document = buildReportExportDocument({
      reportDetail,
      widgetData,
      sourceLabel: 'Ticketing',
      snapshotAt: Date.UTC(2026, 2, 29, 8, 0, 0),
      generatedAt: Date.UTC(2026, 2, 29, 9, 30, 0),
    })

    expect(document.reportName).toBe('Report Direzione')
    expect(document.sourceLabel).toBe('Ticketing')
    expect(document.sections).toHaveLength(3)
    expect(document.flatRows).toHaveLength(5)
    expect(document.sections[0]?.rows[0]?.indicatorLabel).toBe('Ticket chiusi')
    expect(document.sections[1]?.rows).toHaveLength(2)
    expect(document.sections[2]?.rows).toHaveLength(2)
  })

  test('genera csv flat e filename stabile', () => {
    const document = buildReportExportDocument({
      reportDetail,
      widgetData,
    })

    const csv = buildReportExportCsv(document)

    expect(csv).toContain('sectionTitle;sectionKind;widgetId')
    expect(csv).toContain('Ticket chiusi')
    expect(csv).toContain('Ripartizione KPI')
    expect(csv).toContain('Trend SLA')
    expect(buildReportExportFilename(document, 'csv')).toBe('report-direzione.csv')
    expect(buildReportExportFilename(document, 'pdf')).toBe('report-direzione.pdf')
  })
})
