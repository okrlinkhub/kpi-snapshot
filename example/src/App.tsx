import React, { useMemo, useState } from "react";
import { useMutation, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";

type RuleOperation = "sum" | "count" | "avg" | "min" | "max" | "distinct_count";
type FilterOperator = "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in";
type FilterValueType = "string" | "number" | "boolean" | "null" | "json" | "csv";
type CalculationTimeRangeKind = "last_month" | "last_3_months" | "month_to_date" | "year_to_date";
type AdminTab = "setup" | "rules" | "snapshots" | "data";

type FilterDraft = {
  id: string;
  field: string;
  op: FilterOperator;
  valueType: FilterValueType;
  valueText: string;
  valueBoolean: boolean;
};

type RuleDefinition = {
  _id: string;
  indicatorSlug: string | null;
  sourceKey: string | null;
  operation: RuleOperation;
  fieldPath?: string;
  filters: {
    fieldRules: Array<{ field: string; op: FilterOperator; value: unknown }>;
    timeRange?: { kind: CalculationTimeRangeKind };
  };
  groupBy?: Array<string>;
  normalization?: unknown;
  priority: number;
  enabled: boolean;
  ruleVersion?: number;
};

type ProfileDefinitionsPayload = {
  profile: { slug: string; name: string };
  indicators: Array<{ _id: string; slug: string; label: string }>;
  dataSources: Array<{ _id: string; sourceKey: string; label: string }>;
  definitions: Array<RuleDefinition>;
};

const DEFAULT_PROFILE_SLUG = "finance_demo";
const DEFAULT_SOURCE_KEY = "invoices";

const operations: Array<RuleOperation> = ["sum", "count", "avg", "min", "max", "distinct_count"];
const filterOps: Array<FilterOperator> = ["eq", "neq", "gt", "gte", "lt", "lte", "in"];

const operationDescriptions: Record<RuleOperation, string> = {
  count: "Conta le righe che rispettano i filtri. fieldPath non necessario.",
  sum: "Somma numerica del campo indicato da fieldPath.",
  avg: "Media numerica del campo indicato da fieldPath.",
  min: "Minimo numerico del campo indicato da fieldPath.",
  max: "Massimo numerico del campo indicato da fieldPath.",
  distinct_count: "Conta i valori distinti del campo in fieldPath.",
};

const filterOpLabels: Record<FilterOperator, string> = {
  eq: "= (uguale)",
  neq: "!= (diverso)",
  gt: "> (maggiore di)",
  gte: ">= (maggiore o uguale)",
  lt: "< (minore di)",
  lte: "<= (minore o uguale)",
  in: "in (appartiene a lista)",
};

const timeRangeLabels: Record<CalculationTimeRangeKind, string> = {
  last_month: "Ultimo mese",
  last_3_months: "Ultimi 3 mesi",
  month_to_date: "Month to date",
  year_to_date: "Year to date",
};

function uid(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function toFilterValue(filter: FilterDraft): unknown {
  if (filter.valueType === "null") return null;
  if (filter.valueType === "boolean") return filter.valueBoolean;
  if (filter.valueType === "number") return Number(filter.valueText);
  if (filter.valueType === "json") return JSON.parse(filter.valueText || "null");
  if (filter.valueType === "csv") {
    return filter.valueText
      .split(",")
      .map((item) => item.trim())
      .filter((item) => item.length > 0);
  }
  return filter.valueText;
}

function normalizeFilterValueToDraft(value: unknown): Pick<FilterDraft, "valueType" | "valueText" | "valueBoolean"> {
  if (value === null) return { valueType: "null", valueText: "", valueBoolean: false };
  if (typeof value === "boolean") return { valueType: "boolean", valueText: "", valueBoolean: value };
  if (typeof value === "number") return { valueType: "number", valueText: String(value), valueBoolean: false };
  if (Array.isArray(value)) return { valueType: "csv", valueText: value.map((v) => String(v)).join(", "), valueBoolean: false };
  if (typeof value === "object") return { valueType: "json", valueText: JSON.stringify(value), valueBoolean: false };
  return { valueType: "string", valueText: String(value ?? ""), valueBoolean: false };
}

function parseFiltersForEdit(filters: RuleDefinition["filters"] | undefined): Array<FilterDraft> {
  if (!filters?.fieldRules?.length) return [];
  return filters.fieldRules
    .filter((item): item is { field?: unknown; op?: unknown; value?: unknown } => typeof item === "object" && item !== null)
    .map((item) => {
      const op = filterOps.includes(item.op as FilterOperator) ? (item.op as FilterOperator) : "eq";
      const valueDraft = normalizeFilterValueToDraft(item.value);
      return {
        id: uid(),
        field: typeof item.field === "string" ? item.field : "",
        op,
        ...valueDraft,
      };
    });
}

function summarizeFilters(filters: RuleDefinition["filters"] | undefined): string {
  const fieldRules = filters?.fieldRules ?? [];
  const fieldRulesSummary = fieldRules.length === 0
    ? "nessun filtro campo"
    : fieldRules
    .map((item) => {
      if (typeof item !== "object" || item === null) return "Filtro non valido";
      const field = "field" in item ? String((item as { field: unknown }).field) : "?";
      const op = "op" in item ? String((item as { op: unknown }).op) : "?";
      const value = "value" in item ? JSON.stringify((item as { value: unknown }).value) : "?";
      return `${field} ${op} ${value}`;
    })
    .join(" AND ");
  const timeRangeSummary = filters?.timeRange ? timeRangeLabels[filters.timeRange.kind] : "nessun periodo";
  return `${fieldRulesSummary} · ${timeRangeSummary}`;
}

function buildMonthAssignedPreset(): Array<FilterDraft> {
  return [
    {
      id: uid(),
      field: "monthRef",
      op: "neq",
      valueType: "null",
      valueText: "",
      valueBoolean: false,
    },
  ];
}

export default function App() {
  const [activeTab, setActiveTab] = useState<AdminTab>("setup");
  const [opMessage, setOpMessage] = useState<string | null>(null);
  const [isBusy, setIsBusy] = useState(false);
  const [profileSlug, setProfileSlug] = useState(DEFAULT_PROFILE_SLUG);
  const [profileName, setProfileName] = useState("Finance Demo");
  const [profileDescription, setProfileDescription] = useState("Profilo demo per KPI invoices");
  const [sourceKey, setSourceKey] = useState(DEFAULT_SOURCE_KEY);
  const [sourceLabel, setSourceLabel] = useState("Invoices materialized rows");
  const [sourceKind, setSourceKind] = useState<"materialized_rows" | "component_table" | "external_reader">("materialized_rows");
  const [newIndicatorSlug, setNewIndicatorSlug] = useState("invoice_count_all");
  const [newIndicatorLabel, setNewIndicatorLabel] = useState("Invoice Count");
  const [newOperation, setNewOperation] = useState<RuleOperation>("count");
  const [newFieldPath, setNewFieldPath] = useState("");
  const [newPriority, setNewPriority] = useState(100);
  const [newEnabled, setNewEnabled] = useState(true);
  const [newGroupBy, setNewGroupBy] = useState("");
  const [ruleFilters, setRuleFilters] = useState<Array<FilterDraft>>([]);
  const [timeRangeKind, setTimeRangeKind] = useState<CalculationTimeRangeKind | "">("");
  const [editingDefinitionId, setEditingDefinitionId] = useState<string | null>(null);
  const [selectedSnapshotId, setSelectedSnapshotId] = useState<string>("");
  const [snapshotNote, setSnapshotNote] = useState("manual snapshot from example admin page");

  const invoicesByCategory = useQuery(api.seed.listInvoicesByCategory, { profileSlug });
  const invoices = useQuery(api.seed.listInvoices, { profileSlug, limit: 12 });
  const simulated = useQuery(api.seed.simulateSnapshot, { profileSlug });
  const snapshots = useQuery(api.seed.listSnapshots, { profileSlug, limit: 10 });
  const latestExplain = useQuery(api.seed.getLatestSnapshotExplain, { profileSlug });
  const definitionsPayload = useQuery(api.seed.listProfileDefinitions, { profileSlug }) as ProfileDefinitionsPayload | null | undefined;
  const selectedExplain = useQuery(
    api.seed.getSnapshotExplain,
    selectedSnapshotId ? { snapshotId: selectedSnapshotId as any } : "skip"
  );

  const setupDefaultSnapshotConfig = useMutation(api.seed.setupDefaultSnapshotConfig);
  const seedInvoices = useMutation(api.seed.seedInvoices);
  const createManualSnapshot = useMutation(api.seed.createManualSnapshot);
  const createOrUpdateProfile = useMutation(api.seed.createOrUpdateProfile);
  const upsertDataSource = useMutation(api.seed.upsertDataSource);
  const upsertIndicator = useMutation(api.seed.upsertIndicator);
  const upsertCalculationDefinition = useMutation(api.seed.upsertCalculationDefinition);
  const replaceProfileDefinitions = useMutation(api.seed.replaceProfileDefinitions);
  const toggleCalculation = useMutation(api.seed.toggleCalculation);

  const availableDefinitions = definitionsPayload?.definitions ?? [];
  const availableIndicators = definitionsPayload?.indicators ?? [];
  const availableSources = definitionsPayload?.dataSources ?? [];
  const operationNeedsFieldPath = newOperation !== "count";

  const rulePreview = useMemo(() => {
    const filtersSummary =
      ruleFilters.length === 0
        ? "nessun filtro"
        : ruleFilters.map((f) => `${f.field || "?"} ${f.op} ${f.valueType}`).join(" AND ");
    const timeRangeSummary = timeRangeKind ? timeRangeLabels[timeRangeKind] : "nessun periodo";
    return `${newOperation}(${newFieldPath.trim() || "root"}) | priority=${newPriority} | ${filtersSummary} | ${timeRangeSummary}`;
  }, [newFieldPath, newOperation, newPriority, ruleFilters, timeRangeKind]);

  const resetRuleEditor = () => {
    setEditingDefinitionId(null);
    setNewIndicatorSlug("invoice_count_all");
    setNewIndicatorLabel("Invoice Count");
    setNewOperation("count");
    setNewFieldPath("");
    setNewPriority(100);
    setNewEnabled(true);
    setNewGroupBy("");
    setRuleFilters([]);
    setTimeRangeKind("");
  };

  const addFilterDraft = () => {
    setRuleFilters((prev) => [
      ...prev,
      {
        id: uid(),
        field: "",
        op: "eq",
        valueType: "string",
        valueText: "",
        valueBoolean: false,
      },
    ]);
  };

  const updateFilterDraft = (id: string, patch: Partial<FilterDraft>) => {
    setRuleFilters((prev) => prev.map((f) => (f.id === id ? { ...f, ...patch } : f)));
  };

  const removeFilterDraft = (id: string) => {
    setRuleFilters((prev) => prev.filter((f) => f.id !== id));
  };

  const applyPresetCountAll = () => {
    setNewIndicatorSlug("invoice_count_all");
    setNewIndicatorLabel("Invoice Count");
    setNewOperation("count");
    setNewFieldPath("");
    setRuleFilters([]);
    setTimeRangeKind("");
    setNewPriority(100);
    setNewEnabled(true);
  };

  const applyPresetAmountOver1000 = () => {
    setNewIndicatorSlug("invoice_count_over_1000");
    setNewIndicatorLabel("Invoices > 1000");
    setNewOperation("count");
    setNewFieldPath("");
    setRuleFilters([
      {
        id: uid(),
        field: "amount",
        op: "gt",
        valueType: "number",
        valueText: "1000",
        valueBoolean: false,
      },
    ]);
    setTimeRangeKind("");
    setNewPriority(110);
    setNewEnabled(true);
  };

  const applyPresetCategoryIn = () => {
    setNewIndicatorSlug("invoice_count_selected_categories");
    setNewIndicatorLabel("Invoices selected categories");
    setNewOperation("count");
    setNewFieldPath("");
    setRuleFilters([
      {
        id: uid(),
        field: "category",
        op: "in",
        valueType: "csv",
        valueText: "software, services",
        valueBoolean: false,
      },
    ]);
    setTimeRangeKind("");
    setNewPriority(120);
    setNewEnabled(true);
  };

  const applyPresetMonthAssigned = () => {
    setNewIndicatorSlug("count_with_month_assigned");
    setNewIndicatorLabel("Rows with monthRef assigned");
    setNewOperation("count");
    setNewFieldPath("");
    setRuleFilters(buildMonthAssignedPreset());
    setTimeRangeKind("");
    setNewPriority(130);
    setNewEnabled(true);
  };

  const handleSaveRule = async () => {
    if (!profileSlug.trim() || !newIndicatorSlug.trim() || !newIndicatorLabel.trim() || !sourceKey.trim()) return;
    if (operationNeedsFieldPath && !newFieldPath.trim()) {
      setOpMessage(`L'operazione ${newOperation} richiede fieldPath.`);
      return;
    }
    setIsBusy(true);
    setOpMessage(null);
    try {
      await upsertIndicator({
        profileSlug,
        slug: newIndicatorSlug.trim(),
        label: newIndicatorLabel.trim(),
        enabled: true,
      });

      const parsedFilters = ruleFilters
        .filter((f) => f.field.trim().length > 0)
        .map((f) => ({
          field: f.field.trim(),
          op: f.op,
          value: toFilterValue(f),
        }));
      const nextFilters = {
        fieldRules: parsedFilters,
        timeRange: timeRangeKind ? { kind: timeRangeKind } : undefined,
      };

      const parsedGroupBy = newGroupBy
        .split(",")
        .map((chunk) => chunk.trim())
        .filter((chunk) => chunk.length > 0);

      if (!editingDefinitionId) {
        await upsertCalculationDefinition({
          profileSlug,
          indicatorSlug: newIndicatorSlug.trim(),
          sourceKey: sourceKey.trim(),
          operation: newOperation,
          fieldPath: newFieldPath.trim() || undefined,
          filters: nextFilters,
          groupBy: parsedGroupBy.length > 0 ? parsedGroupBy : undefined,
          priority: newPriority,
          enabled: newEnabled,
        });
        setOpMessage("Regola creata con successo.");
      } else {
        const nextDefinitions = availableDefinitions.map((definition) => {
          if (definition._id !== editingDefinitionId) {
            return {
              indicatorSlug: definition.indicatorSlug ?? "",
              sourceKey: definition.sourceKey ?? "",
              operation: definition.operation,
              fieldPath: definition.fieldPath,
              filters: definition.filters,
              groupBy: definition.groupBy,
              normalization: definition.normalization,
              priority: definition.priority,
              enabled: definition.enabled,
              ruleVersion: definition.ruleVersion,
            };
          }
          return {
            indicatorSlug: newIndicatorSlug.trim(),
            sourceKey: sourceKey.trim(),
            operation: newOperation,
            fieldPath: newFieldPath.trim() || undefined,
            filters: nextFilters,
            groupBy: parsedGroupBy.length > 0 ? parsedGroupBy : undefined,
            priority: newPriority,
            enabled: newEnabled,
          };
        });

        await replaceProfileDefinitions({
          profileSlug,
          definitions: nextDefinitions,
        });
        setOpMessage("Regola aggiornata con successo.");
      }
      resetRuleEditor();
    } catch (error) {
      setOpMessage(error instanceof Error ? error.message : "Errore nel salvataggio regola.");
    } finally {
      setIsBusy(false);
    }
  };

  const handleEditRule = (definition: RuleDefinition) => {
    setEditingDefinitionId(definition._id);
    setNewIndicatorSlug(definition.indicatorSlug ?? "");
    const indicator = availableIndicators.find((item) => item.slug === definition.indicatorSlug);
    setNewIndicatorLabel(indicator?.label ?? definition.indicatorSlug ?? "");
    setSourceKey(definition.sourceKey ?? DEFAULT_SOURCE_KEY);
    setNewOperation(definition.operation);
    setNewFieldPath(definition.fieldPath ?? "");
    setNewPriority(definition.priority ?? 100);
    setNewEnabled(definition.enabled);
    setNewGroupBy(Array.isArray(definition.groupBy) ? definition.groupBy.join(", ") : "");
    setRuleFilters(parseFiltersForEdit(definition.filters));
    setTimeRangeKind(definition.filters?.timeRange?.kind ?? "");
    setActiveTab("rules");
  };

  const handleDeleteRule = async (definitionId: string) => {
    if (!confirm("Eliminare questa regola?")) return;
    setIsBusy(true);
    setOpMessage(null);
    try {
      const nextDefinitions = availableDefinitions
        .filter((definition) => definition._id !== definitionId)
        .map((definition) => ({
          indicatorSlug: definition.indicatorSlug ?? "",
          sourceKey: definition.sourceKey ?? "",
          operation: definition.operation,
          fieldPath: definition.fieldPath,
          filters: definition.filters,
          groupBy: definition.groupBy,
          normalization: definition.normalization,
          priority: definition.priority,
          enabled: definition.enabled,
          ruleVersion: definition.ruleVersion,
        }));
      await replaceProfileDefinitions({ profileSlug, definitions: nextDefinitions });
      if (editingDefinitionId === definitionId) {
        resetRuleEditor();
      }
      setOpMessage("Regola eliminata.");
    } catch (error) {
      setOpMessage(error instanceof Error ? error.message : "Errore durante eliminazione regola.");
    } finally {
      setIsBusy(false);
    }
  };

  const handleToggleRule = async (definitionId: string, enabled: boolean) => {
    setIsBusy(true);
    setOpMessage(null);
    try {
      await toggleCalculation({ definitionId, enabled });
      setOpMessage(`Regola ${enabled ? "abilitata" : "disabilitata"}.`);
    } catch (error) {
      setOpMessage(error instanceof Error ? error.message : "Errore aggiornando stato regola.");
    } finally {
      setIsBusy(false);
    }
  };

  return (
    <main className="p-6 max-w-5xl mx-auto space-y-6 text-slate-900 dark:text-slate-100">
      <h1 className="text-2xl font-bold">KPI Snapshot – Admin Example Page</h1>
      <p className="text-sm text-slate-700 dark:text-slate-300">
        Pagina esempio ispirata a un pannello admin reale: setup, regole avanzate con filtri guidati, simulazione e snapshot explain.
      </p>
      {opMessage && (
        <div className="rounded-md border border-blue-300 dark:border-blue-700 bg-blue-100 dark:bg-blue-900/40 text-blue-900 dark:text-blue-100 px-3 py-2 text-sm">
          {opMessage}
        </div>
      )}

      <div className="border-b border-slate-200 dark:border-slate-700">
        <nav className="-mb-px flex gap-6 overflow-x-auto">
          <button onClick={() => setActiveTab("setup")} className={`py-2 border-b-2 text-sm ${activeTab === "setup" ? "border-blue-500 text-blue-700 dark:text-blue-300" : "border-transparent text-slate-700 dark:text-slate-300"}`}>
            Setup
          </button>
          <button onClick={() => setActiveTab("rules")} className={`py-2 border-b-2 text-sm ${activeTab === "rules" ? "border-blue-500 text-blue-700 dark:text-blue-300" : "border-transparent text-slate-700 dark:text-slate-300"}`}>
            Regole
          </button>
          <button onClick={() => setActiveTab("snapshots")} className={`py-2 border-b-2 text-sm ${activeTab === "snapshots" ? "border-blue-500 text-blue-700 dark:text-blue-300" : "border-transparent text-slate-700 dark:text-slate-300"}`}>
            Snapshot
          </button>
          <button onClick={() => setActiveTab("data")} className={`py-2 border-b-2 text-sm ${activeTab === "data" ? "border-blue-500 text-blue-700 dark:text-blue-300" : "border-transparent text-slate-700 dark:text-slate-300"}`}>
            Dati Demo
          </button>
        </nav>
      </div>

      {activeTab === "setup" && (
        <section className="rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900/70 p-4 space-y-4">
          <h2 className="text-lg font-semibold">1) Setup profilo e data source</h2>
          <div className="grid md:grid-cols-3 gap-3">
            <input value={profileSlug} onChange={(e) => setProfileSlug(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent" placeholder="profileSlug" />
            <input value={profileName} onChange={(e) => setProfileName(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent" placeholder="name" />
            <input value={profileDescription} onChange={(e) => setProfileDescription(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent" placeholder="description" />
          </div>
          <div className="grid md:grid-cols-3 gap-3">
            <input value={sourceKey} onChange={(e) => setSourceKey(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent" placeholder="sourceKey" />
            <input value={sourceLabel} onChange={(e) => setSourceLabel(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent" placeholder="source label" />
            <select value={sourceKind} onChange={(e) => setSourceKind(e.target.value as any)} className="px-3 py-2 rounded border border-slate-300 bg-transparent">
              <option value="materialized_rows">materialized_rows</option>
              <option value="component_table">component_table</option>
              <option value="external_reader">external_reader</option>
            </select>
          </div>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              disabled={isBusy}
              className="px-3 py-1.5 bg-slate-800 text-white rounded text-sm hover:bg-slate-700 disabled:opacity-50"
              onClick={async () => {
                setIsBusy(true);
                try {
                  await createOrUpdateProfile({
                    profileSlug,
                    name: profileName,
                    description: profileDescription,
                    isActive: true,
                  });
                  await upsertDataSource({
                    profileSlug,
                    sourceKey,
                    label: sourceLabel,
                    sourceKind,
                    enabled: true,
                  });
                  setOpMessage("Profilo e data source salvati.");
                } finally {
                  setIsBusy(false);
                }
              }}
            >
              Salva profilo + source
            </button>
            <button
              type="button"
              disabled={isBusy}
              className="px-3 py-1.5 bg-slate-700 text-white rounded text-sm hover:bg-slate-600 disabled:opacity-50"
              onClick={async () => {
                setIsBusy(true);
                try {
                  await setupDefaultSnapshotConfig({ profileSlug });
                  setOpMessage("Setup demo completato: profilo + regole base.");
                } finally {
                  setIsBusy(false);
                }
              }}
            >
              Configura demo rapida
            </button>
            <button
              type="button"
              disabled={isBusy}
              className="px-3 py-1.5 bg-emerald-700 text-white rounded text-sm hover:bg-emerald-600 disabled:opacity-50"
              onClick={async () => {
                setIsBusy(true);
                try {
                  const res = await seedInvoices({ profileSlug, count: 40 });
                  setOpMessage(`Seed completato: ${res.inserted} invoices create e materializzate.`);
                } finally {
                  setIsBusy(false);
                }
              }}
            >
              Seed invoices
            </button>
          </div>
        </section>
      )}

      {activeTab === "rules" && (
        <section className="space-y-4">
          <div className="rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900/70 p-4 space-y-3">
            <h2 className="text-lg font-semibold">{editingDefinitionId ? "Modifica regola" : "Nuova regola"}</h2>
            <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
              <p>
                <strong>fieldPath:</strong> campo letto in <code>rowData</code> (es. <code>amount</code>, <code>category</code>, <code>monthRef</code>).
                Con <code>count</code> puoi lasciarlo vuoto.
              </p>
              <p>
                <strong>priority:</strong> ordine di esecuzione (più basso = prima). Il numero visualizzato nelle regole è questa priorità, non il risultato.
              </p>
              <p>
                Se nel tuo dataset un campo manca, spesso viene normalizzato a <code>null</code>. Per escludere record senza valore: <code>field neq null</code>.
              </p>
            </div>

            <div className="flex flex-wrap gap-2">
              <button onClick={applyPresetCountAll} className="px-3 py-1 bg-slate-200 dark:bg-slate-700 rounded text-sm">Preset: count all</button>
              <button onClick={applyPresetAmountOver1000} className="px-3 py-1 bg-slate-200 dark:bg-slate-700 rounded text-sm">Preset: amount &gt; 1000</button>
              <button onClick={applyPresetCategoryIn} className="px-3 py-1 bg-slate-200 dark:bg-slate-700 rounded text-sm">Preset: category in ...</button>
              <button onClick={applyPresetMonthAssigned} className="px-3 py-1 bg-slate-200 dark:bg-slate-700 rounded text-sm">Preset: monthRef assegnato</button>
            </div>

            <div className="grid md:grid-cols-4 gap-3">
              <input value={newIndicatorSlug} onChange={(e) => setNewIndicatorSlug(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent" placeholder="indicator slug" />
              <input value={newIndicatorLabel} onChange={(e) => setNewIndicatorLabel(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent" placeholder="indicator label" />
              <select value={sourceKey} onChange={(e) => setSourceKey(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent">
                <option value="">sourceKey...</option>
                {availableSources.map((source) => (
                  <option key={source._id} value={source.sourceKey}>
                    {source.sourceKey} ({source.label})
                  </option>
                ))}
              </select>
              <select value={newOperation} onChange={(e) => setNewOperation(e.target.value as RuleOperation)} className="px-3 py-2 rounded border border-slate-300 bg-transparent">
                {operations.map((op) => (
                  <option key={op} value={op}>{op}</option>
                ))}
              </select>
            </div>
            <p className="text-xs text-slate-600">{operationDescriptions[newOperation]}</p>

            <div className="grid md:grid-cols-4 gap-3">
              <input value={newFieldPath} onChange={(e) => setNewFieldPath(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent" placeholder={operationNeedsFieldPath ? "fieldPath richiesto" : "fieldPath opzionale"} />
              <input type="number" value={newPriority} onChange={(e) => setNewPriority(Number(e.target.value))} className="px-3 py-2 rounded border border-slate-300 bg-transparent" placeholder="priority" />
              <input value={newGroupBy} onChange={(e) => setNewGroupBy(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent" placeholder="groupBy csv (opzionale)" />
              <label className="inline-flex items-center gap-2 px-3 py-2 rounded border border-slate-300">
                <input type="checkbox" checked={newEnabled} onChange={(e) => setNewEnabled(e.target.checked)} />
                Regola abilitata
              </label>
            </div>

            <div className="space-y-3 rounded border border-slate-200 p-3">
              <div className="grid md:grid-cols-2 gap-3">
                <label className="space-y-1">
                  <span className="text-sm font-semibold">Periodo rolling</span>
                  <select
                    value={timeRangeKind}
                    onChange={(e) => setTimeRangeKind(e.target.value as CalculationTimeRangeKind | "")}
                    className="w-full px-2 py-2 rounded border border-slate-300 bg-transparent"
                  >
                    <option value="">Nessun periodo</option>
                    {Object.entries(timeRangeLabels).map(([value, label]) => (
                      <option key={value} value={value}>{label}</option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="flex items-center justify-between gap-2">
                <h3 className="text-sm font-semibold">Filtri</h3>
                <div className="flex gap-2">
                  <button onClick={() => setRuleFilters((prev) => [...prev, ...buildMonthAssignedPreset()])} className="px-3 py-1 bg-emerald-100 text-emerald-800 rounded text-sm">
                    + monthRef != null
                  </button>
                  <button onClick={addFilterDraft} className="px-3 py-1 bg-slate-200 dark:bg-slate-700 rounded text-sm">
                    + Aggiungi filtro
                  </button>
                </div>
              </div>
              {ruleFilters.length === 0 && <p className="text-sm text-slate-500">Nessun filtro impostato.</p>}
              {ruleFilters.map((filter) => (
                <div key={filter.id} className="grid md:grid-cols-5 gap-2 items-center">
                  <input value={filter.field} onChange={(e) => updateFilterDraft(filter.id, { field: e.target.value })} className="px-2 py-2 rounded border border-slate-300 bg-transparent" placeholder="field" />
                  <select value={filter.op} onChange={(e) => updateFilterDraft(filter.id, { op: e.target.value as FilterOperator })} className="px-2 py-2 rounded border border-slate-300 bg-transparent">
                    {filterOps.map((op) => (
                      <option key={op} value={op}>{filterOpLabels[op]}</option>
                    ))}
                  </select>
                  <select value={filter.valueType} onChange={(e) => updateFilterDraft(filter.id, { valueType: e.target.value as FilterValueType })} className="px-2 py-2 rounded border border-slate-300 bg-transparent">
                    <option value="string">string</option>
                    <option value="number">number</option>
                    <option value="boolean">boolean</option>
                    <option value="null">null</option>
                    <option value="csv">csv array</option>
                    <option value="json">json</option>
                  </select>
                  {filter.valueType === "boolean" ? (
                    <select value={filter.valueBoolean ? "true" : "false"} onChange={(e) => updateFilterDraft(filter.id, { valueBoolean: e.target.value === "true" })} className="px-2 py-2 rounded border border-slate-300 bg-transparent">
                      <option value="true">true</option>
                      <option value="false">false</option>
                    </select>
                  ) : (
                    <input value={filter.valueText} onChange={(e) => updateFilterDraft(filter.id, { valueText: e.target.value })} className="px-2 py-2 rounded border border-slate-300 bg-transparent" placeholder={filter.valueType === "csv" ? "a,b,c" : "value"} />
                  )}
                  <button onClick={() => removeFilterDraft(filter.id)} className="px-3 py-2 bg-red-100 text-red-700 rounded">
                    Rimuovi
                  </button>
                </div>
              ))}
            </div>

            <div className="rounded border border-slate-300 dark:border-slate-700 bg-slate-100 dark:bg-slate-800 p-2 text-xs text-slate-800 dark:text-slate-100">
              <strong>Anteprima regola:</strong> {rulePreview}
            </div>

            <div className="flex flex-wrap gap-2">
              <button disabled={isBusy || !profileSlug || !sourceKey} onClick={handleSaveRule} className="px-3 py-1.5 bg-blue-700 text-white rounded text-sm disabled:opacity-50">
                {editingDefinitionId ? "Salva modifica regola" : "Crea regola"}
              </button>
              {editingDefinitionId && (
                <button onClick={resetRuleEditor} className="px-3 py-1.5 bg-slate-200 dark:bg-slate-700 rounded text-sm">
                  Annulla modifica
                </button>
              )}
            </div>
          </div>

          <div className="rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900/70 p-4 space-y-2">
            <h3 className="text-base font-semibold">Regole presenti ({availableDefinitions.length})</h3>
            {definitionsPayload === undefined ? (
              <p className="text-sm text-slate-500">Caricamento...</p>
            ) : definitionsPayload === null ? (
              <p className="text-sm text-slate-500">Profilo non trovato: crea il profilo nella tab Setup.</p>
            ) : availableDefinitions.length === 0 ? (
              <p className="text-sm text-slate-500">Nessuna regola presente.</p>
            ) : (
              <div className="space-y-2">
                {availableDefinitions.map((definition) => (
                  <div key={definition._id} className="rounded border border-slate-200 p-3 space-y-1">
                    <div className="text-sm font-semibold">
                      {definition.indicatorSlug ?? "indicatore?"} {" <- "} {definition.sourceKey ?? "source?"}
                    </div>
                    <div className="text-xs text-slate-600">
                      op={definition.operation} | fieldPath={definition.fieldPath || "(root)"} | prioritaEsecuzione={definition.priority}
                    </div>
                    <div className="text-xs text-slate-600">filtri: {summarizeFilters(definition.filters)}</div>
                    <div className="flex flex-wrap gap-2 pt-1">
                      <button onClick={() => handleEditRule(definition)} className="px-2 py-1 bg-blue-100 text-blue-700 rounded text-sm">Modifica</button>
                      <button onClick={() => handleToggleRule(definition._id, !definition.enabled)} className="px-2 py-1 bg-amber-100 text-amber-700 rounded text-sm">
                        {definition.enabled ? "Disabilita" : "Abilita"}
                      </button>
                      <button onClick={() => handleDeleteRule(definition._id)} className="px-2 py-1 bg-red-100 text-red-700 rounded text-sm">Elimina</button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </section>
      )}

      {activeTab === "snapshots" && (
        <section className="rounded-lg border border-slate-300 dark:border-slate-700 bg-white dark:bg-slate-900/70 p-4 space-y-3">
          <h2 className="text-lg font-semibold">Snapshot: simulazione, creazione, explain</h2>
          <div className="flex flex-wrap gap-2">
            <input value={snapshotNote} onChange={(e) => setSnapshotNote(e.target.value)} className="px-3 py-2 rounded border border-slate-300 bg-transparent min-w-80" placeholder="note snapshot" />
            <button
              type="button"
              disabled={isBusy}
              className="px-3 py-1.5 bg-emerald-700 text-white rounded text-sm hover:bg-emerald-600 disabled:opacity-50"
              onClick={async () => {
                setIsBusy(true);
                try {
                  const res = await createManualSnapshot({ profileSlug, note: snapshotNote });
                  setOpMessage(`Snapshot creato: ${res.status}, processate ${res.processedCount} regole.`);
                  setSelectedSnapshotId(res.snapshotId as string);
                } finally {
                  setIsBusy(false);
                }
              }}
            >
              Crea snapshot manuale
            </button>
          </div>

          <div className="rounded border border-slate-300 dark:border-slate-700 bg-slate-100 dark:bg-slate-800 p-3">
            <h3 className="font-medium mb-2 text-sm">Simulazione (dry-run)</h3>
            {simulated === undefined ? (
              <p className="text-sm text-slate-500">Caricamento…</p>
            ) : simulated.length === 0 ? (
              <p className="text-sm text-slate-500">Nessuna regola attiva.</p>
            ) : (
              <ul className="list-disc pl-5 text-sm space-y-1">
                {simulated.map((item: any) => (
                  <li key={item.definitionId}>
                    {item.indicatorSlug} - {item.operation} = {String(item.normalizedResult)} (input: {item.inputCount})
                  </li>
                ))}
              </ul>
            )}
          </div>

          <div className="rounded border border-slate-300 dark:border-slate-700 bg-slate-100 dark:bg-slate-800 p-3">
            <h3 className="font-medium mb-2 text-sm">Snapshot creati</h3>
            {snapshots === undefined ? (
              <p className="text-sm text-slate-500">Caricamento…</p>
            ) : snapshots.length === 0 ? (
              <p className="text-sm text-slate-500">Nessuno snapshot ancora.</p>
            ) : (
              <ul className="text-sm space-y-1">
                {snapshots.map((snapshot: any) => (
                  <li key={snapshot._id}>
                    <button
                      className="text-left hover:underline"
                      onClick={() => setSelectedSnapshotId(snapshot._id as string)}
                    >
                      {snapshot.status} - {new Date(snapshot.snapshotAt).toISOString()}
                    </button>
                    {snapshot.errorMessage ? ` - errore: ${snapshot.errorMessage}` : ""}
                  </li>
                ))}
              </ul>
            )}
            {latestExplain && (
              <p className="mt-2 text-sm">
                Ultimo run: <code>{latestExplain.run?.status ?? "n/a"}</code>, values: <code>{latestExplain.values.length}</code>, traces: <code>{latestExplain.traces.length}</code>
              </p>
            )}
            {selectedExplain && (
              <p className="mt-2 text-xs text-slate-600">
                Snapshot selezionato: runItems=<code>{selectedExplain.runItems?.length ?? 0}</code>, values=<code>{selectedExplain.values?.length ?? 0}</code>
              </p>
            )}
          </div>
        </section>
      )}

      {activeTab === "data" && (
        <section className="space-y-4">
          <div className="rounded-lg border border-slate-300 dark:border-slate-700 p-4 bg-slate-100/80 dark:bg-slate-900/60">
            <h2 className="text-lg font-semibold mb-2">Stato dati invoices</h2>
            <div className="grid md:grid-cols-2 gap-4">
              <div>
                <h3 className="font-medium mb-2">Per categoria</h3>
                {invoicesByCategory === undefined ? (
                  <p className="text-slate-500 text-sm">Caricamento…</p>
                ) : invoicesByCategory.length === 0 ? (
                  <p className="text-slate-600 dark:text-slate-400 text-sm">Nessuna invoice.</p>
                ) : (
                  <ul className="list-disc pl-5 text-sm space-y-1">
                    {invoicesByCategory.map((row: any) => (
                      <li key={row.category}>
                        <code>{row.category}</code>: count {row.count}, total {row.total}, max {row.max}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
              <div>
                <h3 className="font-medium mb-2">Ultime invoices</h3>
                {invoices === undefined ? (
                  <p className="text-slate-500 text-sm">Caricamento…</p>
                ) : (
                  <ul className="list-disc pl-5 text-sm space-y-1">
                    {invoices.map((row: any) => (
                      <li key={row._id}>
                        {row.category} - {row.amount} ({new Date(row.issuedAt).toISOString().slice(0, 10)})
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          </div>

        </section>
      )}
    </main>
  );
}
