/**
 * Client entry point per @okrlinkhub/kpi-snapshot.
 * Re-export di tipi e helper per le app che usano il componente.
 */

export type { ComponentApi } from "../component/_generated/component.js";

/** Payload di un singolo valore KPI inviato al writer LinkHub (syncToLinkHub). */
export type IndicatorValuePayload = {
  indicatorSlug: string;
  value: number;
  date: number;
};
