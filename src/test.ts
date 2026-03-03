/**
 * Test helper per convex-test: registra il componente kpi-snapshot nell'istanza di test.
 * Uso: import { register } from "@okrlinkhub/component/test";
 *      const t = convexTest(appSchema, appModules);
 *      register(t, "kpiSnapshot");
 */
import type { TestConvex } from "convex-test";
import type { GenericSchema, SchemaDefinition } from "convex/server";
import schema from "./component/schema.js";

const modules = import.meta.glob("./component/**/*.ts");

/**
 * Registra il componente con l'istanza convex-test.
 * @param t - Istanza di test (es. da convexTest).
 * @param name - Nome del componente, come in convex.config.ts (default "kpiSnapshot").
 */
export function register(
  t: TestConvex<SchemaDefinition<GenericSchema, boolean>>,
  name: string = "kpiSnapshot",
) {
  t.registerComponent(name, schema, modules);
}

export default { register, schema, modules };
