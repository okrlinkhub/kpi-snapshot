import type { Doc, Id } from '../_generated/dataModel.js'
import type { MutationCtx, QueryCtx } from '../_generated/server.js'

type Ctx = QueryCtx | MutationCtx

export async function getLatestCompletedSnapshotForProfileAndSource (
  ctx: Ctx,
  args: {
    profileId: Id<'snapshotProfiles'>
    sourceKey?: string
  }
) {
  if (!args.sourceKey) {
    return null
  }

  const rows = await ctx.db
    .query('snapshots')
    .withIndex('by_profile_and_trigger_source_key_and_status_and_snapshot_at', (q) => (
      q
        .eq('profileId', args.profileId)
        .eq('triggerSourceKey', args.sourceKey)
        .eq('status', 'completed')
    ))
    .order('desc')
    .take(1)

  return rows[0] ?? null
}

export async function resolveReportSnapshotDocument (
  ctx: Ctx,
  args: {
    profileId: Id<'snapshotProfiles'>
    lockedSourceKey?: string | null
    pinnedSnapshotId?: Id<'snapshots'>
  }
): Promise<Doc<'snapshots'> | null> {
  const latestSnapshot = await getLatestCompletedSnapshotForProfileAndSource(ctx, {
    profileId: args.profileId,
    sourceKey: args.lockedSourceKey ?? undefined,
  })

  if (latestSnapshot) {
    return latestSnapshot
  }

  if (!args.pinnedSnapshotId) {
    return null
  }

  const pinnedSnapshot = await ctx.db.get(args.pinnedSnapshotId)
  if (!pinnedSnapshot) {
    return null
  }

  if (pinnedSnapshot.profileId !== args.profileId) {
    return null
  }

  if (args.lockedSourceKey && pinnedSnapshot.triggerSourceKey !== args.lockedSourceKey) {
    return null
  }

  return pinnedSnapshot.status === 'completed' ? pinnedSnapshot : null
}
