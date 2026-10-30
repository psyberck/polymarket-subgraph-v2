import type { PolygonChainState } from '#supervisor/chain.ts';
import type { SupervisorConfig } from '#supervisor/config.ts';
import { outputFolderPath } from '#supervisor/snapshot.ts';

/**
 * Process the snapshot taken at the paused block.
 *
 * Must be idempotent: unlike archive, nothing records that this step ran, so a
 * restart at the same boundary calls it again for the same snapshot. Check
 * for your own output before producing it, or make producing it twice harmless.
 *
 * @param config - Supervisor settings; `outputDir` holds the snapshots.
 * @param chain - Envio's chain state; `progressBlock` is the paused block.
 * @param signal - Aborting it stops the processing.
 */
export async function processAt(
  config: SupervisorConfig,
  chain: PolygonChainState,
  signal: AbortSignal,
): Promise<void> {
  signal.throwIfAborted();
  const folderPath = outputFolderPath(config.outputDir, chain.progressBlock);
  console.debug(`files have been generated at ${folderPath}`);
  // Customized processing logic goes here.
}
