/**
 * Type definitions for plexring.
 */

/** Function handler signature running inside the worker thread */
export type RawJobHandler = (payload: Uint8Array) => Uint8Array | Promise<Uint8Array>;

/** Generic typed handler definition for defineWorker */
export type TypedJobHandler<TInput = any, TOutput = any> = (input: TInput) => TOutput | Promise<TOutput>;

/** Fallback IPC message from Main Thread to Worker Thread */
export interface FallbackJobMessage {
  type: 'fallback_job';
  jobType: number;
  requestId: number;
  payload: Uint8Array;
}

/** Fallback IPC message from Worker Thread to Main Thread */
export interface FallbackResultMessage {
  type: 'fallback_res';
  status: number;
  requestId: number;
  payload: Uint8Array;
}

/** Shutdown signal message */
export interface ShutdownMessage {
  type: 'shutdown';
}

export type WorkerInboundMessage = FallbackJobMessage | ShutdownMessage;
export type WorkerOutboundMessage = FallbackResultMessage;

/** Initialization options for Plex pool */
export interface PlexOptions {
  /** Path to the compiled worker JavaScript file */
  workerFile: string | URL;
  /** Number of worker threads to spawn (defaults to navigator.hardwareConcurrency or os.cpus().length) */
  workers?: number;
}

/** Telemetry statistics for the pool and individual worker rings */
export interface WorkerTelemetry {
  workerId: number;
  pendingJobs: number;
  isSleeping: boolean;
}

export interface PoolStats {
  workerCount: number;
  totalPendingJobs: number;
  workers: WorkerTelemetry[];
}
