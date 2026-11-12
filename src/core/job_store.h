#pragma once

#include "core/clock.h"
#include "core/job.h"

#include <cstddef>
#include <optional>
#include <string>

namespace epochflow {

// Durable home of job state, leases, and generations. Every method is one
// atomic state change: concurrent callers observe either all of it or none.
// Implementations must be safe to call from multiple threads.
//
// Fencing (invariant I3): renew, complete, and fail succeed only when the job
// is RUNNING with exactly the caller's generation. An expired lease alone does
// not make a request stale; ReclaimExpired does, by moving the job out of
// RUNNING. After that, only a new Acquire (with a higher generation) can run it.
//
// Errors: invalid arguments throw std::invalid_argument. Storage failures
// throw; the change is then either fully applied or not at all, and the caller
// may not know which (callers retry with the same generation, which is safe
// because duplicate completions and failures are idempotent).
class JobStore {
 public:
  virtual ~JobStore() = default;

  // Creates a PENDING job with generation 0. Throws std::invalid_argument if
  // spec.max_attempts < 1.
  virtual JobId Submit(const JobSpec& spec) = 0;

  virtual std::optional<JobRecord> Get(JobId id) const = 0;

  // Assigns the oldest PENDING job to `worker`: PENDING -> RUNNING, generation
  // and attempts incremented, lease set to now + `lease`. Returns nullopt when
  // no job is PENDING. Concurrent callers never receive the same assignment.
  // Throws std::invalid_argument if lease <= 0.
  virtual std::optional<Assignment> Acquire(const WorkerId& worker, Duration lease) = 0;

  // Extends the lease to now + `lease` (also after the deadline passed, as long
  // as the job was not reclaimed). kStale if the assignment is not current.
  virtual RenewResult Renew(JobId id, Generation generation, Duration lease) = 0;

  // RUNNING -> COMPLETED with `result`. A duplicate for the generation that
  // completed the job returns kAccepted if `result` matches, else kConflict;
  // the stored result never changes.
  virtual CommitStatus Complete(JobId id, Generation generation,
                                const std::string& result) = 0;

  // Records a failed attempt. Retryable with attempts left: RUNNING -> PENDING.
  // Otherwise RUNNING -> FAILED. A duplicate for the generation that failed the
  // job returns kAccepted if `reason` matches, else kConflict. A duplicate of a
  // retryable failure that re-queued the job returns kStale.
  virtual CommitStatus Fail(JobId id, Generation generation, const std::string& reason,
                            bool retryable) = 0;

  // Applies a ledger entry exactly once per `operation_id` (invariant I4), as
  // part of the caller's assignment. In one atomic step:
  //  - kStale / kNotFound unless the job is RUNNING with `generation`, checked
  //    before anything else, so a stale worker learns nothing and changes
  //    nothing;
  //  - a new operation_id: records the operation and creates the entry
  //    (deduplicated = false);
  //  - an existing operation_id with the same entry: returns the existing
  //    entry id (deduplicated = true), whichever job or attempt created it;
  //  - an existing operation_id with different content, or used for another
  //    kind of operation: kConflict, nothing changes.
  // The fencing check holds for the whole step: the job cannot be reclaimed or
  // reassigned between the check and the effect. Throws std::invalid_argument
  // for an empty operation_id or account.
  virtual OperationOutcome ApplyLedger(JobId id, Generation generation,
                                       const std::string& operation_id,
                                       const LedgerEntry& entry) = 0;

  // Records the intent to call an external system that has no idempotency
  // key, before calling it, so a later attempt can tell that a call may already
  // have happened. Fenced like ApplyLedger (kStale / kNotFound first). Then:
  //  - new operation_id: intent recorded for this attempt -> kStart;
  //  - intent recorded by this same job and generation (a retried request)
  //    -> kStart again;
  //  - intent recorded by another attempt or job, never confirmed -> kInDoubt;
  //  - already confirmed -> kConfirmed with the provider's answer;
  //  - reconciled as never performed -> kFailed with the operator's evidence;
  //  - different request, or a ledger operation with that id -> kConflict.
  // Throws std::invalid_argument for an empty operation_id.
  virtual ExternalBegin BeginExternal(JobId id, Generation generation,
                                      const std::string& operation_id,
                                      const std::string& request) = 0;

  // Records the provider's confirmation for a started external operation.
  // Deliberately not fenced: the provider's answer is a fact about the outside
  // world, worth keeping even if the reporting attempt has gone stale, because
  // it lets later attempts finish instead of becoming UNKNOWN. Idempotent for
  // the same confirmation; kConflict for a different one, a different
  // request, or an operation reconciled as never performed; kNotFound for an
  // unknown operation.
  virtual CommitStatus ConfirmExternal(const std::string& operation_id,
                                       const std::string& request,
                                       const std::string& confirmation) = 0;

  // RUNNING -> UNKNOWN for the caller's generation: the outcome of an external
  // effect cannot be determined, so the job is neither retried nor failed
  // (invariant I5). Releases the lease; UNKNOWN jobs are never reclaimed or
  // acquired. A duplicate for the same generation is idempotent on `reason`
  // (kAccepted / kConflict).
  virtual CommitStatus MarkUnknown(JobId id, Generation generation,
                                   const std::string& reason) = 0;

  // Operator decision with outside evidence: UNKNOWN -> COMPLETED (`detail`
  // becomes the result) or FAILED (`detail` becomes the error). Repeating the
  // same decision is kAccepted; any other job state is kStale, and so is a job
  // that still owns a STARTED external operation (resolve it with
  // ReconcileExternal instead, so job and operation cannot diverge). Throws
  // std::invalid_argument unless `outcome` is kCompleted or kFailed.
  virtual CommitStatus Reconcile(JobId id, JobState outcome, const std::string& detail) = 0;

  // Operator decision for an UNKNOWN job and the external operation it
  // created, applied to both in one atomic step (ADR 0006):
  //   kCompleted: operation STARTED -> CONFIRMED with `evidence` as its result
  //               (or already CONFIRMED with exactly `evidence`); job ->
  //               COMPLETED with `evidence` as its result.
  //   kFailed:    operation STARTED -> FAILED with `evidence`; job -> FAILED
  //               with `evidence` as its error.
  // Repeating the decision that resolved the pair is kAccepted. Returns
  // kNotFound for an unknown job or operation; kConflict if the operation is
  // not an EXTERNAL operation created by this job, or the decision contradicts
  // an existing confirmation; kStale if the job is not UNKNOWN. A rejected call
  // changes nothing. Throws std::invalid_argument for an empty operation id or
  // an outcome other than kCompleted or kFailed.
  virtual CommitStatus ReconcileExternal(JobId id, const std::string& operation_id,
                                         JobState outcome, const std::string& evidence) = 0;

  // Recovers every RUNNING job whose lease deadline is strictly in the past:
  // PENDING if attempts remain, otherwise FAILED. The generation is unchanged;
  // the next Acquire increments it. Returns the number of jobs recovered.
  // Recovery is permission to re-run, not proof the old worker stopped.
  virtual std::size_t ReclaimExpired() = 0;
};

}  // namespace epochflow
