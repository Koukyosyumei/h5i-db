import LabKernel
import H5iAppLib
open Aeneas Aeneas.Std Result Aeneas.Std.WP H5iAppLib

-- Derived equality and clone for the kernel's structs, so `step*` crosses them.
h5i_derive_all

namespace lab_kernel

attribute [simp] u64_val_eq u64_bne

@[step] theorem one_spec (w : Write) : one w ⦃ v => v.val = [w] ⦄ := by
  unfold one; step*

@[step] theorem two_spec (a b : Write) : two a b ⦃ v => v.val = [a, b] ⦄ := by
  unfold two; step*

@[step] theorem four_spec (a b c d : Write) : four a b c d ⦃ v => v.val = [a, b, c, d] ⦄ := by
  unfold four
  step*
  have hlen : ws2.val.length = 3 := by
    simp only [ws2_post, ws1_post, ws_post, alloc.vec.Vec.from_val, List.length_append,
      List.length_singleton, List.nil_append]
  exact usize_lt_max (by simp [hlen])

@[step] theorem find_agent_spec (v : alloc.vec.Vec Agent) (id : U64) :
    find_agent v id ⦃ o => o = v.val.find? (fun a => a.id.val = id.val) ⦄ := by
  unfold find_agent find_agent_loop
  apply WP.spec_mono (loop_search v.val (fun a => decide (a.id = id)) (fun o : Option Agent => o)
    (fun _ a => some a) none _ ?_ 0#usize (by simp))
  · intro r hr; rw [search_find _ _ _ hr]; simp
  · intro j hj; unfold find_agent_loop.body; h5i_step

@[step] theorem find_family_spec (v : alloc.vec.Vec Family) (id : U64) :
    find_family v id ⦃ o => o = v.val.find? (fun f => f.id.val = id.val) ⦄ := by
  unfold find_family find_family_loop
  apply WP.spec_mono (loop_search v.val (fun f => decide (f.id = id)) (fun o : Option Family => o)
    (fun _ f => some f) none _ ?_ 0#usize (by simp))
  · intro r hr; rw [search_find _ _ _ hr]; simp
  · intro j hj; unfold find_family_loop.body; h5i_step

@[step] theorem find_pending_spec (v : alloc.vec.Vec Pending) (id : U64) :
    find_pending v id ⦃ o => o = v.val.find? (fun p => p.id.val = id.val) ⦄ := by
  unfold find_pending find_pending_loop
  apply WP.spec_mono (loop_search v.val (fun p => decide (p.id = id)) (fun o : Option Pending => o)
    (fun _ p => some p) none _ ?_ 0#usize (by simp))
  · intro r hr; rw [search_find _ _ _ hr]; simp
  · intro j hj; unfold find_pending_loop.body; h5i_step

/-- An agent cannot record a trial. Only the worker can, so a failed trial
cannot be dropped by never reporting it. -/
theorem record_requires_worker
    (actor : Principal) (s : Snapshot)
    (pending data_version score corrected sharpe skew kurt samples tokens : U64)
    (h : actor.role ≠ ROLE_WORKER) :
    record_trial actor s pending data_version score corrected sharpe skew kurt samples tokens =
      ok (.Err .Forbidden) := by
  unfold record_trial
  rw [u64_bne]
  simp [h]

/-- A successful promote read a family whose holdout had already passed. -/
theorem promote_requires_pass (actor : Principal) (s : Snapshot) (family : U64) :
    promote actor s family ⦃ r => ∀ ws rep, r = .Ok (ws, rep) →
      ∃ f, s.families.val.find? (fun g => g.id.val = family.val) = some f ∧
        f.holdout = HOLDOUT_PASS ⦄ := by
  unfold promote
  step*

/-- A successful promote read a family that had not been rejected.
A rejected strategy therefore has no promoting execution. -/
theorem rejected_never_promotes (actor : Principal) (s : Snapshot) (family : U64) :
    promote actor s family ⦃ r => ∀ ws rep, r = .Ok (ws, rep) →
      ∃ f, s.families.val.find? (fun g => g.id.val = family.val) = some f ∧
        f.rejected = 0#u64 ⦄ := by
  unfold promote
  step*

/-- Reject, comment, and promote are the human's. Any other role is refused
before the ledger changes. -/
theorem review_requires_human
    (actor : Principal) (s : Snapshot) (family : U64) (text : alloc.vec.Vec U8)
    (h : actor.role ≠ ROLE_HUMAN) :
    reject actor s family text = ok (.Err .Forbidden) ∧
    comment actor s family text = ok (.Err .Forbidden) ∧
    promote actor s family = ok (.Err .Forbidden) := by
  refine ⟨?_, ?_, ?_⟩
  · unfold reject; rw [u64_bne]; simp [h]
  · unfold comment; rw [u64_bne]; simp [h]
  · unfold promote; rw [u64_bne]; simp [h]

/-- A report returns the corrected score stored on the family and the trial
count. The raw score is not part of that reply. -/
theorem reported_is_the_stored_correction (actor : Principal) (s : Snapshot) (family : U64) :
    report actor s family ⦃ r => ∀ ws trials corrected,
      r = .Ok (ws, .Metric trials corrected) →
        ∃ f, s.families.val.find? (fun g => g.id.val = family.val) = some f ∧
          trials = f.trials ∧ corrected = f.corrected ⦄ := by
  unfold report
  step*

/-- A successful evaluation read a family that was still queued, so a second
evaluation of the same family does not succeed. -/
theorem holdout_is_once (actor : Principal) (s : Snapshot) (family passed : U64) :
    evaluate_holdout actor s family passed ⦃ r => ∀ ws rep, r = .Ok (ws, rep) →
      ∃ f, s.families.val.find? (fun g => g.id.val = family.val) = some f ∧
        f.holdout = HOLDOUT_QUEUED ⦄ := by
  unfold evaluate_holdout
  step*

/-- A recorded trial increases that family's trial count by one and stores the
corrected score it was given. -/
theorem record_counts_the_trial
    (actor : Principal) (s : Snapshot)
    (pending data_version score corrected sharpe skew kurt samples tokens : U64) :
    record_trial actor s pending data_version score corrected sharpe skew kurt samples tokens ⦃ r =>
      ∀ ws rep, r = .Ok (ws, rep) →
        ∃ (f wrote : Family),
          f ∈ s.families.val ∧
          wrote.id = f.id ∧
          wrote.trials.val = f.trials.val + 1 ∧
          wrote.corrected = corrected ∧
          Write.PutFamily wrote ∈ ws.val ⦄ := by
  unfold record_trial
  h5i_steps
  all_goals try (simp only [core.num.U64.MAX, U64.rMax] at *; scalar_tac)
  all_goals (
    intro ws rep hr
    obtain ⟨rfl, rfl⟩ := ok_inj hr
    let hf : f ∈ s.families.val := List.mem_of_find?_eq_some (o1_post ▸ ‹o1 = some f›)
    let wrote : Family := { f with trials := i2, corrected := corrected }
    refine ⟨f, wrote, hf, ?_, ?_, ?_, ?_⟩
    · simp [wrote]
    · simpa [wrote] using i2_post
    · simp [wrote]
    · simp [v_post, wrote])

end lab_kernel
