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

@[step] theorem five_spec (a b c d e : Write) : five a b c d e ⦃ v => v.val = [a, b, c, d, e] ⦄ := by
  unfold five
  step*
  · exact usize_lt_max (by simp [ws2_post, ws1_post, ws_post])
  · have hlen : ws3.val.length = 4 := by simp [ws3_post, ws2_post, ws1_post, ws_post]
    exact usize_lt_max (by simp [hlen])
  · simp [v_post, ws3_post, ws2_post, ws1_post, ws_post]

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

@[step] theorem find_trial_spec (v : alloc.vec.Vec Trial) (id : U64) :
    find_trial v id ⦃ o => o = v.val.find? (fun t => t.id.val = id.val) ⦄ := by
  unfold find_trial find_trial_loop
  apply WP.spec_mono (loop_search v.val (fun t => decide (t.id = id)) (fun o : Option Trial => o)
    (fun _ t => some t) none _ ?_ 0#usize (by simp))
  · intro r hr; rw [search_find _ _ _ hr]; simp
  · intro j hj; unfold find_trial_loop.body; h5i_step

@[step] theorem find_text_spec (v : alloc.vec.Vec Text) (kind : U64) (body : alloc.vec.Vec U8) :
    find_text v kind body ⦃ o =>
      o = (v.val.find? (fun t => decide (t.kind = kind ∧ t.body = body))).map (fun t => t.id) ⦄ := by
  unfold find_text find_text_loop
  apply WP.spec_mono (loop_search v.val (fun t => decide (t.kind = kind ∧ t.body = body))
    (fun o : Option U64 => o) (fun _ t => some t.id) none _ ?_ 0#usize (by simp))
  · intro r hr
    rw [hr, searchFrom_find, UScalar.ofNatCore_val_eq, List.drop_zero]
  · intro j hj; unfold find_text_loop.body; h5i_step

/-- The id `intern` returns names a text of that kind with exactly that body:
either a row already in the ledger, or the new row it returns to be written
at `next`. So the code, parameters, and hypothesis a pending or family row
points to are the bytes the agent submitted. -/
theorem intern_names_the_text (v : alloc.vec.Vec Text) (kind : U64)
    (nm body : alloc.vec.Vec U8) (next : U64) :
    intern v kind nm body next ⦃ r => ∀ id row n, r = .Ok (id, row, n) →
      (row = none ∧ n = next ∧ ∃ t ∈ v.val, t.id = id ∧ t.kind = kind ∧ t.body = body) ∨
      (id = next ∧ row = some { id := next, kind := kind, «name» := nm, body := body }) ⦄ := by
  unfold intern
  step*
  · simp only [core.num.U64.MAX, U64.rMax] at *; scalar_tac
  · intro i0 row n hr
    simp only [core.result.Result.Ok.injEq, Prod.mk.injEq] at hr
    obtain ⟨rfl, rfl, rfl⟩ := hr
    refine Or.inl ⟨rfl, rfl, ?_⟩
    rename_i hsome
    rw [hsome, eq_comm, Option.map_eq_some_iff] at o_post
    obtain ⟨t, ht, hid⟩ := o_post
    have hp := List.find?_some ht
    simp only [decide_eq_true_eq] at hp
    exact ⟨t, List.mem_of_find?_eq_some ht, hid, hp.1, hp.2⟩

/-- An agent cannot record a trial. Only the worker can, so a failed trial
cannot be dropped by never reporting it. -/
theorem record_requires_worker
    (actor : Principal) (s : Snapshot)
    (pending data_version score corrected sharpe skew kurt samples tokens : U64)
    (curve : alloc.vec.Vec U8)
    (h : actor.role ≠ ROLE_WORKER) :
    record_trial actor s pending data_version score corrected sharpe skew kurt samples tokens curve =
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
    (pending data_version score corrected sharpe skew kurt samples tokens : U64)
    (curve : alloc.vec.Vec U8) :
    record_trial actor s pending data_version score corrected sharpe skew kurt samples tokens curve ⦃ r =>
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
    · simp [v1_post, wrote])

/-- A successful freeze copied a trial the family had already counted: the
frozen code, parameters, and data version are that trial's. An agent cannot
freeze a strategy that was never run and recorded. -/
theorem freeze_requires_a_counted_trial (actor : Principal) (s : Snapshot) (family trial : U64) :
    freeze actor s family trial ⦃ r => ∀ ws rep, r = .Ok (ws, rep) →
      ∃ t, s.trials.val.find? (fun x => x.id.val = trial.val) = some t ∧
        t.family.val = family.val ∧
        ∃ f, Write.PutFamily f ∈ ws.val ∧ f.frozen_code = t.code ∧
          f.frozen_params = t.params ∧ f.frozen_data = t.data_version ⦄ := by
  unfold freeze freeze_trial
  step*
  intro ws rep hr
  obtain ⟨rfl, rfl⟩ := ok_inj hr
  have hfam : t.family = f.id := by simpa [u64_bne] using ‹¬(t.family != f.id) = true›
  have hf := List.find?_some (o_post ▸ ‹o = some f›)
  simp only [decide_eq_true_eq] at hf
  refine ⟨t, o1_post ▸ ‹o1 = some t›, by rw [hfam]; exact hf,
    { f with frozen_code := t.code, frozen_params := t.params, frozen_data := t.data_version },
    by simp [v_post], rfl, rfl, rfl⟩

end lab_kernel
