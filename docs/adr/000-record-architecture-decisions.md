# 000. Record architecture decisions

- Status: Accepted
- Date: 2026-09-29
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

This project makes many decisions: tools, configuration values, data models,
partitioning, naming, trade-offs. The cahier des charges describes the target
system, but not the reasoning behind choices made while building it, nor the
places where implementation diverges from it. Reasoning that only lives in chat
sessions or commit messages is lost or re-argued later. The author must also be
able to explain every decision in an interview (spec 13.1), and the spec asks
for ADRs explicitly (sections 10 and 13.2, rule 6).

## Options considered

1. **No records**: reasoning stays in chats and commit messages. Not searchable, easily lost.
2. **Everything in the spec**: mixes "what" with "why"; bloats the spec; every
   implementation detail would need a spec revision.
3. **External wiki (Notion, Confluence)**: lives away from the code and drifts from it.
4. **Architecture Decision Records in the repository** (Michael Nygard's format),
   versioned and reviewed with the code.

## Decision

Option 4.

- One Markdown file per decision in `docs/adr/`, named `NNN-short-title.md`.
  Numbers have three digits and are assigned in the order ADRs are written. The
  list in spec section 10 gives examples; it does not reserve numbers.
- Sections: Status, Context, Options considered, Decision, Consequences
  (positive and negative), References. One page at most.
- Lifecycle: `Proposed` → `Accepted` (after Souhail's review) → optionally
  `Superseded by NNN` or `Deprecated`. An accepted ADR is never rewritten in
  substance; a changed decision gets a new ADR that supersedes the old one.
- Claude Code drafts the ADR when the decision is made; Souhail reviews it and
  either accepts it or asks questions.
- An ADR that changes the cahier des charges is accepted only together with
  the matching spec revision.
- When to write one: the decision is hard to reverse, had real alternatives, or
  someone could reasonably ask "why?" six months from now. Smaller choices go in
  commit messages and the learning log.

## Consequences

- ✅ Reasoning is kept next to the code, reviewed like code, and doubles as interview preparation.
- ✅ The superseded chain shows how the design evolved, not just where it ended up.
- ❌ Writing an ADR costs time for every significant decision.
- ❌ Risk of writing ADRs for trivial choices; the "when to write one" rule above limits this.

## References

- Michael Nygard, "Documenting Architecture Decisions" (2011)
- adr.github.io
