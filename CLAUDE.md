# theLook CDC Lakehouse — working agreement for Claude Code

This is a **learning project**. You may design and write all of the code, but Souhail must understand everything you did and why you did it. Work he can't explain in an interview doesn't count as done, even if it works.

The full specification is `docs/cahier-des-charges.md` (section 13 repeats these rules; `cahier-des-charges-v1.6.docx` is a frozen Word snapshot of v1.6; later revisions exist only in the Markdown file). Treat it as the source of truth for scope and architecture.

At the start of each session, read the latest entry in `docs/learning-log.md` and continue from its "Where we stopped" section.

## Core principle

**You may make the decisions and write the code. Souhail must be able to explain every decision and every line.**

## Rules

1. **Explain every decision** when you make it: what you decided, which alternatives you considered, and why you chose this one. Covers tools, config values, schemas, data models, partitioning, naming, any trade-off.
2. **Ask only when the cahier des charges would change.** Decisions inside the spec: take them and explain them. Anything that changes architecture, scope or objectives: propose options with reasoning and wait for Souhail's approval.
3. **Explain every change.** After each step, summarise what was built, how the pieces connect, and for each non-trivial setting or piece of logic: what it does, why it's needed, what would break without it.
4. **Check understanding, not just delivery.** After each important step, ask one or two short questions or ask Souhail to explain the step back in his own words. Correct misunderstandings before moving on.
5. **Go deeper on request.** When Souhail asks "why", give the full reasoning, the underlying concept, and a pointer to the relevant docs.
6. **Record decisions as ADRs** in `docs/adr/` (context, options, decision, consequences). Souhail reviews and approves each one.
7. **Explain failures.** When something breaks, show the symptom, how you found the cause, and why the fix works, so he learns the debugging method, not only the fix.
8. **Checkpoint at the end of each phase.** Ask 3–5 interview-style questions (examples in section 13.4 of the spec). Close gaps before starting the next phase.
9. **Keep a learning log** in `docs/learning-log.md`: concepts covered, decisions taken, open questions, one entry per session.
10. **One step at a time.** Small increments and small commits with clear messages. Souhail must be able to explain every commit in an interview.

## Who does what

| Work | Claude Code | Souhail |
|---|---|---|
| Architecture within the spec | Decides and explains the reasoning | Understands; can challenge any choice |
| Changes to the cahier des charges | Proposes options with trade-offs | Approves or rejects |
| Code, config, Spark jobs, DAGs, Terraform | Writes and explains | Reads, asks questions, explains it back |
| ADRs and learning log | Writes | Reviews and approves |
| Debugging | Diagnoses and fixes, showing the method | Follows the reasoning |
| Phase checkpoints | Asks the questions | Answers without looking at the code |
