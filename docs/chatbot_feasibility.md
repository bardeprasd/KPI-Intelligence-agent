# Chatbot Feasibility Demonstration

## Purpose
The assessment email adds a second expectation beyond the KPI one-pager: demonstrate the feasibility of a chatbot that can query KPI and performance data using natural language and maintain contextual memory across follow-up questions.

This submission addresses that requirement with a lightweight, auditable chatbot layer placed on top of the deterministic KPI engine.

## Scope of the Demo
The chatbot is intentionally positioned as a **feasibility prototype**, not a production conversational platform.

It supports:
- natural-language KPI questions
- section-level summaries
- top risks and recommendations
- warehouse-specific KPI lookups for warehouse-grained domains
- warehouse comparisons
- short-turn contextual memory for follow-up questions such as "How about WH-03?"

It does **not** replace the deterministic KPI pipeline. It consumes already-computed metrics or recomputes warehouse-grained slices from the same deterministic functions.

## Architecture
The chatbot layer is implemented in two files:

- `agent/chatbot.py`
  - intent parsing
  - KPI alias mapping
  - warehouse extraction
  - conversational memory
  - response generation
- `chatbot_demo.py`
  - CLI entry point
  - interactive chat mode
  - single-query mode
  - scripted demo mode
  - transcript export

## Why this is appropriate for the assignment
This design shows the requested feasibility without overengineering:
- deterministic KPI engine remains the source of truth
- chatbot answers are traceable to computed KPIs
- follow-up questions reuse prior context
- warehouse comparison logic is explicit and reviewable
- no hallucination is required for core performance answers

## Example Questions Supported
- What is the fill rate?
- Show the inventory section.
- Which warehouse has the highest error rate?
- Compare WH-01 and WH-02 on SLA.
- How about WH-03?
- What are the top risks?
- What are the recommendations?

## Contextual Memory Example
Example conversation:
1. User: Compare WH-01 and WH-02 on SLA.
2. Assistant: returns both values.
3. User: How about WH-03?
4. Assistant: understands that the follow-up still refers to **SLA Adherence %** and answers for WH-03.

This demonstrates the requested contextual memory behavior without needing a full production memory store.

## Design Trade-offs
- The prototype uses rule-based parsing instead of a full semantic retrieval stack.
- This keeps behavior easy to explain in interview and avoids turning the demo into an opaque LLM chatbot.
- For production, a retrieval + tool-calling architecture could be layered on top while preserving the same deterministic KPI service.

## Run Instructions
Fastest interview/demo flow:
```bash
python run_agent.py --start-chatbot
```

This generates the one-pager outputs first and then opens the grounded chatbot on the same reporting payload.

Interactive mode:
```bash
python chatbot_demo.py
```

Single query:
```bash
python chatbot_demo.py --query "What is the fill rate?"
```

Scripted demo with transcript export:
```bash
python chatbot_demo.py --demo-script --save-transcript output/chatbot_demo_transcript.md
```

Scoped to selected warehouses:
```bash
python chatbot_demo.py --warehouses WH-01,WH-02
```

Deterministic-only chatbot mode:
```bash
python chatbot_demo.py --deterministic-chat
```

## Output Artifact
A sample scripted transcript is included at:
- `output/chatbot_demo_transcript.md`

This artifact can be shown during the interview as evidence of feasibility and conversational memory.


## OpenAI-backed chat mode
The chatbot now attempts OpenAI-backed mode by default for natural-language interpretation, answer phrasing, and contextual follow-up handling. In this mode, the model is grounded only on the computed KPI payload, reporting period, warehouse scope, and recent conversation memory. KPI calculations are never delegated to the model. If OpenAI is unavailable, the chatbot falls back to deterministic answers so the demo remains usable.

Example commands:
```bash
python chatbot_demo.py
python chatbot_demo.py --warehouses WH-01,WH-02
python run_agent.py --start-chatbot
```
