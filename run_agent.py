"""
Batch entrypoint for the KPI intelligence agent.

This file sits in the orchestration layer of the project. It loads source data,
derives the reporting period, computes deterministic KPIs, optionally refines
the narrative with OpenAI, and writes JSON, Excel, HTML, and text outputs.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from agent.chatbot import KPIChatbot
from agent.config import LLM_SUMMARY_CONFIG, PathsConfig
from agent.ingest import derive_default_period, load_all
from agent.kpi import compute_all_kpis
from agent.output import build_payload, write_excel, write_html, write_json, write_run_summary
from agent.llm_summary import LLMPolishError, generate_narrative_with_openai
from agent.summarize import build_insights_risks_and_recommendations, summarize_sections


load_dotenv(override=True)




def parse_args() -> argparse.Namespace:
    # ================================
    # Function: parse_args
    # Purpose: Defines CLI options for running the KPI intelligence pipeline.
    # Inputs:
    #   - None
    # Output:
    #   - argparse.Namespace containing run configuration
    # ================================
    parser = argparse.ArgumentParser(description="Run the KPI intelligence agent.")
    parser.add_argument("--start", type=str, help="Reporting start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="Reporting end date (YYYY-MM-DD)")
    parser.add_argument("--warehouses", type=str, help="Comma-separated warehouse ids, e.g. WH-01,WH-02")
    parser.add_argument("--sku-families", type=str, help="Optional comma-separated SKU families, e.g. Engine,Electrical")
    parser.add_argument("--no-html", action="store_true", help="Disable HTML output generation")
    parser.add_argument("--llm-model", type=str, default=LLM_SUMMARY_CONFIG["model"], help="OpenAI model for narrative generation")
    parser.add_argument(
        "--deterministic-summary",
        action="store_true",
        help="Disable the default OpenAI narrative refinement and keep deterministic wording only",
    )
    parser.add_argument(
        "--use-llm-summary",
        action="store_true",
        help="Deprecated compatibility flag. OpenAI narrative refinement is now enabled by default unless --deterministic-summary is passed.",
    )
    parser.add_argument(
        "--start-chatbot",
        action="store_true",
        help="Start an interactive chatbot session immediately after generating outputs",
    )
    parser.add_argument(
        "--deterministic-chat",
        action="store_true",
        help="When launching the chatbot from this script, disable the default OpenAI chat mode and use deterministic responses only",
    )
    return parser.parse_args()


def start_chatbot_session(bot: KPIChatbot, *, use_openai: bool, model: str) -> None:
    # ================================
    # Function: start_chatbot_session
    # Purpose: Runs an interactive chatbot session after the KPI batch output.
    # Inputs:
    #   - bot (KPIChatbot): initialized chatbot using the just-generated payload
    #   - use_openai (bool): whether to use the OpenAI response path by default
    #   - model (str): model name for the OpenAI response path
    # Output:
    #   - None
    # ================================
    chat_mode = f"LLM enabled ({model})" if use_openai else "Deterministic only (LLM disabled)"
    print(f"\nChatbot session started. Mode: {chat_mode}. Type 'exit' to quit. Type 'help' for example questions.\n")
    while True:
        question = input("You: ").strip()
        if question.lower() in {"exit", "quit"}:
            print("Assistant: Goodbye.")
            break
        answer = bot.answer_auto(question, use_openai=use_openai, model=model)
        print(f"Assistant: {answer}\n")


def main() -> None:
    # ================================
    # Function: main
    # Purpose: Orchestrates the end-to-end KPI pipeline from ingestion to output.
    # Inputs:
    #   - None (reads CLI arguments)
    # Output:
    #   - None
    # Important Logic:
    #   - Loads and validates datasets
    #   - Computes KPI sections and summary tables
    #   - Optionally calls OpenAI for narrative refinement only
    #   - Writes the packaged leadership outputs
    # ================================
    args = parse_args()
    paths = PathsConfig()
    paths.output_dir.mkdir(parents=True, exist_ok=True)

    datasets, row_counts, validation_warnings = load_all(paths.data_dir)

    if args.start and args.end:
        start = pd.to_datetime(args.start)
        end = pd.to_datetime(args.end)
        # Use a month label when the requested range stays within one calendar
        # month; otherwise show the explicit start/end dates.
        period_label = start.strftime("%B %Y") if start.to_period("M") == end.to_period("M") else f"{start.date()} to {end.date()}"
    else:
        # Default reporting logic selects the latest full month that exists
        # across all core operational datasets.
        start, end, period_label = derive_default_period(datasets)

    warehouses = [w.strip() for w in args.warehouses.split(",") if w.strip()] if args.warehouses else None
    sku_families = [s.strip() for s in args.sku_families.split(",") if s.strip()] if args.sku_families else None

    sections, kpi_table = compute_all_kpis(datasets, start, end, warehouses, sku_families)
    # Deterministic rules generate the first-pass narrative before any optional
    # LLM refinement is applied.
    sections = summarize_sections(sections)
    insights, risks, recommendations = build_insights_risks_and_recommendations(sections)

    use_llm_summary = not args.deterministic_summary
    llm_used = False

    if use_llm_summary:
        print(f"LLM summary mode: ENABLED | Model: {args.llm_model}")
    else:
        print("LLM summary mode: DISABLED | Using deterministic summary only")

    if use_llm_summary:
        try:
            # OpenAI only polishes wording. KPI values, statuses, and formulas
            # remain computed by deterministic code upstream.
            sections, insights, recommendations = generate_narrative_with_openai(
                sections=sections,
                model=args.llm_model,
            )
            llm_used = True
            print(f"LLM summary status: USED | Model: {args.llm_model}")
        except LLMPolishError as exc:
            print(f"LLM summary status: NOT USED | Falling back to deterministic summary | Reason: {exc}")

    payload = build_payload(
        sections=sections,
        kpi_table=kpi_table,
        insights=insights,
        risks=risks,
        recommendations=recommendations,
        row_counts=row_counts,
        validation_warnings=validation_warnings,
        start_date=start,
        end_date=end,
        period_label=period_label,
        warehouse_filter=warehouses,
        sku_family_filter=sku_families,
        llm_used=llm_used,
    )

    # Persist the same payload in multiple stakeholder-friendly formats.
    excel_path = write_excel(payload, paths.output_dir)
    html_path = None if args.no_html else write_html(payload, paths.output_dir)
    generated_files = ["leadership_summary.json", "leadership_summary_detailed.json", Path(excel_path).name, "run_summary.txt"]
    if html_path:
        generated_files.append(Path(html_path).name)
    payload["metadata"]["generated_files"] = generated_files
    json_path = write_json(payload, paths.output_dir)
    summary_path = write_run_summary(payload, paths.output_dir)

    print("Run complete")
    print(f"Reporting period: {payload['reporting_period']['label']}")
    print(f"Overall status: {payload['header']['overall_status'].upper()}")
    print(f"LLM used in this run: {'YES' if llm_used else 'NO'}")
    print(f"JSON: {json_path}")
    print(f"Excel: {excel_path}")
    if html_path:
        print(f"HTML: {html_path}")
    print(f"Run summary: {summary_path}")

    if args.start_chatbot:
        bot = KPIChatbot(
            datasets=datasets,
            start=start,
            end=end,
            payload=payload,
            warehouse_scope=warehouses,
        )
        start_chatbot_session(
            bot,
            use_openai=not args.deterministic_chat,
            model=args.llm_model,
        )


if __name__ == "__main__":
    main()






# The default period is the latest full month available across all core datasets
# Leadership summary should compare all domains over the same complete period
