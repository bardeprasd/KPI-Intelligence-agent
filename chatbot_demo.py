"""
Interactive demo entrypoint for the KPI chatbot.

This file sits in the UI/demo layer of the project. It loads the computed KPI
payload through `KPIChatbot`, supports a scripted demo or live Q&A, and can
optionally use OpenAI for grounded natural-language responses.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from agent.chatbot import KPIChatbot

# Demo questions used to showcase follow-up memory and warehouse-specific Q&A.
DEMO_QUESTIONS = [
    "What is the fill rate?",
    "What are the top risks?",
    "Show the inventory section.",
    "Which warehouse has the highest error rate?",
    "Compare WH-01 and WH-02 on SLA.",
    "How about WH-03?",
    "What are the recommendations?",
]


load_dotenv()


def parse_args() -> argparse.Namespace:
    # ================================
    # Function: parse_args
    # Purpose: Defines CLI options for the chatbot demo entrypoint.
    # Inputs:
    #   - None
    # Output:
    #   - argparse.Namespace containing user-supplied runtime options
    # ================================
    parser = argparse.ArgumentParser(description="Run the KPI chatbot demo for KPI data.")
    parser.add_argument("--start", type=str, help="Reporting start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="Reporting end date (YYYY-MM-DD)")
    parser.add_argument("--warehouses", type=str, help="Optional comma-separated warehouse scope, e.g. WH-01,WH-02")
    parser.add_argument("--sku-families", type=str, help="Optional comma-separated SKU families, e.g. Engine,Electrical")
    parser.add_argument("--query", type=str, help="Ask one question and exit")
    parser.add_argument("--demo-script", action="store_true", help="Run a canned transcript that demonstrates contextual memory")
    parser.add_argument("--save-transcript", type=str, help="Optional markdown file path to save the scripted or interactive transcript")
    parser.add_argument(
        "--deterministic-chat",
        action="store_true",
        help="Disable the default OpenAI chatbot mode and use deterministic responses only",
    )
    parser.add_argument(
        "--deterministic-summary",
        action="store_true",
        help="Disable the default OpenAI narrative refinement used to build the chatbot payload",
    )
    parser.add_argument(
        "--use-openai",
        action="store_true",
        help="Deprecated compatibility flag. OpenAI chatbot mode is now enabled by default unless --deterministic-chat is passed.",
    )
    parser.add_argument(
        "--use-llm-summary",
        action="store_true",
        help="Deprecated compatibility flag. OpenAI narrative refinement is now enabled by default unless --deterministic-summary is passed.",
    )
    parser.add_argument("--model", type=str, default="gpt-4.1-mini", help="OpenAI model to use for chatbot responses")
    return parser.parse_args()


def main() -> None:
    # ================================
    # Function: main
    # Purpose: Runs the chatbot demo in one-shot, scripted, or interactive mode.
    # Inputs:
    #   - None (reads CLI arguments)
    # Output:
    #   - None
    # Important Logic:
    #   - Builds warehouse and SKU scope from CLI filters
    #   - Initializes the chatbot from project data and KPI outputs
    #   - Optionally saves the final transcript to markdown
    # ================================
    args = parse_args()
    # Normalize comma-separated CLI filters into clean lists for downstream use.
    warehouses = [w.strip() for w in args.warehouses.split(",") if w.strip()] if args.warehouses else None
    sku_families = [s.strip() for s in args.sku_families.split(",") if s.strip()] if args.sku_families else None
    use_openai = not args.deterministic_chat
    use_llm_summary = not args.deterministic_summary
    bot = KPIChatbot.from_project(
        start=args.start,
        end=args.end,
        warehouses=warehouses,
        sku_families=sku_families,
        use_llm_summary=use_llm_summary,
        llm_model=args.model,
    )

    transcript_lines = [
        "# KPI Chatbot Transcript",
        "",
        f"Reporting period: {bot.payload['reporting_period']['label']}",
        f"Warehouse scope: {', '.join(warehouses) if warehouses else 'All supported warehouses'}",
        f"SKU family scope: {', '.join(sku_families) if sku_families else 'All supported families'}",
        f"OpenAI mode: {'enabled' if use_openai else 'disabled'} ({args.model})",
        "",
    ]

    def ask(q: str) -> str:
        # Route every question through the same wrapper so transcript modes
        # consistently respect the OpenAI toggle and selected model.
        return bot.answer_auto(q, use_openai=use_openai, model=args.model)

    if args.query:
        answer = ask(args.query)
        print(f"User: {args.query}\nAssistant: {answer}")
        transcript_lines.extend([f"**User**: {args.query}", "", f"**Assistant**: {answer}", ""])
    elif args.demo_script:
        for question in DEMO_QUESTIONS:
            answer = ask(question)
            print(f"User: {question}")
            print(f"Assistant: {answer}\n")
            transcript_lines.extend([f"**User**: {question}", "", f"**Assistant**: {answer}", ""])
    else:
        print("KPI chatbot demo started. Type 'exit' to quit. Type 'help' for example questions.\n")
        while True:
            question = input("You: ").strip()
            if question.lower() in {"exit", "quit"}:
                print("Assistant: Goodbye.")
                break
            answer = ask(question)
            print(f"Assistant: {answer}\n")
            transcript_lines.extend([f"**User**: {question}", "", f"**Assistant**: {answer}", ""])

    if args.save_transcript:
        path = Path(args.save_transcript)
        # Create the parent directory on demand so transcript export works even
        # when the caller points to a new folder.
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(transcript_lines), encoding="utf-8")
        print(f"Transcript saved to: {path}")


if __name__ == "__main__":
    main()
