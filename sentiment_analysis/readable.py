from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / "topic_exports"
OUTPUT_SUFFIX = "_readable.txt"


def parse_cli_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Create human-readable text summaries from topic export CSV files."
	)
	parser.add_argument(
		"--questions",
		help="Comma-separated question numbers to export (for example: 1,3 or Q1,Q3). Default: all available.",
	)
	return parser.parse_args()


def parse_question_selection(raw_value: str | None) -> set[int] | None:
	if not raw_value:
		return None

	selected_indices: set[int] = set()
	tokens = [token.strip() for token in raw_value.split(",") if token.strip()]
	if not tokens:
		raise ValueError("--questions was provided but no valid question identifiers were found.")

	for token in tokens:
		normalized = token.upper()
		if normalized.startswith("Q"):
			normalized = normalized[1:]
		if not normalized.isdigit():
			raise ValueError(
				f"Invalid question identifier '{token}'. Use values like 1,3 or Q1,Q3."
			)
		selected_indices.add(int(normalized))

	return selected_indices


def get_available_questions(export_dir: Path) -> list[int]:
	question_numbers: set[int] = set()
	for file_path in export_dir.glob("Q*_topic_assignments.csv"):
		match = re.match(r"Q(\d+)_topic_assignments\.csv", file_path.name)
		if match:
			question_numbers.add(int(match.group(1)))
	return sorted(question_numbers)


def split_topic_ids(topic_ids_text: str) -> list[int]:
	if not topic_ids_text:
		return []
	ids: list[int] = []
	for topic_id in topic_ids_text.split("|"):
		topic_id = topic_id.strip()
		if not topic_id:
			continue
		ids.append(int(topic_id))
	return ids


def parse_list_literal(value: object) -> list[str]:
	if not isinstance(value, str) or not value.strip():
		return []
	try:
		parsed = ast.literal_eval(value)
		if isinstance(parsed, list):
			return [str(item) for item in parsed if str(item).strip()]
	except (ValueError, SyntaxError):
		return []
	return []


def collect_responses_for_topic(assignments: pd.DataFrame, topic_id: int) -> list[str]:
	responses: list[str] = []
	if topic_id == -1:
		for row in assignments.itertuples(index=False):
			multi_ids = split_topic_ids(getattr(row, "multi_topic_ids") or "")
			if not multi_ids:
				responses.append(str(getattr(row, "response")))
		return responses

	for row in assignments.itertuples(index=False):
		multi_ids = split_topic_ids(getattr(row, "multi_topic_ids") or "")
		if topic_id in multi_ids:
			responses.append(str(getattr(row, "response")))
	return responses


def render_question_report(question_number: int, export_dir: Path) -> Path:
	assignments_path = export_dir / f"Q{question_number}_topic_assignments.csv"
	summary_path = export_dir / f"Q{question_number}_topic_summary.csv"
	thresholded_summary_path = export_dir / f"Q{question_number}_topic_summary_thresholded.csv"
	output_path = export_dir / f"Q{question_number}{OUTPUT_SUFFIX}"

	if not assignments_path.exists() or not summary_path.exists() or not thresholded_summary_path.exists():
		raise FileNotFoundError(
			f"Missing one or more export files for Q{question_number}. Expected assignments, summary, and thresholded summary CSV files."
		)

	assignments = pd.read_csv(assignments_path).fillna("")
	summary = pd.read_csv(summary_path).fillna("")
	thresholded_summary = pd.read_csv(thresholded_summary_path).fillna("")

	question_text = ""
	if not assignments.empty:
		question_text = str(assignments.iloc[0].get("question_text", "")).strip()

	thresholded_lookup: dict[int, pd.Series] = {}
	for _, row in thresholded_summary.iterrows():
		try:
			topic_id = int(row["topic_id"])
		except (TypeError, ValueError):
			continue
		thresholded_lookup[topic_id] = row

	lines: list[str] = []
	lines.append(f"Q{question_number}: {question_text}")
	lines.append("")

	for _, summary_row in summary.iterrows():
		topic_value = summary_row.get("Topic", "")
		if str(topic_value) == "TOTAL":
			continue
		try:
			topic_id = int(topic_value)
		except (TypeError, ValueError):
			continue

		topic_name = str(summary_row.get("Name", "Unknown"))
		count = int(summary_row.get("Count", 0))
		responses = collect_responses_for_topic(assignments, topic_id)

		lines.append(f"Topic {topic_id}: {topic_name} was found in {count} responses")

		representation = parse_list_literal(summary_row.get("Representation", ""))
		if representation:
			lines.append(f"Representation: {', '.join(representation)}")

		representative_docs = parse_list_literal(summary_row.get("Representative_Docs", ""))
		if representative_docs:
			lines.append("Representative docs:")
			for index, doc in enumerate(representative_docs, start=1):
				lines.append(f"{index}. {doc}")

		thresholded_row = thresholded_lookup.get(topic_id)
		if thresholded_row is not None:
			thresholded_count = int(thresholded_row.get("thresholded_count", 0))
			thresholded_share = float(thresholded_row.get("thresholded_share", 0.0))
			lines.append(
				f"Thresholded count/share: {thresholded_count} ({thresholded_share:.2%})"
			)

		lines.append("Responses:")
		if responses:
			for index, response in enumerate(responses, start=1):
				lines.append(f"{index}. {response}")
		else:
			lines.append("None")
		lines.append("")

	total_rows = summary[summary["Topic"].astype(str) == "TOTAL"]
	if not total_rows.empty:
		total_count = int(total_rows.iloc[0].get("Count", 0))
		lines.append(f"Total responses analyzed: {total_count}")

	output_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
	return output_path


def main() -> None:
	args = parse_cli_args()
	selected_questions = parse_question_selection(args.questions)
	available_questions = get_available_questions(EXPORT_DIR)

	if not available_questions:
		raise ValueError(f"No question assignment exports found in {EXPORT_DIR}")

	if selected_questions is None:
		question_numbers = available_questions
	else:
		missing = sorted(set(selected_questions) - set(available_questions))
		if missing:
			raise ValueError(
				f"Requested questions do not have export files: {', '.join(f'Q{num}' for num in missing)}"
			)
		question_numbers = sorted(selected_questions)

	for question_number in question_numbers:
		output_path = render_question_report(question_number, EXPORT_DIR)
		print(f"Saved readable report: {output_path}")


if __name__ == "__main__":
	main()


