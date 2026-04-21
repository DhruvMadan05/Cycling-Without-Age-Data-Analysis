from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path

import pandas as pd
from transformers import pipeline


BASE_DIR = Path(__file__).resolve().parent
RESPONSES_PATH = BASE_DIR / "topic_inputs" / "aggregate_responses.csv"
EXPORT_DIR = BASE_DIR / "sentiment_exports"
COMBINED_SUMMARY_PATH = EXPORT_DIR / "all_questions_sentiment_summary.txt"
MODEL_NAME = "nlptown/bert-base-multilingual-uncased-sentiment"

LABEL_TO_NAME = {
	"1 star": "Strong Negative",
	"2 stars": "Negative",
	"3 stars": "Neutral",
	"4 stars": "Positive",
	"5 stars": "Strong Positive",
}

LABEL_TO_VALUE = {
	"1 star": 1,
	"2 stars": 2,
	"3 stars": 3,
	"4 stars": 4,
	"5 stars": 5,
}


def parse_cli_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Run sentiment analysis for each question column and export one file per question."
	)
	parser.add_argument(
		"--questions",
		help="Comma-separated question numbers to run (for example: 1,3 or Q1,Q3). Default: all.",
	)
	return parser.parse_args()


def parse_question_selection(raw_value: str | None, total_questions: int) -> set[int] | None:
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

		question_number = int(normalized)
		if question_number < 1 or question_number > total_questions:
			raise ValueError(
				f"Question '{token}' is out of range. Valid range is Q1 to Q{total_questions}."
			)
		selected_indices.add(question_number)

	return selected_indices


def load_question_responses(csv_path: Path) -> list[tuple[str, list[str]]]:
	with csv_path.open(newline="", encoding="utf-8") as file:
		reader = csv.reader(file)
		try:
			header_row = next(reader)
		except StopIteration as exc:
			raise ValueError(f"No data found in {csv_path}") from exc

		questions = [header.strip() or f"Q{index + 1}" for index, header in enumerate(header_row)]
		responses_by_question: list[list[str]] = [[] for _ in questions]

		for row in reader:
			for index, question_responses in enumerate(responses_by_question):
				cell = row[index].strip() if index < len(row) else ""
				if cell:
					question_responses.append(cell)

	return list(zip(questions, responses_by_question))


def analyze_sentiment(texts: list[str], classifier):
	return classifier(texts, truncation=True)


def interpret_label(label: str) -> str:
	return LABEL_TO_NAME.get(label, "Unknown")


def compute_average_label_name(average_score: float | None) -> str:
	if average_score is None:
		return "N/A"
	rounded_score = int(round(average_score))
	rounded_score = max(1, min(5, rounded_score))
	if rounded_score == 1:
		return "Strong Negative"
	if rounded_score == 2:
		return "Negative"
	if rounded_score == 3:
		return "Neutral"
	if rounded_score == 4:
		return "Positive"
	return "Strong Positive"


def build_question_export(
	question_label: str,
	question_text: str,
	responses: list[str],
	classifier,
) -> pd.DataFrame:
	if not responses:
		summary_row = {
			"record_type": "summary",
			"question_label": question_label,
			"question_text": question_text,
			"response_index": "",
			"response": "",
			"model_label": "",
			"confidence": "",
			"classification": "",
			"classification_value": "",
			"count_strong_negative": 0,
			"count_negative": 0,
			"count_neutral": 0,
			"count_positive": 0,
			"count_strong_positive": 0,
			"average_classification_value": "",
			"average_classification": "N/A",
		}
		return pd.DataFrame([summary_row])

	results = analyze_sentiment(responses, classifier)
	response_rows = []
	classification_counter: Counter[str] = Counter()
	classification_values: list[int] = []

	for index, (response, result) in enumerate(zip(responses, results), start=1):
		label = str(result["label"])
		confidence = float(result["score"])
		classification_name = interpret_label(label)
		classification_value = LABEL_TO_VALUE.get(label)

		classification_counter[classification_name] += 1
		if classification_value is not None:
			classification_values.append(classification_value)

		response_rows.append(
			{
				"record_type": "response",
				"question_label": question_label,
				"question_text": question_text,
				"response_index": index,
				"response": response,
				"model_label": label,
				"confidence": round(confidence, 6),
				"classification": classification_name,
				"classification_value": classification_value if classification_value is not None else "",
				"count_strong_negative": "",
				"count_negative": "",
				"count_neutral": "",
				"count_positive": "",
				"count_strong_positive": "",
				"average_classification_value": "",
				"average_classification": "",
			}
		)

	average_value: float | None = None
	if classification_values:
		average_value = sum(classification_values) / len(classification_values)

	summary_row = {
		"record_type": "summary",
		"question_label": question_label,
		"question_text": question_text,
		"response_index": "",
		"response": "",
		"model_label": "",
		"confidence": "",
		"classification": "",
		"classification_value": "",
		"count_strong_negative": classification_counter.get("Strong Negative", 0),
		"count_negative": classification_counter.get("Negative", 0),
		"count_neutral": classification_counter.get("Neutral", 0),
		"count_positive": classification_counter.get("Positive", 0),
		"count_strong_positive": classification_counter.get("Strong Positive", 0),
		"average_classification_value": round(average_value, 4) if average_value is not None else "",
		"average_classification": compute_average_label_name(average_value),
	}

	return pd.DataFrame(response_rows + [summary_row])


def extract_summary_metrics(export_df: pd.DataFrame) -> dict[str, object]:
	summary_df = export_df[export_df["record_type"] == "summary"]
	if summary_df.empty:
		raise ValueError("Could not find summary row in question export.")

	summary_row = summary_df.iloc[0]
	return {
		"question_label": str(summary_row["question_label"]),
		"question_text": str(summary_row["question_text"]),
		"count_strong_negative": int(summary_row["count_strong_negative"] or 0),
		"count_negative": int(summary_row["count_negative"] or 0),
		"count_neutral": int(summary_row["count_neutral"] or 0),
		"count_positive": int(summary_row["count_positive"] or 0),
		"count_strong_positive": int(summary_row["count_strong_positive"] or 0),
		"average_classification_value": summary_row["average_classification_value"],
		"average_classification": str(summary_row["average_classification"]),
	}


def write_combined_summary(summary_rows: list[dict[str, object]], output_path: Path) -> None:
	lines: list[str] = []
	lines.append("Sentiment Summary By Question")
	lines.append("=" * 30)
	lines.append("")

	for row in summary_rows:
		question_label = str(row["question_label"])
		question_text = str(row["question_text"])
		average_value = row["average_classification_value"]
		average_text = "N/A" if average_value == "" else str(average_value)

		lines.append(f"{question_label}: {question_text}")
		lines.append(f"Strong Negative: {row['count_strong_negative']}")
		lines.append(f"Negative: {row['count_negative']}")
		lines.append(f"Neutral: {row['count_neutral']}")
		lines.append(f"Positive: {row['count_positive']}")
		lines.append(f"Strong Positive: {row['count_strong_positive']}")
		lines.append(
			f"Average Classification: {row['average_classification']} (score: {average_text})"
		)
		lines.append("-" * 30)

	output_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> None:
	question_response_pairs = load_question_responses(RESPONSES_PATH)
	selected_questions = parse_question_selection(parse_cli_args().questions, len(question_response_pairs))

	if selected_questions is None:
		question_numbers = list(range(1, len(question_response_pairs) + 1))
	else:
		question_numbers = sorted(selected_questions)

	EXPORT_DIR.mkdir(parents=True, exist_ok=True)
	classifier = pipeline("sentiment-analysis", model=MODEL_NAME)  # type: ignore[call-overload, arg-type]
	combined_summary_rows: list[dict[str, object]] = []

	for question_number in question_numbers:
		question_text, responses = question_response_pairs[question_number - 1]
		question_label = f"Q{question_number}"
		export_df = build_question_export(question_label, question_text, responses, classifier)
		combined_summary_rows.append(extract_summary_metrics(export_df))
		output_path = EXPORT_DIR / f"{question_label}_sentiment.csv"
		export_df.to_csv(output_path, index=False)
		print(f"Saved sentiment export: {output_path}")

	write_combined_summary(combined_summary_rows, COMBINED_SUMMARY_PATH)
	print(f"Saved combined summary: {COMBINED_SUMMARY_PATH}")


if __name__ == "__main__":
	main()
