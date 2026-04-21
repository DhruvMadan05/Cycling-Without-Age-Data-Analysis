from __future__ import annotations

import argparse
from collections import Counter
import csv
from operator import itemgetter
from pathlib import Path
from typing import Iterable

import pandas as pd
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer


BASE_DIR = Path(__file__).resolve().parent
RESPONSES_PATH = BASE_DIR / "topic_inputs" / "aggregate_responses.csv"
SEED_TOPICS_PATH = BASE_DIR / "topic_inputs" / "seed_topics.csv"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
ZERO_SHOT_MIN_SIMILARITY = 0.4
MULTI_TOPIC_THRESHOLD = 0.15
EXPORT_DIR = BASE_DIR / "topic_exports"


def parse_cli_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Run BERTopic analysis on response columns with optional question filtering."
	)
	parser.add_argument(
		"--questions",
		help="Comma-separated question numbers to run (for example: 1,3,5 or Q1,Q3,Q5).",
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


def load_seed_topics(csv_path: Path, question_count: int) -> list[list[str]]:
	seed_topics_by_question: list[list[str]] = []

	with csv_path.open(newline="", encoding="utf-8") as file:
		for raw_line in file:
			line = raw_line.strip()
			if not line:
				seed_topics_by_question.append([])
				continue

			_, separator, seed_text = line.partition("|")
			if not separator:
				seed_topics_by_question.append([])
				continue

			seed_text = seed_text.strip()
			if not seed_text:
				seed_topics_by_question.append([])
				continue

			parsed_topics = next(csv.reader([seed_text]))
			seed_topics_by_question.append(
				[
					topic.strip().strip('"').strip("'")
					for topic in parsed_topics
					if topic.strip().strip('"').strip("'")
				]
			)

	if len(seed_topics_by_question) != question_count:
		raise ValueError(
			f"Expected {question_count} seed topic rows in {csv_path}, found {len(seed_topics_by_question)}"
		)

	return seed_topics_by_question


def build_topic_model(seed_topics: Iterable[str]) -> BERTopic:
	embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
	topic_kwargs = {
		"embedding_model": embedding_model,
		"min_topic_size": 5,
		"zeroshot_min_similarity": ZERO_SHOT_MIN_SIMILARITY,
		"verbose": False,
	}
	seed_topic_list = [topic for topic in seed_topics if topic]
	if seed_topic_list:
		topic_kwargs["zeroshot_topic_list"] = seed_topic_list

	return BERTopic(
		**topic_kwargs,
	)


def get_multi_topics_for_response(
	distribution_row,
	topic_ids,
	topic_names,
	threshold: float,
):
	selected = [
		(topic_id, float(score))
		for topic_id, score in zip(topic_ids, distribution_row)
		if float(score) >= threshold
	]

	selected.sort(key=lambda item: item[1], reverse=True)

	ids = "|".join(str(topic_id) for topic_id, _ in selected)
	names = "|".join(topic_names.get(topic_id, "Unknown") for topic_id, _ in selected)
	scores = "|".join(f"{score:.4f}" for _, score in selected)

	return ids, names, scores


def get_primary_topic_for_response(
	distribution_row,
	topic_ids,
	topic_names,
	threshold: float,
):
	selected = [
		(topic_id, float(score))
		for topic_id, score in zip(topic_ids, distribution_row)
		if float(score) >= threshold
	]

	if not selected:
		return -1, "Unknown", 0.0

	selected.sort(key=lambda item: item[1], reverse=True)
	primary_topic_id, primary_topic_score = selected[0]
	return primary_topic_id, topic_names.get(primary_topic_id, "Unknown"), primary_topic_score


def analyze_question(
	question_label: str,
	question_text: str,
	responses: list[str],
	seed_topics: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
	if not responses:
		empty_assignments = pd.DataFrame(
			columns=[
				"question_label",
				"question_text",
				"response",
				"raw_primary_topic",
				"raw_primary_topic_name",
				"primary_topic",
				"primary_topic_name",
				"primary_topic_score",
				"multi_topic_ids",
				"multi_topic_names",
				"multi_topic_scores",
			]
		)
		empty_summary = pd.DataFrame(columns=["Topic", "Count", "Name", "Representation", "Representative_Docs"])
		empty_thresholded_summary = pd.DataFrame(
			columns=["topic_id", "topic_name", "thresholded_count", "thresholded_share"]
		)
		return empty_assignments, empty_summary, empty_thresholded_summary

	topic_model = build_topic_model(seed_topics)
	raw_primary_topics, _ = topic_model.fit_transform(responses)
	topic_info = topic_model.get_topic_info()
	topic_names = topic_info.set_index("Topic")["Name"].to_dict()
	topic_distribution, _ = topic_model.approximate_distribution(responses)
	assignable_topic_ids = [topic_id for topic_id in topic_info["Topic"].tolist() if topic_id != -1]

	raw_primary_topic_names = [topic_names.get(topic, "Unknown") for topic in raw_primary_topics]
	primary_topics = []
	primary_topic_names = []
	primary_topic_scores = []
	multi_topic_ids = []
	multi_topic_names = []
	multi_topic_scores = []
	for row, raw_topic in zip(topic_distribution, raw_primary_topics):
		primary_topic_id, primary_topic_name, primary_topic_score = get_primary_topic_for_response(
			row,
			assignable_topic_ids,
			topic_names,
			MULTI_TOPIC_THRESHOLD,
		)
		if primary_topic_id == -1:
			primary_topic_id = raw_topic
			primary_topic_name = topic_names.get(raw_topic, "Unknown")
			primary_topic_score = 0.0
		ids, names, scores = get_multi_topics_for_response(
			row,
			assignable_topic_ids,
			topic_names,
			MULTI_TOPIC_THRESHOLD,
		)
		primary_topics.append(primary_topic_id)
		primary_topic_names.append(primary_topic_name)
		primary_topic_scores.append(primary_topic_score)
		multi_topic_ids.append(ids)
		multi_topic_names.append(names)
		multi_topic_scores.append(scores)

	results = pd.DataFrame(
		{
			"question_label": question_label,
			"question_text": question_text,
			"response": responses,
			"raw_primary_topic": raw_primary_topics,
			"raw_primary_topic_name": raw_primary_topic_names,
			"primary_topic": primary_topics,
			"primary_topic_name": primary_topic_names,
			"primary_topic_score": primary_topic_scores,
			"multi_topic_ids": multi_topic_ids,
			"multi_topic_names": multi_topic_names,
			"multi_topic_scores": multi_topic_scores,
		}
	)

	multi_topic_counter: Counter[int] = Counter()
	outlier_count = 0
	for topic_id_text in multi_topic_ids:
		if not topic_id_text:
			outlier_count += 1
			continue
		for topic_id in topic_id_text.split("|"):
			multi_topic_counter[int(topic_id)] += 1

	if outlier_count:
		multi_topic_counter[-1] += outlier_count

	topic_info_lookup = topic_info.set_index("Topic")
	topic_summary_rows = []
	for topic_id, count in multi_topic_counter.items():
		if topic_id not in topic_info_lookup.index and topic_id != -1:
			continue
		if count <= 0:
			continue
		topic_row = topic_info_lookup.loc[topic_id] if topic_id in topic_info_lookup.index else None
		topic_summary_rows.append(
			{
				"Topic": topic_id,
				"Count": int(count),
				"Name": topic_row["Name"] if topic_row is not None else "Unknown",
				"Representation": topic_row["Representation"] if topic_row is not None else None,
				"Representative_Docs": topic_row["Representative_Docs"] if topic_row is not None else None,
			}
		)

	topic_summary = pd.DataFrame(sorted(topic_summary_rows, key=itemgetter("Topic")))
	total_row = pd.DataFrame(
		[
			{
				"Topic": "TOTAL",
				"Count": len(responses),
				"Name": "Total Responses Analyzed",
				"Representation": None,
				"Representative_Docs": None,
			}
		]
	)
	topic_summary = pd.concat([topic_summary, total_row], ignore_index=True)

	thresholded_summary_rows = []
	for topic_id, count in multi_topic_counter.items():
		if topic_id == -1:
			continue
		topic_row = topic_info_lookup.loc[topic_id]
		thresholded_summary_rows.append(
			{
				"topic_id": topic_id,
				"topic_name": topic_row["Name"],
				"representation": topic_row["Representation"],
				"representative_docs": topic_row["Representative_Docs"],
				"thresholded_count": count,
				"thresholded_share": count / len(responses),
			}
		)

	if thresholded_summary_rows:
		thresholded_summary = pd.DataFrame(sorted(thresholded_summary_rows, key=itemgetter("topic_id")))
	else:
		thresholded_summary = pd.DataFrame(
			columns=[
				"topic_id",
				"topic_name",
				"representation",
				"representative_docs",
				"thresholded_count",
				"thresholded_share",
			]
		)

	return results, topic_summary, thresholded_summary


def main() -> None:
	args = parse_cli_args()
	EXPORT_DIR.mkdir(exist_ok=True)
	question_pairs = load_question_responses(RESPONSES_PATH)
	seed_topics_by_question = load_seed_topics(SEED_TOPICS_PATH, len(question_pairs))
	selected_questions = parse_question_selection(args.questions, len(question_pairs))
	if selected_questions:
		selected_labels = ", ".join(f"Q{index}" for index in sorted(selected_questions))
		print(f"Running selected questions: {selected_labels}")
	else:
		print(f"Running all questions: Q1-Q{len(question_pairs)}")
	print()

	for index, ((question_text, responses), seed_topics) in enumerate(
		zip(question_pairs, seed_topics_by_question),
		start=1,
	):
		if selected_questions and index not in selected_questions:
			continue

		question_label = f"Q{index}"
		assignments_path = EXPORT_DIR / f"{question_label}_topic_assignments.csv"
		summary_path = EXPORT_DIR / f"{question_label}_topic_summary.csv"
		thresholded_summary_path = EXPORT_DIR / f"{question_label}_topic_summary_thresholded.csv"

		if not responses:
			print(f"{question_label}: no responses found, skipping")
			continue

		results, topic_info, thresholded_summary = analyze_question(
			question_label,
			question_text,
			responses,
			seed_topics,
		)

		results.to_csv(assignments_path, index=False)
		topic_info.to_csv(summary_path, index=False)
		thresholded_summary.to_csv(thresholded_summary_path, index=False)

		print(f"{question_label}: {question_text}")
		print(f"Seed topics: {', '.join(seed_topics) if seed_topics else 'none'}")
		print(f"Saved assignments to: {assignments_path}")
		print(f"Saved summary to: {summary_path}")
		print(f"Saved thresholded summary to: {thresholded_summary_path}")
		print(f"Multi-topic threshold: {MULTI_TOPIC_THRESHOLD}")
		print()
		print(topic_info.to_string(index=False))
		print()
		print("Response assignments:")
		for row in results.itertuples(index=False):
			print(f"[{row.primary_topic}] {row.response}")
			print(f"  -> {row.multi_topic_names} ({row.multi_topic_scores})")
		print()


if __name__ == "__main__":
	main()
