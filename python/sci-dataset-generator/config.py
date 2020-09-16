import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

OUTPUT_DIR = os.path.join(BASE_DIR, "output")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

write_head = False

node_count = 100000000
rel_count = node_count * 17

paper_count = int(node_count * 0.65)
person_count = int(node_count * 0.33)
org_count = int(node_count * 0.0017)
topic_count = int(node_count * 0.0183)
be_cited_count = int(rel_count * 0.06)
paper_belong_topic_count = int(rel_count * 0.31)
person_belong_topic_count = int(rel_count * 0.1)
person_citation_count = int(rel_count * 0.26)
person_publication_count = int(rel_count * 0.15)
related_to_related_to_count = int(rel_count * 0.015)
paper_reference_related_to_count = int(rel_count * 0.017)

topic_belong_topic_count = int(rel_count * 0.003)
work_for_count = int(rel_count * 0.013)
write_paper_count = int(rel_count * 0.072)

paper_csv_path = os.path.join(OUTPUT_DIR, "data_paper.csv")
paper_ids_path = os.path.join(OUTPUT_DIR, "data_paper_ids.bin")

person_csv_path = os.path.join(OUTPUT_DIR, "data_person.csv")
person_ids_path = os.path.join(OUTPUT_DIR, "data_person_ids.bin")

org_csv_path = os.path.join(OUTPUT_DIR, "data_org.csv")
org_ids_path = os.path.join(OUTPUT_DIR, "data_org_ids.bin")

topic_csv_path = os.path.join(OUTPUT_DIR, "data_topic.csv")
topic_ids_path = os.path.join(OUTPUT_DIR, "data_topic_ids.bin")

citations_csv_path = os.path.join(OUTPUT_DIR, "data_citations.csv")
publications_csv_path = os.path.join(OUTPUT_DIR, "data_publications.csv")


be_cited_csv_path = os.path.join(OUTPUT_DIR, "rel_be_cited.csv")

paper_belong_topic_csv_path = os.path.join(
    OUTPUT_DIR, "rel_paper_belong_topic.csv")

person_belong_topic_csv_path = os.path.join(
    OUTPUT_DIR, "rel_person_belong_topic.csv")

person_citation_csv_path = os.path.join(
    OUTPUT_DIR, "rel_person_citation.csv")

person_publication_csv_path = os.path.join(
    OUTPUT_DIR, "rel_person_publication.csv")

related_to_related_to_csv_path = os.path.join(
    OUTPUT_DIR, "rel_related_to_related_to.csv")

paper_reference_related_to_csv_path = os.path.join(
    OUTPUT_DIR, "rel_paper_reference_related_to.csv")

topic_belong_topic_csv_path = os.path.join(
    OUTPUT_DIR, "rel_topic_belong_topic.csv")

work_for_csv_path = os.path.join(
    OUTPUT_DIR, "rel_work_for.csv")

write_paper_csv_path = os.path.join(
    OUTPUT_DIR, "rel_write_paper.csv")
