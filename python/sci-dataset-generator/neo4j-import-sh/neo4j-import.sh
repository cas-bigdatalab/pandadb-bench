nohup \
./bin/neo4j-admin import --id-type=STRING \
--delimiter=, \
--ignore-extra-columns=true \
--ignore-duplicate-nodes=true \
--ignore-missing-nodes=true \
--database=graph.db \
--nodes:paper="/root/pandadb/generate-graph-data/output/header/data_paper.csv,/root/pandadb/generate-graph-data/output/data_paper.csv"  \
--nodes:person="/root/pandadb/generate-graph-data/output/header/data_person.csv,/root/pandadb/generate-graph-data/output/data_person.csv" \
--nodes:topic="/root/pandadb/generate-graph-data/output/header/data_topic.csv,/root/pandadb/generate-graph-data/output/data_topic.csv" \
--nodes:organization="/root/pandadb/generate-graph-data/output/header/data_org.csv,/root/pandadb/generate-graph-data/output/data_org.csv" \
--nodes:citations="/root/pandadb/generate-graph-data/output/header/data_citations.csv,/root/pandadb/generate-graph-data/output/data_citations.csv" \
--nodes:publications="/root/pandadb/generate-graph-data/output/header/data_publications.csv,/root/pandadb/generate-graph-data/output/data_publications.csv" \
--relationships:paper_reference="/root/pandadb/generate-graph-data/output/header/rel_paper_reference_related_to.csv,/root/pandadb/generate-graph-data/output/rel_paper_reference_related_to.csv" \
--relationships:related_to="/root/pandadb/generate-graph-data/output/header/rel_related_to_related_to.csv,/root/pandadb/generate-graph-data/output/rel_related_to_related_to.csv" \
--relationships:be_cited="/root/pandadb/generate-graph-data/output/header/rel_be_cited.csv,/root/pandadb/generate-graph-data/output/rel_be_cited.csv" \
--relationships:paper_belong_topic="/root/pandadb/generate-graph-data/output/header/rel_paper_belong_topic.csv,/root/pandadb/generate-graph-data/output/rel_paper_belong_topic.csv" \
--relationships:topic_belong_topic="/root/pandadb/generate-graph-data/output/header/rel_topic_belong_topic.csv,/root/pandadb/generate-graph-data/output/rel_topic_belong_topic.csv" \
--relationships:person_belong_topic="/root/pandadb/generate-graph-data/output/header/rel_person_belong_topic.csv,/root/pandadb/generate-graph-data/output/rel_person_belong_topic.csv" \
--relationships:person_citation="/root/pandadb/generate-graph-data/output/header/rel_person_citation.csv,/root/pandadb/generate-graph-data/output/rel_person_citation.csv" \
--relationships:person_publication="/root/pandadb/generate-graph-data/output/header/rel_person_publication.csv,/root/pandadb/generate-graph-data/output/rel_person_publication.csv" \
--relationships:work_for="/root/pandadb/generate-graph-data/output/header/rel_work_for.csv,/root/pandadb/generate-graph-data/output/rel_work_for.csv" \
--relationships:write_paper="/root/pandadb/generate-graph-data/output/header/rel_write_paper.csv,/root/pandadb/generate-graph-data/output/rel_write_paper.csv" 
> nohup.log 2>&1 &