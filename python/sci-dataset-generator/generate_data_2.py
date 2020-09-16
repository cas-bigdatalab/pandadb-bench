# 生成节点数据

from multiprocessing import Process
from threading import Thread
import sys
import random
import time
import util
import config


@util.stat_consume_time
def gen_data_paper_nodes(count=0, save_path=None, delimiter=',') -> "List[str]":
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["paperId:ID(paper)","titleEn","doi","type","language","country","publishDate","citation:INT"]
    p_id_list = []
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        k = 0
        for i in range(count):
            p_id = str(i) #util.gen_uuid()
            # p_id_list.append(p_id)
            p_title_en = util.gen_title_en()
            p_doi = util.gen_doi()
            p_type = util.gen_paper_type()
            p_lang = util.gen_language()
            p_country = util.gen_country()
            p_pub_date = util.gen_date()
            p_citation = str(util.gen_citation())
            f.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (p_id, p_title_en, p_doi, p_type, p_lang,
                                                p_country, p_pub_date, p_citation))
            k = k + 1
            if k == 10000:
                f.flush()
    return p_id_list


@util.stat_consume_time
def gen_data_person_nodes(count=0, save_path=None, delimiter=',') -> "List[str]":
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["personId:ID(person)", "nameEn", "nationality", "org",
              "publications:INT", "citations:INT", "publications5:INT", "citations5:INT"]
    p_id_list = []
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        k = 0
        for i in range(count):
            p_id = str(i) #util.gen_uuid()
            # p_id_list.append(p_id)
            p_name_en = util.gen_name_en()
            p_nat = util.gen_country()
            p_org = util.gen_title_en() #util.gen_org_en()
            # p_publications = str(util.gen_publications())
            # p_citations = str(util.gen_citation())
            # p_publications5 = str(util.gen_publications())
            # p_citations5 = str(util.gen_citation())
            tmp = util.gen_publications()
            p_publications = str(tmp)
            p_citations = str(tmp + 5)
            p_publications5 = str(tmp + 21)
            p_citations5 = str(tmp + 32)

            f.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (p_id, p_name_en, p_nat, p_org, p_publications,
                                                   p_citations, p_publications5, p_citations5))
            k = k + 1
            if k == 10000:
                f.flush()
    return p_id_list

@util.stat_consume_time
def gen_data_org_nodes(count=0, save_path=None, delimiter=',') -> "List[str]":
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["orgId:ID(organization)", "nameEn", "nameCn", "longitude:DOUBLE", "latitude:DOUBLE",
        "country","publications:INT","citations:INT","publications5:INT","citations5:INT"]
    org_id_list = []
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        k = 0
        for i in range(count):
            org_id = str(i) #util.gen_uuid()
            # org_id_list.append(org_id)
            org_name_en = util.gen_org_en()
            org_name_cn = util.gen_org_zh()
            org_lon = util.gen_longitude()
            org_lat = util.gen_latitude()
            org_country = util.gen_country()
            org_publications = str(util.gen_publications())
            org_citations = str(util.gen_citation())
            org_publications5 = str(util.gen_publications())
            org_citations5 = str(util.gen_citation())

            f.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (org_id, org_name_en, org_name_cn, org_lon, org_lat,
                                org_country, org_publications, org_citations, org_publications5,org_citations5))
            k = k + 1
            if k == 10000:
                f.flush()
    return org_id_list

@util.stat_consume_time
def gen_data_topic_nodes(count=0, save_path=None, delimiter=',') -> "List[str]":
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["topicId:ID(topic)", "name", "rank:INT"]
    topic_id_list = []
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        t_rank_max = count//100 if count >100 else 10
        k = 0
        for i in range(count):
            t_id = str(i)
            # topic_id_list.append(t_id)
            t_name = util.gen_sentence_en(3)
            t_rank = random.randint(1, t_rank_max)

            f.write("%s,%s,%s\n" % (t_id, t_name, t_rank))
            k = k + 1
            if k == 10000:
                f.flush()
    return topic_id_list

@util.stat_consume_time
def gen_data_citations(save_path=None, delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["desc:ID(citations)"]
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        else:        
            f.write("citations")

@util.stat_consume_time
def gen_data_publications(save_path=None, delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["desc:ID(publications)"]
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        else:
            f.write("publications")
@util.stat_consume_time
def gen_rel_be_cited(count=0, save_path=None, paper_count=0, paper_ids=[], delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["paper_id:END_ID(paper)","cite_id:START_ID(paper)"]
    topic_id_list = []
    k = 0
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        for i in range(count):            
            # paper_id, cite_id = random.choices(paper_ids, k=2)
            paper_id = random.randint(0, paper_count)
            cite_id = random.randint(0, paper_count)
            f.write("%s,%s\n" % (paper_id, cite_id))
            k = k + 1
            if k == 10000:
                f.flush()
    return count

@util.stat_consume_time
def gen_rel_paper_belong_topic(count=0, save_path=None, paper_count=0, paper_ids=[], topic_count=0,topic_ids=[], delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["paper_id:START_ID(paper)", "tag_id:END_ID(topic)"]
    k = 0
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        for i in range(count):
            # paper_id = random.choice(paper_ids)
            # tag_id = random.choice(topic_ids)
            paper_id = random.randint(0, paper_count)
            tag_id = random.randint(0, topic_count)
            f.write("%s,%s\n" % (paper_id, tag_id))
            k = k + 1
            if k == 10000:
                f.flush()
    return count

@util.stat_consume_time
def gen_rel_person_belong_topic(count=0, save_path=None, person_count=0, person_ids=[], topic_count=0, topic_ids=[], delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["person_id:START_ID(person)", "tag_id:END_ID(topic)"]
    k = 0
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        for i in range(count):
            # person_id = random.choice(person_ids)
            # tag_id = random.choice(topic_ids)
            person_id = random.randint(0, person_count)
            tag_id = random.randint(0, topic_count)
            f.write("%s,%s\n" % (person_id, tag_id))
            k = k + 1
            if k == 10000:
                f.flush()
    return count

@util.stat_consume_time
def gen_rel_person_citation(count=0, save_path=None, person_count=0, person_ids=[], citations=["citations"], delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["pesonId:START_ID(person)", "year", "value:INT", "desc:END_ID(citations)"]
    k = 0
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        for i in range(count):
            # person_id = random.choice(person_ids)
            person_id = random.randint(0, person_count)
            year = util.gen_year()
            value = random.randint(1, 1000)
            desc = citations[0]
            f.write("%s,%s,%s,%s\n" % (person_id, year, value, desc))
            k = k + 1
            if k == 10000:
                f.flush()
    return count

@util.stat_consume_time
def gen_rel_person_publication(count=0, save_path=None, person_count=0, person_ids=[],
                                publications=["publications"], delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["personId:START_ID(person)", "year", "value:INT", "desc:END_ID(publications)"]
    k = 0
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        for i in range(count):
            person_id = random.randint(0, person_count)
            # person_id = random.choice(person_ids)
            year = util.gen_year()
            value = random.randint(1, 1000)
            desc = publications[0]
            f.write("%s,%s,%s,%s\n" % (person_id, year, value, desc))
            k = k + 1
            if k == 10000:
                f.flush()
    return count

@util.stat_consume_time
def gen_rel_paper_related_to_paper(count=0, save_path=None, paper_count=0, paper_ids=[], delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["paper_id:START_ID(paper)", "rela_id:END_ID(paper)"]
    k = 0
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        for i in range(count):
            # paper_id, rela_id = random.choices(paper_ids, k=2)
            paper_id = random.randint(0, paper_count)
            rela_id = random.randint(0, paper_count)
            f.write("%s,%s\n" % (paper_id, rela_id))
            k = k + 1
            if k == 10000:
                f.flush()
    return count

@util.stat_consume_time
def gen_rel_topic_belong_topic(count=0, save_path=None, topic_ids=[], topic_count=0, delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["tagId:START_ID(topic)", "parentId:END_ID(topic)"]
    k = 0
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        for i in range(count):
            tag_id = random.randint(0, topic_count)
            parent_id = random.randint(0, topic_count)
            # tag_id, parent_id = random.choices(topic_ids, k=2)
            f.write("%s,%s\n" % (tag_id, parent_id))
            k = k + 1
            if k == 10000:
                f.flush()
    return count

@util.stat_consume_time
def gen_rel_work_for(count=0, save_path=None, person_count=0, org_count=0, person_ids=[], org_ids=[], delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["personId:START_ID(person)", "orgId:END_ID(organization)"]
    k = 0
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        for i in range(count):
            # person_id = random.choice(person_ids)
            # org_id = random.choice(org_ids)
            person_id = random.randint(0, person_count)
            org_id = random.randint(0, org_count)
            f.write("%s,%s\n" % (person_id, org_id))
            k = k + 1
            if k == 10000:
                f.flush()
    return count

@util.stat_consume_time
def gen_rel_write_paper(count=0, save_path=None, person_count=0, paper_count=0, person_ids=[], paper_ids=[], delimiter=','):
    if save_path is None:
        raise Exception("save_path is None")
    heads = ["paper_id:END_ID(paper)", "person_id:START_ID(person)", "isFirstauthor:INT"]
    k = 0
    with open(save_path, "w", encoding="utf-8") as f:
        if config.write_head:
            f.write(",".join(heads))
            f.write("\n")
        for i in range(count):
            # paper_id = random.choice(paper_ids)
            # person_id = random.choice(person_ids)
            paper_id = random.randint(0, paper_count)
            person_id = random.randint(0, person_count)
            is_fa = random.randint(0, 1)
            f.write("%s,%s,%s\n" % (paper_id, person_id, str(is_fa)))
            k = k + 1
            if k == 10000:
                f.flush()
    return count


def main1():
    begin = time.time()
    paper_ids = gen_data_paper_nodes(
        count=config.paper_count, save_path=config.paper_csv_path)
    util.do_dump(paper_ids, config.paper_ids_path)
    person_ids = gen_data_person_nodes(
        count=config.person_count, save_path=config.person_csv_path)
    util.do_dump(person_ids, config.person_ids_path)
    org_ids = gen_data_org_nodes(
        count=config.org_count, save_path=config.org_csv_path)
    util.do_dump(org_ids, config.org_ids_path)
    topic_ids = gen_data_topic_nodes(
        count=config.topic_count, save_path=config.topic_csv_path)
    util.do_dump(topic_ids, config.topic_ids_path)
    gen_data_citations(save_path=config.citations_csv_path)
    gen_data_publications(save_path=config.publications_csv_path)

    gen_rel_be_cited(count=config.be_cited_count,
                     save_path=config.be_cited_csv_path, paper_count=config.paper_count)
    gen_rel_paper_belong_topic(
        count=config.paper_belong_topic_count, save_path=config.paper_belong_topic_csv_path,
        paper_count=config.paper_count, topic_count=config.topic_count)
    gen_rel_person_belong_topic(
        count=config.person_belong_topic_count, save_path=config.person_belong_topic_csv_path,
        person_count=config.person_count, topic_count=config.topic_count)
    gen_rel_person_citation(count=config.person_citation_count,
                            save_path=config.person_citation_csv_path,
                            person_count=config.person_count)
    gen_rel_person_publication(count=config.person_publication_count,
                               save_path=config.person_publication_csv_path,
                               person_count=config.person_count)
    gen_rel_paper_related_to_paper(
        count=config.related_to_related_to_count,
        save_path=config.related_to_related_to_csv_path,
        paper_count=config.paper_count)
    gen_rel_paper_related_to_paper(
        count=config.paper_reference_related_to_count,
        save_path=config.paper_reference_related_to_csv_path,
        paper_count=config.paper_count)
    gen_rel_topic_belong_topic(
        count=config.topic_belong_topic_count, save_path=config.topic_belong_topic_csv_path,
        topic_count=config.topic_count)
    gen_rel_work_for(
        count=config.work_for_count, save_path=config.work_for_csv_path,
        person_count=config.person_count, org_count=config.org_count)
    gen_rel_write_paper(count=config.write_paper_count, save_path=config.write_paper_csv_path,
                        paper_count=config.paper_count, person_count=config.person_count)

    end = time.time()
    print("Time used: "+str(end-begin))


def main2():
    begin = time.time()
    t1 = Thread(target=gen_data_paper_nodes, 
        kwargs={"count":config.paper_count, "save_path":config.paper_csv_path})

    t2 = Thread(target=gen_data_person_nodes,
                          kwargs={"count": config.person_count, "save_path": config.person_csv_path})
    
    t3 = Thread(target=gen_data_org_nodes,
                          kwargs={"count": config.org_count, "save_path": config.org_csv_path})

    t4 = Thread(target=gen_data_topic_nodes,
                          kwargs={"count": config.topic_count, "save_path": config.topic_csv_path})

    gen_data_citations(save_path=config.citations_csv_path)
    gen_data_publications(save_path=config.publications_csv_path)

    t5 = Thread(target=gen_rel_be_cited,
                          kwargs={"count": config.be_cited_count, 
                          "save_path": config.be_cited_csv_path, 
                                  "paper_count": config.paper_count})

    t6 = Thread(target=gen_rel_paper_belong_topic,
                          kwargs={"count":config.paper_belong_topic_count, 
                                  "save_path":config.paper_belong_topic_csv_path,
                                  "paper_count":config.paper_count, 
                                  "topic_count":config.topic_count})

    t7 = Thread(target=gen_rel_person_belong_topic,
                          kwargs={"count":config.person_belong_topic_count, 
                                  "save_path":config.person_belong_topic_csv_path,
                                  "person_count":config.person_count, 
                                  "topic_count":config.topic_count})

    t8 = Thread(target=gen_rel_person_citation,
                          kwargs={"count":config.person_citation_count,
                                  "save_path":config.person_citation_csv_path,
                                  "person_count":config.person_count})

    t9 = Thread(target=gen_rel_person_publication,
                          kwargs={"count":config.person_publication_count,
                                  "save_path":config.person_publication_csv_path,
                                  "person_count":config.person_count})

    t10 = Thread(target=gen_rel_paper_related_to_paper,
                           kwargs={"count":config.related_to_related_to_count,
                                   "save_path":config.related_to_related_to_csv_path,
                                   "paper_count":config.paper_count})

    t11 = Thread(target=gen_rel_paper_related_to_paper,
                           kwargs={"count": config.paper_reference_related_to_count,
                                   "save_path": config.paper_reference_related_to_csv_path,
                                   "paper_count": config.paper_count})

    t12 = Thread(target=gen_rel_topic_belong_topic,
                           kwargs={"count": config.topic_belong_topic_count,
                                   "save_path": config.topic_belong_topic_csv_path,
                                   "topic_count":config.topic_count})
    
    t13 = Thread(target=gen_rel_work_for,
                           kwargs={"count": config.work_for_count,
                                   "save_path": config.work_for_csv_path,
                                   "person_count":config.person_count, 
                                   "org_count":config.org_count})

    t14 = Thread(target=gen_rel_write_paper,
                           kwargs={"count": config.write_paper_count,
                                   "save_path": config.write_paper_csv_path,
                                   "paper_count": config.paper_count, 
                                   "person_count": config.person_count})
    
    t1.start()
    t1.join()
    t2.start()
    t2.join()
    t3.start()
    t3.join()
    t4.start()
    t4.join()
    t5.start()
    t5.join()
    t6.start()
    t6.join()
    t7.start()
    t7.join()
    t8.start()
    t8.join()
    t9.start()
    t9.join()
    t10.start()
    t10.join()
    t11.start()
    t11.join()
    t12.start()
    t12.join()
    t13.start()
    t13.join()
    t14.start()
    t14.join()

    end = time.time()
    print("Time used: "+str(end-begin))


def main3():
    begin = time.time()
    t1 = Process(target=gen_data_paper_nodes,
                 kwargs={"count": config.paper_count, "save_path": config.paper_csv_path})

    t2 = Process(target=gen_data_person_nodes,
                 kwargs={"count": config.person_count, "save_path": config.person_csv_path})

    t3 = Process(target=gen_data_org_nodes,
                 kwargs={"count": config.org_count, "save_path": config.org_csv_path})

    t4 = Process(target=gen_data_topic_nodes,
                 kwargs={"count": config.topic_count, "save_path": config.topic_csv_path})

    gen_data_citations(save_path=config.citations_csv_path)
    gen_data_publications(save_path=config.publications_csv_path)

    t5 = Process(target=gen_rel_be_cited,
                 kwargs={"count": config.be_cited_count,
                         "save_path": config.be_cited_csv_path,
                         "paper_count": config.paper_count})

    t6 = Process(target=gen_rel_paper_belong_topic,
                 kwargs={"count": config.paper_belong_topic_count,
                         "save_path": config.paper_belong_topic_csv_path,
                         "paper_count": config.paper_count,
                         "topic_count": config.topic_count})

    t7 = Process(target=gen_rel_person_belong_topic,
                 kwargs={"count": config.person_belong_topic_count,
                         "save_path": config.person_belong_topic_csv_path,
                         "person_count": config.person_count,
                         "topic_count": config.topic_count})

    t8 = Process(target=gen_rel_person_citation,
                 kwargs={"count": config.person_citation_count,
                         "save_path": config.person_citation_csv_path,
                         "person_count": config.person_count})

    t9 = Process(target=gen_rel_person_publication,
                 kwargs={"count": config.person_publication_count,
                         "save_path": config.person_publication_csv_path,
                         "person_count": config.person_count})

    t10 = Process(target=gen_rel_paper_related_to_paper,
                  kwargs={"count": config.related_to_related_to_count,
                          "save_path": config.related_to_related_to_csv_path,
                          "paper_count": config.paper_count})

    t11 = Process(target=gen_rel_paper_related_to_paper,
                  kwargs={"count": config.paper_reference_related_to_count,
                          "save_path": config.paper_reference_related_to_csv_path,
                          "paper_count": config.paper_count})

    t12 = Process(target=gen_rel_topic_belong_topic,
                  kwargs={"count": config.topic_belong_topic_count,
                          "save_path": config.topic_belong_topic_csv_path,
                          "topic_count": config.topic_count})

    t13 = Process(target=gen_rel_work_for,
                  kwargs={"count": config.work_for_count,
                          "save_path": config.work_for_csv_path,
                          "person_count": config.person_count,
                          "org_count": config.org_count})

    t14 = Process(target=gen_rel_write_paper,
                  kwargs={"count": config.write_paper_count,
                          "save_path": config.write_paper_csv_path,
                          "paper_count": config.paper_count,
                          "person_count": config.person_count})

    #t1.start()
    #t1.join()
    #t2.start()
    #t2.join()
    #t3.start()
    #t3.join()
    #t4.start()
    #t4.join()
    #t5.start()
    #t5.join()
    t6.start()
    t6.join()
    t7.start()
    t7.join()
    t8.start()
    t8.join()
    t9.start()
    t9.join()
    t10.start()
    t10.join()
    t11.start()
    t11.join()
    t12.start()
    t12.join()
    t13.start()
    t13.join()
    t14.start()
    t14.join()

    end = time.time()
    print("Time used: "+str(end-begin))

if __name__ == "__main__":
    main3()
