import random
import uuid
import time
import faker
import pickle
from data.country import country_names
from data.words import english_words
from data.paper_types import paper_types
from data.language import languages


fk_en = faker.Faker(locale="en-us")
fk_zh = faker.Faker(locale="zh-cn")


def do_dump(obj, file_path):
    """使用pickle，持久化"""
    with open(file_path, "wb") as f:
        pickle.dump(obj, f)


def do_load(file_path):
    """加载持久化的数据"""
    with open(file_path, "rb") as f:
        obj = pickle.load(f)
    return obj


def stat_consume_time(func):
    """统计花费时间装饰器"""
    def inner(*args, **kwargs):
        begin = time.time()
        res = func(*args, **kwargs)
        end = time.time()
        print("run func <%s> ...\n usingtime: %s s" %
              (str(func), (end-begin)))
        return res
    return inner


def gen_date() -> str:   
    return fk_en.date().replace("-", "")

def gen_year() -> str:
    return fk_en.year()


def gen_country() -> str:
    return random.choice(country_names)


def gen_sentence_en(length: int=10) -> str:
    st_words = random.choices(english_words, k=length)
    return " ".join(st_words)
    

def gen_paper_type() -> str:
    return random.choice(paper_types)


def gen_uuid() -> str:
    return uuid.uuid4().hex


def gen_language() -> str:
    return random.choice(languages)


def gen_doi() -> str:
    return "10.0121/" + str(time.time())


def gen_name_en() -> str:
    return fk_en.name()



def gen_org_en() -> str:
    return fk_en.company().replace(",", "-")


def gen_org_zh() -> str:
    return fk_zh.company().replace(",", "-")


def gen_title_en() -> int:
    return gen_sentence_en(random.randint(5, 20))


def gen_citation() -> int:
    return random.randint(0, 1000)


def gen_publications(start=0, end=1000) -> int:
    return random.randint(start, end)


def gen_latitude() -> str:
    return str(fk_en.latitude())


def gen_longitude() -> str:
    return str(fk_en.longitude())
