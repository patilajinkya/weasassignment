import weaviate
import os
import weaviate.classes as wvc
from weaviate.auth import AuthCredentials
from weaviate.collections import Collection
import pandas as pd
from weaviate.client import WeaviateClient
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure, Property, DataType, ReferenceProperty
from weaviate.classes.query import MetadataQuery, QueryReference
import warnings
from tqdm import tqdm
import subprocess
warnings.filterwarnings("ignore", category=DeprecationWarning)
movie1_name = []
movie2_name = []
movie_name =""
src_list_of_linkon = []
src_list_of_prop_name = {}
tgt_list_of_linkon = []
tgt_list_of_prop_name = {}
src_from_uuid=[]
tgt_to_uuid=[]
refs = {}
from_uuid = []
to_uuid = []
src_col = []
tgt_col = []
migr_ref_names = []
uuids = []
weave_url = os.environ["WEAV_URL"]
weave_api = os.environ["WEAV_API"]
coll_names = ["Movie1", "Movie2"]
refnames = ["hasactor", "hasrating"]
link_on_props = ["hasactor", "hasrating"]
tgt_collection = ["Movie2", "Movie1"]
# coll_names = ["Country", "Currency"]
# refnames = ["hascurrency", "hascapital"]
# link_on_props = ["hascurrency", "hascapital"]
# tgt_collection = ["Currency", "Country"]

client_src = weaviate.connect_to_local(
    headers={
        "X-OpenAI-Api-Key": os.getenv("OPENAI_APIKEY")
    }
)

client_tgt = weaviate.connect_to_wcs(
    cluster_url=weave_url,
    auth_credentials=Auth.api_key(weave_api),

    headers={
        "X-OpenAI-Api-Key": os.getenv("OPENAI_APIKEY")
    },
        skip_init_checks=True
)

def create_collection(client_in: WeaviateClient, collection_name: str, enable_mt=False, ):

    movies = client_in.collections.create(
        name=collection_name,
        multi_tenancy_config=wvc.config.Configure.multi_tenancy(enabled=enable_mt),




    )

    return movies

def add_references(coll_src, coll_tgt, name): #Adding reference properties
    category = client_tgt.collections.get(coll_src)
    category.config.add_reference(
        ReferenceProperty(
            name=name,
            target_collection=coll_tgt
        )
    )
    return True


def migrate_data(collection_src: Collection, collection_tgt: Collection): #migrating data objects and preserving the uuids
    with collection_tgt.batch.fixed_size(batch_size=100) as batch:
        for q in tqdm(collection_src.iterator(include_vector=True)):
            batch.add_object(
                properties=q.properties,
                vector=q.vector["title_vector"],
                uuid=q.uuid,




            )

    return True




def getting_refs_props(coll_name, uuids, list_of_prop_name, link_on_prop): #getting uuids for both collections
    collection = client_tgt.collections.get(coll_name)

    response = collection.query.fetch_objects(limit=100,
                                              return_references=QueryReference(
                                                  link_on=link_on_prop,
                                                  return_properties=["movie_name"],
                                              )
                                              )
    response_obj = response.objects
    l = 0
    while l < len(response_obj):
        for index, value in enumerate(response_obj):
            data_string = str(response_obj[index])
            start_uuid = data_string.find("_WeaviateUUIDInt('") + len("_WeaviateUUIDInt('")
            end_uuid = data_string.find("')", start_uuid)
            uuid = data_string[start_uuid:end_uuid]
            uuids.append(uuid)
            l += 1


    return uuids

def gettings_refs(coll_name, uuids, list_of_prop_name, link_on_prop): #getting reference property which is common and creating two dict with mvoie name as common name


    for uuid in uuids:
        collection = client_src.collections.get(coll_name)
        response = collection.query.fetch_object_by_id(uuid, return_references=QueryReference(
            link_on=link_on_prop,
            return_properties=["movie_name"],
            return_references=[]))
        movie_name = response.properties["movie_name"]

        #src_list_of_linkon.append(prop_name)
        list_of_prop_name[uuid] = movie_name

        for prop_name in response.references:

            prop_name = prop_name

            movie_name = response.properties["movie_name"]

            src_list_of_linkon.append(prop_name)
        list_of_prop_name[uuid] = movie_name








def sorting_from_to_uuid():      #sorting and getting two uuids from_uuid and to_uuid
    for key1, value1 in src_list_of_prop_name.items():
        for key2, value2 in tgt_list_of_prop_name.items():
            if value1 == value2:
                from_uuid.append(key1)
                to_uuid.append(key2)
                movie1_name.append(value1)
                movie2_name.append(value2)

    return from_uuid, to_uuid

def add_cross_ref(coll_name, src_uuid, ref_name, tgt_uuid): #adding cross reference
    collection = client_tgt.collections.get(coll_name)
    collection.data.reference_add(
        from_uuid=src_uuid,
        from_property=ref_name,
        to=tgt_uuid
    )






for i, val in enumerate (coll_names):

    movies_create = create_collection(client_tgt, val, enable_mt=False)


for i, val in enumerate (coll_names):
    movies_src = client_src.collections.get(val)
    movies_tgt = client_tgt.collections.get(val)
    add_references(val, tgt_collection[i], refnames[i])
    migrate_data(movies_src, movies_tgt)

for index, value in enumerate(coll_names):
    if value == coll_names[0]:
        getting_refs_props(value, src_from_uuid, src_list_of_prop_name, link_on_props[index])
    if value == coll_names[1]:
        getting_refs_props(value, tgt_to_uuid, tgt_list_of_prop_name, link_on_props[index])



for index, value in enumerate(coll_names):
    if value == coll_names[0]:
        gettings_refs(value, src_from_uuid, src_list_of_prop_name, link_on_props[index])
    if value == coll_names[1]:

        gettings_refs(value, tgt_to_uuid, tgt_list_of_prop_name, link_on_props[index])


sorting_from_to_uuid()




for index, value in enumerate(from_uuid):
    add_cross_ref(coll_names[0], value, refnames[0], to_uuid[index])
for index, value in enumerate(to_uuid):
    add_cross_ref(coll_names[1], value, refnames[1], from_uuid[index])
client_src.close()
client_tgt.close()
