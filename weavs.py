import os
import weaviate
import warnings
import pandas as pd
from weaviate.classes.config import Configure, Property, DataType, ReferenceProperty
from weaviate.util import generate_uuid5
warnings.filterwarnings("ignore", category=DeprecationWarning)
from weaviate.classes.init import Auth

weave_url = os.environ["WEAV_URL"]
weave_api = os.environ["WEAV_API"]
coll_name1 = "Movie1" # <<<< OS ENV VARIABLE FOR COLLECTION NAME
coll_name2 = "Movie2"
hug_api = os.environ["HUG_API"]  # <<<< OS ENV VARIABLE HUGGINGFACE_API_KEY
data1 = "/Users/ajinkya/Downloads/movie_release.json"
data2 = "/Users/ajinkya/Downloads/movie_actors_director.json"
coll_dict = {coll_name1 : data1, coll_name2 : data2}
coll1_uuid = []
coll2_uuid = []

props_coll1 = []
props_coll2 = []
source_props1 = []
source_props2 = []
def connect_cloud():
    client =  weaviate.connect_to_wcs(
    cluster_url=weave_url,
    auth_credentials=Auth.api_key(weave_api),

    headers={
        "X-OpenAI-Api-Key": os.getenv("OPENAI_APIKEY")
    },
        skip_init_checks=True,
  )
    return client
def connect_local():
    client = weaviate.connect_to_local()
    return client


def get_props(df, props, source_props):  # <<<< GETTING THE PROPERTIES FROM THE PROVIDED JSON DATA AND SETTING IT TO A LIST PROPS
    df = pd.read_json(df)
    huggingface_key = os.getenv(hug_api)
    headers = {
        "X-HuggingFace-Api-Key": huggingface_key,
    }
    for i in df.keys():
        props.append(Property(name=i, data_type=DataType.TEXT))
        source_props.append(i)
        return props


def collection_create(coll_name, props, source_props):
    client.collections.delete(coll_name)  # <<<<< DELETING A COLLECTION NAME IF IT EXISTS AND CREATING A COLLECTION WITH PROPERTIES TAKEN FROM LIST PROPS

    client.collections.create(
        coll_name,
        properties=props,
        generative_config=Configure.Generative.openai(
            model="gpt-4o",
            frequency_penalty=0,
            max_tokens=5000,
            presence_penalty=0,
            temperature=0.7,
            top_p=0.7
        ),
        vectorizer_config=[
            Configure.NamedVectors.text2vec_openai(
                name="title_vector",
                source_properties=source_props,
                model="text-embedding-3-large",
                dimensions=1024

            )
        ],

    )

    return True


def load_data(coll_name, uuid_list, df, source_props):  # <<<< ADDING DATA TO COLLECTION CREATED AND PRINTING FAILED OBJECTS
    df = pd.read_json(df)

    collection = client.collections.get(coll_name)
    l = 0
    weaviate_obj = {}
    while l < len(source_props):
        weaviate_obj[source_props[l]] = source_props[l]

        for i, src_obj in df.iterrows():
            d = {**weaviate_obj, **src_obj}
            uuid = collection.data.insert(d)
            uuid_list.append(uuid)

        l += 1


    failed_objs_a = client.batch.failed_objects
    print(failed_objs_a)

    return failed_objs_a, uuid_list

def add_references(coll_src, coll_tgt, name):
    category = client.collections.get(coll_src)
    category.config.add_reference(
        ReferenceProperty(
            name=name,
            target_collection=coll_tgt
        )
    )

def add_cross_ref(coll_name, src_uuid, name, tgt_uuid):
    collection = client.collections.get(coll_name)
    collection.data.reference_add(
        from_uuid=src_uuid,
        from_property=name,
        to=tgt_uuid
    )


client = connect_local()

for i,k in coll_dict.items():
    if i == coll_name1:
        get_props(k,props_coll1, source_props1)
        collection_create(i,props_coll1,source_props1)
        load_data(i,coll1_uuid, k, source_props1)
    elif i == coll_name2:
        get_props(k,props_coll2, source_props2)
        collection_create(i,props_coll2,source_props2)
        load_data(i, coll2_uuid, k, source_props1)

print(coll1_uuid)
print(coll2_uuid)
add_references(coll_name1, coll_name2, "hasactor")
add_references(coll_name2, coll_name1, "hasrating")
for l1, val  in enumerate (coll1_uuid):
    add_cross_ref(coll_name1, val, "hasactor", coll2_uuid[l1])
for l2,val in enumerate(coll2_uuid):
    add_cross_ref(coll_name2, val, "hasrating", coll1_uuid[l2])
client.close()

