Skip to content
Navigation Menu
patilajinkya
weasassignment

Type / to search
Code
Issues
Pull requests
Actions
Projects
Security
Insights
Settings
Files
Go to file
t
migration.py
weavs.py
Editing migration.py in weasassignment
Breadcrumbsweasassignment
/
migration.py
in
main

Edit

Preview
Indent mode

Spaces
Indent size

4
Line wrap mode

No wrap
Editing migration.py file contents
Selection deleted
147
148
149
150
151
152
153
154
155
156
157
158
159
160
161
162
163
164
165
166
167
168
169
170
171
172
173
174
175
176
177
178
179
180
181
182
183
184
185
186
187
188
189
190
191
192
193
194
195
196
197
198
199
200
201
202
203
204
205
206
207
208
209
210
211
212
213
214
215




def sorting_from_to_uuid():
    for key1, value1 in src_list_of_prop_name.items():
        for key2, value2 in tgt_list_of_prop_name.items():
            if value1 == value2:
                from_uuid.append(key1)
                to_uuid.append(key2)
                movie1_name.append(value1)
                movie2_name.append(value2)

    return from_uuid, to_uuid

def add_cross_ref(coll_name, src_uuid, ref_name, tgt_uuid):
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
        print(value)
        print(link_on_props[index])
        gettings_refs(value, tgt_to_uuid, tgt_list_of_prop_name, link_on_props[index])


sorting_from_to_uuid()







for index, value in enumerate(from_uuid):
    add_cross_ref(coll_names[0], value, refnames[0], to_uuid[index])
for index, value in enumerate(to_uuid):
    add_cross_ref(coll_names[1], value, refnames[1], from_uuid[index])
client_src.close()
client_tgt.close()
Use Control + Shift + m to toggle the tab key moving focus. Alternatively, use esc then tab to move to the next interactive element on the page.
Editing weasassignment/migration.py at main · patilajinkya/weasassignment 
