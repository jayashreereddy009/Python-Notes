# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Python data types for data engineering
# MAGIC
# MAGIC Python values have types that determine which operations are valid, how values behave, and how they should be validated or serialized. In a data pipeline, choosing a suitable type helps prevent incorrect arithmetic, lost precision, malformed records, and timestamp errors.
# MAGIC
# MAGIC This notebook introduces Python's principal built-in data types, plus standard-library types commonly used in data engineering. Examples use familiar pipeline concepts such as event records, IDs, measurements, and timestamps.
# MAGIC
# MAGIC | Family | Types | Typical pipeline use |
# MAGIC |---|---|---|
# MAGIC | Numeric | `int`, `float`, `complex`, `bool` | IDs/counts, measurements, specialized scientific values, flags |
# MAGIC | Text | `str` | Names, categories, JSON text, paths, and source-system values |
# MAGIC | Sequences | `list`, `tuple`, `range` | Ordered records, fixed row shapes, generated batches |
# MAGIC | Sets | `set`, `frozenset` | Deduplication, membership tests, immutable lookup keys |
# MAGIC | Mapping | `dict` | JSON objects, configuration, and column-to-value records |
# MAGIC | Binary | `bytes`, `bytearray`, `memoryview` | Encoded files, network payloads, and efficient buffer access |
# MAGIC | Special / temporal | `NoneType`, `datetime.date`, `datetime.time`, `datetime.datetime`, `datetime.timedelta`, `datetime.timezone` | Missing values, calendar dates, times, instants, and durations |
# MAGIC | Precision numeric | `decimal.Decimal` (standard library, not a built-in) | Exact decimal arithmetic such as currency and measured quantities |
# MAGIC
# MAGIC Python types are not the same as Spark SQL types. When moving values into Spark, schema choices such as `IntegerType`, `LongType`, `DoubleType`, `DecimalType`, `StringType`, `BooleanType`, `ArrayType`, `MapType`, `BinaryType`, and timestamp/date types should be made deliberately. Python values alone do not guarantee the desired Spark schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Numeric and logical types
# MAGIC
# MAGIC ### `int` — whole numbers
# MAGIC `int` represents an integer of arbitrary precision in Python. Use it for row counts, retry numbers, partition indexes, and identifiers that are genuinely whole-number values. Do not use a numeric type for identifiers that may contain leading zeros or letters; keep values such as `"000184"` or `"ORD-184"` as `str`.
# MAGIC
# MAGIC In Spark, choose an integer width that fits the domain and downstream schema (`IntegerType` or `LongType`). Python's arbitrary precision does not mean every database or Spark integer type can store an arbitrarily large value.
# MAGIC
# MAGIC ### `float` — approximate real numbers
# MAGIC `float` is a binary floating-point type. It is useful for approximate measurements such as sensor readings, ratios, and model scores. Many decimal fractions cannot be represented exactly in binary, so repeated calculations can produce small rounding differences. `NaN` and positive/negative infinity are also possible and may require explicit data-quality handling.
# MAGIC
# MAGIC For money, exact decimal quantities, or values subject to reconciliation, prefer `decimal.Decimal` in Python and a suitably sized Spark `DecimalType(precision, scale)` rather than `float`/`DoubleType`.
# MAGIC
# MAGIC ### `complex` — complex numbers
# MAGIC `complex` stores real and imaginary components (for example, `5 + 10j`). It is uncommon in ordinary ETL, but is useful in scientific and signal-processing pipelines such as Fourier transforms, electrical engineering, or other algorithms that operate on complex-valued measurements. Most relational and Spark SQL schemas do not have a direct complex-number type; store components separately or serialize them using a documented representation.
# MAGIC
# MAGIC ### `bool` — true/false values
# MAGIC `bool` has two values, `True` and `False`, and is a subclass of `int` (`True == 1`, `False == 0`). Use it for explicit flags such as `is_valid`, `is_deleted`, or `is_current`. Avoid relying on the numeric equivalence when counting or joining; convert and validate flags deliberately. In Spark, represent these as `BooleanType`, not as strings such as `"true"` / `"false"` unless the source format requires text.
# MAGIC
# MAGIC ### `None` / `NoneType` — absence of a value
# MAGIC `None` is Python's single value meaning “no value”; its type is `NoneType` (`type(None)`). It is not the same as `0`, `False`, or an empty string. Use it to represent an absent optional value in Python records. When writing to Spark, `None` generally maps to SQL `NULL` for a nullable field. Distinguish missing data from a business value such as an empty string, and define how nulls affect filtering, aggregation, and required-field validation.
# MAGIC
# MAGIC ### `decimal.Decimal` — exact decimal values (standard library)
# MAGIC `Decimal` is not a built-in type, but it is important in financial and other exact-decimal pipelines. Construct it from a string (for example, `Decimal("19.99")`) to avoid importing a binary float approximation. Define precision, scale, rounding, and overflow rules consistently with the destination `DecimalType`.

# COMMAND ----------

v_float=100.56
type(v_float)

# COMMAND ----------

#complex data type
complex_data=5+10j
type(complex_data)
print("Real part is:",complex_data.real)
print("imaginary part is:",complex_data.imag)


String='Sample String'
# Example: values that arrive as strings should be parsed and validated before arithmetic.


record_count = int("125")
temperature_c = float("21.75")
is_valid = True
missing_value = None
print(record_count, temperature_c, is_valid, missing_value)
print(type(record_count), type(temperature_c), type(is_valid), type(missing_value))



from decimal import Decimal
unit_price = Decimal("19.99")
quantity = 3
print("Invoice total:", unit_price * quantity)
print(type(unit_price), type(quantity))


#range(sequence type)(start_num,end_num-1,increment)
rangevariable=range(11)
print(rangevariable)
vRange2=range(1,len(String)+1,3)
print(vRange2)
set_var=set(vRange2)
list_var=list(rangevariable)
print(set_var, list_var)

# COMMAND ----------

# this code snippet gets the installed packages location
import site

# Print site-packages directories where installed Python packages live
print("Site-packages locations:")
for path in site.getsitepackages():
    print(" ", path)

# Also show the user-specific site directory
print("User site directory:")
print(" ", site.getusersitepackages())

# Alternatively, show the location of a specific package (e.g., pip)
import pip
print("\nExample — pip location:")
print(" ", pip.__file__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Text and ordered sequences
# MAGIC
# MAGIC ### `str` — Unicode text
# MAGIC `str` is an immutable sequence of Unicode characters. It is the right type for names, categories, source identifiers with leading zeros, JSON/CSV text, and file paths. String methods create new strings rather than changing the original. Normalize and validate text at ingestion (for example, whitespace, casing, and encoding assumptions); do not assume that a string containing digits is already a number.
# MAGIC
# MAGIC ### `list` — mutable ordered collection
# MAGIC A `list` preserves order, supports indexing and slicing, allows duplicates, and can be changed in place. It is useful for a batch of records, a sequence of parsed values, or an in-memory work queue. Lists may contain mixed types, but consistent element types are easier to validate, serialize, and convert to Spark `ArrayType`. Appending one item differs from extending with all items in another iterable.
# MAGIC
# MAGIC ### `tuple` — immutable ordered collection
# MAGIC A `tuple` preserves order and supports indexing, but its membership cannot be reassigned after creation. Use it for a fixed-shape in-memory record or a stable composite key, such as `(source_system, event_id)`. A tuple is only shallowly immutable: a mutable object stored inside it can still change. Spark rows/structs are schema-defined records rather than Python tuples.
# MAGIC
# MAGIC ### `range` — arithmetic integer sequence
# MAGIC `range(start, stop, step)` represents a compact sequence of integers; `stop` is excluded and the step defaults to one. It is lazy in the sense that it does not allocate a list of every number, so it is suitable for iteration over batch indexes or bounded retry numbers. Convert it to `list` only when an actual materialized list is needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### List operations in a pipeline
# MAGIC Indexes start at zero; negative indexes count from the end. Slices select a range without changing the source. `append` adds one value, `extend` adds each value from another iterable, and `insert` adds at a selected position. `remove` deletes the first matching value, while `pop` returns and removes by position. Use `del` to remove an item or binding when appropriate. These operations mutate the list, so retain a copy when the original batch is needed for comparison or retry.

# COMMAND ----------

#list[] ordered,indexed and mutable

list1=[1,2,3,4,4,5,6,'string',True,5+6j]
list2=[8,9,10]
print(list1)
print(list1[4],list1[-3],list1[3:5]) #index referencing and slicing
list_variable=list1[7] #variable assignment
print(list_variable)
type(list_variable)
list1.append(1000)
list1.append(['Naveen','Rajesh'])
# list1.append('New value') # to insert any element at the end of the list
list1.insert(5,True) #to insert any element into the list for a given index
print(list1)
list1[5]=False # to update the list for a given index position
list1.remove('string') # to remove a element with element value given
print(list1)
list1[-1].remove('Naveen') # mutate the nested list at the final position
list1.pop() # remove and return the last element
list1.pop(2) # remove and return the element at index 2
del list1[0] # remove the element at index 0
del list1 # remove the entire list
list1.extend(list2) # add every element in list2 to list1
print(list1)
list3=list1+list2
print(list3,list1)

# COMMAND ----------

print(list1)

# COMMAND ----------

sample_list=[1,'Today is Monday',True,5+6j]

# COMMAND ----------

# type(sample_list)
print(sample_list)

# COMMAND ----------

# list sort,reverse,index,copy,clear
list1=[7,2,100,1,2,3,4,4,5,6]
list2=['Apple','Xmas','Bulb','orange']
# list2=['apple','Xmas','Bulb','orange',True,50]
list1.sort()
print(list1)
list2.sort()
print(list2)
list2.sort(key=len)
# list2.sort()
list1.sort(reverse=True)
print(list1)
print(list2)
list3=list1+list2 #merging
print(list3)

#reverse
list3.reverse()
print(list3)
print('index of apple is: ', list3.index('Apple'))
print('index of apple is: ', list3.index(4))
list3[:list3.index('Apple')+1]
print(list3.index(2))
list4=list3 #shallow clone ( only indexes will be referenced from the old list to new list but not the data)
print(list4)
print(list3)
list5=list3.copy() # the data will be copied to a new list
list3.append('NewValue')
list4.append('NewValue2')
print(list3)
print(list4)
print(list5)

list4.clear() # empty the list
print(list4)
print(list3)
print(list5)

# COMMAND ----------

num1=5
num2=num1
print(num1,num2)
num1=6
print(num1,num2)

# COMMAND ----------

list5=[45,47,89,32,55]
list6=['apple','pavan','sowmya','king']
list7=['pavan',3,6,90,'queen']
print(list5)
list5=list6 # shallow clone(only indexes will be referenced from old list to new list
list5=list7.copy() #the data will be copied to a new list
list5.append(99)
print(list6)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Sets and mappings
# MAGIC
# MAGIC ### `set` — mutable collection of unique, hashable values
# MAGIC A `set` removes duplicates and supports fast membership checks and operations such as union, intersection, and difference. It is useful for comparing distinct source IDs, checking whether a key is in a reference list, or finding records present in one feed but not another. Sets have no guaranteed order and do not support indexing. Elements must be hashable (for example, strings and integers, but not lists or dictionaries). For large datasets, use Spark DataFrame joins and set operations rather than collecting all keys to the driver.
# MAGIC
# MAGIC ### `frozenset` — immutable set
# MAGIC `frozenset` has set membership and set operations, but cannot be changed after construction. Because it is hashable when its elements are hashable, it can be used as a dictionary key or as a member of another set. It can represent a fixed set of permissions or a stable combination of dimensions, but it is not a substitute for a sorted or indexed collection.
# MAGIC
# MAGIC ### `dict` — mapping from keys to values
# MAGIC A `dict` maps unique, hashable keys to values. It preserves insertion order in supported modern Python versions, but do not treat that as a substitute for an explicit ordering rule in a data contract. Assigning an existing key replaces its value. Dictionaries are useful for one JSON object, a record's field values, configuration, and lookups; nested dictionaries commonly represent parsed JSON. For structured Spark data, use an explicit `StructType` or `MapType` and validate expected keys and value types.

# COMMAND ----------

#tuple ordered,indexed and immutable,concat
tuple1=(1,23,4,4,5)
tuple2=('A','B','C')
type(tuple1)
print(tuple1[1],tuple1[2:])
tuple3=tuple1+tuple2
print(tuple3)
# tuple2=tuple1.copy()
tuple2
# tuple1.reverse()
tuple1


# COMMAND ----------

# MAGIC %md
# MAGIC ### Set operations and empty collection syntax
# MAGIC `set.add` inserts one member; `discard` removes a member without error if absent, while `remove` raises `KeyError` if it is absent. `pop` removes an arbitrary member, so never rely on which one it returns. Union combines unique members, intersection finds common members, and difference finds members present only on the left. An empty set is written `set()`; `{}` creates an empty dictionary. Set iteration order is not a data-order guarantee.

# COMMAND ----------

#set unordered,unindexed , mutable,unique
set1={100,5,1,2,3,4,5,5,5,6}
print(set1)
set1.add(7)
set1.add(8)
print(set1)
set1.discard(7)
set1.remove(5)
print(set1)
set1.pop()
print(set1)
set1
set2=set()
type(set2)
empty_dict={}
type(empty_dict)

# COMMAND ----------

list_sample=[]
tuple_sample=()
set_sample=set()
print(type(list_sample))
print(type(tuple_sample))
print(type(set_sample))
print(type({}))

# COMMAND ----------

#set clear,copy,remove,add,discard,pop,union,intersection,difference,

set1={1,10,15,26,28}
set2={56,100,10,1,0}
set3=set2.copy()
print(set3)
set2.add(1000)
print(set3)
print(set2)
# set2.remove(-99)
set2.discard(-99)
set2
set2.pop()
print(set1)
print(set2)
set4=set1.union(set2)
print(set4)
set5=set1.intersection(set2)
print(set5)
set6=set1.difference(set2)
print(set6)
set7=set2.difference(set1)
print(set6,set7)


# COMMAND ----------

set123={1,1,1,1,1}
print(set123)
list123=list(set123)
print(list123)
tuple123=(1,1,1,1)
list2=set(tuple123)
set123
print(list2)

# COMMAND ----------

#dict key value pairs,ordered,doent allow duplicate keys,copy,clear
dict1={'name':'Naveen','profession':'engineer','location':'India'}
print(dict1)
print(dict1['profession'])
dict_profession=dict1['profession']
print(dict_profession)
dictionary_items=dict1.items()
print(dictionary_items)

# COMMAND ----------

nested_json={
  "employee": {
    "id": 101,
    "name": "John Doe",
    "department": {
      "id": "D001",
      "name": "Engineering"
    }
  }
}
type(nested_json)

# COMMAND ----------

dict1['location']='USA'
print(dict1)
del dict1['location']
print(dict1)
dict1['city']='Newyork'
print(dict1)
dict1.pop('city')
print(dict1)
dict1.popitem()
print(dict1)
dict2=dict1.copy()
print(dict2)
dict1.clear()
print(dict2)
print(dict1)
nested_json['employee']['department']['id']='D002'
print(nested_json)


# COMMAND ----------

# del dict1
print(dict1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `bytes`, `bytearray`, and `memoryview` — binary data
# MAGIC Binary types hold byte values, not Unicode text. Decode bytes with the correct character encoding to obtain `str`; encode text to create bytes. Incorrect encodings can corrupt text, and arbitrary binary files (such as compressed or image data) should not be decoded as text.
# MAGIC
# MAGIC - `bytes` is an immutable sequence of integers from 0 through 255. Use it for an encoded payload or file content that should not be modified.
# MAGIC - `bytearray` is a mutable byte sequence. Use it when building or editing a binary buffer in place.
# MAGIC - `memoryview` provides a view over a bytes-like object's buffer without copying the entire data. It can help efficient processing of large payloads, but the underlying buffer's lifetime and mutability still matter.
# MAGIC
# MAGIC In data engineering these types appear when reading binary files, handling compressed data, or calling APIs that exchange byte payloads. Spark's `BinaryType` stores binary values; keep text and binary representations distinct.

# COMMAND ----------

#frozen set unordered,unidexed,immutable
set2=frozenset({1,2,3,4,5,5})
type(set2)
# print(set2)

# COMMAND ----------

#bytes encoded datatype(utf-8,utf-16,utf-32...),immutable
a=b'apple'
b=bytes(10)
c=bytes('Orange','utf-16')
print(a,b,c)
type(c)

# COMMAND ----------

payload = bytearray(b'event-payload')
payload_view = memoryview(payload)
print(payload_view[0:5].tobytes())
print(type(payload_view))

# COMMAND ----------

#bytearray indexed,mutable
a=bytearray(10)
b=bytearray('apple','utf-32')
print(a)
a[2]=3
print(a,b)

# COMMAND ----------

decoded_b = b.decode('utf-32')
print(decoded_b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date and time types
# MAGIC
# MAGIC The `datetime` module supplies several distinct types:
# MAGIC
# MAGIC - `date` represents a calendar date without a time or timezone, useful for business dates and partition keys.
# MAGIC - `time` represents a time of day without a date; by itself it does not identify a unique instant.
# MAGIC - `datetime` combines date and time. A naive `datetime` has no timezone information; an aware `datetime` has a timezone/offset and identifies an instant more safely for cross-region event processing.
# MAGIC - `timedelta` represents a duration or difference, such as a processing interval or retention window.
# MAGIC - `timezone` represents a fixed UTC offset. For daylight-saving-aware regional rules, use an IANA timezone implementation such as `zoneinfo.ZoneInfo` (or a compatible library) rather than assuming a fixed offset.
# MAGIC
# MAGIC Use UTC as the canonical representation for event instants where possible, preserve the source timezone/offset when required for auditability, and avoid silently mixing naive and aware timestamps. Spark distinguishes date and timestamp columns; choose and document the intended timezone semantics when parsing and writing them.

# COMMAND ----------

from datetime import date, datetime, time, timedelta, timezone

business_date = date(2026, 10, 22)
cutoff_time = time(17, 30)
event_time_utc = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
retention_window = timedelta(days=30)
print(type(business_date), type(cutoff_time), type(event_time_utc))
print("Retention ends:", event_time_utc + retention_window)

# COMMAND ----------

#datetime functions
from datetime import datetime 
today_date=datetime.now()
print(today_date)
type(today_date)
print(today_date.year)
print(today_date.month)
today_day=today_date.day
print(today_day)
print(today_date.strftime("%a"))
print(today_date.strftime("%B"))

# COMMAND ----------

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc
IST = ZoneInfo('Asia/Kolkata')
US_CENTRAL = ZoneInfo('America/Chicago')

print("US Central time:", datetime.now(US_CENTRAL))
print("India time:", datetime.now(IST))

datetime_utc = datetime.now(UTC)
print("Date & Time in UTC:", datetime_utc.strftime('%Y:%m:%d %H:%M:%S %Z %z'))

datetime_ist = datetime.now(IST)
print("Date & Time in IST:", datetime_ist.strftime('%Y-%m-%d %H:%M:%S %Z %z'))

# COMMAND ----------

type(datetime_ist)

# COMMAND ----------

# V_bool=True
V_bool=False
type(V_bool)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other built-in types and special values
# MAGIC
# MAGIC The types above are the everyday value types used in most data pipelines. Python also has built-ins that describe program structure or special operations rather than ordinary data fields:
# MAGIC
# MAGIC - `object` is the common base class of ordinary Python classes.
# MAGIC - `type` is the type of classes and is used by `type(value)` to inspect a value's class. In production validation, explicit checks or schema validation are often clearer than using a type check alone.
# MAGIC - `slice` describes a slicing operation, such as the object created by `slice(1, 5, 2)`. It is mainly useful when writing generic indexing code.
# MAGIC - `Ellipsis` (written `...`) and `NotImplemented` are special singleton values. `...` is used in some array libraries for multidimensional indexing; `NotImplemented` can be returned by a custom operator implementation to signal that another implementation should be tried.
# MAGIC
# MAGIC These values are rarely fields in an ingested dataset. Python 3 also uses one `int` type for whole numbers; there is no separate `long` type as there was in Python 2.
# MAGIC