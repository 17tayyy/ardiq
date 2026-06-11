---
title: Serialization
description: How task arguments and results are encoded on the wire, and how to swap msgpack for pickle.
---

ArdiQ has to turn your task arguments and return values into bytes to store them in Redis,
and back again on the other side. By default it uses [msgpack](https://msgpack.org/) — fast,
compact, and cross-language.

## The default: msgpack

You don't have to configure anything; msgpack is used out of the box:

```python
app = Ardiq(queue_name="default")   # msgpack
```

msgpack handles the common cases cleanly: `None`, booleans, numbers, strings, bytes, lists,
and dicts. That covers most task signatures.

:::caution[msgpack limits]
msgpack can't natively serialize `datetime`, `set`, `tuple`-as-distinct-from-list, or
arbitrary objects. If your tasks pass those around, msgpack will raise — switch to pickle
(below) or stick to JSON-friendly types.
:::

## Switching to pickle

To send richer Python objects, pass `pickle.dumps` / `pickle.loads` as the serializer pair:

```python
import pickle
from ardiq import Ardiq

app = Ardiq(
    queue_name="default",
    serializer=pickle.dumps,
    deserializer=pickle.loads,
)
```

Now `datetime`, `set`, custom classes, and anything else picklable round-trips through the
queue.

:::danger[Pickle is not safe across trust boundaries]
`pickle.loads` executes arbitrary code embedded in the payload. Only use pickle when **every
producer and consumer of the queue is trusted** — never deserialize data from untrusted
sources.
:::

## Custom codecs

The serializer is any `Callable[[Any], bytes]` and the deserializer any
`Callable[[bytes], Any]`, so you can plug in your own — JSON, CBOR, a schema-based codec,
etc.:

```python
import json

app = Ardiq(
    serializer=lambda obj: json.dumps(obj).encode(),
    deserializer=lambda data: json.loads(data.decode()),
)
```

The same codec is used for both **arguments** (producer side) and **results** (worker side),
so every process talking to a given queue must agree on it. Keep the serializer config
identical across your enqueuers and your workers.
