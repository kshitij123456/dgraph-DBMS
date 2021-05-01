import pydgraph
import json
import sys
from query import data_generator

with open("credentials.json") as f:
    data = json.load(f)

try:
    client_stub = pydgraph.DgraphClientStub.from_slash_endpoint(
        data["endpoint"], data["apikey"]
    )
    client = pydgraph.DgraphClient(client_stub)
    print("Success")
except:
    print("Some error has occured")
    sys.exit(0)

txn = client.txn()
try:
    for i in range(10):
        data_point = data_generator()
        mu = pydgraph.Mutation(set_json=json.dumps(data_point).encode("utf8"))
        txn.mutate(mu)
    txn.commit()
except pydgraph.AbortedError as err:
    print(err)
finally:
    # Clean up. Calling this after txn.commit() is a no-op
    # and hence safe.
    txn.discard()
