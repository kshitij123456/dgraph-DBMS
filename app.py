import pydgraph
import json
import sys
from query import data_generator

with open("credentials-97l.json") as f:
    data = json.load(f)

try:
    client_stub = pydgraph.DgraphClientStub.from_slash_endpoint(
        data["endpoint"], data["apikey-client"]
    )
    client = pydgraph.DgraphClient(client_stub)
    print("Success")
except:
    print("Some error has occured")
    sys.exit(0)

txn = client.txn()
try:
    # data_point = data_generator()
    # print(data_point)
    # # mu = pydgraph.Mutation(set_json=json.dumps(data_point).encode("utf8"))
    # txn.mutate(set_obj = data_point)
    # txn.commit()

    query = """query all($a: string) {
            all(func: eq(name, $a))
            {
                name
            }
        }"""
    variables = {'$a': 'Alice'}

    res = txn.query(query, variables=variables)

    # If not doing a mutation in the same transaction, simply use:
    # res = client.txn(read_only=True).query(query, variables=variables)

    ppl = json.loads(res.json)

    # Print results.
    print('Number of people named "Alice": {}'.format(len(ppl['all'])))
    for person in ppl['all']:
        print(person)

except pydgraph.AbortedError as err:
    print(err)
finally:
    # Clean up. Calling this after txn.commit() is a no-op
    # and hence safe.
    txn.discard()
