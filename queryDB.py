import datetime
import json
import pydgraph
from query import data_generator 
from pprint import pprint
from json2csv import json2csv

# Create a client stub.
def create_client_stub():
    return pydgraph.DgraphClientStub('localhost:9080')


# Create a client.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


# Drop All - discard all data and start from a clean slate.
def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))


# Set schema.
def set_schema(client):
    schema = """
    name: string @index(exact) .
    age: string @index(exact) .
    phonenumber: int .
    type Person {
        name
        age
        phonenumber
    }
    """
    return client.alter(pydgraph.Operation(schema=schema))


# Create data using JSON.
def create_data(client, ind, quan):
    # Create a new transaction.
    data = data_generator()
    txn = client.txn()

    try:
        # Create data.
        p = {
            'uid': '_:' + data['id'],
            'dgraph.type': 'Person',
            'name': data['namePerson'],
            'age': data['age'],
            'phonenumber': data['phonenumber']
        }

        # Run mutation.
        response = txn.mutate(set_obj = p)

        # Commit transaction.
        txn.commit()

        # Get uid of the outermost object (person named "Alice").
        # response.uids returns a map from blank node names to uids.
        if ind % 100 == 0:
            print('{} out of {}'.format(ind, quan))
    except pydgraph.AbortedError as err:
        print(err)
    finally:
        # Clean up. Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def create_bulk_data(client):

    quan = 1000
    for i in range(quan):
        create_data(client, i + 1, quan)


# Deleting a data
def delete_data(client):
    # Create a new transaction.
    txn = client.txn()
    try:
        query1 = """query all($a: string) {
            all(func: eq(name, $a)) {
               uid
            }
        }"""
        variables1 = {'$a': 'Bob'}
        res1 = client.txn(read_only=True).query(query1, variables=variables1)
        ppl1 = json.loads(res1.json)
        for person in ppl1['all']:
            print("Bob's UID: " + person['uid'])
            txn.mutate(del_obj=person)
            print('Bob deleted')
        txn.commit()

    finally:
        txn.discard()


# Query for data.
def query_alice(client):
    # Run query.
    query = """query all($a: string) {
        all(func: eq(age, $a)) {
            uid
            firstname
            lastname
            age
            phonenumber
        }
    }"""

    variables = {'$a': '90'}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    try:
        pprint(ppl['all'])
        json2csv(ppl['all'])
    except:
        print("No Data Available")
        json2csv([])
    # print('Number of people named "Alice": {}'.format(len(ppl['all'])))


# Query to check for deleted node
def query_bob(client):
    query = """query all($b: string) {
            all(func: eq(name, $b)) {
                uid
                name
                age
                friend {
                    uid
                    name
                    age
                }
                ~friend {
                    uid
                    name
                    age
                }
            }
        }"""

    variables = {'$b': 'Bob'}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print('Number of people named "Bob": {}'.format(len(ppl['all'])))


def main():
    # client_stub = pydgraph.DgraphClientStub.from_slash_endpoint(
    #     data["endpoint"], data["apikey-client"]
    # )
    # client = pydgraph.DgraphClient(client_stub)
    client_stub = create_client_stub()
    client = create_client(client_stub)
    print("Success")

    # drop_all(client)
    # set_schema(client)

    # create_bulk_data(client)
    query_alice(client)  # query for Alice
    # query_bob(client)  # query for Bob
    # delete_data(client)  # delete Bob
    # query_alice(client)  # query for Alice
    # query_bob(client)  # query for Bob

    # Close the client stub.
    client_stub.close()


if __name__ == '__main__':
    
    main()
    print('DONE!')