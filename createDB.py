import datetime
import json
import pydgraph
from query import data_generator 
from pprint import pprint
import os 

clear = lambda: os.system('clear')

# Create a client stub.
def create_client_stub():
    return pydgraph.DgraphClientStub(
        'localhost:9080', 
        options=[
            ('grpc.max_send_message_length', 1024*1024*1024),
            ('grpc.max_receive_message_length', 1024*1024*1024),
        ]
    )


# Create a client.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


# Drop All - discard all data and start from a clean slate.
def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))


# Set schema.
def set_schema(client):
    schema = """
    firstname: string @index(term) .
    lastname: string @index(term) .
    age: int @index(int) .
    phonenumber: string @index(term) .
    type Person {
        firstname
        lastname
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
            'firstname': data['firstname'],
            'lastname': data['lastname'],
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

    quan = 1000000
    for i in range(quan):
        create_data(client, i + 1, quan)
        if (i + 1) % 10000 == 0:
            clear()


def main():
    
    client_stub = create_client_stub()
    client = create_client(client_stub)
    print("Success")

    drop_all(client)
    set_schema(client)

    create_bulk_data(client)
    
    client_stub.close()


if __name__ == '__main__':
    
    main()
    print('DONE!')