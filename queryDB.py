import datetime
import json
import pydgraph
from query import data_generator 
from pprint import pprint
from json2csv import json2csv
import time 
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


# Query for data.
def query_client(client):
    # Run query.
    query = """query all($a: int) {
        all(func: gt(age, $a), orderasc: age, first: 1000000)
        {
            uid
            firstname
            lastname
            age
            phonenumber
        }
    }"""

    variables = {'$a': '20'}

    start = time.process_time()
    res = client.txn(read_only=True).query(query, variables=variables)
    end = time.process_time()

    print(f"Total runtime of the program is {(end - start)*1000} milliseconds")

    ppl = json.loads(res.json)

    # Print results.
    try:
        json2csv(ppl['all'])
    except:
        print("No Data Available")
        json2csv([])
    # print('Number of people named "Alice": {}'.format(len(ppl['all'])))

def main():
    
    client_stub = create_client_stub()
    client = create_client(client_stub)
    print("Success")

    query_client(client)  # query for client

    # Close the client stub.
    client_stub.close()


if __name__ == '__main__':
    
    main()
    print('DONE!')