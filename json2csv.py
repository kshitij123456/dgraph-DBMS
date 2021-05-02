import csv

def json2csv(data):

    if len(data) == 0:
        with open('data.csv', 'w', newline='') as csvfile:
            pass
        return
            

    csv_columns = data[0].keys()
    csv_file = "data.csv"
    try:
        with open(csv_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = csv_columns)
            writer.writeheader()
            writer.writerows(data)
    except:
        print("Some error happened")