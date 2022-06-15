import csv

def split_csv_to_headers_and_data(csv_path):
    with open(csv_path, newline="") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        headers = next(csv_reader)
        data = [row for row in csv_reader]
        return headers, data


def write_to_csv(file_path, headers, data):
    with open(file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(headers)
        csv_writer.writerows(data)
