from kafka import KafkaProducer
from time import sleep
from datetime import datetime
import time

if __name__ == '__main__':

    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    # producer = KafkaProducer(bootstrap_servers='xxx:9092')
    filename = 'dataset/bus-breakdown-and-delays.csv'
    K = 1 / 8000  # fattore di compressione
    f = open(filename, "r")

    f.readline()

    line_count = 0
    date_before = 0
    for line_count, line in enumerate(f):
        if len(line.split(";")) == 21:
            date_time_str = line.split(";")[7]
            date_object1 = datetime.strptime(date_time_str, '%Y-%m-%dT%H:%M:%S.%f')
            current_date = time.mktime(date_object1.timetuple())
            if line_count == 0:
                date_before = current_date
            sleep((current_date - date_before) * K)
            producer.send('flink', str.encode(line))
            date_before = current_date
        else:
            continue

        print(line_count)

    print("Lines proccessed:", line_count)

    f.close()
