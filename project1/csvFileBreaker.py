import csv

NO_OF_LINES_PER_FILE = 20000

def again(count_file_header,count):
    f3 = open('write_'+count_file_header+'.csv', 'at')
    with open('exports/Master.csv', 'rb') as csvfile:
        candidate_info_reader = csv.reader(csvfile, delimiter=',', quoting=csv.QUOTE_ALL)
        co = 0      
        for row in candidate_info_reader:
            co = co + 1
            count  = count + 1
            if count <= count:
                pass
            elif count >= NO_OF_LINES_PER_FILE:
                count_file_header = count + NO_OF_LINES_PER_FILE
                again(count_file_header,count)
            else:
                writer = csv.writer(f3,delimiter = ',', lineterminator='\n',quoting=csv.QUOTE_ALL)
                writer.writerow(row)

def read_write():
    f3 = open('write_'+NO_OF_LINES_PER_FILE+'.csv', 'at')
    with open('exports/Master4.csv', 'rb') as csvfile:
        candidate_info_reader = csv.reader(csvfile, delimiter=',', quoting=csv.QUOTE_ALL)
        count = 0       
        for row in candidate_info_reader:
            count  = count + 1
            if count >= NO_OF_LINES_PER_FILE:
                count_file_header = count + NO_OF_LINES_PER_FILE
                again(count_file_header,count)
            else:
                writer = csv.writer(f3,delimiter = ',', lineterminator='\n',quoting=csv.QUOTE_ALL)
                writer.writerow(row)

read_write()
