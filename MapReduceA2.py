#Derek Aguirre

#Please use the following command to run the program:
# $python3 mapReduce.py
#The program will prompt users to enter the amound of threads to run
#Please enter a number up to 8 for that field and it will execute

import pymp
import time
import re

def main():
    thread_amount = input("Please enter thread amount (up to 8)\n>")
    results = {}
    if(int(thread_amount) >= 8):
        print("Executing the program with 8 threads")
        start_h_coded = time.time()
        results = map_reduce(8)
        end_h_coded = time.time()
        print(f'Total elapsed time { end_h_coded-start_h_coded:.2f} seconds')
    else:
        print("Executing the program with " + str(thread_amount) + " threads")
        start_dynamic = time.time()
        results = map_reduce(int(thread_amount))
        end_dynamic = time.time()
        print(f'Total elapsed time { end_dynamic-start_dynamic:.2f} seconds')
    print_dict(results)

def load_files():
    file_list = []
    for i in range(1, 9):
        filename = "shakespeare" + str(i) + ".txt"
        file_list.append(open(filename))
    return file_list

def ret_word_list():
    words = ["hate", "love", "death", "night", "sleep", "time", "henry", "hamlet", "you", "my", "blood", "poison", "macbeth", "king", "heart", "honest"]
    return words
    
def map_reduce(threads):
    start_load = time.time()
    file_list = load_files()
    loaded_files = []
    
    for file in file_list:
        loaded_files.append(file.read())
    end_load = time.time()
    print("Seconds elapsed for reading files:", end_load - start_load)
    
    words = ret_word_list()
    complete_dict = pymp.shared.dict()

    with pymp.Parallel(threads) as p:
        for i in words:
            complete_dict[i] = 0
        for curr_file in p.iterate(loaded_files):
            for i in words:
                regex = '(?<![\w\d]>)' + i
                start_count = time.time()
                occurrences = re.findall(regex, curr_file, re.IGNORECASE)
                end_count = time.time()
                complete_dict[i] += len(occurrences)
            print("Thread ", p.thread_num," read a document for: ",end_count - start_count, "seconds")        
    return complete_dict

def print_dict(complete_dict):
    print("\nOccurrences found of the following words:")
    for target_word in complete_dict:
        print(target_word, ":", complete_dict[target_word])

if __name__ == '__main__':
    main()