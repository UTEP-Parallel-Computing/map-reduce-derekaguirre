# Derek Aguirre

# Please use the following command to run the program:
# $python3 mapReduceMPI.py

from mpi4py import MPI
import time
import re

globalListOfDocs = ["shakespeare1.txt", "shakespeare2.txt", "shakespeare3.txt", "shakespeare4.txt",
                    "shakespeare5.txt", "shakespeare6.txt", "shakespeare7.txt", "shakespeare8.txt"]

words = ["hate", "love", "death", "night", "sleep", "time", "henry", "hamlet",
         "you", "my", "blood", "poison", "macbeth", "king", "heart", "honest"]

def main():
    start_time = time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
    dictionary = map_reduce(globalListOfDocs, words)
    end_time = time.clock_gettime(time.CLOCK_MONOTONIC_RAW)
    print_dict(dictionary)
    totalTime = end_time - start_time
    print("Total elapsed time: ", totalTime)

def count_occur(globalListOfDocs, words, word_count):
    file = open(globalListOfDocs, 'r')
    for occur in words:
        occurrences = re.findall(occur, file.read(), re.IGNORECASE)
        word_count[occur] += len(occurrences)
        file.seek(0, 0)

def map_reduce(globalListOfDocs, words):
    # get the world communicator
    comm = MPI.COMM_WORLD

    # get the size of the communicator in # processes
    thread_count = comm.Get_size()

    # get our rank (process #)
    rank = comm.Get_rank()

    word_dict = {}

    if rank == 0:
        for word in words:
            word_dict[word] = 0

        if thread_count >= 2:
            for i in range(1, thread_count):
                comm.send(word_dict, dest=i)

    else:
        word_dict = comm.recv(source=0)

    document_count = len(globalListOfDocs)

    doc_thread_assignment = int(document_count / thread_count)

    doc_index = doc_thread_assignment * rank

    assign_limit = int(doc_thread_assignment * (rank + 1))

    # distribute docs
    for iter in range(doc_index, assign_limit):
        count_occur(globalListOfDocs[iter], words, word_dict)
    # nonzero threads sends count
    if rank != 0:
        comm.send(word_dict, dest=0, )
    # sum all counts
    elif rank == 0:
        complete_dict = word_dict
        if thread_count >= 2:
            for msg in range(1, thread_count):
                msg = comm.recv(source=msg)
                for curr_item, count in msg.items():
                    if curr_item not in complete_dict:
                        complete_dict[curr_item] = count
                    else:
                        complete_dict[curr_item] += count
        return complete_dict

def print_dict(dictionary):
    print("\nOccurrences found of the following words:")
    for target_word in dictionary:
        print(target_word, ":", dictionary[target_word])


if __name__ == "__main__":
    main()