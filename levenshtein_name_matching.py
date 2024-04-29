import os
import pandas as pd
from tqdm import tqdm
import csv
import multiprocessing
from multiprocessing import Pool
from Levenshtein import ratio
import timeit

os.chdir('/Users/mstudio/Library/CloudStorage/Box-Box/us_election_2020/Jay-toxicity/parallel-results')

df1 = pd.read_csv('set_lower_twitter.csv')
df2 = pd.read_csv('set_lower_parler.csv')

set_lower_twitter = df1['username'].tolist()
set_lower_parler = df2['username'].tolist()

def process_levenshtein(combination):
    if ratio(combination[0], combination[1]) >= 0.9:
        return (combination[0], combination[1])
    else:
        pass

def generate_combination(list1, list2):
    for item1 in list1:
        for item2 in list2:
            combination = (item1, item2)
            yield combination

def main():
    combination = generate_combination(set_lower_twitter, set_lower_parler)
    results_list = []
    num_cores = multiprocessing.cpu_count()
    time_results = []
    pool = Pool(num_cores)
    chunk_size = 2000  # Adjust the chunk size as per your requirements

    with tqdm(total=len(set_lower_twitter) * len(set_lower_parler)) as pbar:
        results = pool.imap(process_levenshtein, combination, chunksize=chunk_size)
        for result in results:
            if result:
                results_list.append(result)
            pbar.update()

    return results_list

if __name__ == '__main__':
    multiprocessing.freeze_support()
    start_time = timeit.default_timer()
    outputs = main()
    end_time = timeit.default_timer()
    execution_time = (end_time - start_time) / 3600

    with open('parallel-results-mac.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for output in outputs:
            writer.writerow(output)

    print(f"Execution time: {execution_time} hours")
