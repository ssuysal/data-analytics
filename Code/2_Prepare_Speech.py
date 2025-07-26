import os
import pandas as pd
import nltk
import time
import gc
import traceback
import ssl

# Change: to avoid SSL errors
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# Change: imports should come after download
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('universal_tagset')
nltk.download('averaged_perceptron_tagger')

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import *
from concurrent.futures import ThreadPoolExecutor

parent = os.path.dirname(os.getcwd())
INPUT_FOLDER_PATH = os.path.join(parent, 'Input')
FINAL_FOLDER_PATH = os.path.join(parent, 'Final')
processed_data = "processeddata.csv"

# get the set of stopwords in English
stop_words = set(stopwords.words("english"))

# create a list of selected types of words
typeselect = ['ADV', 'VERB', 'NOUN']

# Change: slightly-changed the banned words
banned_words = ['i', 'thank', 'you', 'canada', 'minister', 'speaker', 'mr.', 'mrs.', 'ms.', 'madame', 'hon', 'dear']

# minimum length of speeches
word_count = 1000

# Stemming
stem = True

# no sampling, could be set to 0.1 or 0.25
sampling = 1

seed = 123

stemmer = PorterStemmer()


def create_folder_if_not_exists(folder_name):
    path = os.path.join(folder_name)
    if not os.path.exists(path):
        os.makedirs(path)


def load_data(year, step, wordcount):
    df = pd.read_excel(os.path.join(INPUT_FOLDER_PATH, 'Speeches' + str(year) + '_' + str(step) + '.xlsx'))
    df = df[~df['speakername'].isnull()]
    df = df[~df['speechtext'].isnull()]
    s = pd.Series(df['speechdate'])
    s2 = s.str.split(r"-", n=-1, expand=True)
    year = s2.iloc[:, 0]
    year = pd.DataFrame(year)
    year.columns = ['year']
    document = pd.merge(df, year, left_index=True, right_index=True)
    document = document[['basepk', 'speechtext', 'speakername', 'year']]
    document.dropna(inplace=True)
    document.drop_duplicates(subset=['speechtext'], inplace=True)
    document["speechlength"] = document["speechtext"].str.len()
    document = document.loc[document["speechlength"] > wordcount]
    gc.collect()
    del df
    return document


def clean_string(text, wordlist, stem=False):
    text = text.lower()
    tokenized_word = word_tokenize(text)

    # create an empty list to store filtered words
    filtered_list = []
    for w in tokenized_word:
        # check if the word is not a stopword and not in the wordlist
        if w not in stop_words and w not in wordlist:
            # stem the word if it's set to true
            if stem:
                w = stemmer.stem(w)
            # add the word to the filtered list
            filtered_list.append(w)

    # perform part-of-speech tagging on the filtered list of words
    words = nltk.pos_tag(filtered_list, tagset='universal')

    # convert the tagged words into a DataFrame object
    w = pd.DataFrame(words)
    w.columns = ['word', 'type']
    # keep only the rows where the type of word is in the selected list
    rslt_df = w.loc[w['type'].isin(typeselect)]
    h = list(rslt_df['word'])
    sentence = ' '.join(h)
    # return the cleaned string
    return sentence


def pre_process_data(year, step, wordcount, wordlist, stem, sampling):
    # Change: read the data for the complete decade
    total = load_data(year, step, wordcount)
    data = total.sample(frac=sampling, replace=False, random_state=seed)
    rc = data.columns.get_loc("speechtext")
    smp = pd.DataFrame(data)
    lendoc = len(smp)
    print("We load " + str(lendoc) + " speeches from " + str(year) + "_" + str(step))
    for r in range(0, lendoc, 1):
        # Change: to avoid "AssertionError: Multiple entries for original tag:" error in clean_string
        try:
            if r % 100 == 0 and r > 0:
                p = r / lendoc
                print('{:2.2%}'.format(p) + " in year " + str(year) + "_" + str(step))
            smp.iat[r, rc] = smp.iat[r, rc].lower()
            smp.iat[r, rc] = re.sub(r'[0-9]', "", smp.iat[r, rc])
            smp.iat[r, rc] = clean_string(smp.iat[r, rc], wordlist, stem)
        except:
            traceback.print_exc()
    return smp


def pre_process_speech(year, step):
    print("Preprocessing with sampling : " + str(sampling) + " " + str(year) + " " + str(step))
    begin = time.time()
    df = pre_process_data(year, step, word_count, banned_words, stem, sampling)
    print("Duration of preprocessing year " + str(year) + "_" + str(step) + ": " + str('{:8.2f}'.format(time.time() - begin)) + " sec.")
    return df



create_folder_if_not_exists(FINAL_FOLDER_PATH)

# List to store the future objects returned by the ThreadPoolExecutor
futures = []

# Specify the decades range
min_year = 1980
max_year = 2020
step = 10

# Change: using ThreadPoolExecutor for faster execution
with ThreadPoolExecutor() as executor:
    for year in range(min_year, max_year, step):
        for step in range(0, 4, 1):
            future = executor.submit(pre_process_speech, year, step)
            futures.append(future)

# Change: wait for all the futures to complete and retrieve their results
document = pd.DataFrame()
for future in futures:
    df = future.result()
    frame = [document, df]
    document = pd.concat(frame)

document.to_csv(os.path.join(FINAL_FOLDER_PATH, processed_data), index=False, sep=";")
print("Done")
