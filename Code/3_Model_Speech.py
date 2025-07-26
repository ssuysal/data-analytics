import os
import pandas as pd
import time
import joblib
import math
import matplotlib.pyplot as plt

from nltk.tokenize import RegexpTokenizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from tqdm import tqdm

sampling = 1
cluster_groups = 10
ngrams = 2
relevant_column = 'speechtext'
seed = 123

parent = os.path.dirname(os.getcwd())
FINAL_FOLDER_PATH = os.path.join(parent, 'Final')

processed_data = os.path.join(FINAL_FOLDER_PATH, "processeddata.csv")
prediction_file = os.path.join(FINAL_FOLDER_PATH, "Prediction.csv")
vectorizer_path = os.path.join(FINAL_FOLDER_PATH, "vectorizer.sav")
model_path = os.path.join(FINAL_FOLDER_PATH, "NLTK_Model.sav")
cluster_path = os.path.join(FINAL_FOLDER_PATH, "NLTK_Cluster_" + str(cluster_groups) + ".csv")

# define a custom tokenizer function that uses a RegexpTokenizer
tokenizer = RegexpTokenizer('[a-zA-Z]+')


def create_folder_if_not_exists(folder_name):
    path = os.path.join(folder_name)
    if not os.path.exists(path):
        os.makedirs(path)


def model_speech(min_year, max_year):
    print("Processing Years: " + str(min_year) + " " + str(max_year))
    beg = time.time()

    total = pd.read_csv(processed_data, sep=";", low_memory=False)
    total = total.sample(frac=sampling, replace=False, random_state=seed)

    # Create a tokenizer using the TfidfVectorizer class, which converts text data into a matrix of numerical values
    vectorizer = TfidfVectorizer(lowercase=True, stop_words='english', tokenizer=tokenizer.tokenize,
                                 ngram_range=(1, ngrams))

    tfidf = vectorizer.fit(total[relevant_column])
    joblib.dump(tfidf, vectorizer_path)
    text_counts = vectorizer.fit_transform(total[relevant_column])

    print("Duration of transformation: " + str('{:8.2f}'.format(time.time() - beg) + " sec."))
    print("We have in total " + str(text_counts.shape[1]) + " words in " + str(text_counts.shape[0]) + " speeches")

    model = KMeans(n_clusters=cluster_groups, init='k-means++', max_iter=300, n_init=10)
    model.fit(text_counts)
    joblib.dump(model, model_path)
    print("Duration of training: " + str('{:8.2f}'.format(time.time() - beg) + " sec."))

    order_centroids = model.cluster_centers_.argsort()[:, ::-1]
    terms = vectorizer.get_feature_names_out()
    print("terms " + str(terms))

    word_list = []
    for ind in order_centroids[0, :10]:
        print('%s' % terms[ind])
        word_list.append(terms[ind])
    df = pd.DataFrame(word_list)

    for i in range(1, cluster_groups):
        word_list = []
        # Find the top 10 important words in the current cluster and create a DataFrame from them
        for ind in order_centroids[i, :10]:
            print('%s' % terms[ind])
            word_list.append(terms[ind])
        df_app = pd.DataFrame(word_list)
        final = pd.concat([df, df_app], axis=1)
        df = final

    # Set the column names of the DataFrame to the cluster numbers
    df.columns = list(range(0, cluster_groups))
    df.to_csv(cluster_path, index=False)

    ### Predicting:
    vectorizer = joblib.load(open(vectorizer_path, 'rb'))
    model = joblib.load(open(model_path, 'rb'))

    total = pd.read_csv(processed_data, delimiter=";", low_memory=False)

    # Transform the text data in the relevant column of `total` into a numerical matrix using the previously fitted vectorizer
    text_counts = vectorizer.transform(total[relevant_column].values.astype('U'))

    # Use the previously trained model to predict the cluster assignments for each row of the transformed text_counts matrix, and store the predicted cluster assignments in a new column called "prediction" in the `total` DataFrame
    total["prediction"] = model.predict(text_counts)

    # Save the `total` DataFrame to a CSV file
    total.to_csv(prediction_file, index=False)

    # Print a message indicating that a plot of the cluster history across time will be produced
    print('Produce Plot of Cluster History across Time')

    # Group the `total` DataFrame by year and predicted cluster assignment, and count the number of speeches in each group
    plotfile = total.groupby(["year", "prediction"], as_index=False).count()

    # Group the `plotfile` DataFrame by year, and sum the number of speeches in each year
    group_by = plotfile.groupby('year').sum()

    # Create a dictionary mapping each year to the total number of speeches in that year
    dict_sums = dict(zip(group_by.index, group_by.basepk))

    # Add a new column to the `plotfile` DataFrame containing the fraction of speeches in each group relative to the total number of speeches in the corresponding year
    plotfile['year_sum'] = plotfile['year'].apply(lambda x: dict_sums[x])
    plotfile['fraction'] = plotfile['basepk'] / plotfile['year_sum']

    # Define the plot based on the number of clusters
    if cluster_groups == 40:
        fig, axes = plt.subplots(5, 8, figsize=(50, 25), sharex=True)
    if cluster_groups == 20:
        fig, axes = plt.subplots(4, 5, figsize=(30, 15), sharex=True)
    if cluster_groups == 10:
        fig, axes = plt.subplots(2, 5, figsize=(30, 15), sharex=True)

    # Flatten the `axes` array to a 1D array
    axes = axes.flatten()

    # Determine the maximum share of speeches in any cluster, and round up to the nearest tenth
    max_share = plotfile['fraction'].max()
    max_share = math.ceil(max_share * 10) / 10

    # Determine the minimum and maximum years in the `total` DataFrame, and set the x and y limits of the subplots accordingly
    min_year = total['year'].min()
    max_year = total['year'].max()
    plt.setp(axes, xlim=(min_year, max_year), ylim=(0, max_share))

    # Create a subplot for each cluster, and plot the proportion of speeches in that cluster for each year
    for topic_idx in range(0, cluster_groups, 1):
        years = plotfile[plotfile['prediction'] == topic_idx]['year'].tolist()
        shares = plotfile[plotfile['prediction'] == topic_idx]['fraction'].tolist()

        ax = axes[topic_idx]
        ax.plot(years, shares)
        ax.tick_params(axis="both", which="major", labelsize=10)
        for i in "top right left".split():
            ax.spines[i].set_visible(False)

    fig.suptitle('Frequency of Clusters', fontsize=20)
    plt.subplots_adjust(top=0.90, bottom=0.05, wspace=0.90, hspace=0.3)

    cluster_freq_path = os.path.join(FINAL_FOLDER_PATH, "Cluster_Frequency_" + str(min_year) + "_" + str(max_year) + ".png")
    plt.savefig(cluster_freq_path, dpi=300)

    ### Plotting Cluster Frequencies over time
    print('Produce word frequency plot')

    # read in a CSV file using pandas and store it in the clusters variable
    clusters = pd.read_csv(cluster_path)

    index_list = list(range(0, cluster_groups * 2, 1))
    index_list = [str(x) for x in index_list]
    freqs_df = pd.DataFrame(columns=[index_list])
    j = 0

    # loop through each topic
    for topic in tqdm(range(0, cluster_groups, 1)):
        # filter the total dataframe to get all speeches with the current topic prediction
        speeches = total[total['prediction'] == topic][relevant_column]
        # concatenate all speeches into one string
        string = ""
        for i in range(0, len(speeches)):
            string = string + ' ' + str(speeches.iat[i])
        string = string + " "

        # get the list of words for the current topic from the clusters dataframe
        wordlist = clusters[str(topic)].tolist()
        # count the number of occurrences of each word in all speeches
        freqs = []
        for word in wordlist:
            # add spaces to the beginning and end of each word to avoid counting partial words
            word = " " + word + " "
            # count the number of occurrences of the current word in the concatenated string
            freq = string.count(word)
            freqs.append(freq)

        # fill dataframe
        k = j + 1
        # fill the jth column of the dataframe with the list of words for the current topic
        freqs_df.iloc[:, j] = wordlist
        # fill the kth column of the dataframe with the list of frequencies for the current topic
        freqs_df.iloc[:, k] = freqs
        j += 2

    # Create a list of odd numbers from 1 to (cluster_groups*2)-1
    index_list = list(range(1, cluster_groups * 2, 2))

    # Normalize the word frequencies in the dataframe by dividing each column by its sum
    for i in index_list:
        sum = freqs_df.iloc[:, i].sum()
        freqs_df.iloc[:, i] = freqs_df.iloc[:, i] / sum

    # Plot the word frequencies based on the number of cluster groups
    # Create subplots with different sizes based on the number of cluster groups
    # Flatten the axes to a 1D array for easy iteration
    # For each cluster group, get the top features and their weights from the dataframe
    # Create a horizontal bar plot with the top features as the y-axis and their weights as the x-axis
    # Invert the y-axis to show the most representative words at the top
    if cluster_groups == 40:
        fig, axes = plt.subplots(5, 8, figsize=(50, 25), sharex=True)
    if cluster_groups == 20:
        fig, axes = plt.subplots(4, 5, figsize=(30, 15), sharex=True)
    if cluster_groups == 10:
        fig, axes = plt.subplots(2, 5, figsize=(30, 15), sharex=True)

    axes = axes.flatten()
    for topic_idx in range(0, cluster_groups, 1):
        top_features = freqs_df.iloc[:, index_list[topic_idx] - 1].tolist()
        weights = freqs_df.iloc[:, index_list[topic_idx]].tolist()

        ax = axes[topic_idx]
        ax.barh(top_features, weights, height=0.7)
        ax.invert_yaxis()
        ax.tick_params(axis="both", which="major", labelsize=10)
        for i in "top right left".split():
            ax.spines[i].set_visible(False)
        fig.suptitle(
            'Frequency of the Most Representative Word in each Cluster (' + str(min_year) + ', ' + str(max_year) + ')',
            fontsize=20)

    plt.subplots_adjust(top=0.90, bottom=0.05, wspace=0.90, hspace=0.3)
    word_freq_path = os.path.join(FINAL_FOLDER_PATH, "Word_Frequency_" + str(min_year) + "_" + str(max_year) + ".png")
    plt.savefig(word_freq_path, dpi=300)
    return total


model_speech(1980, 2019)
print("Done")