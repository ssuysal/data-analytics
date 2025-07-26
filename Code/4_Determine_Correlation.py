import re
import numpy as np
import pandas as pd
import Levenshtein as lev
import os

sampling = 1
seed = 123
num_topics = 10  # maximum number of topics in each cluster
upper_limit = 1

speakername_col = 'speakername'

parent = os.path.dirname(os.getcwd())
FINAL_FOLDER_PATH = os.path.join(parent, 'Final')
INPUT_FOLDER_PATH = os.path.join(parent, 'Input')

mp_data = pd.read_csv(os.path.join(FINAL_FOLDER_PATH, 'MPData.csv'), sep=';', low_memory=False)
mp_data.rename(columns={'Date of Death (yyyy-mm-dd):': 'DateDeath'}, inplace=True)
mp_data['deathyear'] = mp_data['DateDeath'].str.extract('(\d{4})').fillna(0).astype(int)
mp_data = mp_data[(mp_data['deathyear'] >= 1980) | (mp_data['deathyear'] == 0)]
mp_data['name_lower'] = mp_data['Name'].str.lower()

### Read existing files
fed_results = pd.read_csv(os.path.join(INPUT_FOLDER_PATH, 'FED_Results.csv'), sep=';', low_memory=False)
final = pd.read_csv(os.path.join(FINAL_FOLDER_PATH, 'Final.csv'), sep=';', low_memory=False)
clusters_df = pd.read_csv(os.path.join(FINAL_FOLDER_PATH, 'NLTK_Cluster_10.csv'), low_memory=False)
predictions = pd.read_csv(os.path.join(FINAL_FOLDER_PATH, 'Prediction.csv'), low_memory=False)
predictions = predictions.sample(frac=sampling, replace=False, random_state=seed)


# Function to clean the names, e.g. "(Minister of ...)"
def clean_name(name):
    # Remove everything inside and including brackets
    cleaned = re.sub(r'\(.*\)', '', name)
    # Remove multiple spaces and strip the string
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def find_matching_mp_id(name):
    lowercase_name = name.lower()
    distances = [lev.distance(lowercase_name, mp_name) for mp_name in mp_data['name_lower']]
    min_distance_idx = np.argmin(distances)
    min_distance = distances[min_distance_idx]
    mp_id = mp_data.iloc[min_distance_idx]['MP_ID']
    return mp_id, min_distance


# Change: remove these from speakernames
prefixes = ["Mr.", "Ms.", "Mrs.", "The", "Hon.", "Dr."]

# "Some Hon. Members:" cannot be a valid speakername
predictions = predictions[~predictions[speakername_col].str.contains(':')]

# Remove clusters 1 and 4 (procedure)
predictions = predictions.loc[~predictions['prediction'].isin([1])]


# Change: Check if any entry in the Name column contains @
# to remove suffixes such as M.P, P.C. etc. they exist
if (predictions[speakername_col].str.contains('@')).any():
    predictions[speakername_col] = predictions[speakername_col].str.split('@').str.get(0)

predictions[speakername_col] = predictions[speakername_col].apply(clean_name)

# Remove prefixes
for item in prefixes:
    pattern = r'\s*{}\s+'.format(item)
    predictions[speakername_col] = predictions[speakername_col].str.replace(pattern, '', case=False, regex=True)

predictions['MP_ID'], predictions['Similarity'] = zip(*predictions[speakername_col].map(find_matching_mp_id))
initial_size = predictions.size

# Filter out the ones with similarity > 1, only one character difference should not impact the equality
predictions = predictions[predictions['Similarity'] <= 1]
new_size = predictions.size
print("Filtered " + str(initial_size - new_size) + " values")

predictions = predictions.drop(columns=['Similarity'])
mp_id_column = predictions.pop('MP_ID')
predictions.insert(0, 'MP_ID', mp_id_column)
predictions.sort_values(['MP_ID'], inplace=True)

predictions.to_csv(os.path.join(FINAL_FOLDER_PATH, 'predictions_with_MP_ID.csv'), index=False, sep=';')
#################################################################
predictions = pd.read_csv(os.path.join(FINAL_FOLDER_PATH, 'predictions_with_MP_ID.csv'), sep=';', low_memory=False)


final = final[(final['electionyear'] >= 1980)]
final.rename(columns={'Date of Death (yyyy-mm-dd):': 'DateDeath'}, inplace=True)
final['deathyear'] = final['DateDeath'].str.extract('(\d{4})').fillna(0).astype(int)
final = final[(final['deathyear'] >= 1980) | (final['deathyear'] == 0)]

# Cleaning
final.dropna(subset=['Years of Service:'], inplace=True)
final = final[final['Years of Service:'].str.contains(' days')]
# Use a regular expression to extract the number of days from Years of Service column
final['duration_of_service'] = final['Years of Service:'].str.split(' days').str[0].astype(int)
print(final.head())

df = final.merge(predictions, on='MP_ID')
print(df.head())

# Change: replace NaN values with 0 in electionyear
df['electionyear'] = df['electionyear'].fillna(0)

# The time difference between electionyear and year should be min=0, max=given amount
df = df[(df['electionyear'] - df['year'] <= upper_limit) & (df['electionyear'] - df['year'] >= 0)]
print(df.head())

# Convert clusters_df to a dictionary where each key is the cluster and the value is a list of topics
dominant_topics = clusters_df.to_dict(orient='list')

# Convert keys to integers
dominant_topics_int_keys = {int(k): v for k, v in dominant_topics.items()}

# For each topic, create a new column in the df dataframe
for i in range(1, num_topics + 1):
    col_name = f'topic_{i}'
    df[col_name] = df['prediction'].apply(lambda x: dominant_topics_int_keys[x][i - 1])

df['result_binary'] = df['Result'].apply(lambda x: 1 if x == "Elected" else 0)
df.dropna(subset=['Name'], inplace=True)
df.dropna(subset=['FED_ID'], inplace=True)
df.dropna(subset=['electionyear'], inplace=True)
df['FED_ID'] = df['FED_ID'].astype(int)
df['uid'] = df['Name'] + '#' + df['FED_ID'].astype(str) + '#' + df['electionyear'].astype(str)

# Only consider General elections, otherwise duplicates will occur when Year and Constituency are combined
fed_results = fed_results[(fed_results['Type'] == 'General')]
fed_results = fed_results[(fed_results['Year'] >= 1980)]
fed_results.dropna(subset=['Name'], inplace=True)
fed_results.dropna(subset=['FED_ID'], inplace=True)
fed_results.dropna(subset=['Year'], inplace=True)

# Create a unique identifier to avoid possible duplicates (within name and/or year), there could be same name with different province and different FED_ID
# e.g. Timiskaming (Name) 1935 (Year) 9227 (FED_ID); Timiskaming(Name)	1917 (Year) 9226 (FED_ID)
fed_results['uid'] = fed_results['Name'] + '#' + fed_results['FED_ID'].astype(str) + '#' + fed_results['Year'].astype(str)

# 2. Sum all Votes* columns
votes_columns = [col for col in fed_results.columns if 'Votes' in col and col[-1].isdigit()]
fed_results['total_votes'] = fed_results[votes_columns].sum(axis=1)

fed_results = fed_results[['Name', 'total_votes', 'uid']]

has_duplicates = fed_results['uid'].duplicated().any()
print(has_duplicates)  # should be false

df = df.merge(fed_results, on=['uid'])

# cleaning
df.drop(columns=['Name_y'], inplace=True)
df.dropna(subset=['total_votes'], inplace=True)
df.dropna(subset=['Votes'], inplace=True)

# avoid division by zero
df = df[df['total_votes'] != 0]
df = df[df['Votes'] != 0]
df['perc_votes'] = df['Votes'] / df['total_votes']

df.to_csv(os.path.join(FINAL_FOLDER_PATH, 'df_intermediate_' + str(upper_limit) + '.csv'), index=False, sep=';')
#################################################################
df = pd.read_csv(os.path.join(FINAL_FOLDER_PATH, 'df_intermediate_' + str(upper_limit) + '.csv'), sep=';', low_memory=False)

print(df.head())


def compute_frequencies(mp_id, election_year):
    subset = df[(df['MP_ID'] == mp_id) &
                         ((election_year - df['year']) <= upper_limit) &
                         (election_year - df['year'] >= 0)]
    frequencies = subset['prediction'].value_counts(normalize=True)
    # Ensure all topics are covered by reindexing with a range and filling NaN with 0
    return frequencies.reindex(range(10), fill_value=0)

# Using the apply method to get topic frequencies
topics_df = df.apply(lambda x: compute_frequencies(x['MP_ID'], x['electionyear']), axis=1)

# Renaming the columns (taking into account clusters start from 0)
topics_df.columns = [f'cluster_freq_{i}' for i in range(10)]

# Concatenate the original df with the topics_df
df = pd.concat([df, topics_df], axis=1)

df.to_csv(os.path.join(FINAL_FOLDER_PATH, 'data_' + str(upper_limit) + '_with_topic_frequencies.csv'), index=False, sep=';')
print("Done")