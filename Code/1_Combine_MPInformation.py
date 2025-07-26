import glob
import traceback

import matplotlib.pyplot as plt
import pandas as pd
import os


###
parent = os.path.dirname(os.getcwd())
final_folder = os.path.join(parent, 'Final')
output_folder = os.path.join(parent, 'Output')

# define paths
MPDATA = os.path.join(final_folder, 'MPData.csv')
ELECDATA = os.path.join(final_folder, 'ElecData.csv')
final_path = os.path.join(final_folder, 'Final.csv')

df_csv_append = pd.DataFrame()
csv_files = glob.glob(os.path.join(output_folder, 'MP_ID_[0-9]*.csv'))

# Change: remove these from parl. names
prefixes = ["Mr.", "Ms.", "Mrs.", "The", "Right", "Hon.", "Dr.", "Sir"]

# append all MP_ID Files
for file in csv_files:
    try:
        df = pd.read_csv(file, sep=';')
        df['MP_ID'] = file.replace(os.path.join(output_folder, 'MP_ID_'), '').replace('.csv', '')

        # Change: remove semicolons from Name column if it exists
        df['Name'] = df['Name'].str.replace(';', '')

        # Change: Check if any entry in the Name column contains a comma
        # to remove suffixes such as M.P, P.C., Q.C. etc.
        if (df['Name'].str.contains(',')).any():
            df['Name'] = df['Name'].str.split(',').str.get(0)

        # Change: use regex to remove prefix/suffix
        for item in prefixes:
            pattern = r'\s*{}\s+'.format(item)
            df['Name'] = df['Name'].str.replace(pattern, '', case=False, regex=True)
        # Change: Remove leading and trailing whitespaces
        df['Name'] = df['Name'].str.strip()
        frame = [df_csv_append, df]
        df_csv_append = pd.concat(frame)
    except:
        traceback.print_exc()
        print("No Way")

# rearrange columns:
first_column = df_csv_append.pop('MP_ID')
# insert this column infront:
df_csv_append.insert(0, 'MP_ID', first_column)

# rename column:
df_csv_append.rename(columns={'Date of Birth (yyyy-mm-dd):': 'DateBirth'}, inplace=True)
df_csv_append['DateBirth'] = df_csv_append['DateBirth'].astype(str)
df_csv_append['birthyear'] = df_csv_append['DateBirth'].str.extract('(\d+)').fillna(0).astype('int')

years = df_csv_append['birthyear'].values.tolist()
years = [i for i in years if i > 0]
plt.figure()
plt.hist(years)
plt.title('Distribution of Birth Years')
plt.show()
plt.close()

# save data
df_csv_append.to_csv(MPDATA, index=False, sep=";")

########################
# Append the Electoral History files
df_append = pd.DataFrame()
files = glob.glob(os.path.join(output_folder, 'ElectoralHistory_[0-9]*.xlsx'))

for file in files:
    try:
        df = pd.read_excel(file)
        df['MP_ID'] = file.replace(os.path.join(output_folder, 'ElectoralHistory_'), '').replace('.xlsx', '')
        frame = [df_append, df]
        df_append = pd.concat(frame)
    except:
        traceback.print_exc()
        print("No")

# rearrange columns
first_column = df_append.pop('MP_ID')
# insert column using insert(position,column_name,# first_column) function
df_append.insert(0, 'MP_ID', first_column)
df_append.sort_values(['MP_ID', 'Parliament'])

df_append.rename(columns={'Election Date': 'ElectionDate'}, inplace=True)
df_append['ElectionDate'] = df_append['ElectionDate'].astype(str)
df_append['electionyear'] = df_append['ElectionDate'].str.extract('(\d+)').fillna(0).astype('int')
# Change: filter out values with Election Type = By-Election
df_append = df_append[df_append['Election Type'] != 'By-Election']

# Some looks at the data:
years = df_append['electionyear'].values.tolist()
years = [i for i in years if i > 0]
plt.figure()
plt.hist(years)
plt.title('Distribution of Election Years')
plt.show()
plt.close()

# Save data:
df_append.to_csv(ELECDATA, index=False, sep=';')

####################
#  Get FEDs
####################

# initialize Data
df_append = pd.DataFrame()
files = glob.glob(os.path.join(output_folder, 'MP_ID_FED_[0-9]*.csv'))

# loop:
for file in files:
    try:
        df = pd.read_csv(file, sep=";", header=None).T
        cols = df.shape[1]
        df2 = df.iloc[:, 0]
        for i in range(cols):
            if i > 0:
                df3 = df.iloc[:, i]
                frame = [df2, df3]
                df2 = pd.concat(frame)
            else:
                print("only one column")
        df = df2.to_frame()
        df.columns = ['entry']
        df['entry'] = df['entry'].astype(str)
        df = df[df['entry'].str.contains('OrganizationId')]
        ID = df['entry'].str.extract('(\d+)').fillna(0).astype('int')
        NAME = df['entry'].to_frame()
        NAME = NAME['entry'].str.split(">", n=1, expand=True)
        NAME.columns = ['e1', 'e2']
        NAME = NAME['e2'].str.split("<", n=1, expand=True)
        frame = [ID, NAME]
        final = pd.concat(frame, axis=1)
        final.columns = ['FED_ID', 'Name', 'rest']
        final = final[['FED_ID', 'Name']]
        final = final.drop_duplicates()
        frame = [df_append, final]
        df_append = pd.concat(frame, ignore_index=True)
    except:
        traceback.print_exc()
        print("no")
df_append = df_append.drop_duplicates()

elec = pd.read_csv(ELECDATA, sep=";")
mp = pd.read_csv(MPDATA, sep=";")

# Change: avoid confusion with Constituency
mp.rename(columns={'Name': 'MP_Name'}, inplace=True)

final = elec.merge(df_append, left_on="Constituency", right_on="Name", how='left')
print(len(df_append['Name'].unique()))
print(len(df_append['FED_ID'].unique()))
print(len(final['Name'].unique()))

final = final.merge(mp, left_on="MP_ID", right_on="MP_ID", how='left')

final['Age_at_Election'] = final['electionyear'] - final['birthyear']
# Change: drop invalid values
final.dropna(subset=['Age_at_Election'], inplace=True)
# print(final['Age_at_Election'][final['Age_at_Election'] < 200].describe())

## Histograms
years = final['Age_at_Election'].values.tolist()
years = [i for i in years if i > 0 and i < 100]
plt.figure()
plt.hist(years, density=True, bins=20)
plt.xlim(0, 100)  # Set the x-axis limits
plt.title('Age Distribution at all Elections')
plt.show()
plt.close()

## Their first election:
first = final.sort_values(['MP_ID', 'Parliament'], ascending=[True, True])
first = first.loc[first.groupby(['MP_ID']).Age_at_Election.idxmin()].reset_index(drop=True)
years = first['Age_at_Election'].values.tolist()
years = [i for i in years if i > 0 and i < 100]
plt.figure()
plt.hist(years, density=True, bins=20)
plt.xlim(0, 100)  # Set the x-axis limits
plt.title('Age Distribution at first Election')
plt.show()
plt.close()

old = final['Age_at_Election'][(abs(final['Age_at_Election']) < 100) & (final['Age_at_Election'] > 0)]
old.plot.kde(bw_method=0.2)
young = first['Age_at_Election'][(abs(first['Age_at_Election']) < 100) & (first['Age_at_Election'] > 0)]
young.plot.kde(bw_method=0.2)

upp = final[final['Age_at_Election'] > 100]
down = final[final['Age_at_Election'] < 0]
frame = [upp, down]
extreme = pd.concat(frame)

final.to_csv(final_path, index=False, sep=";")
print("Done")