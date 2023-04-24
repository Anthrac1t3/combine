import os
import pandas as pd


def remove_undefined_unicode_and_quotes(input_file, output_file):
    charactersToRemove = ['"', "“", "”", "'"]

    # Read input .csv file using pandas
    df = pd.read_csv(input_file, encoding='utf-8')
    # Remove undefined Unicode characters from the specified column
    df.iloc[:, 2] = df.iloc[:, 2].apply(lambda x: ''.join([
        c for c in x if c.isprintable() and c not in charactersToRemove]))

    # Write cleaned data to output .csv file
    df.to_csv(output_file, encoding='utf-8', index=False)

filesToClean = ["alone", "depressed", "loneliness", "vacation", "stressed", "blissful", "outing", "travel", "bored", "happy", "sad", "joyful", "food", "snack"]

for file in filesToClean:
    # Input and output file paths
    inputFile = 'results/' + file + '.csv'
    outputFile = 'upload/' + file + '-clean.csv'

    # Call the function to remove undefined Unicode characters and double quotes
    remove_undefined_unicode_and_quotes(inputFile, outputFile)

    print("Quotes removed from column 3. Updated data written to", outputFile)
