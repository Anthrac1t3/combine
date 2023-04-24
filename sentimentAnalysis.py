import csv
from textblob import TextBlob

# Load CSV file
csv_file_path = "upload/alone-clean.csv"  # Replace with your CSV file path
tweets = []
with open(csv_file_path, "r") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        tweets.append(row["content"])

# Perform sentiment analysis and assign labels
sentiment_labels = []
for tweet in tweets:
    analysis = TextBlob(tweet)
    polarity = analysis.sentiment.polarity
    if polarity > 0:
        sentiment_labels.append("positive")
    elif polarity < 0:
        sentiment_labels.append("negative")
    else:
        sentiment_labels.append("neutral")

# Write results to a new CSV file
output_csv_file_path = "alone_results.csv"  # Replace with your desired output file path
with open(output_csv_file_path, "w", newline="") as csvfile:
    fieldnames = ["text", "sentiment"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for i in range(len(tweets)):
        writer.writerow({"text": tweets[i], "sentiment": sentiment_labels[i]})

print("Sentiment analysis completed. Results written to", output_csv_file_path)
