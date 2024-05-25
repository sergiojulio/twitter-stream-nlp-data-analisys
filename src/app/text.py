import re
from textblob import TextBlob


def clean_tweet(tweet):
    stopwords = ["for", "on", "an", "a", "of", "and", "in", "the", "to", "from", "#"]
    temp = tweet.lower()
    temp = re.sub("'", "", temp) 
    temp = re.sub("@[A-Za-z0-9_]+","", temp)
    #temp = re.sub("#[A-Za-z0-9_]+","", temp)
    temp = re.sub(r'http\S+', '', temp)
    temp = re.sub('[()!?]', ' ', temp)
    temp = re.sub('\[.*?\]',' ', temp)
    temp = re.sub("[^a-z0-9]"," ", temp)
    temp = temp.split()
    temp = [w for w in temp if not w in stopwords]
    temp = " ".join(word for word in temp)
    return temp


def polarity(string):

    if type(string) != str:
        return 0

    blob = TextBlob(string)
    p = c = i = 0
    for sentence in blob.sentences:
        c = sentence.sentiment.polarity + c
        i += 1
        
    if i > 0:
        p = c / i
        p = round(p,2)
    else:
        p = 0

    return p

content ='Fast Politics with Molly Jong-Fast - Rick Wilson, Sen. Bernie Sanders &amp; Nikki McCann Ramirez: https://omny.fm/shows/fast-politics-with-molly-jong-fast/rick-wilson-sen-bernie-sanders-nikki-mccann-ramireRick Wilson of The Lincoln Project skewers MTGs latest embarrassment in Congress. Senator Bernie Sanders examines how progressives can come back to supporting Joe Biden. Nikki McCann Ramirez of Rolling Stone gives us a survey of the personality of John McEntee, a likely Trump administration official who loves bragging about his cruelty to others. See omnystudio.com/listener for privacy information. https://omny.fm/shows/fast-politics-with-molly-jong-fast/rick-wilson-sen-bernie-sanders-nikki-mccann-ramire #fastpolitics #mtg #BernieSanders #trump #TrumpTrial #maga #fascism #StopFascism #VoteBlue #BidenHarris #bidenharris2024 #biden #SaveDemocracy'

text = clean_tweet(content)

print(text)

print(polarity(text))

             
