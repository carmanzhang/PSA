import snowballstemmer

snowballstemmer = snowballstemmer.stemmer('english')  # faster than others


class Stemmer:
    @staticmethod
    def stem(word):
        return snowballstemmer.stemWords([word])[0]
