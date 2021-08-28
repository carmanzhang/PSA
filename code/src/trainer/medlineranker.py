"""Calculates feature scores from occurrence counts"""

from __future__ import division

import nltk
import numpy as np
import string
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer

from metric.all_metric import eval_metrics
from myio.data_reader import DBReader

stop_words = set(stopwords.words('english'))
punctuation = set(string.punctuation)
ps = PorterStemmer()


def update(obj, vars, exclude=['self']):
    """Update instance attributes (using a dictionary)

    Example: C{update(self, locals())}

    @param obj: Instance to update using setattr()
    @param vars: Dictionary of variables to store
    @param exclude: Variables to exclude, defaults to ['self']
    """
    for k, v in vars.items():
        if k not in exclude:
            setattr(obj, k, v)


def delattrs(obj, *vars):
    """Remove named attributes from instance

    Example: C{delattrs(self, "_property", "_other")}

    @param objs: Object to update via delattr
    @param vars: Instance attribute names to delete
    """
    for ivar in vars:
        try:
            delattr(obj, ivar)
        except AttributeError:
            pass


class FeatureMapping:
    """Persistent mapping between string features and feature IDs

    Feature types used with L{__getitem__}, L{get_type_mask} and
    L{add_article} are "mesh", "qual", "issn". A feature string could have more
    than one type.

    This is really a table with columns (id,type,name,count), and keys of id
    and (type,name).

    @ivar featfile: Path to text file with list of terms

    @ivar featfile_new: Temporary feature file used while writing

    @ivar numdocs: Number of documents used in creating the mapping

    @ivar features: List, such that features[id] == (name,type)

    @ivar feature_ids: Mapping, such that feature_ids[type][name] == id

    @ivar counts: List, such that counts[id] == number of occurrences.  For
    score calculation this is the only column needed.
    """

    def __init__(self):
        """Initialise the database, setting L{featfile}"""
        self.numdocs = 0
        self.features = []
        self.feature_ids = {}
        self.counts = []

    def __getitem__(self, key):
        """Given a feature ID, return (feature, feature type). Given (feature,
        feature type), returns feature ID"""
        if isinstance(key, int):
            return self.features[key]
        elif isinstance(key, tuple) and len(key) == 2:
            return self.feature_ids[key[1]][key[0]]
        else:
            raise KeyError("Invalid key: %s" % str(key))

    def __len__(self):
        """Return number of distinct features"""
        return len(self.features)

    def get_type_mask(self, exclude_types):
        """Get a mask for excluded features

        @param exclude_types: Types of features to exclude

        @return: Boolean array for excluded features (but returns None if
        exclude_types is None)
        """
        if not exclude_types:
            return None
        exclude_feats = np.zeros(len(self.features), np.bool)
        for ftype in exclude_types:
            for fid in self.feature_ids[ftype].itervalues():
                exclude_feats[fid] = True
        return exclude_feats

    def update_feature_map(self, **kwargs):
        """Add an article, given lists of features of different types.

        @note: Dynamically creates new features IDs and feature types as necessary.

        @param kwargs: Mapping from feature types to lists of features for that
        type. e.g. C{mesh=["Term A","Term B"]}

        @return: Numpy array of uint16 feature IDs
        """
        result = []
        self.numdocs += 1
        for ftype, fstrings in kwargs.items():
            if ftype not in self.feature_ids:
                self.feature_ids[ftype] = {}
            fdict = self.feature_ids[ftype]
            for feat in fstrings:
                if feat not in fdict:
                    featid = len(self.features)
                    self.features.append((feat, ftype))
                    self.counts.append(1)
                    fdict[feat] = featid
                else:
                    self.counts[fdict[feat]] += 1
                result.append(fdict[feat])
        return np.array(result, np.uint16)


class Storage(dict):
    """Dictionary supporting d.foo attribute access to keys.

    Raises AttributeError instead of KeyError when attribute-style access
    fails."""

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as k:
            raise AttributeError(k)

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        try:
            del self[key]
        except KeyError as k:
            raise AttributeError(k)

    def __str__(self):
        return "Storage(\n" + \
               "\n".join("   " + k + " = " + repr(v) + ","
                         for k, v in self.iteritems()) + "\n)"

    def __repr__(self):
        return '<Storage ' + dict.__repr__(self) + '>'


class FeatureScores(object):
    """Feature score calculation and saving, with choice of calculation method,
    and methods to exclude certain kinds of features.

    @group Set via constructor: featmap, pseudocount, mask, make_scores, get_postmask

    @ivar featmap: L{FeatureMapping} object

    @ivar pseudocount: Prior psuedocount to use for features, or None
    to use feature counts equal to Medline frequency.

    @ivar mask: Either None or a boolean array to mask some features scores
    to zero (this is to exclude features by category, not by score).

    @ivar make_scores: Method used to calculate the feature scores.

    @ivar get_postmask: Method used to calculate a dynamic mask
    array once the feature scores are known.


    @group Set by update: pos_counts, neg_counts, pdocs, ndocs, prior

    @ivar pos_counts: Array of feature counts in positive documents

    @ivar neg_counts: Array of feature counts in negatives documents

    @ivar pdocs: Number of positive documents

    @ivar ndocs: Number of negative documents

    @ivar prior: Bayes prior to add to the score.  If None, estimate
    using the ratio of relevant to irrelevant articles in the data.


    @group Set via make_scores: scores, pfreqs, nfreqs, base

    @ivar scores: Score of each feature

    @ivar pfreqs: Numerator of score fraction

    @ivar nfreqs: Denominator of score fraction

    @ivar base: Value to be added to all article scores
    """

    def __init__(self,
                 featmap,
                 pseudocount=None,
                 mask=None,
                 get_postmask=None):
        """Initialise FeatureScores object (parameters are instance variables)"""
        # if isinstance(get_postmask, np.basestring):
        # get_postmask = getattr(self, get_postmask)
        prior = 0
        update(self, locals())

    def scores_of(self, featdb, pmids):
        """Calculate vector of scores given an iterable of PubMed IDs.

        @param featdb: Mapping from PMID to feature vector
        @param pmids: Iterable of keys into L{featdb}
        @return: Vector containing document scores corresponding to the pmids.
        """
        off = self.base + self.prior
        sc = self.scores
        return np.array([off + np.sum(sc[featdb[d]]) for d in pmids], np.float32)

    def __len__(self):
        """Number of features"""
        return len(self.featmap)

    def update(self, pos_counts, neg_counts, pdocs, ndocs, prior=None):
        """Change the feature counts and numbers of documents, clear
        old score calculations, and calculate new scores."""
        if prior is None:
            if pdocs == 0 or ndocs == 0:
                prior = 0
            else:
                prior = np.log(pdocs / ndocs)
        base = 0
        update(self, locals())
        self.scores_bayes()
        # self._mask_scores()
        # delattrs(self, "_stats")

    def scores_bayes(s):
        """Document generated using multivariate Bernoulli distribution.

        Feature non-occurrence is modeled as a base score for the
        document with no features, and an adjustment to the
        feature occurrence scores."""
        s._make_pseudovec()
        # Posterior term frequencies in relevant articles
        s.pfreqs = (s.pseudocount + s.pos_counts) / (1 + s.pdocs)
        # Posterior term frequencies in irrelevant articles
        s.nfreqs = (s.pseudocount + s.neg_counts) / (1 + s.ndocs)
        # Support scores for bernoulli successes
        s.present_scores = np.log(s.pfreqs / s.nfreqs)
        # Support scores for bernoulli failures
        s.absent_scores = np.log((1 - s.pfreqs) / (1 - s.nfreqs))
        # Conversion to base score (no terms) and occurrence score
        s.base = np.sum(s.absent_scores)
        s.scores = s.present_scores - s.absent_scores

    def _make_pseudovec(s):
        """Calculates a pseudocount vector based on background frequencies
        if no constant pseudocount was specified"""
        if s.pseudocount is None:
            s.pseudocount = \
                np.array(s.featmap.counts, np.float32) / s.featmap.numdocs

    @property
    def stats(self):
        """A Storage instance with statistics about the features

        The following keys are present:
            - pos_occurrences: Total feature occurrences in positives
            - neg_occurrences: Total feature occurrences in negatives
            - feats_per_pos: Number of features per positive article
            - feats_per_neg: Number of features per negative article
            - distinct_feats: Number of distinct features
            - pos_distinct_feats: Number of of distinct features in positives
            - neg_distinct_feats: Number of of distinct features in negatives
        """
        try:
            return self._stats
        except AttributeError:
            pass
        s = Storage()
        s.pdocs = self.pdocs
        s.ndocs = self.ndocs
        s.num_feats = len(self)
        s.pos_occurrences = int(np.sum(self.pos_counts))
        s.feats_per_pos = 0.0
        if self.pdocs > 0:
            s.feats_per_pos = s.pos_occurrences / s.pdocs
        s.neg_occurrences = int(np.sum(self.neg_counts))
        s.feats_per_neg = 0.0
        if self.ndocs > 0:
            s.feats_per_neg = s.neg_occurrences / s.ndocs
        s.pos_distinct_feats = len(np.nonzero(self.pos_counts)[0])
        s.neg_distinct_feats = len(np.nonzero(self.neg_counts)[0])
        self._stats = s
        return self._stats


def FeatureCounts(nfeats, featdb, docids):
    """Count occurrenes of each feature in a set of articles

    @param nfeats: Number of distinct features (length of L{docids})

    @param featdb: Mapping from document ID to array of feature IDs

    @param docids: Iterable of document IDs whose features are to be counted

    @return: Array of length L{nfeats}, containing occurrence count of each feature
    """
    counts = np.zeros(nfeats, np.int32)
    for docid in docids:
        counts[featdb[docid]] += 1
    return counts



# function to test if something is a noun
def extract_nouns(content: str) -> list:
    is_noun = lambda pos: pos[:2] == 'NN'
    tokenized = nltk.word_tokenize(content)
    nouns = [word for (word, pos) in nltk.pos_tag(tokenized) if is_noun(pos)]
    return nouns

if __name__ == '__main__':
    # Note 1. prepare the dataset: Abstract
    ds_name = 'trec_genomic_2005'
    # ds_name = 'relish_v1'

    #  Note used information: title + absract
    df = DBReader.tcp_model_cached_read("XXXX",
                                        '''select q_id,
       concat(q_content[1], ' ', q_content[2]) as q_content,
       arrayMap(x->
                    (tupleElement(x, 1),
                     concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                     tupleElement(x, 3))
           , c_tuples)                                            as c_tuples
    from sp.eval_data_%s_with_content where rand()%%100 <1 limit 10;''' % ds_name,
                                        cached=False)

    featmap = FeatureMapping()
    featured_articles = {}
    for i, row in df.iterrows():
        q_id, q_content, c_tuples = row
        q_content_nouns = extract_nouns(q_content)
        if q_id not in featured_articles:
            featured_articles[q_id] = featmap.update_feature_map(content_nouns=q_content_nouns)
        for c_id, c_content, score in c_tuples:
            c_content_nouns = extract_nouns(c_content)
            featured_articles[c_id] = featmap.update_feature_map(content_nouns=c_content_nouns)

    # Note 2. mapping article into features, i.e., nouns in Title+Abstract -> ID assigned

    # f = FeatureScores(featmap, pseudocount=None)
    # f.update(s.pfreqs, s.nfreqs, s.pdocs, s.ndocs)
    all_query_ranks = []
    for i, row in df.iterrows():
        c_tuples = row[-1]
        q_ids = [row[0]] * len(c_tuples)
        # c_tuples = np.array(row[-1], dtype=object)
        # for c_id, c_mesh_headings, c_mesh_qualifiers, c_journal, score in c_tuples:
        c_ids = [n[0] for n in c_tuples]
        orders = [n[-1] for n in c_tuples]
        c_pos_ids = [n[0] for n in c_tuples if n[-1] > 0]
        c_neg_ids = [n[0] for n in c_tuples if n[-1] == 0]

        # Note Count features from the positive articles
        # Note Count features from the negatives articles
        p_docs = len(c_pos_ids)
        n_docs = len(c_neg_ids)
        pos_counts = FeatureCounts(len(featmap), featured_articles, c_pos_ids)
        neg_counts = FeatureCounts(len(featmap), featured_articles, c_neg_ids)

        # ndocs = 0
        # for docid, date, features in docs:
        #     featcounts[features] += 1
        #     ndocs += 1

        # Note Evaluating feature scores from the Positive and Negative counts
        featinfo = FeatureScores(featmap=featmap, pseudocount=None)
        featinfo.update(pos_counts, neg_counts, p_docs, n_docs, prior=None)
        scores = featinfo.scores_of(featured_articles, c_ids)

        print(len(scores), scores)

        query_rank = sorted(zip(q_ids, scores, orders), key=lambda x: x[1], reverse=True)
        all_query_ranks.append(query_rank)

    eval_metrics(all_query_ranks, 'mscanner')

    # logging.info("Pfreqs (bayes): %s", pp.pformat(f.pfreqs))
    # logging.info("Nfreqs (bayes): %s", pp.pformat(f.nfreqs))
    # logging.info("PresScores (bayes): %s", pp.pformat(f.present_scores))
    # logging.info("AbsScores (bayes): %s", pp.pformat(f.absent_scores))
    # logging.info("Scores (bayes): %s", pp.pformat(f.scores))
    # logging.info("Base score (bayes): %f", f.base)
    # s.assertTrue(np.allclose(
    #     f.scores, np.array([-0.57054485, 1.85889877, 0.29626582])))
