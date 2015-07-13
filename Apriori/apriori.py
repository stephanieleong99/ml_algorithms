#coding=utf-8
from pyspark import SparkContext
import math
import operator

class Apriori():
    '''spark implementation of apriori algorithm'''
    def __init__(self, data_path, frequent_output, min_support, min_confidence, sep):
        self.data_path = data_path
        self.frequent_output = frequent_output
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.sep = sep

    def genCandidate(self, itemset_0):
        items = itemset_0.flatMap(lambda x: x.split(self.sep)).distinct().collect()
        itemset_1 = itemset_0.flatMap(lambda x: self.newItemset(x, items)).distinct()
        return itemset_1

    def newItemset(self, pair, items):
        candidates = []
        for item in items:
            if item not in pair:
                candidates.append(pair+self.sep+item)

        sorted_candidates = list()
        for candidate in candidates:
            tmp = candidate.split(self.sep)
            tmp.sort()
            sorted_candidate = ""
            for item in tmp:
                sorted_candidate += item + self.sep
            sorted_candidates.append(sorted_candidate[:-1])
        return sorted_candidates

    def occur(self, transaction, candidates):
        occur_itemset = []
        for candidate in candidates:
            items = candidate.split(self.sep)
            flag = True
            for item in items:
                if item not in transaction:
                    flag = False
                    break
            if flag:
                occur_itemset.append(candidate)
        return occur_itemset

    def genFrequentItemset(self, sc):
        data = sc.textFile(self.data_path).map(lambda x: x.split(',')[1:])
        data.cache()
        iteration = 1
        # generate 1-frequent itemset
        itemset = data.flatMap(lambda x: [(item, 1) for item in x]).reduceByKey(operator.add).filter(lambda x: x[1] >= self.min_support)
        itemset.saveAsTextFile(self.frequent_output + str(iteration))
        itemset_1 = itemset.map(lambda x: x[0])
        # generate k+1 frequent itemset by k frequent itemset
        notConverged = True
        itemset_k = itemset_1
        while notConverged:
            iteration += 1
            candidates = self.genCandidate(itemset_k).collect()
            itemset_new = data.flatMap(lambda x: self.occur(x, candidates)).map(lambda x: (x, 1)).reduceByKey(operator.add).filter(lambda x: x[1] >= self.min_support)
            if itemset_new.count() > 0:
                itemset_new.saveAsTextFile(self.frequent_output + str(iteration))
            else:
                notConverged = False
            itemset_k = itemset_new.map(lambda x: x[0])

    def genRuleCandidates(self, pair):
        items = pair[0].split(self.sep)
        n = len(items)
        freq = pair[1]
        ruleCandidates = []
        masks = [1<<j for j in xrange(n)]
        for i in xrange(1, 2**n-1):
            ruleCandidates.append(("".join([str(items[j])+'-' for j in range(n) if (masks[j] & i)]))[:-1])
        ruleTriples = []
        for condition in ruleCandidates:
            ruleTriples.append((condition, pair[0], freq))
        return ruleTriples

    def genAssociationRules(self, sc):
        # read all generated frequent itemsets
        frequent_itemsets = sc.textFile(self.frequent_output+"[0-9]*").map(lambda x: x.replace('(', '').replace(')', '').replace('u', '').replace('\'', '').replace(' ', '')).map(lambda x: (x.split(',')[0], int(x.split(',')[1])))
        itemset_freq_pairs = {}
        for itemset_freq in frequent_itemsets.collect():
            itemset_freq_pairs[itemset_freq[0]] = itemset_freq[1]
        # generate association rules candidates
        ruleCandidates = frequent_itemsets.filter(lambda x: len(x[0].split(self.sep))>1).flatMap(lambda x: self.genRuleCandidates(x))
        # rule triple:(condition items, items, confidence), rule: condition items --> items - condition items
        rules = ruleCandidates.map(lambda x: (x[0], x[1], x[2]/float(itemset_freq_pairs[x[0]]))).filter(lambda x: x[2] >= self.min_confidence)
        print rules.collect()

if __name__ == '__main__':
    data_path = "/user/liangting/demo/apriori/1000-out1.csv"
    frequent_output = "/user/liangting/demo/apriori/frequent_itemsets/"
    min_support = 30
    min_confidence = 0.8
    sc = SparkContext("local", "Simple App")
    apriori_model = Apriori(data_path, frequent_output, min_support, min_confidence, "-")
    apriori_model.genFrequentItemset(sc)
    apriori_model.genAssociationRules(sc)
    sc.stop()
