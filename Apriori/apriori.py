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
        data = sc.textFile(self.data_path).map(lambda x: x.split(self.sep)[1:])
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
            print candidates
            itemset_new = data.flatMap(lambda x: self.occur(x, candidates)).map(lambda x: (x, 1)).reduceByKey(operator.add)
            print itemset_new.collect()
            itemset_new = itemset_new.filter(lambda x: x[1] >= self.min_support)
            print itemset_new.collect()
            if itemset_new.count() > 0:
                itemset_new.saveAsTextFile(self.frequent_output + str(iteration))
            else:
                notConverged = False
            itemset_k = itemset_new.map(lambda x: x[0])

if __name__ == '__main__':
    data_path = "/user/liangting/demo/apriori/1000-out1.csv"
    frequent_output = "/user/liangting/demo/apriori/frequent_itemsets/"
    min_support = 40
    min_confidence = 0.8
    sc = SparkContext("local", "Simple App")
    apriori_model = Apriori(data_path, frequent_output, min_support, min_confidence, ",")
    apriori_model.genFrequentItemset(sc)
    sc.stop()
