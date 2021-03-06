from pyspark import SparkContext
import math
import operator

class KNN():
    '''SPARK implementation of K-nearest neighbors'''
    def __init__(self, data_path, K=3):
        self.data_path = data_path
        self.K = K

    def cal_distance(self, list1, list2):
        distance = 0
        for i in xrange(len(list1)):
            distance += math.pow((list1[i] - list2[i]), 2)
        return distance

    def my_sort(self, pair):
        index = pair[0]
        list_of_pair = list(pair[1])
        sorted_list = sorted(list_of_pair, key=lambda tup: tup[0])
        return (index, sorted_list[:5])

    def my_topK(self, pair):
        index = pair[0]
        sorted_dist_index = pair[1]
        return (index, sorted_dist_index[0:self.K])

    def vote(self, pair):
        index = pair[0]
        top_K = pair[1]
        count = {}
        for (dist, i) in top_K:
            if count.has_key(i):
                count[i] += 1
            else:
                count[i] = 1
        label = max(count.iteritems(), key=operator.itemgetter(1))[0]
        return (index, label)

    def precision(self, y_test, predict):
        count = 0.0
        for (index, label) in y_test.iteritems():
            if predict[index] == label:
                count += 1
        return count / len(y_test)

    def predict(self):
        sc = SparkContext("local", "Simple App")
        data = sc.textFile(self.data_path).map(lambda x: (x.split(",")[0], x.split(",")[1:]))
        train = data.sample(False, 0.9, 47)
        train_collect = train.collect()
        test = data.filter(lambda x: x not in train_collect)

        X_train = train.map(lambda x: (str(x[1][-1]), [float(i) for i in x[1][:-1]]))
        X_test = test.map(lambda x: (int(x[0]), [float(i) for i in x[1][:-1]]))
        y_test = test.map(lambda x: (int(x[0]), str(x[1][-1]))).collect()

        K = 5
        crossed = X_test.cartesian(X_train)
        distances = crossed.map(lambda x: (x[0][0], (self.cal_distance(x[0][1], x[1][1]), x[1][0]))).groupByKey().map(self.my_sort)
        top = distances.map(lambda x: self.my_topK(x))
        predict = top.map(self.vote).collect()

        print dict(y_test)
        print dict(predict)
        print self.precision(dict(y_test), dict(predict))
        sc.stop()

if __name__ == '__main__':
    knn_model = KNN(data_path="/user/liangting/demo/knn/iris.data", K=5)
    knn_model.predict()
