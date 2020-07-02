import pyspark
import sys

# initialize
sc = pyspark.SparkContext("local", "PageRank")
sc.setCheckpointDir('./checkpoints')
file, *args = sys.argv
input_path, alpha, max_iter, beta = args
alpha, max_iter, beta = float(alpha), int(max_iter), float(beta)
n_partitions = 4
############

# preprocess
lines = sc.textFile(input_path, n_partitions)

def not_loop(link):
    return link[0] != link[1][0]

links = lines\
        .map(lambda line: line.split())\
        .map(lambda line: (int(line[0]), (int(line[1]), float(line[2]))))\
        .filter(lambda link: not_loop(link))

links = links.coalesce(n_partitions)
links.persist()

nodes = links.map(lambda link: (link[0], link[1][0])).flatMap(lambda x: x).distinct().coalesce(n_partitions)
n = nodes.count()
pi = nodes.map(lambda node: (node, 1/n)).coalesce(n_partitions)
pi.persist()
total_weight = links.map(lambda link: (link[0], link[1][1])).reduceByKey(lambda w1, w2: w1 + w2).coalesce(n_partitions)

def link_prob(link):
    x, ((y, w), sw) = link
    return x, (y, w / sw)

H_non_zero = links.join(total_weight, n_partitions).map(link_prob).coalesce(n_partitions)
H_non_zero.persist()

sink_nodes = nodes.subtract(H_non_zero.keys()).collect()

def check_sink(node, sink_nodes):
    return node in sink_nodes

is_sink = nodes.map(lambda x: (x, check_sink(x, sink_nodes))).collectAsMap()
sink_nodes = sc.parallelize(sink_nodes)
sink_nodes.persist()
pi.persist()
############

# iteration
def compute_by_H(link, alpha):
    x, ((y, p), pi) = link
    return y, (1 - alpha) * (p * pi)

def compute_by_A(node, next_pi, pi, is_sink, sink_pi_sum, alpha):
    if is_sink[node]:
        return node, (1 - alpha) * (sink_pi_sum - pi) / (n - 1) + next_pi
    return node, (1 - alpha) * sink_pi_sum / (n - 1) + next_pi

for t in range(max_iter):
    augmented_H = H_non_zero.join(pi, n_partitions)
    next_pi = augmented_H.map(lambda link: compute_by_H(link, alpha)).reduceByKey(lambda p1, p2: p1 + p2).coalesce(n_partitions)
    next_pi = next_pi.map(lambda node: (node[0], node[1] + alpha/n)).coalesce(n_partitions)
    sink_pi_vals = sink_nodes.map(lambda node: (node, None)).join(pi, n_partitions).map(lambda x: (x[0], x[1][1])).coalesce(n_partitions)
    sink_pi_sum = sink_pi_vals.map(lambda x: x[1]).sum()
    next_pi = next_pi.join(pi).map(lambda x: compute_by_A(x[0], x[1][0], x[1][1], is_sink, sink_pi_sum, alpha)).coalesce(n_partitions)
    L_1 = next_pi.join(pi).map(lambda x: abs(x[1][0] - x[1][1])).sum()

    pi = next_pi.coalesce(n_partitions)
    pi.persist()

    if t % 3 == 0:
        pi.coalesce(1).checkpoint()

    if L_1 < beta:
        break
###########

# save
pi.coalesce(1).saveAsTextFile('./result')
######
