from pyspark import SparkContext, SparkConf
from random import randint
import argparse, time

def sleep_task(x):
    time.sleep(0.5)
    return (randint(0, 5), x)

def main():

    conf = SparkConf().setAppName("Spark Wait")
    sc = SparkContext.getOrCreate(conf=conf)

    parser = argparse.ArgumentParser(description='Dummy program to help test pilot scheduling')
    parser.add_argument('-p', '--partitions', type=int, default=5, help="number of partitions")
    parser.add_argument('-c', '--checkpoint', type=str, help="checkpoint directory")
    args = parser.parse_args()

    if args.checkpoint is not None:
        sc.setCheckpointDir(args.checkpoint)

    waitRDD = sc.parallelize([x for x in range(0,1000)], args.partitions) \
                .map(lambda x: sleep_task(x)) \
                .reduceByKey(lambda x,y: x+y)

    waitRDD.checkpoint()

    waitRDD = waitRDD.map(lambda x: sleep_task(x[1])) \
                .reduceByKey(lambda x,y: x+y) \
                .map(lambda x: sleep_task(x[1]))

    waitRDD.checkpoint()

    result = waitRDD.collect()

if __name__ == '__main__':
    main()
