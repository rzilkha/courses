from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

def parseLine(line):
    line = line.split(',')
    stationId = line[0]
    entry = line[2]
    temp =  float(line[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationId,entry,temp)


lines = sc.textFile("1800.csv")
weather=lines.map(parseLine)
weather = weather.filter(lambda line : 'TMAX' in line[1])
weather = weather.map(lambda x : (x[0],x[2]))
weather = weather.reduceByKey(lambda x,y : max(x,y))

results = weather.collect();
for res in results:
    print res
