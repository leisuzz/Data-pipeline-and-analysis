import sys
from pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from dictionary import Df

# create output file path seperately
file_path1 = 'Jun_12_2019.txt'
file_path1_2 = 'Jun_12_2019#2.txt'
file_path2 = 'Jun_13_2019.txt'
file_path2_2 = 'Jun_13_2019#2.txt'

# create dict for sql
app12 = {}
platform12 = {}
app13 = {}
platform13 = {}

# create dataframe
spark = SparkSession.builder.appName('Dataframe').getOrCreate()

# read the dataset
df_pyspark1 = spark.read.option('head', 'true').json('June_12_2019#1.json')
df_pyspark1_2 = spark.read.option('head', 'true').json('June_12_2019#2.json')
df_pyspark2 = spark.read.option('head', 'true').json('June_13_2019#1.json')
df_pyspark2_2 = spark.read.option('head', 'true').json('June_13_2019#2.json')


def jsontotxt():
    # get class model
    df = Df()

    # print output for Jun12#1_2019
    for d1 in df_pyspark1.collect():
        # transform row into dict
        d1.asDict()
        df.printout(d1, file_path1)

    # print output for Jun12#2_2019
    for d2 in df_pyspark1_2.collect():
        d2.asDict()
        df.printout(d2, file_path1_2)

    # print output for Jun13#1_2019
    for d3 in df_pyspark2.collect():
        d3.asDict()
        df.printout(d3, file_path2)

    # print output for Jun13#2_2019
    for d4 in df_pyspark2_2.collect():
        d4.asDict()
        df.printout(d4, file_path2_2)


def sqlfind():
    df_pyspark2.createOrReplaceTempView('table1')
    df_pyspark2_2.createOrReplaceTempView('table2')

    # select column for event time and user_creation_time
    query2 = '''
            SELECT DISTINCT event_time, user_creation_time
            FROM table1
        '''
    query2_2 = '''
                SELECT DISTINCT event_time, user_creation_time
                FROM table2
            '''
    x1 = spark.sql(query2)
    x2 = spark.sql(query2_2)

    # create new table for combination
    x1.createOrReplaceTempView('x2')
    x2.createOrReplaceTempView('x2_2')

    # combine values together
    query13 = '''
            SELECT * FROM x2
            UNION
            SELECT * FROM x2_2
            '''
    x13 = spark.sql(query13)
    x13.createOrReplaceTempView('x13')

    # find users and remove duplicate by finding same creation_time
    queryj = '''
                SELECT DISTINCT user_creation_time
                FROM x13
                WHERE event_time LIKE '%2019-06-12%'
                '''
    x_join = spark.sql(queryj)
    x_join.createOrReplaceTempView('x_join')

    # count number of users
    count = sqlContext.sql("SELECT COUNT (*) AS number_of_users FROM x_join ")
    sys.stdout = open('user.txt', "a")
    count.show()

    # print user_creation time
    sys.stdout = open('user.txt', "a")
    x_join.show()

if __name__ == "__main__":
    jsontotxt()
    sqlfind()
