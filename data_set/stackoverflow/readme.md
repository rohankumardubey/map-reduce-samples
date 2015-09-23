# Loading Stack Exchange Data Dumps to Hadoop and Hive

These days I am learning a lot about Hadoop, and as part of that I need some data to play with. A lot of Hadoop examples run against Twitter data, or (airline data)[http://stat-computing.org/dataexpo/2009/the-data.html], but I decided it would be fun to look at the (StackExchange)[stackexchange.com] data dumps instead. 
The people at StackExchange kindly supply data dumps in XML format for the various StackExchange sites. As I write this, the dump file contains the following files:

 * Badges.xml
 * Comments.xml
 * PostHistory.xml
 * PostLinks.xml
 * Posts.xml
 * Tags.xml
 * Users.xml
 * Votes.xml

I guess the file format can change from time to time, but the overall definition seems fairly consistent with (this post)[http://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede]. The major difference is that some of the tables mentioned in the post are not in the dump, and the static lookup tables are missing, but they can easily be created.

Hive can deal with XML files, and the XML from the data dumps is pretty simple, with each row being wrapped in a <row /> tag with the column values encoded inside attributes, for example:

    <row Id="1" PostId="1" VoteTypeId="2" CreationDate="2009-04-30T00:00:00.000" />

In an effort to learn a bit about Avro, and make things hard for myself, I decided to convert all the XML files into avro files and then load them to Hive.

The easiest way to do this, is to create a Hive table pointing at each XML file and then create a new Avro table using a CREATE TABLE AS SELECT statement. I decided it would be more educational to write a map reduce job to do this instead, making things even more difficult for myself.

## Load Raw Data To Hadoop

The Stack Exchange dumps come compressed in 7z format (which I had never used before). The first step is to decompress them and load to Hadoop. I put them into a folder called input in my HDFS home directory:

```
[vagrant@standalone serverfault.com]$ ls -ltrh
total 2.0G
-rw-r--r-- 1 vagrant vagrant 168M Sep 23 15:23 Comments.xml
-rw-r--r-- 1 vagrant vagrant  39M Sep 23 15:23 Badges.xml
-rw-r--r-- 1 vagrant vagrant 2.9M Sep 23 15:23 PostLinks.xml
-rw-r--r-- 1 vagrant vagrant 916M Sep 23 15:23 PostHistory.xml
-rw-r--r-- 1 vagrant vagrant 139M Sep 23 15:23 Votes.xml
-rw-r--r-- 1 vagrant vagrant  64M Sep 23 15:23 Users.xml
-rw-r--r-- 1 vagrant vagrant 224K Sep 23 15:23 Tags.xml
-rw-r--r-- 1 vagrant vagrant 625M Sep 23 15:23 Posts.xml

[vagrant@standalone serverfault.com]$ hadoop fs -put *.xml input/

[vagrant@standalone serverfault.com]$ hadoop fs -ls input
Found 8 items
-rw-r--r--   3 vagrant vagrant   40661120 2015-09-23 15:24 input/Badges.xml
-rw-r--r--   3 vagrant vagrant  176150364 2015-09-23 15:24 input/Comments.xml
-rw-r--r--   3 vagrant vagrant  959680230 2015-09-23 15:24 input/PostHistory.xml
-rw-r--r--   3 vagrant vagrant    2988058 2015-09-23 15:24 input/PostLinks.xml
-rw-r--r--   3 vagrant vagrant  654708460 2015-09-23 15:24 input/Posts.xml
-rw-r--r--   3 vagrant vagrant     228933 2015-09-23 15:24 input/Tags.xml
-rw-r--r--   3 vagrant vagrant   66168226 2015-09-23 15:24 input/Users.xml
-rw-r--r--   3 vagrant vagrant  144769493 2015-09-23 15:25 input/Votes.xml

```

## Convert XML to Avro

Next step is to run a map only map reduce job to convert the XML files to Avro. I have (documented that process)[/posts] previously.

After running that job, there should be a bunch of avro files in the output directory:


We can ignore the zero byte files

## Create Hive Directory Structure

A Hive table points at a directory, and we want to to have a table for each of the original Stack Exchange files, so the next step is to create a simple directory structure and moved the files in the output directory into the correct place. The folling bash script should do the trick:

```
BASE_DIR=/user/vagrant/hive/stackoverflow

dirs=("users" "posts" "comments" "tags" "votes" "badges" "postlinks")
for i in "${dirs[@]}"
do
    hadoop fs -mkdir -p ${BASE_DIR}/${i}
    hadoop fs -mv output/${i}*.avro ${BASE_DIR}/${i}/
done
```

## Create Hive Tables

Now that the data is converted and moved into place, all that is left is to create some Hive tables. Current Hive versions (>= 0.14 I think) make creating tables over Avro files very simple - you don't even need to specify the Avro schema in the table definition, as it is derived from columns in the create table statement. So this step is as simple as create table statements, eg:

```
CREATE EXTERNAL TABLE users(
  id string,
  reputation string,
  creationdate string,
  displayname string,
  lastaccessdate string,
  websiteurl string,
  location string,
  aboutme string,
  views string,
  upvotes string,
  downvotes string,
  age string,
  accountid string,
  profileimageurl string
  )
STORED AS AVRO location '/user/vagrant/hive/stackoverflow/users';
```

## TODOs

You may have noticed I have cheated in a major area - Every column in every table is a string, including the dates. This would be much more useful if those were true Hive timestamp columns. 
