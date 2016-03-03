/*
 * un by "$pig -x local clean_tweets.pig" if file in local node instead of HDFS.
 */

tweets = LOAD '/home/cs5604s16_cm/CS5604S16/small_data/z_541/part-m-00000' USING PigStorage('\t') AS  (id:CHARARRAY, text:CHARARRAY);

-- Remove urls
urls = FOREACH tweets GENERATE id, REPLACE(text, '(http://\\S+)', '');
STORE urls INTO 'cleaned_tweets';

