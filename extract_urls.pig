/*
 * run by "$pig -x local extract_urls.pig" if file in local file system.
 */

-- Data loading
tweets = LOAD '/home/cs5604s16_cm/CS5604S16/small_data/z_541/part-m-00000' USING PigStorage('\t') AS  (id:INT, text:CHARARRAY);
-- Extract URL using regular expression
urls = FOREACH tweets GENERATE REGEX_EXTRACT(text, '.*(http://\\S+).*', 1) AS url;
-- Trim unnecessary characters
urls_clean = FOREACH urls GENERATE REPLACE(url, 'â€¦','...');
-- Store URL with and without duplicates
STORE urls_clean INTO 'urls';
urls_uniq = distinct urls_clean;
STORE urls_uniq INTO 'urls_uniq';
