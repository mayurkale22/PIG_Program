/*
 * PIG Program
 * This script processes a tweet log file (parse and tokenize it) and searches the 
 * given lines of input for specific tokens and output the number of lines that contained the search term.
 */

-- Input: Load raw data.
RawData = LOAD '/user/cloudera/pig/tweets.txt';

-- Call the Lower function to change the raw data field to lowercase.
RawDataLowerCase = FOREACH RawData GENERATE LOWER(((chararray)$0)) as tweets;

-- Use the FOREACH-GENERATE command to tokenize tweets word.
TokenizedData = FOREACH RawDataLowerCase GENERATE flatten(TOKENIZE(tweets)) as word;

-- Use the Replace function to replace existing characters in a string with new characters.
ReplaceData = FOREACH TokenizedData GENERATE REPLACE(word, '.*dec.*', 'dec') as word;
ReplaceData = FOREACH ReplaceData GENERATE REPLACE(word, '.*hackathon.*', 'hackathon') as word;

-- Use the FILTER command to get words.
FilteredData = FILTER ReplaceData BY (word == 'hackathon' OR word == 'dec' OR word == 'chicago' OR word == 'java');

wordGroup = GROUP FilteredData BY word;

-- Use the COUNT function to get the count (occurrences) of each word.
wordCnt = FOREACH wordGroup GENERATE group AS tweetWord, COUNT(FilteredData);

-- print output result.
DUMP wordCnt;
