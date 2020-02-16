ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, 
rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|') 
AS (movieID:int, movieTitle:chararray, releaseDate:chararray, 
videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, 
AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS ratingCount;

worstMovies = FILTER avgRatings BY avgRating < 2.0;

worstMoviesWithData = JOIN worstMovies BY movieID, nameLookup BY movieID;

finalResults = FOREACH worstMoviesWithData GENERATE nameLookup::movieTitle AS movieName,
worstMovies::avgRating AS avgRating, worstMovies::ratingCount AS ratingCount;

sortedFinalResults = ORDER finalResults BY ratingCount DESC;

DUMP sortedFinalResults;