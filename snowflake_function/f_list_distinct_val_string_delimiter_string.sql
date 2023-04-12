create function F_LIST_DISTINCT(VAL VARCHAR, DELIMITER VARCHAR)
    returns VARCHAR
    language JAVASCRIPT
as
$$
if (VAL == "") return null;
var arr = VAL.split(DELIMITER);
return arr.filter( (i,s) => arr.indexOf(i) == s) // Remove duplicates
.join(DELIMITER);
$$;
