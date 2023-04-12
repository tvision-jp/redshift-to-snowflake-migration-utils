CREATE OR REPLACE FUNCTION PROD.PUBLIC.ARRAY_FLATTEN(SRC ARRAY)
RETURNS ARRAY
LANGUAGE JAVASCRIPT
AS '
        let ret=[];
        SRC.forEach(arr => ret = ret.concat(arr));
        return ret;
    ';