import re


class RedshiftNamingHelper:
    def check_reserved_keyword(self, name: str):
        reserved = "AES128, AES256, ALL, ALLOWOVERWRITE, ANALYSE, ANALYZE, AND, ANY, ARRAY, AS, ASC, AUTHORIZATION, BACKUP, BETWEEN, BINARY, BLANKSASNULL, BOTH, BYTEDICT, BZIP2, CASE, CAST, CHECK, COLLATE, COLUMN, CONSTRAINT, CREATE, CREDENTIALS, CROSS, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_USER, CURRENT_USER_ID, DEFAULT, DEFERRABLE, DEFLATE, DEFRAG, DELTA, DELTA32K, DESC, DISABLE, DISTINCT, DO, ELSE, EMPTYASNULL, ENABLE, ENCODE, ENCRYPT, ENCRYPTION, END, EXCEPT, EXPLICIT, FALSE, FOR, FOREIGN, FREEZE, FROM, FULL, GLOBALDICT256, GLOBALDICT64K, GRANT, GROUP, GZIP, HAVING, IDENTITY, IGNORE, ILIKE, IN, INITIALLY, INNER, INTERSECT, INTO, IS, ISNULL, JOIN, LANGUAGE, LEADING, LEFT, LIKE, LIMIT, LOCALTIME, LOCALTIMESTAMP, LUN, LUNS, LZO, LZOP, MINUS, MOSTLY13, MOSTLY32, MOSTLY8, NATURAL, NEW, NOT, NOTNULL, NULL, NULLS, OFF, OFFLINE, OFFSET, OID, OLD, ON, ONLY, OPEN, OR, ORDER, OUTER, OVERLAPS, PARALLEL, PARTITION, PERCENT, PERMISSIONS, PLACING, PRIMARY, RAW, READRATIO, RECOVER, REFERENCES, RESPECT, REJECTLOG, RESORT, RESTORE, RIGHT, SELECT, SESSION_USER, SIMILAR, SNAPSHOT , SOME, SYSDATE, SYSTEM, TABLE, TAG, TDES, TEXT255, TEXT32K, THEN, TIMESTAMP, TO, TOP, TRAILING, TRUE, TRUNCATECOLUMNS, UNION, UNIQUE, USER, USING, VERBOSE, WALLET, WHEN, WHERE, WITH, WITHOUT".split()

        reserved = [x.strip(',') for x in reserved]
        return (name.upper() in reserved)

    def validate_name(self, name: str):
        if self.check_reserved_keyword(name):
            return tuple([False, f'name: {name} cannot be a reserved SQL keyword'])
        elif not bool(re.match(r"^[a-zA-Z0-9_]", name)):
            return tuple([False, f'name: {name} can only start with an alphanumeric or an underscore'])
        elif bool(re.search("([ '\"])", name)):
            return tuple([False, f'name: {name} cannot contain spaces or quotations'])
        elif len(name) < 1 or len(name) > 127:
            return tuple([False, f'name: {name} must be between 1 and 127 characters'])
        else:
            return tuple([True, None])
