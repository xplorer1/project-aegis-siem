grammar AQL;

// Parser Rules
query
    : sourceClause timeRangeClause? whereClause? statsClause? sortClause? headClause? EOF
    ;

sourceClause
    : 'source' '=' STRING
    ;

timeRangeClause
    : 'earliest' '=' timeValue 'latest' '=' timeValue
    ;

timeValue
    : STRING           // Absolute time: "2022-01-01 00:00:00"
    | relativeTime     // Relative time: "-7d", "-1h"
    ;

relativeTime
    : MINUS NUMBER timeUnit
    ;

timeUnit
    : 'd' | 'h' | 'm' | 's'
    ;

whereClause
    : 'where' expression
    ;

expression
    : expression AND expression                    #andExpression
    | expression OR expression                     #orExpression
    | field op value                               #comparisonExpression
    | field 'contains' STRING                      #containsExpression
    | '(' expression ')'                           #parenExpression
    ;

op
    : '=' | '!=' | '>' | '<' | '>=' | '<='
    ;

statsClause
    : 'stats' aggregationList ('by' fieldList)?
    ;

aggregationList
    : aggregation (',' aggregation)*
    ;

aggregation
    : aggFunction '(' field ')' ('as' IDENTIFIER)?
    ;

aggFunction
    : 'count' | 'sum' | 'avg' | 'min' | 'max'
    ;

sortClause
    : 'sort' sortFieldList
    ;

sortFieldList
    : sortField (',' sortField)*
    ;

sortField
    : field sortOrder?
    ;

sortOrder
    : 'asc' | 'desc'
    ;

headClause
    : 'head' NUMBER
    ;

fieldList
    : field (',' field)*
    ;

field
    : IDENTIFIER ('.' IDENTIFIER)*
    ;

value
    : STRING
    | NUMBER
    | BOOLEAN
    ;

// Lexer Rules
AND : 'AND' | 'and' ;
OR : 'OR' | 'or' ;
MINUS : '-' ;
BOOLEAN : 'true' | 'false' ;
NUMBER : [0-9]+ ('.' [0-9]+)? ;
IDENTIFIER : [a-zA-Z_][a-zA-Z0-9_]* ;
STRING : '"' (~["\r\n])* '"' | '\'' (~['\r\n])* '\'' ;
WS : [ \t\r\n]+ -> skip ;
