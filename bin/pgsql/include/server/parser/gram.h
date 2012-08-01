/* A Bison parser, made by GNU Bison 2.5.  */

/* Bison interface for Yacc-like parsers in C
   
      Copyright (C) 1984, 1989-1990, 2000-2011 Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     IDENT = 258,
     FCONST = 259,
     SCONST = 260,
     BCONST = 261,
     XCONST = 262,
     Op = 263,
     ICONST = 264,
     PARAM = 265,
     TYPECAST = 266,
     DOT_DOT = 267,
     COLON_EQUALS = 268,
     ABORT_P = 269,
     ABSOLUTE_P = 270,
     ACCESS = 271,
     ACTION = 272,
     ADD_P = 273,
     ADMIN = 274,
     AFTER = 275,
     AGGREGATE = 276,
     ALL = 277,
     ALSO = 278,
     ALTER = 279,
     ALWAYS = 280,
     ANALYSE = 281,
     ANALYZE = 282,
     AND = 283,
     ANY = 284,
     ARRAY = 285,
     AS = 286,
     ASC = 287,
     ASSERTION = 288,
     ASSIGNMENT = 289,
     ASYMMETRIC = 290,
     AT = 291,
     ATTRIBUTE = 292,
     AUTHORIZATION = 293,
     BACKWARD = 294,
     BEFORE = 295,
     BEGIN_P = 296,
     BETWEEN = 297,
     BIGINT = 298,
     BINARY = 299,
     BIT = 300,
     BOOLEAN_P = 301,
     BOTH = 302,
     BY = 303,
     CACHE = 304,
     CALLED = 305,
     CASCADE = 306,
     CASCADED = 307,
     CASE = 308,
     CAST = 309,
     CATALOG_P = 310,
     CHAIN = 311,
     CHAR_P = 312,
     CHARACTER = 313,
     CHARACTERISTICS = 314,
     CHECK = 315,
     CHECKPOINT = 316,
     CLASS = 317,
     CLOSE = 318,
     CLUSTER = 319,
     COALESCE = 320,
     COLLATE = 321,
     COLLATION = 322,
     COLUMN = 323,
     COMMENT = 324,
     COMMENTS = 325,
     COMMIT = 326,
     COMMITTED = 327,
     CONCURRENTLY = 328,
     CONFIGURATION = 329,
     CONNECTION = 330,
     CONSTRAINT = 331,
     CONSTRAINTS = 332,
     CONTENT_P = 333,
     CONTINUE_P = 334,
     CONVERSION_P = 335,
     COPY = 336,
     COST = 337,
     CREATE = 338,
     CROSS = 339,
     CSV = 340,
     CURRENT_P = 341,
     CURRENT_CATALOG = 342,
     CURRENT_DATE = 343,
     CURRENT_ROLE = 344,
     CURRENT_SCHEMA = 345,
     CURRENT_TIME = 346,
     CURRENT_TIMESTAMP = 347,
     CURRENT_USER = 348,
     CURSOR = 349,
     CYCLE = 350,
     DATA_P = 351,
     DATABASE = 352,
     DAY_P = 353,
     DEALLOCATE = 354,
     DEC = 355,
     DECIMAL_P = 356,
     DECLARE = 357,
     DEFAULT = 358,
     DEFAULTS = 359,
     DEFERRABLE = 360,
     DEFERRED = 361,
     DEFINER = 362,
     DELETE_P = 363,
     DELIMITER = 364,
     DELIMITERS = 365,
     DESC = 366,
     DICTIONARY = 367,
     DISABLE_P = 368,
     DISCARD = 369,
     DISTINCT = 370,
     DO = 371,
     DOCUMENT_P = 372,
     DOMAIN_P = 373,
     DOUBLE_P = 374,
     DROP = 375,
     EACH = 376,
     ELSE = 377,
     ENABLE_P = 378,
     ENCODING = 379,
     ENCRYPTED = 380,
     END_P = 381,
     ENUM_P = 382,
     ESCAPE = 383,
     EXCEPT = 384,
     EXCLUDE = 385,
     EXCLUDING = 386,
     EXCLUSIVE = 387,
     EXECUTE = 388,
     EXISTS = 389,
     EXPLAIN = 390,
     EXTENSION = 391,
     EXTERNAL = 392,
     EXTRACT = 393,
     FALSE_P = 394,
     FAMILY = 395,
     FETCH = 396,
     FIRST_P = 397,
     FLOAT_P = 398,
     FOLLOWING = 399,
     FOR = 400,
     FORCE = 401,
     FOREIGN = 402,
     FORWARD = 403,
     FREEZE = 404,
     FROM = 405,
     FULL = 406,
     FUNCTION = 407,
     FUNCTIONS = 408,
     GLOBAL = 409,
     GRANT = 410,
     GRANTED = 411,
     GREATEST = 412,
     GROUP_P = 413,
     HANDLER = 414,
     HAVING = 415,
     HEADER_P = 416,
     HOLD = 417,
     HOUR_P = 418,
     IDENTITY_P = 419,
     IF_P = 420,
     ILIKE = 421,
     IMMEDIATE = 422,
     IMMUTABLE = 423,
     IMPLICIT_P = 424,
     IN_P = 425,
     INCLUDING = 426,
     INCREMENT = 427,
     INDEX = 428,
     INDEXES = 429,
     INHERIT = 430,
     INHERITS = 431,
     INITIALLY = 432,
     INLINE_P = 433,
     INNER_P = 434,
     INOUT = 435,
     INPUT_P = 436,
     INSENSITIVE = 437,
     INSERT = 438,
     INSTEAD = 439,
     INT_P = 440,
     INTEGER = 441,
     INTERSECT = 442,
     INTERVAL = 443,
     INTO = 444,
     INVOKER = 445,
     IS = 446,
     ISNULL = 447,
     ISOLATION = 448,
     JOIN = 449,
     KEY = 450,
     LABEL = 451,
     LANGUAGE = 452,
     LARGE_P = 453,
     LAST_P = 454,
     LC_COLLATE_P = 455,
     LC_CTYPE_P = 456,
     LEADING = 457,
     LEAKPROOF = 458,
     LEAST = 459,
     LEFT = 460,
     LEVEL = 461,
     LIKE = 462,
     LIMIT = 463,
     LISTEN = 464,
     LOAD = 465,
     LOCAL = 466,
     LOCALTIME = 467,
     LOCALTIMESTAMP = 468,
     LOCATION = 469,
     LOCK_P = 470,
     MAPPING = 471,
     MATCH = 472,
     MAXVALUE = 473,
     MINUTE_P = 474,
     MINVALUE = 475,
     MODE = 476,
     MONTH_P = 477,
     MOVE = 478,
     NAME_P = 479,
     NAMES = 480,
     NATIONAL = 481,
     NATURAL = 482,
     NCHAR = 483,
     NEXT = 484,
     NO = 485,
     NONE = 486,
     NOT = 487,
     NOTHING = 488,
     NOTIFY = 489,
     NOTNULL = 490,
     NOWAIT = 491,
     NULL_P = 492,
     NULLIF = 493,
     NULLS_P = 494,
     NUMERIC = 495,
     OBJECT_P = 496,
     OF = 497,
     OFF = 498,
     OFFSET = 499,
     OIDS = 500,
     ON = 501,
     ONLY = 502,
     OPERATOR = 503,
     OPTION = 504,
     OPTIONS = 505,
     OR = 506,
     ORDER = 507,
     OUT_P = 508,
     OUTER_P = 509,
     OVER = 510,
     OVERLAPS = 511,
     OVERLAY = 512,
     OWNED = 513,
     OWNER = 514,
     PARSER = 515,
     PARTIAL = 516,
     PARTITION = 517,
     PASSING = 518,
     PASSWORD = 519,
     PLACING = 520,
     PLANS = 521,
     POSITION = 522,
     PRECEDING = 523,
     PRECISION = 524,
     PRESERVE = 525,
     PREPARE = 526,
     PREPARED = 527,
     PRIMARY = 528,
     PRIOR = 529,
     PRIVILEGES = 530,
     PROCEDURAL = 531,
     PROCEDURE = 532,
     QUOTE = 533,
     RANGE = 534,
     READ = 535,
     REAL = 536,
     REASSIGN = 537,
     RECHECK = 538,
     RECURSIVE = 539,
     REF = 540,
     REFERENCES = 541,
     REINDEX = 542,
     RELATIVE_P = 543,
     RELEASE = 544,
     RENAME = 545,
     REPEATABLE = 546,
     REPLACE = 547,
     REPLICA = 548,
     RESET = 549,
     RESTART = 550,
     RESTRICT = 551,
     RETURNING = 552,
     RETURNS = 553,
     REVOKE = 554,
     RIGHT = 555,
     ROLE = 556,
     ROLLBACK = 557,
     ROW = 558,
     ROWS = 559,
     RULE = 560,
     SAVEPOINT = 561,
     SCHEMA = 562,
     SCROLL = 563,
     SEARCH = 564,
     SECOND_P = 565,
     SECURITY = 566,
     SELECT = 567,
     SEQUENCE = 568,
     SEQUENCES = 569,
     SERIALIZABLE = 570,
     SERVER = 571,
     SESSION = 572,
     SESSION_USER = 573,
     SET = 574,
     SETOF = 575,
     SHARE = 576,
     SHOW = 577,
     SIMILAR = 578,
     SIMPLE = 579,
     SMALLINT = 580,
     SNAPSHOT = 581,
     SOME = 582,
     STABLE = 583,
     STANDALONE_P = 584,
     START = 585,
     STATEMENT = 586,
     STATISTICS = 587,
     STDIN = 588,
     STDOUT = 589,
     STORAGE = 590,
     STRICT_P = 591,
     STRIP_P = 592,
     SUBSTRING = 593,
     SYMMETRIC = 594,
     SYSID = 595,
     SYSTEM_P = 596,
     TABLE = 597,
     TABLES = 598,
     TABLESPACE = 599,
     TEMP = 600,
     TEMPLATE = 601,
     TEMPORARY = 602,
     TEXT_P = 603,
     THEN = 604,
     TIME = 605,
     TIMESTAMP = 606,
     TO = 607,
     TRAILING = 608,
     TRANSACTION = 609,
     TREAT = 610,
     TRIGGER = 611,
     TRIM = 612,
     TRUE_P = 613,
     TRUNCATE = 614,
     TRUSTED = 615,
     TYPE_P = 616,
     TYPES_P = 617,
     UNBOUNDED = 618,
     UNCOMMITTED = 619,
     UNENCRYPTED = 620,
     UNION = 621,
     UNIQUE = 622,
     UNKNOWN = 623,
     UNLISTEN = 624,
     UNLOGGED = 625,
     UNTIL = 626,
     UPDATE = 627,
     USER = 628,
     USING = 629,
     VACUUM = 630,
     VALID = 631,
     VALIDATE = 632,
     VALIDATOR = 633,
     VALUE_P = 634,
     VALUES = 635,
     VARCHAR = 636,
     VARIADIC = 637,
     VARYING = 638,
     VERBOSE = 639,
     VERSION_P = 640,
     VIEW = 641,
     VOLATILE = 642,
     WHEN = 643,
     WHERE = 644,
     WHITESPACE_P = 645,
     WINDOW = 646,
     WITH = 647,
     WITHOUT = 648,
     WORK = 649,
     WRAPPER = 650,
     WRITE = 651,
     XML_P = 652,
     XMLATTRIBUTES = 653,
     XMLCONCAT = 654,
     XMLELEMENT = 655,
     XMLEXISTS = 656,
     XMLFOREST = 657,
     XMLPARSE = 658,
     XMLPI = 659,
     XMLROOT = 660,
     XMLSERIALIZE = 661,
     YEAR_P = 662,
     YES_P = 663,
     ZONE = 664,
     NULLS_FIRST = 665,
     NULLS_LAST = 666,
     WITH_TIME = 667,
     POSTFIXOP = 668,
     UMINUS = 669
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{

/* Line 2068 of yacc.c  */
#line 160 "gram.y"

	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	FuncWithArgs		*funwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	WindowDef			*windef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;



/* Line 2068 of yacc.c  */
#line 503 "gram.h"
} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif



#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif



