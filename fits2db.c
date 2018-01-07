/**
 *  FITS2DB -- Convert FITS Binary Tables to one or more database files.
 *
 *    Usage:
 *		fits2db [<otps>] [ <input> ..... ]
 *
 *    where <opts> include:
 *
 *      -h,--help                this message
 *      -d,--debug               set debug flag
 *      -v,--verbose             set verbose output flag
 *      -n,--noop                set no-op flag
 *
 *                                   INPUT PROCESSING OPTIONS
 *      -b,--bundle=<N>          bundle <N> files at a time
 *      -c,--chunk=<N>           process <N> rows at a time
 *      -e,--extnum=<N>          process table in FITS extension number <N>
 *      -E,--extname=<name>      process table in FITS extension name <name>
 *      -i,--input=<file>        set input filename
 *      -o,--output=<file>       set output filename
 *      -r,--rowrange=<range>    convert rows within given <range>
 *      -s,--select=<expr>       select rows based on <expr>
 *
 *                                   PROCESSING OPTIONS
 *      -C,--concat              concatenate all input files to output
 *      -H,--noheader            suppress CSV column header
 *      -N,--nostrip             don't strip strings of whitespace
 *      -Q,--noquote             don't quote strings in text formats
 *      -S,--singlequote         use single quotes for strings
 *      -X,--explode             explode array cols to separate columns
 *
 *                                   FORMAT OPTIONS
 *      --asv                    output an ascii-separated value table
 *      --bsv                    output a bar-separated value table
 *      --csv                    output a comma-separated value table
 *      --tsv                    output a tab-separated value table
 *      --ipac                   output an IPAC formatted table
 *
 *                                   SQL OPTIONS
 *      -B,--binary              output binary SQL
 *      -O,--oids                create table with OIDs (Postgres only)
 *      -t,--table=<name>        create table named <name>
 *      -Z,--noload              don't create table load commands
 *
 *      --add=<colname>          Add the nameed column (needs type info)
 *      --sql=<db>               output SQL correct for <db> type
 *      --drop                   drop existing DB table before conversion
 *      --dbname=<name>          create DB of the given name
 *      --sid=<colname>          add a sequential-ID column (integer)
 *      --rid=<colname>          add a random-ID column (float: 0.0 -> 100.0)
 *
 *      --create                 create DB table from input table structure
 *      --truncate               truncate DB table before loading
 *      --pkey=<colname>         create a serial ID column named <colname>
 *
 *
 *  @file       fits2db.c
 *  @author     Mike Fitzpatrick, NOAO Data Lab Project, Tucson, AZ, USA
 *  @date       10/1/16
 *  @version    1.0
 *
 *  @brief     Convert FITS Binary Tables to one or more database files.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include <fcntl.h>
#include <getopt.h>
#include <arpa/inet.h>

#include "fitsio.h"


// Utility values
#define MAX_CHUNK               100000
#define MAX_COLS                1024

#define	SZ_RESBUF	        8192
#define SZ_COLNAME              32
#define SZ_EXTNAME              32
#define SZ_COLVAL               1024
#define SZ_ESCBUF               1024
#define SZ_LINEBUF              10240
#define SZ_PATH                 512
#define SZ_FNAME                256
#define SZ_VALBUF               256

#define PARG_ERR                -512000000

#define RANDOM_SCALE		100.0		// scale for random numbers

#ifndef OK
#define OK                      0
#endif
#ifndef ERR
#define ERR                     1
#endif
#ifndef TRUE
#define TRUE                    1
#endif
#ifndef FALSE
#define FALSE                   0
#endif

//  Output Format Codes
#define TAB_DELIMITED           000             // delimited ascii table
#define TAB_IPAC                001             // IPAC table format
#define TAB_POSTGRES            002             // SQL -- PostgreSQL
#define TAB_MYSQL               004             // SQL -- MySQL
#define TAB_SQLITE              010             // SQL -- SQLite

#define TAB_DBTYPE(t)           (t>TAB_IPAC)

#define TAB_SERIAL              999             // Serial ID column type

// Default values
#define DEF_CHUNK               10000
#define DEF_ONAME               "root"

#define DEF_FORMAT              TAB_DELIMITED
#define DEF_DELIMITER           ','
#define DEF_QUOTE               '"'
#define DEF_MODE                "w+"


/*  Table column descriptor
 */
typedef struct {
    int       colnum;
    int       dispwidth;
    int       type;
    int       nelem;
    int       ndim;
    int       nrows;
    int       ncols;
    long      width;
    long      repeat;
    char      colname[SZ_COLNAME];
    char      coltype[SZ_COLNAME];
    char      colunits[SZ_COLNAME];
} Col, *ColPtr;

Col inColumns[MAX_COLS];
Col outColumns[MAX_COLS];

int numInCols 		= 0;		// number of input columns
int numOutCols 		= 0;		// number of output columns


char    esc_buf[SZ_ESCBUF];             // escaped value buffer
char   *obuf, *optr;                    // output buffer pointers
long    olen = 0;                       // output buffer length

char   *prog_name       = NULL;         // program name

char   *extname         = NULL;         // extension name
char   *iname           = NULL;         // input file name
char   *oname           = NULL;         // output file name
char   *basename        = NULL;         // base output file name
char   *rows            = NULL;         // row selection string
char   *expr            = NULL;         // selection expression string
char   *tablename       = NULL;         // database table name
char   *sidname         = NULL;         // serial ID column name
char   *ridname         = NULL;         // random ID column name
char   *dbname          = NULL;         // database name name (MySQL create)
char   *addname         = NULL;         // column name to be added

char    delimiter       = DEF_DELIMITER;// default to CSV
char    quote_char      = DEF_QUOTE;    // string quote character
char   *omode           = DEF_MODE;     // output file mode

int     format          = DEF_FORMAT;   // default output format
int     mach_swap       = 0;            // is machine swapped relative to FITS?
int     do_binary       = 0;            // do binary SQL output
int     do_quote        = 1;            // quote ascii values?
int     do_escape       = 0;            // escape strings for quotes?
int     do_strip        = 1;            // strip leading/trailing whitespace?
int     do_drop         = 0;            // drop db table before creating new one
int     do_create       = 0;            // create new db table
int     do_truncate     = 0;            // truncate db table before load
int     do_load         = 1;            // load db table
int     do_oids         = 0;            // use table OID (Postgres only)?
int     bundle          = 1;            // number of input files to bundle
int     nfiles          = 0;            // number of input files
int     noop            = 0;            // no-op ??

int     concat          = 0;            // concat input file to single output?
int     explode         = 0;            // explode arrays to new columns?
int     extnum          = -1;           // extension number
int     header          = 1;            // prepend column headers
int     number          = 0;            // number rows ?
int     single          = 0;            // load rows one at a time?
int     chunk_size      = DEF_CHUNK;    // processing chunk size

int     serial_number   = 0;            // ID serial number

int     debug           = 0;            // debug flag
int     verbose         = 0;            // verbose output flag

char   *pgcopy_hdr      = "PGCOPY\n\377\r\n\0\0\0\0\0";
int     len_pgcopy_hdr  = 15;

size_t  sz_char         = sizeof (char);
size_t  sz_short        = sizeof (short);
size_t  sz_int          = sizeof (int);
size_t  sz_long         = sizeof (long);
size_t  sz_longlong     = sizeof (long long);
size_t  sz_float        = sizeof (float);
size_t  sz_double       = sizeof (double);



/*  Task specific option declarations.  Task options are declared using the
 *  getopt_long(3) syntax.
static Task  self       = {  "fits2db",  fits2db,  0,  0,  0  };
 */

static char  *opts 	= "hdvnb:c:e:E:i:o:r:s:t:BCHNOQSXZ012345:678L:U:A:D:";
static struct option long_opts[] = {
    { "help",         no_argument,          NULL,   'h'},
    { "debug",        no_argument,          NULL,   'd'},
    { "verbose",      no_argument,          NULL,   'v'},
    { "noop",         no_argument,          NULL,   'n'},

    { "bundle",       required_argument,    NULL,   'b'},
    { "chunk",        required_argument,    NULL,   'c'},
    { "extnum",       required_argument,    NULL,   'e'},
    { "extname",      required_argument,    NULL,   'E'},
    { "input",        required_argument,    NULL,   'i'},
    { "output",       required_argument,    NULL,   'o'},
    { "rowrange",     required_argument,    NULL,   'r'},
    { "select",       required_argument,    NULL,   's'},
    { "table",        required_argument,    NULL,   't'},

    { "binary",       no_argument,          NULL,   'B'},
    { "concat",       no_argument,          NULL,   'C'},
    { "noheader",     no_argument,          NULL,   'H'},
    { "nostrip",      no_argument,          NULL,   'N'},
    { "oid",          no_argument,          NULL,   'O'},
    { "noquote",      no_argument,          NULL,   'Q'},
    { "singlequote",  no_argument,          NULL,   'S'},
    { "explode",      no_argument,          NULL,   'X'},
    { "noload",       no_argument,          NULL,   'Z'},

    { "asv",          no_argument,          NULL,   '0'},
    { "bsv",          no_argument,          NULL,   '1'},
    { "csv",          no_argument,          NULL,   '2'},
    { "tsv",          no_argument,          NULL,   '3'},
    { "ipac",         no_argument,          NULL,   '4'},

    { "sql",          required_argument,    NULL,   '5'},
    { "drop",         no_argument,          NULL,   '6'},
    { "create",       no_argument,          NULL,   '7'},
    { "truncate",     no_argument,          NULL,   '8'},
    { "sid",          required_argument,    NULL,   'L'},
    { "rid",          required_argument,    NULL,   'U'},
    { "add",          required_argument,    NULL,   'A'},
    { "dbname",       required_argument,    NULL,   'D'},

    { NULL,           0,                    0,       0 }
};


/*  All tasks should declare a static Usage() method to print the help 
 *  text in response to a '-h' or '--help' flag.  The help text should 
 *  include a usage summary, a description of options, and some examples.
 */
static void Usage (void);

static void dl_escapeCSV (char* in);
static void dl_quote (char* in);
static void dl_fits2db (char *iname, char *oname, int filenum, 
                            int bnum, int nfiles);
static void dl_printHdr (int firstcol, int lastcol, FILE *ofd);
static void dl_printIPACTypes (char *tablename, fitsfile *fptr, int firstcol,
                                int lastcol, FILE *ofd);
static void dl_createSQLTable (char *tablename, fitsfile *fptr, int firstcol,
                                int lastcol, FILE *ofd);
static void dl_printSQLHdr (char *tablename, fitsfile *fptr, int firstcol,
                                int lastcol, FILE *ofd);
static void dl_printHdrString (char *tablename);
static void dl_getColInfo (fitsfile *fptr, int firstcol, int lastcol);
static int  dl_validateColInfo (fitsfile *fptr, int firstcol, int lastcol);
static void dl_getOutputCols (fitsfile *fptr, int firstcol, int lastcol);

static unsigned char *dl_printCol (unsigned char *dp, ColPtr col, char end_ch);
static unsigned char *dl_printString (unsigned char *dp, ColPtr col);
static unsigned char *dl_printLogical (unsigned char *dp, ColPtr col);
static unsigned char *dl_printByte (unsigned char *dp, ColPtr col);
static unsigned char *dl_printShort (unsigned char *dp, ColPtr col);
static unsigned char *dl_printInt (unsigned char *dp, ColPtr col);
static unsigned char *dl_printLong (unsigned char *dp, ColPtr col);
static unsigned char *dl_printFloat (unsigned char *dp, ColPtr col);
static unsigned char *dl_printDouble (unsigned char *dp, ColPtr col);
static void           dl_printSerial (void);
static void           dl_printRandom (void);
static void           dl_printValue (int value);

static int dl_atoi (char *v);
static int dl_isFITS (char *v);
static int dl_isGZip (char *v);
static void dl_error (int exit_code, char *error_message, char *tag);

static char *dl_colType (ColPtr col);
static char *dl_IPACType (ColPtr col);
static char *dl_SQLType (ColPtr col);
static char *dl_makeTableName (char *fname);
static char *dl_fextn (void);

static char **dl_paramInit (int argc, char *argv[], char *opts, 
                    struct option long_opts[]);
static int    dl_paramNext (char *opts, struct option long_opts[], 
                    int argc, char *argv[], char *optval, int *posindex);
static void   dl_paramFree (int argc, char *argv[]);

static void bswap2 (char *a, char *b, int nbytes);
static void bswap4 (char *a, int aoff, char *b, int boff, int nbytes);
static void bswap8 (char *a, int aoff, char *b, int boff, int nbytes);
static char *sstrip (char *s);
static int is_swapped (void);



/**
 *  Application entry point.  All DLApps tasks MUST contain this 
 *  method signature.
 */
int
main (int argc, char **argv)
{
    char **pargv, optval[SZ_FNAME], *prog_name;
    char **iflist = NULL, **ifstart = NULL;
    char  *iname = NULL, *oname = NULL, tmp[SZ_FNAME];
    int    i, ch = 0, status = 0, pos = 0;


    /*  Initialize local task values.
     */
    ifstart = calloc (argc, sizeof (char *));
    iflist = ifstart;


    /*  Initialize the randome number generator.
     */
    srand ((unsigned int)time(NULL));


    /*  Parse the argument list.  The use of dl_paramInit() is required to
     *  rewrite the argv[] strings in a way dl_paramNext() can be used to
     *  parse them.  The programmatic interface allows "param=value" to
     *  be passed in, but the getopt_long() interface requires these to
     *  be written as "--param=value" so they are not confused with 
     *  positional parameters (i.e. any param w/out a leading '-').
     */
    prog_name = argv[0];
    pargv = dl_paramInit (argc, argv, opts, long_opts);
    memset (optval, 0, SZ_FNAME);
    while ((ch = dl_paramNext(opts,long_opts,argc,pargv,optval,&pos)) != 0) {
        if (ch == PARG_ERR)
            continue;
        if (ch > 0) {
	    /*  If the 'ch' value is > 0 we are parsing a single letter
	     *  flag as defined in the 'opts string.
	     */
	    switch (ch) {
	    case 'h':  Usage ();			return (OK);
	    case 'd':  debug++;                         break;  // --debug
	    case 'v':  verbose++;                       break;  // --verbose
	    case 'n':  noop++;                          break;  // --noop

	    case 'b':  bundle = dl_atoi (optval);	break;  // --bundle
	    case 'c':  chunk_size = dl_atoi (optval);	break;  // --chunk_size
	    case 'e':  extnum = dl_atoi (optval);	break;  // --extnum
	    case 'E':  extname = strdup (optval);	break;  // --extname
	    case 'r':  rows = strdup (optval);		break;  // --rows
	    case 's':  expr = strdup (optval);	        break;  // --select
	    case 't':  tablename = strdup (optval);	break;  // --table

	    case 'B':  do_binary++;			break;  // --binary
	    case 'C':  concat++;			break;  // --concat
	    case 'X':  explode++;			break;  // --explode
	    case 'H':  header = 0;			break;  // --noheader
	    case 'Q':  do_quote = 0;			break;  // --noquote
	    case 'N':  do_strip = 0;			break;  // --nostrip
	    case 'O':  do_oids = 0;			break;  // --oid
	    case 'Z':  do_load = 0;			break;  // --noload
	    case 'S':  quote_char = '\'';		break;  // --quote

	    case 'i':  iname = strdup (optval);		break;  // --input
	    case 'o':  oname = strdup (optval);		break;  // --output

	    case '0':  delimiter = ' ';			break;  // ASV
	    case '1':  delimiter = '|';			break;  // BSV
	    case '2':  delimiter = ',';			break;  // CSV
	    case '3':  delimiter = '\t';		break;  // TSV
	    case '4':  delimiter = '|'; 
                       format = TAB_IPAC; 	        break;

	    case '5':  if (optval[0] == 'm') {          // MySQL ouptut
                            format = TAB_MYSQL;
                            delimiter = ',';
                            do_quote = 1;
                            quote_char = '"';
                       } else if (optval[0] == 's') {   // MySQL ouptut
                           format = TAB_SQLITE;
                       } else {                         // default to Postgres
                            format = TAB_POSTGRES;
                            delimiter = '\t';
                            do_quote = 0;
                       }
                       break;
	    case '6':  do_drop++, do_create++;          break;  // --drop
	    case '7':  do_create++;                     break;  // --create
	    case '8':  do_truncate++;                   break;  // --truncate
	    case 'L':  sidname = strdup (optval);       break;  // --sid
	    case 'U':  ridname = strdup (optval);       break;  // --rid
	    case 'D':  dbname = strdup (optval);        break;  // --dbname
	    case 'A':  addname = strdup (optval);       break;  // --add

	    default:
		fprintf (stderr, "%s: Invalid option '%s'\n", 
					prog_name, optval);
		return (ERR);
	    }

	} else {
	    /*  All non-opt arguments are input files to process.
	     */
	    *iflist++ = strdup (optval);
            nfiles++;
	}

        memset (optval, 0, SZ_FNAME);
    }
    *iflist = NULL;


    if (debug) {
        fprintf (stderr, "do_create=%d  do_drop=%d  do_truncate=%d\n",
            do_create, do_drop, do_truncate);
        fprintf (stderr, "extnum=%d  extname='%s' rows='%s' expr='%s'\n",
            extnum, extname, rows, expr);
        fprintf (stderr, "delimiter='%c' dbname='%s' sidname='%s' ridname='%s'\n",
            delimiter, dbname, sidname, ridname);
        fprintf (stderr, "table = '%s'\n", (tablename ? tablename : "<none>"));
        for (i=0; i < nfiles; i++)
             fprintf (stderr, "in[%d] = '%s'\n", i, ifstart[i]);
        if (noop)
            return (0);
    }

    /*  Sanity checks.  Tasks should validate input and accept stdin/stdout
     *  where it makes sense.
     */
    if (iname && *ifstart == NULL)
        *ifstart = *iflist = iname;
    if (*ifstart == NULL) {
        dl_error (2, "no input files specified", NULL);
        return (ERR);
    }
    if (extnum >= 0 && extname) {
        dl_error (3, "Only one of 'extname' or 'extnum' may be specified\n",
            NULL);
        return (ERR);
    }
    if (rows) {
        fprintf (stderr,
            "Warning: 'rows' option not yet implemented, skipping\n");
        return (ERR);
    }
    if (do_binary)
        bundle = 1;


    /*  Generate the output file lists if needed.
     */
    if (nfiles == 1 || concat) {
        /*  If we have 1 input file, output may be to stdout or to the named
         *  file only.
         */
        if (oname && strcmp (oname, "-") == 0)
            free (oname), oname = NULL;
        if (oname == NULL) 
            oname = strdup ("stdout");
    } else {
        if (oname)
            /*  For multiple files, the output arg specifies a root filename.
             *  We append the file number and a ".csv" extension.
             */
            basename = oname;
        else
            /*  If we don't specify an output name, use the input filename
             *  and replace the extension.
             */
            basename = NULL;
    }


    /* Compute and output the image metadata. */
    if (*ifstart == NULL) {
        dl_error (2, "no input source specified", NULL);
        return (ERR);

    } else {
        char ofname[SZ_PATH], ifname[SZ_PATH];
        int  ndigits = (int) log10 (nfiles) + 1, bnum = 0;


        if (debug) {
            for (iflist=ifstart, i=0; *iflist; iflist++, i++) {
                fprintf (stderr, "%d: '%s'\n", i, *iflist);
            }
        }

        for (iflist=ifstart, i=0; *iflist; iflist++, i++) {

            memset (ifname, 0, SZ_PATH);
            memset (ofname, 0, SZ_PATH);

            /*  Construct the input filename and append filename modifiers
             *  to do table/row filtering.
             */
            strcpy (ifname, *iflist);
            if (access (ifname, F_OK) < 0) {
                fprintf (stderr, "Error: Cannot access file '%s'\n", ifname);
                continue;
            }

            if (extnum >= 0) {
                memset (tmp, 0, SZ_FNAME);
                sprintf (tmp, "%s[%d]", ifname, extnum);
                strcpy (ifname, tmp);
            }
            if (extname) {
                memset (tmp, 0, SZ_FNAME);
                sprintf (tmp, "%s[%s]", ifname, extname);
                strcpy (ifname, tmp);
            }
            if (expr) {
                memset (tmp, 0, SZ_FNAME);
                sprintf (tmp, "%s[%s]", ifname, expr);
                strcpy (ifname, tmp);
            }

            /*  Construct the output filename.
             */
            if (basename) {
                if (concat) {
                    if (i == 0)
                        sprintf (ofname, "%s.%s", basename, dl_fextn());
                    else if (i > 0)
                        sprintf (ofname, "%s%*d.%s", basename, ndigits, 
                                    i, dl_fextn());
                } else 
                    sprintf (ofname, "%s%*d.%s", basename, ndigits, 
                                i, dl_fextn());

            } else if (oname) {
                strcpy (ofname, oname);

            } else {
                char *in = strdup (ifname);
                char *ip = (in + strlen(in) - 1);

                do {
                    *ip-- = '\0';
                } while (*ip != '.' && ip > in);
                *ip = '\0';
                sprintf (ofname, "%s.%s", in, dl_fextn());

                free ((char *) in);
            }

            omode = ((concat && i > 0) ? "a+" : "w+");

            if (debug)
                fprintf (stderr, "ifname='%s'  ofname='%s'\n", ifname, ofname);


            /*  Do the conversion if we have a FITS file.
             */
            if (dl_isFITS (ifname) || dl_isGZip (ifname)) {
                if (verbose)
                    fprintf (stderr, "Processing file: %s\n", ifname);

                if (!noop)
                    dl_fits2db (ifname, ofname, i, bnum, nfiles);

                /* Increment the filenumber within the bundle so we can keep
                 * track of headers.
                 */
                bnum = ((bnum+1) == bundle ? 0 : (bnum+1));
            } else
                fprintf (stderr, "Error: Skipping non-FITS file '%s'.\n", 
                                    ifname);
        }
    }

    if (status)
        fits_report_error (stderr, status);     // print any error message


    /*  Clean up.  Rememebr to free whatever pointers were created when
     *  parsing arguments.
     */
    for (iflist=ifstart; *iflist; iflist++)     // free the file list
        free ((void *) *iflist), *iflist = NULL;

    if (rows) free (rows);
    if (expr) free (expr);
    if (iname) free (iname);
    if (oname) free (oname);
    if (extname) free (extname);
    if (tablename) free (tablename);
    if (ifstart) free ((void *) ifstart);

    dl_paramFree (argc, pargv);

    return (status);	/* status must be OK or ERR (i.e. 0 or 1)     	*/
}


/**
 *  DL_FITS2DB -- Convert a FITS file to a database, i.e. actual SQL code or
 *  some ascii 'database' table like a CSV.
 */
static void
dl_fits2db (char *iname, char *oname, int filenum, int bnum, int nfiles)
{
    fitsfile *fptr = (fitsfile *) NULL;
    int   status = 0;
    long  jj, nrows;
    int   hdunum, hdutype, ncols, i, j;
    int   firstcol = 1, lastcol = 0, firstrow = 1;
    int   nelem, chunk = chunk_size;

    FILE  *ofd = (FILE *) NULL;
    //ColPtr col = (ColPtr) NULL;

    unsigned char *data = NULL, *dp = NULL;
    long   naxis1, rowsize = 0, nbytes = 0, firstchar = 1, totrows = 0;


    mach_swap = is_swapped ();

    if (!fits_open_file (&fptr, iname, READONLY, &status)) {
        if ( fits_get_hdu_num (fptr, &hdunum) == 1 )
            /*  This is the primary array;  try to move to the first extension
             *  and see if it is a table.
             */
            fits_movabs_hdu (fptr, 2, &hdutype, &status);
         else 
            fits_get_hdu_type (fptr, &hdutype, &status); /* Get the HDU type */

        if (hdutype == IMAGE_HDU) 
            printf ("Error: this program only converts tables, not images\n");

        else {
            fits_get_num_rows (fptr, &nrows, &status);
            fits_get_num_cols (fptr, &ncols, &status);

            lastcol = ncols;
            chunk = (chunk > nrows ? nrows : chunk);
            nelem = chunk;
            

            /*  Get the optimal I/O row size.
             */
            fits_get_rowsize (fptr, &rowsize, &status);
            nelem = rowsize;


            /*  Open the output file.
             */
            if (strcasecmp (oname, "stdout") == 0 || oname[0] == '-')
                ofd = stdout;
            else {
                if ((ofd = fopen (oname, omode)) == (FILE *) NULL)
                    dl_error (3, "Error opening output file '%s'\n", oname);
            }

            /*  Print column names as column headers when writing a new file,
             *  skip if we're appending output.
             *
             *  FIXME -- Need to add a check that new file matches columns
             *           when we have multi-file input.
             */
            fits_read_key (fptr, TLONG, "NAXIS1", &naxis1, NULL, &status);
            if (filenum == 0 || !concat) {
		dl_getColInfo (fptr, firstcol, lastcol);

                if (!tablename) 
                    tablename = dl_makeTableName (iname);

                if (format == TAB_DELIMITED)
                    dl_printHdr (firstcol, lastcol, ofd);
                else if (format == TAB_IPAC)
                    dl_printIPACTypes (iname, fptr, firstcol, lastcol, ofd);
                else {
                    int c = 0;

                    /*  Binary mode is only supported for Postgres, and not
                     *  for array operations.  Disable if needed but issue
                     *  a warning.
                     */
                    if (do_binary) {
                        for (c=firstcol; c <= lastcol; c++) {
                            ColPtr col = (ColPtr) &inColumns[c];
                            if (col->type != TSTRING && col->repeat > 1) {
                                fprintf (stderr, "Warning: binary mode not "
                                    "supported for array columns, disabling\n");
                                fflush (stderr);
                                do_binary = 0;
                                break;
                            }
                        }
                    }

                    // This is some sort of SQL output.
                    if (do_create)
                        dl_createSQLTable (tablename, fptr, firstcol, lastcol,
                            ofd);
                    if (do_truncate)
                        fprintf (ofd, "TRUNCATE TABLE %s;\n", tablename);

                }
            } else {
                // Make sure this file has the same columns.
                if (dl_validateColInfo (fptr, firstcol, lastcol)) {
                    fprintf (stderr, "Skipping unmatching table '%s'\n", 
                        iname);
                    return;
                }
            }


            /*  If we're not loading the database, close the file and return.
             */
            if (do_load == 0) {
                fits_close_file (fptr, &status);
                if (status)                 /* print any error message */
                    fits_report_error (stderr, status);
                return;
            }


            /*  At the beginning of each file bundle, print the appropriate
             *  COPY/INSERT statement.  This helps avoid memory problems in
             *  the database clients we write to.
             */
            if (bnum == 0 && TAB_DBTYPE(format))
                dl_printSQLHdr (tablename, fptr, firstcol, lastcol, ofd);


            /*  Allocate the I/O buffer.
             */                
            nbytes = nelem * naxis1;
            if (debug)
                fprintf (stderr, "nelem=%d  naxis1=%ld  nbytes=%ld  nrows=%d\n",
                    nelem, naxis1, nbytes, (int)nrows);
                
            data = (unsigned char *) calloc (1, nbytes * 8);
            obuf = (char *) calloc (1, nbytes * 8);
            olen = 0;

            /*  Loop over the rows in the table in optimal chunk sizes.
             */
            for (jj=firstrow; jj <= nrows; jj += nelem) {
                if ( (jj + nelem) >= nrows)
                    nelem = (nrows - jj + 1);

                /*  Read a chunk of data from the file.
                 */
                nbytes = nelem * naxis1;
                fits_read_tblbytes (fptr, firstrow, firstchar, nbytes,
                    data, &status);
                if (status) {                   /* print any error message */
                    fits_report_error (stderr, status);
                    break;
                }

                /* Process the chunk by parsing the binary data and printing
                 * out according to column type.
                 */
                dp = data;
                optr = obuf;
                olen = 0;

                for (j=firstrow; j <= nelem; j++) {
                    if (format == TAB_POSTGRES && do_binary) {
                        unsigned short val = 0;
                        val = (explode ? htons ((short) numOutCols) : 
                                         htons ((short) ncols));
                        memcpy (optr, &val, sz_short);
                        optr += sz_short;
                        olen += sz_short;

                    } else if (single && 
                        (format == TAB_SQLITE || format == TAB_MYSQL)) {
                            // For SQLite we print the header for each row.
                            dl_printHdrString (tablename);
                    }
                
                    /*  Print all the columns in the table.
                     */
                    for (i=firstcol; i <= ncols; i++)
                        dp = dl_printCol (dp, &inColumns[i], 
                                    (i < ncols ? delimiter : '\n'));


                    if (format == TAB_MYSQL || format == TAB_SQLITE) {
                        // Add a comma for all but the last row of a table.
                        if (j < nelem)
                            *optr++ = ',', olen++;

                        // Add a comma if there will be more tables to follow.
                        else if (filenum < (nfiles-1) && bnum < (bundle-1))
                            *optr++ = ',', olen++;
                    }

                    if (! do_binary)
                        *optr++ = '\n', olen++;     // terminate the row
                }
                write (fileno(ofd), obuf, olen);
                fflush (ofd);

                /*  Advance the offset counters in the file.
                 */
                firstchar += nbytes;
                totrows += nelem;
            }


            /*  Terminate the output stream.
             */
            if ((concat && filenum == (nfiles-1)) || 
                (bnum > 0 && bnum == (bundle-1))) {

                if (format == TAB_POSTGRES) {
                    optr = obuf, olen = 0;
//fprintf (stderr, "writing EOF  f=%d nf=%d  bn=%d bun=%d\n",
//        filenum, nfiles, bnum, bundle);
                    memset (optr, 0, nbytes);
                    if (do_binary) {
                        short  eof = -1;
                        memcpy (optr, &eof, sz_short),   olen += sz_short;
                    } else
                        memcpy (optr, "\\.\n", 3),       olen += 3;

                    write (fileno(ofd), obuf, olen);
                    fflush (ofd);

                } else if (format == TAB_MYSQL || format == TAB_SQLITE) {
                    write (fileno(ofd), ";\n" , 2);
                    fflush (ofd);
                }
            }


            /*  Free the column structures and data pointers.
             */
            if (data) free ((char *) data);
            if (obuf) free ((void *) obuf);
            //for (ii = firstcol; ii <= lastcol; ii++) {
            //    col = (ColPtr) &inColumns[ii];
            //    free ((void *) col);
            //}

            /*  Close the output file.
             */
            if (ofd != stdout)
                fclose (ofd);
        }
    }
    fits_close_file (fptr, &status);

    if (status)                                 /* print any error message */
        fits_report_error (stderr, status);
}


/**
 *  DL_GETCOLINFO -- Get information about the columns in teh table.
 */
static void
dl_getColInfo (fitsfile *fptr, int firstcol, int lastcol)
{
    register int i;
    char  keyword[FLEN_KEYWORD], dims[FLEN_KEYWORD];
    ColPtr icol = (ColPtr) NULL;
    int   status = 0;


    /* Gather information about the input columns.
     */
    for (i = firstcol, numInCols = 0; i <= lastcol; i++, numInCols++) {
        icol = (ColPtr) &inColumns[i];
        memset (icol, 0, sizeof(Col));

        status = 0;                             // reset CFITSIO status
        memset (keyword, 0, FLEN_KEYWORD);
        fits_make_keyn ("TTYPE", i, keyword, &status);
        fits_read_key (fptr, TSTRING, keyword, icol->colname, NULL, &status);
        fits_get_coltype (fptr, i, &icol->type, &icol->repeat,
            &icol->width, &status);
        fits_get_col_display_width (fptr, i, &icol->dispwidth, &status);
        if (icol->type == TSTRING && do_quote) 
            icol->dispwidth+= 2;
        icol->colnum = i;

        icol->ndim = 1;		                // default dimensions
        icol->nrows = 1;
        icol->ncols = icol->repeat;

        if (icol->repeat > 1 && icol->type != TSTRING && explode) {
            memset (keyword, 0, FLEN_KEYWORD);
            fits_make_keyn ("TDIM", i, keyword, &status);
            fits_read_key (fptr, TSTRING, keyword, dims, NULL, &status);
            if (status == 0)    // Dimension string usually means a 2-D array
                icol->ndim = sscanf (dims, "(%d,%d)", 
                    &icol->nrows, &icol->ncols);
            status = 0;
        }
    }

    if (debug) {
        fprintf (stderr, "Input Columns [%d]:\n", numInCols);
        for (i=1; i <= numInCols; i++) {
            icol = (ColPtr) &inColumns[i];
            fprintf (stderr, "  %d  '%s'  rep=%ld nr=%d nc=%d\n", icol->colnum, 
                icol->colname, icol->repeat, icol->nrows, icol->ncols);
        }
    }

    /* Now expand to create the output column information so we don't need 
     * to repeat this for each table type.  If we're exploding columns
     * compute all the output columns names, otherwise simply copy the input
     * names.
     */
    dl_getOutputCols (fptr, firstcol, lastcol);
}


/**
 *  DL_VALIDATECOLINFO -- Validate that this file has the same column
 *  information.
 */
static int
dl_validateColInfo (fitsfile *fptr, int firstcol, int lastcol)
{
    register int i;
    char   keyword[FLEN_KEYWORD], dims[FLEN_KEYWORD];
    Col    newColumns[MAX_COLS], *col = (ColPtr) NULL, *icol = (ColPtr) NULL;
    int    numCols, status = 0;


    /* Gather information about the input columns.
     */
    for (i = firstcol, numCols = 0; i <= lastcol; i++, numCols++) {
        col = (ColPtr) &newColumns[i];
        memset (col, 0, sizeof(Col));
        col->colnum = i;

        status = 0;                             // reset CFITSIO status
        memset (keyword, 0, FLEN_KEYWORD);
        fits_make_keyn ("TTYPE", i, keyword, &status);
        fits_read_key (fptr, TSTRING, keyword, col->colname, NULL, &status);
        fits_get_coltype (fptr, i, &col->type, &col->repeat, &col->width,
            &status);

        col->ndim = 1;				// default dimensions
        col->nrows = 1;
        col->ncols = col->repeat;

        if (col->repeat > 1 && col->type != TSTRING && explode) {
            memset (keyword, 0, FLEN_KEYWORD);
            fits_make_keyn ("TDIM", i, keyword, &status);
            fits_read_key (fptr, TSTRING, keyword, dims, NULL, &status);
            if (status == 0)
                col->ndim = sscanf (dims, "(%d,%d)", &col->nrows, &col->ncols);
            status = 0;
        }
    }

    if (debug) {
        fprintf (stderr, "Table Columns [%d]:\n", numCols);
        for (i=1; i <= numCols; i++) {
            col = (ColPtr) &newColumns[i];
            fprintf (stderr, "  %d  '%s'  rep=%ld nr=%d nc=%d\n", col->colnum, 
                col->colname, col->repeat, col->nrows, col->ncols);
        }
    }

    /*  Check column names, dimensionality, and type for equality.
     */
    for (i=firstcol; i <= lastcol; i++) {
        col = (ColPtr) &newColumns[i];
        icol = (ColPtr) &inColumns[i];

        if (strcmp (col->colname, icol->colname))
            return (1);
        if (col->type != icol->type ||
            col->ndim != icol->ndim ||
            col->nrows != icol->nrows)
                return (1);
        if (col->type != TSTRING)
            if (col->ncols != icol->ncols || col->repeat != icol->repeat)
                return (1);
    }


    /*  If we have a valid set of columns, copy the current table structure
     *  so we process the input file correctly.
     */
    if (status == 0)
        memcpy (&inColumns[0], &newColumns[0], ((numCols + 1) * sizeof (Col)));

    return (status);                                    // No error
}


/**
 *  DL_GETOUTPUTCOLS -- Get the output column information.
 */
static void
dl_getOutputCols (fitsfile *fptr, int firstcol, int lastcol)
{
    register int i, j, ii, jj;
    ColPtr icol = (ColPtr) NULL;
    ColPtr ocol = (ColPtr) NULL;


    if (explode) {
        jj = firstcol;
        for (ii = firstcol; ii <= lastcol; ii++) {
            icol = (ColPtr) &inColumns[ii];

            if (icol->repeat > 1 && icol->type != TSTRING) {
                if (icol->ndim > 1) {                           // 2-D array
                    for (i=1; i <= icol->nrows; i++) {
                        for (j=1; j <= icol->ncols; j++)  {
                            ocol = (ColPtr) &outColumns[jj++];
                            memset (ocol->colname, 0, SZ_COLNAME);
                            sprintf (ocol->colname, "%s_%d_%d",
                                icol->colname, i, j);
                            strcpy (ocol->coltype, dl_colType (icol));
                            ocol->dispwidth = icol->dispwidth;
                        }
                    }
                } else {                                        // 1-D array
                    for (i=1; i <= icol->repeat; i++) {
                        ocol = (ColPtr) &outColumns[jj++];
                        memset (ocol->colname, 0, SZ_COLNAME);
                        sprintf (ocol->colname, "%s_%d", icol->colname, i);
                        strcpy (ocol->coltype, dl_colType (icol));
                        ocol->dispwidth = icol->dispwidth;
                    }
                }
            } else {
                ocol = (ColPtr) &outColumns[jj++];
                memset (ocol->colname, 0, SZ_COLNAME);
                strcpy (ocol->colname, icol->colname);
                strcpy (ocol->coltype, dl_colType (icol));
                ocol->dispwidth = icol->dispwidth;
            }
        }
        numOutCols = jj - 1;

    } else {
        for (i = firstcol, numOutCols = 0; i <= lastcol; i++, numOutCols++) {
            icol = (ColPtr) &inColumns[i];
            ocol = (ColPtr) &outColumns[i];
            memcpy (ocol, icol, sizeof(Col));

            if (format == TAB_IPAC)
                strcpy (ocol->coltype, dl_IPACType (icol));
            else if (TAB_DBTYPE(format))
                strcpy (ocol->coltype, dl_SQLType (icol));
        }
        numOutCols = numInCols;
    }


    /*  Add a column to the output table.  For now, assume it's an integer
     *  column we'll default to a zero value.
     */
    if (addname) {
        ocol = (ColPtr) &outColumns[++numOutCols];
        memset (ocol->colname, 0, SZ_COLNAME);
        strcpy (ocol->colname, addname);
        strcpy (ocol->coltype, "integer");
    }

    /*  If we're creating a serial ID column, add it to the output list.
     */
    if (sidname) {
        ocol = (ColPtr) &outColumns[numOutCols+1];
        memset (ocol->colname, 0, SZ_COLNAME);
        strcpy (ocol->colname, sidname);

        if (format == TAB_IPAC)
            strcpy (ocol->coltype, "integer");
        else if (format == TAB_POSTGRES) {
            /*  For the serial ID column we need to create it as a simple
             *  integer column to allow for parallel ingests of large
             *  catalogs or numbers of files.  Once the table is fully
             *  ingested the column type can be changed to a 'serial' type
             *  using the commands:
             *
             *     CREATE SEQUENCE <id_seq>;
             *     ALTER TABLE <table> ALTER COLUMN <id>
             *          SET DEFAULT nextval('<id_seq>'::regclass);
             *
             *  Once created, a sequence can be reset (e.g. when rows are
             *  deleted or re-ordered) with the commands:
             *
             *     ALTER SEQUENCE <seq> RESTART [ WITH <start_num> ];
             *     UPDATE <table> set <id_column> = DEFAULT;
             */
            strcpy (ocol->coltype, "integer");
            //strcpy (ocol->coltype, "serial primary key");
        }
        numOutCols++;
    }

    /*  If we're creating a serial ID column, add it to the output list.
     */
    if (ridname) {
        ocol = (ColPtr) &outColumns[numOutCols+1];
        memset (ocol->colname, 0, SZ_COLNAME);
        strcpy (ocol->colname, ridname);

        if (format == TAB_IPAC || format == TAB_POSTGRES) 
            strcpy (ocol->coltype, "real");
        numOutCols++;
    }


    if (debug) {
        fprintf (stderr, "Output Columns [%d]:\n", numOutCols);
        for (i=1; i <= numOutCols; i++) {
            ocol = (ColPtr) &outColumns[i];
            fprintf (stderr, "  %d  %-24s  '%s'\n", ocol->colnum, 
                ocol->colname, ocol->coltype);
        }
    }
}


/**
 *  DL_PRINTHDR -- Print the CSV column headers.
 */
static void
dl_printHdr (int firstcol, int lastcol, FILE *ofd)
{
    register int i, ncols = numOutCols;
    ColPtr col = (ColPtr) NULL;


    //if (*omode == 'a')
    //    return;

    if (format == TAB_IPAC)
        fprintf (ofd, "|");

    /*  If we're using a serial ID column it isn't included in the data list
     *  since the database fills in the value for us.  So, don't include it in 
     *  the header when doing Postgres. 
    if (format == TAB_POSTGRES && sidname)
        ncols--;
     */

    for (i=1; i <= ncols; i++) {           // print column types
        col = (ColPtr) &outColumns[i];

        if (format == TAB_IPAC)                 // FIXME
            fprintf (ofd, "%-*s", col->dispwidth, col->colname);
        else
            fprintf (ofd, "%-s", col->colname);
        if (i < ncols)
            //fprintf (ofd, "%c", delimiter);
            fprintf (ofd, "%c", ',');
    }

    if (format == TAB_IPAC)
        fprintf (ofd, "|");

    if (format == TAB_IPAC || format == TAB_DELIMITED)
        fprintf (ofd, "\n");
    fflush (ofd);
}


/**
 *  DL_PRINTHDRSTRING -- Print the CSV column headers.
 */
static void
dl_printHdrString (char *tablename)
{
    register int i, ncols = numOutCols, len;
    char   buf[160];
    ColPtr col = (ColPtr) NULL;


    memset (buf, 0, 160);
    sprintf (buf, "INSERT INTO %s (", tablename);
    memcpy (optr, buf, (len = strlen (buf)));
    optr += len, olen += len;


    for (i=1; i <= ncols; i++) {                // print column types
        col = (ColPtr) &outColumns[i];

        len = strlen (col->colname);
        memcpy (optr, col->colname, len);
        optr += len;
        olen += len;

        if (i < ncols) {
            *optr++ = ',';
            olen += 1;
        }
    }

    memset (buf, 0, 160);
    sprintf (buf, ") VALUES ");
    memcpy (optr, buf, (len = strlen (buf)));
    optr += len, olen += len;
}


/**
 *  DL_CREATESQLTABLE -- Print the SQL CREATE command.
 */
static void
dl_createSQLTable (char *tablename, fitsfile *fptr, int firstcol, int lastcol,
                    FILE *ofd)
{
    register int  i;
    ColPtr col = (ColPtr) NULL;


    /*  For MySQL we assume the output is being piped to the 'mysql' client,
     *  if the database is being created we need to also create it before 
     *  creating the table.  If we don't specify the 'dbname' assume it exists
     *  and is an arg to the mysql client, so simply create the table.
     */
    if (dbname && format == TAB_MYSQL) {
        fprintf (ofd, "CREATE DATABASE IF NOT EXISTS %s;\n", dbname);
        fprintf (ofd, "USE %s;\n", dbname);
    }

                    
    if (do_drop)
        fprintf (ofd, "DROP TABLE IF EXISTS %s CASCADE;\n", tablename);
                        
    fprintf (ofd, "CREATE TABLE IF NOT EXISTS %s (\n", tablename);

    for (i=1; i <= numOutCols; i++) {             // print column types
        col = (ColPtr) &outColumns[i];
        fprintf (ofd, "    %s\t%s", col->colname, col->coltype);
        if (i < numOutCols)
            fprintf (ofd, ",\n");
    }

    if (do_oids && format == TAB_POSTGRES)
        // For Postgres only, allow creation of OIDS.
        fprintf (ofd, "\n) WITH OIDS;\n\n");
    else
        fprintf (ofd, "\n);\n\n");

    fflush (ofd);
}


/**
 *  DL_PRINTSQLHDR -- Print the SQL COPY headers.
 */
static void
dl_printSQLHdr (char *tablename, fitsfile *fptr, int firstcol, int lastcol,
                    FILE *ofd)
{
    int   hdr_extn = 0;
    char  copy_buf[160];


    if (! do_load)
        return;

    if (do_binary && format == TAB_POSTGRES) {
        memset (copy_buf, 0, 160);
        sprintf (copy_buf, "COPY %s FROM stdin WITH BINARY;\n", tablename);

        if (!noop)
            write (fileno(ofd), copy_buf, strlen(copy_buf));   // header string

        write (fileno(ofd), pgcopy_hdr, len_pgcopy_hdr);   // header string
        write (fileno(ofd), &hdr_extn, sz_int);            // header extn length

    } else {
        if (format == TAB_POSTGRES) {
            fprintf (ofd, "\nCOPY %s (", tablename);
            dl_printHdr (firstcol, lastcol, ofd);
            fprintf (ofd, ") from stdin;\n");
        } else if (format == TAB_MYSQL || format == TAB_SQLITE) {
            fprintf (ofd, "\nINSERT INTO %s (", tablename);
            dl_printHdr (firstcol, lastcol, ofd);
            fprintf (ofd, ") VALUES\n");
        }
    }
    fflush (ofd);
}


/**
 *  DL_PRINTIPACTYPES -- Print the IPAC column type headers.
 */
static void
dl_printIPACTypes (char *tablename, fitsfile *fptr, int firstcol, int lastcol,
                    FILE *ofd)
{
    register int i;
    ColPtr col = (ColPtr) NULL;


    if (*omode == 'a' || format != TAB_IPAC)
        return;

    dl_printHdr (firstcol, lastcol, ofd);               // print column names

    fprintf (ofd, "|");
    for (i=1; i <= numOutCols; i++) {                   // print column types
        col = (ColPtr) &outColumns[i];
        fprintf (ofd, "%-*s|", col->dispwidth, col->coltype);
    }

    fprintf (ofd, "\n");
    fflush (ofd);
}


/**
 * DL_COLTYPE -- Get the type string for the column.
 */
static char *
dl_colType (ColPtr col)
{
    if (format == TAB_POSTGRES)
        return dl_SQLType (col);

    else if (format == TAB_MYSQL)
        return dl_SQLType (col);

    else if (format == TAB_SQLITE)
        return dl_SQLType (col);

    else //if (format == TAB_IPAC)
        return dl_IPACType (col);
}


/**
 * DL_SQLTYPE -- Get the SQL type string for the column.
 */
static char *
dl_SQLType (ColPtr col)
{
    static char *type = NULL;
    static char tbuf[SZ_VALBUF];


    switch (col->type) {
    case TBIT:                                          // NYI
    case TCOMPLEX:
    case TDBLCOMPLEX:      
                    break;
    case TSTRING:   type = ((col->repeat > 1) ? "text" : "char");
                    break;
    case TLOGICAL:  type = "smallint";
                    break;

    case TBYTE:
    case TSBYTE:    type = "smallint";
		    break;
    case TSHORT:
    case TUSHORT:   type = "smallint";
		    break;
    case TINT:
    case TUINT:
    case TINT32BIT: type = "integer";
		    break;

    case TLONGLONG: type = "bigint";
                    break;
    case TFLOAT:    type = "real";
		    break;
    case TDOUBLE:   type = "double precision";
		    break;

    default:        fprintf (stderr, "Error: unsupported type %d\n", col->type);
    }

    memset (tbuf, 0, SZ_VALBUF);
    if (!explode && col->repeat > 1 && col->type != TSTRING)
        sprintf (tbuf, "%s[%ld]", type, col->repeat);
    else
        strcpy (tbuf, type);

    return (tbuf);
}


/**
 * DL_IPACTYPE -- Get the IPAC-table type string for the column.
 */
static char *
dl_IPACType (ColPtr col)
{
    char *type = NULL;

    switch (col->type) {
    case TBIT:                                          // NYI
    case TCOMPLEX:                                      // NYI
    case TDBLCOMPLEX:                                   // NYI
                    break;

    case TSTRING:   type = "char";          
                    break;
    case TLOGICAL:
    case TBYTE:
    case TSBYTE:
    case TSHORT:
    case TUSHORT:
    case TINT:
    case TUINT:
    case TINT32BIT:
    case TLONGLONG: type = "int";
                    break;
    case TFLOAT:    type = "real";
                    break;
    case TDOUBLE:   type = "double";
                    break;
    default:        type = " ";
    }

    return (type);
}


/**
 *  DL_PRINTCOL -- Print the column value(s).
 *
 *  FIXME -- We don't handled unsigned or long correctly yet.
 */

#define SZ_TXTBUF               16738

static unsigned char *
dl_printCol (unsigned char *dp, ColPtr col, char end_char)
{
    if (!explode && !do_binary && col->type != TSTRING && col->repeat > 1) {
        if (format == TAB_DELIMITED) {
            *optr++ = quote_char, *optr++ = '(';
            olen += 2;
        } else {
            *optr++ = '{';
            olen += 1;
        }
    }

                    
    if (format == TAB_IPAC && col->colnum == 1)
        *optr++ = '|', olen++;
    if ((format == TAB_MYSQL || format == TAB_SQLITE) && col->colnum == 1)
        *optr++ = '(', olen++;

    switch (col->type) {
    case TBIT:                          // TFORM='X'    bit
        fprintf (stderr, "Error: Unsupported column type, col[%s] = %d\n", 
            col->colname, col->type);
        break;
    case TCOMPLEX:                      // TFORM='C'    complex
    case TDBLCOMPLEX:                   // TFORM='M'    double complex
        fprintf (stderr, "Error: Unsupported column type, col[%s] = %d\n", 
            col->colname, col->type);
        break;

    case TSTRING:                       // TFORM='A'    8-bit character
        dp = dl_printString (dp, col);
        break;

    case TLOGICAL:                      // TFORM='L'    8-bit logical (boolean)
        dp = dl_printLogical (dp, col);
        break;

    case TBYTE:                         // TFORM='B'    1 unsigned byte
    case TSBYTE:                        // TFORM='S'    8-bit signed byte
        dp = dl_printByte (dp, col);
        break;

    case TSHORT:                        // TFORM='I'    16-bit integer
    case TUSHORT:                       // TFORM='U'    unsigned 16-bit integer
        dp = dl_printShort (dp, col);
        break;

    case TINT:                          // TFORM='J'    32-bit integer
    case TUINT:                         // TFORM='V'    unsigned 32-bit integer
    case TINT32BIT:                     // TFORM='J'    signed 32-bit integer
        dp = dl_printInt (dp, col);
        break;

    case TLONGLONG:                     // TFORM='K'    64-bit integer
        dp = dl_printLong (dp, col);
        break;

    case TFLOAT:                        // TFORM='E'    single precision float
        dp = dl_printFloat (dp, col);
        break;

    case TDOUBLE:                       // TFORM='D'    double precision float
        dp = dl_printDouble (dp, col);
        break;

    default:
        fprintf (stderr, "Error: Unknown column type, col[%s] = %d\n", 
            col->colname, col->type);
        break;
    }

    if (!explode && !do_binary && col->type != TSTRING && col->repeat > 1) {
        if (format == TAB_DELIMITED) {
            *optr++ = quote_char, *optr++ = ')';
            olen += 2;
        } else {
            *optr++ = '}';
            olen += 1;
        }
    }


    if (end_char == '\n') {
        if (format == TAB_IPAC)
            *optr++ = '|', olen++;
        if ((format == TAB_MYSQL || format == TAB_SQLITE))
            *optr++ = ')', olen++;
    }
    
    /*  For Postgres binary output where we've specified a serial value, add
     *  it to the output stream at the end of a row.  For text formats, add
     *  it as the last column of data.
     */
    if (end_char == '\n') {
        if (addname) {
            if (!do_binary)
                *optr++ = delimiter, olen++;     // append the comma or newline
            dl_printValue (1);
        }
        if (sidname) {
            //if (format == TAB_POSTGRES && do_binary) {
            if (format == TAB_POSTGRES) {
		if (!do_binary)
                    *optr++ = delimiter, olen++; // append the comma or newline
                dl_printSerial ();
            } else if ((format == TAB_DELIMITED || format == TAB_IPAC)) {
                *optr++ = delimiter, olen++;     // append the comma or newline
                dl_printSerial ();
            } else
		printf ("Unsupported serial format\n");
        }
        if (ridname) {
            //if (format == TAB_POSTGRES && do_binary) {
            if (format == TAB_POSTGRES) {
		if (!do_binary)
                    *optr++ = delimiter, olen++; // append the comma or newline
                dl_printRandom ();
            } else if ((format == TAB_DELIMITED || format == TAB_IPAC)) {
                *optr++ = delimiter, olen++;     // append the comma or newline
                dl_printRandom ();
            } else
		printf ("Unsupported random format\n");
        }
    }

    if (!do_binary && end_char != '\n')
        *optr++ = end_char, olen++;     // append the comma or newline


    return (dp);
}


/**
 *  DL_PRINTSTRING -- Print the column as a string value.
 */
static unsigned char *
dl_printString (unsigned char *dp, ColPtr col)
{
    char  buf[SZ_TXTBUF], *bp;
    int   len = 0;


    if (do_binary) {
        unsigned int val = 0;
        val = htonl (col->repeat);

        //memcpy (optr, &val, sz_int);            optr += sz_int;
        //memcpy (optr, dp, col->repeat);         optr += col->repeat;

        memset (buf, 0, SZ_TXTBUF);
        memcpy (buf, dp, col->repeat);
        //len = strlen ((bp = sstrip(buf)));
        len = strlen ((bp = buf));
//fprintf (stderr, "STR:  '%s'  rep=%d  len=%d\n", buf, col->repeat, len);
        val = htonl (len);

        memcpy (optr, &val, sz_int);            optr += sz_int;
        memcpy (optr, bp, len);                 optr += len;

        olen += sz_int + len;

    } else {
        memset (buf, 0, SZ_TXTBUF);
        memcpy (buf, dp, col->repeat);
        if (do_escape) {
            dl_escapeCSV ((do_strip ? sstrip (buf) : buf));
            memcpy (optr, esc_buf, (len = strlen (esc_buf)));
        } else {
            if (do_quote) {
                dl_quote ((do_strip ? sstrip (buf) : buf));
                memcpy (optr, esc_buf, (len = strlen (esc_buf)));
            } else {
                bp =  sstrip(buf);
                memcpy (optr, bp, (len = strlen(bp)));
            }
        }

        olen += len;
        optr += len;
    }
    dp += col->repeat;

    return (dp);
}


/**
 *  DL_PRINTLOGICAL -- Print the column as logical values.
 */
static unsigned char *
dl_printLogical (unsigned char *dp, ColPtr col)
{
    char ch;
    char  valbuf[SZ_VALBUF];
    int   i, j, len = 0;


    if (do_binary) {
        unsigned int sz_val = 0;
        unsigned short lval = 0;
        if (explode) {
            len = sz_short;
            sz_val = htonl(sz_short);
            for (i=1; i <= col->nrows; i++) {
                for (j=1; j <= col->ncols; j++) {
                    memcpy (optr, &sz_val, sz_int);     optr += sz_int;
                    ch = (char) *dp++;
                    lval = ((tolower((int)ch) == 't') ? htons(1) : 0);
                    memcpy (optr, &lval, sz_short);     optr += sz_short;
                    olen += sz_int + len;
                }
            }
        } else {
            len = col->repeat * sz_short;
            sz_val = htonl(col->repeat * sz_short);
            memcpy (optr, &sz_val, sz_int);             optr += sz_int;

            for (i=1; i <= col->nrows; i++) {
                for (j=1; j <= col->ncols; j++) {
                    ch = (char) *dp++;
                    lval = ((tolower((int)ch) == 't') ? htons(1) : 0);
                    memcpy (optr, &lval, sz_short);     optr += sz_short;
                    olen += sz_short + len;
                }
            }
            olen += sz_int + sz_val;
        }

    } else {
        for (i=1; i <= col->nrows; i++) {
            for (j=1; j <= col->ncols; j++) {
                memset (valbuf, 0, SZ_VALBUF);

                ch = (char) *dp++;
                if (format == TAB_IPAC)
                    sprintf (valbuf, "%d", ((tolower((int)ch) == 't') ? 1 : 0));
                else
                    sprintf (valbuf, "%*d", col->dispwidth, 
                        ((tolower((int)ch) == 't') ? 1 : 0));
                memcpy (optr, valbuf, (len = strlen (valbuf)));
                olen += len;
                optr += len;
                if (col->repeat > 1 && j < col->ncols)
                    *optr++ = delimiter,  olen++;
            }
            if (col->repeat > 1 && i < col->nrows)
                *optr++ = delimiter,  olen++;
        }
    }

    return (dp);
}


/**
 *  DL_PRINTBYTE -- Print the column as byte values.
 */
static unsigned char *
dl_printByte (unsigned char *dp, ColPtr col)
{
    char ch;
    unsigned char uch;
    char  valbuf[SZ_VALBUF];
    int   i, j, len = 0;


    if (do_binary) {
        unsigned int sz_val = 0;
        short sval = 0;
        if (explode) {
            len = sz_short;
            sz_val = htonl(sz_short);
            for (i=1; i <= col->nrows; i++) {
                for (j=1; j <= col->ncols; j++) {
                    memcpy (optr, &sz_val, sz_int);     optr += sz_int;
                    sval = htons((short) *dp++);
                    memcpy (optr, &sval, sz_short);     optr += sz_short;
                    olen += sz_int + len;
                }
            }
        } else {
            len = col->repeat * sz_short;
            sz_val = htonl(col->repeat * sz_short);
            memcpy (optr, &sz_val, sz_int);             optr += sz_int;
            olen += sz_int;

            for (i=1; i <= col->nrows; i++) {
                for (j=1; j <= col->ncols; j++) {
                    sval = htons((short) *dp++);
                    memcpy (optr, &sval, sz_short);     optr += sz_short;
                    olen += sz_short;
                }
            }
        }
    } else {
        for (i=1; i <= col->nrows; i++) {
            for (j=1; j <= col->ncols; j++) {
                memset (valbuf, 0, SZ_VALBUF);
                if (col->type == TBYTE) {
                    uch = (unsigned char) *dp++;
                    if (format == TAB_IPAC)
                        sprintf (valbuf, "%*d", col->dispwidth, uch);
                    else
                        sprintf (valbuf, "%d", uch);
                } else {
                    ch = (char) *dp++;
                    if (format == TAB_IPAC)
                        sprintf (valbuf, "%*d", col->dispwidth, ch);
                    else
                        sprintf (valbuf, "%d", ch);
                }
                memcpy (optr, valbuf, (len = strlen (valbuf)));
                olen += len;
                optr += len;
                if (col->repeat > 1 && j < col->ncols)
                    *optr++ = delimiter,  olen++;
            }
            if (col->repeat > 1 && i < col->nrows)
                *optr++ = delimiter,  olen++;
        }
    }

    return (dp);
}


/**
 *  DL_PRINTSHORT -- Print the column as short integer values.
 */
static unsigned char *
dl_printShort (unsigned char *dp, ColPtr col)
{
    short sval = 0.0;
    unsigned short usval = 0.0;
    char  valbuf[SZ_VALBUF];
    int   i, j, len = 0;


    if (mach_swap && !do_binary)
        bswap2 ((char *)dp, (char *)dp, sz_short * col->repeat);

    if (do_binary) {
        unsigned int sz_val = 0;
        if (explode) {
            len = sz_short;
            sz_val = htonl(sz_short);
            for (i=1; i <= col->nrows; i++) {
                for (j=1; j <= col->ncols; j++) {
                    memcpy (optr, &sz_val, sz_int);     optr += sz_int;
                    memcpy (optr, dp, sz_short);        optr += len;
                    olen += sz_int + len;
                    dp += sz_short;
                }
            }
        } else {
            len = col->repeat * sz_short;
            sz_val = htonl(col->repeat * sz_short);
            memcpy (optr, &sz_val, sz_int);             optr += sz_int;
            memcpy (optr, dp, sz_short * col->repeat);  optr += len;
            olen += sz_int + len;
            dp += col->repeat * sz_short;
        }

    } else {
        for (i=1; i <= col->nrows; i++) {
            for (j=1; j <= col->ncols; j++) {
                memset (valbuf, 0, SZ_VALBUF);
                if (col->type == TUSHORT) {
                    memcpy (&usval, dp, sz_short);
                    if (format == TAB_IPAC)
                        sprintf (valbuf, "%*d", col->dispwidth, usval);
                    else
                        sprintf (valbuf, "%d", usval);
                } else {
                    memcpy (&sval, dp, sz_short);
                    if (format == TAB_IPAC)
                        sprintf (valbuf, "%*d", col->dispwidth, sval);
                    else
                        sprintf (valbuf, "%d", sval);
                }
                memcpy (optr, valbuf, (len = strlen (valbuf)));
                olen += len;
                optr += len;
                dp += sz_short;
                if (col->repeat > 1 && j < col->ncols)
                    *optr++ = delimiter,  olen++;
            }
            if (col->repeat > 1 && i < col->nrows)
                *optr++ = delimiter,  olen++;
        }
    }

    return (dp);
}


/**
 *  DL_PRINTINT -- Print the column as integer values.
 */
static unsigned char *
dl_printInt (unsigned char *dp, ColPtr col)
{
    int   ival = 0.0;
    unsigned int uival = 0.0;
    char  valbuf[SZ_VALBUF];
    int   i, j, len = 0;


    if (mach_swap && do_binary)
        bswap4 ((char *)dp, 1, (char *)dp, 1, sz_int * col->repeat);

    if (do_binary) {
        unsigned int sz_val = 0;
        if (explode) {
            len = sz_int;
            sz_val = htonl(sz_int);
            for (i=1; i <= col->nrows; i++) {
                for (j=1; j <= col->ncols; j++) {
                    memcpy (optr, &sz_val, sz_int);     optr += sz_int;
                    memcpy (optr, dp, sz_int);          optr += len;
                    olen += sz_int + len;
                    dp += sz_int;
                }
            }
        } else {
            len = col->repeat * sz_int;
            sz_val = htonl(col->repeat * sz_int);
            memcpy (optr, &sz_val, sz_int);             optr += sz_int;
            memcpy (optr, dp, sz_int * col->repeat);    optr += len;
            olen += sz_int + len;
            dp += col->repeat * sz_int;
        }

    } else {
        for (i=1; i <= col->nrows; i++) {
            for (j=1; j <= col->ncols; j++) {
                memset (valbuf, 0, SZ_VALBUF);
                if (col->type == TUINT) {
                    memcpy (&uival, dp, sz_int);
                    if (format == TAB_IPAC)
                        sprintf (valbuf, "%*d", col->dispwidth, uival);
                    else
                        sprintf (valbuf, "%d", uival);
                } else {
                    memcpy (&ival, dp, sz_int);
                    if (format == TAB_IPAC)
                        sprintf (valbuf, "%*d", col->dispwidth, ival);
                    else
                        sprintf (valbuf, "%d", ival);
                }
                memcpy (optr, valbuf, (len = strlen (valbuf)));
                olen += len;
                optr += len;
                dp += sz_int;
                if (col->repeat > 1 && j < col->ncols)
                    *optr++ = delimiter,  olen++;
            }
            if (col->repeat > 1 && i < col->nrows)
                *optr++ = delimiter,  olen++;
        }
    }

    return (dp);
}


/**
 *  DL_PRINTLONG -- Print the column as long integer values.
 */
static unsigned char *
dl_printLong (unsigned char *dp, ColPtr col)
{
    long  lval = 0.0;
    char  valbuf[SZ_VALBUF];
    int   i, j, len = 0;


    if (mach_swap && do_binary)
        bswap8 ((char *)dp, 1, (char *)dp, 1, sizeof(long) * col->repeat);
        // FIXME -- We're in trouble if we comes across a 64-bit int column
        //bswap4 ((char *)dp, 1, (char *)dp, 1, sz_long * col->repeat);

    if (do_binary) {
        unsigned int sz_val = 0;
        if (explode) {
            len = sz_long;
            sz_val = htonl(sz_long);
            for (i=1; i <= col->nrows; i++) {
                for (j=1; j <= col->ncols; j++) {
                    memcpy (optr, &sz_val, sz_int);     optr += sz_int;
                    memcpy (optr, dp, sz_long);         optr += len;
                    olen += sz_int + len;
                    dp += sz_long;
                }
            }
        } else {
            len = col->repeat * sz_long;
            sz_val = htonl(col->repeat * sz_long);
            memcpy (optr, &sz_val, sz_int);             optr += sz_int;
            memcpy (optr, dp, sz_long * col->repeat);   optr += len;
            olen += sz_int + len;
            dp += col->repeat * sz_long;
        }

    } else {
        for (i=1; i <= col->nrows; i++) {
            for (j=1; j <= col->ncols; j++) {
                memset (valbuf, 0, SZ_VALBUF);
                memcpy (&lval, dp, sz_long);
                if (format == TAB_IPAC)
                    sprintf (valbuf, "%*ld", col->dispwidth, lval);
                else
                    sprintf (valbuf, "%ld", lval);
                memcpy (optr, valbuf, (len = strlen (valbuf)));
                olen += len;
                optr += len;
                dp += sz_long;
                if (col->repeat > 1 && j < col->ncols)
                    *optr++ = delimiter,  olen++;
            }
            if (col->repeat > 1 && i < col->nrows)
                *optr++ = delimiter,  olen++;
        }
    }

    return (dp);
}


/**
 *  DL_PRINTFLOAT -- Print the column as floating-point values.
 */
static unsigned char *
dl_printFloat (unsigned char *dp, ColPtr col)
{
    float rval = 0.0;
    char  valbuf[SZ_VALBUF];
    int   i, j, sign = 1, len = 0;


    if (mach_swap && do_binary)
        bswap4 ((char *)dp, 1, (char *)dp, 1, sz_float * col->repeat);

    if (do_binary) {
        unsigned int sz_val = 0;
        if (explode) {
            len = sz_float;
            sz_val = htonl(sz_float);
            for (i=1; i <= col->nrows; i++) {
                for (j=1; j <= col->ncols; j++) {
                    memcpy (optr, &sz_val, sz_int);     optr += sz_int;
                    memcpy (optr, dp, sz_float);        optr += len;
                    olen += sz_int + len;
                    dp += sz_float;
                }
            }
        } else {
            len = col->repeat * sz_float;
            sz_val = htonl(col->repeat * sz_float);
            memcpy (optr, &sz_val, sz_int);             optr += sz_int;
            memcpy (optr, dp, sz_float * col->repeat);  optr += len;
            olen += sz_int + len;
            dp += col->repeat * sz_float;
        }

    } else {
        for (i=1; i <= col->nrows; i++) {
            for (j=1; j <= col->ncols; j++) {
                memset (valbuf, 0, SZ_VALBUF);
                memcpy (&rval, dp, sz_float);

                if (isnan (rval) ) {
                    if (format == TAB_SQLITE || format == TAB_MYSQL)
                        memcpy (optr, "'NaN'", (len = strlen ("'NaN'")));
                    else if (format == TAB_POSTGRES)
                        memcpy (optr, "NaN", (len = strlen ("NaN")));
                    else {
                        sprintf (valbuf, "%lf", (double) rval);
                        memcpy (optr, valbuf, (len = strlen (valbuf)));
                    }
                    olen += len, optr += len;

                } else if ((sign = isinf (rval)) ) {
                    if (format == TAB_SQLITE || format == TAB_MYSQL) {
                        char *val = (sign ? "'Infinity'" : "'-Infinity'");
                        memcpy (optr, val, (len = strlen (val)));

                    } else if (format == TAB_POSTGRES) {
                        char *val = (sign ? "Infinity" : "-Infinity");
                        memcpy (optr, val, (len = strlen (val)));

                    } else {
                        sprintf (valbuf, "%lf", (double) rval);
                        memcpy (optr, valbuf, (len = strlen (valbuf)));
                    }
                    olen += len, optr += len;

                } else {
                    if (format == TAB_IPAC)
                        sprintf (valbuf, "%*f", col->dispwidth, (double) rval);
                    else
                        sprintf (valbuf, "%f", (double) rval);

                    memcpy (optr, valbuf, (len = strlen (valbuf)));
                    olen += len, optr += len;
                }
                dp += sz_float;
                if (col->repeat > 1 && j < col->ncols)
                    *optr++ = delimiter,  olen++;
            }
            if (col->repeat > 1 && i < col->nrows)
                *optr++ = delimiter,  olen++;
        }
    }

    return (dp);
}


/**
 *  DL_PRINTDOUBLE -- Print the column as double-precision values.
 */
static unsigned char *
dl_printDouble (unsigned char *dp, ColPtr col)
{
    double dval = 0.0;
    char  valbuf[SZ_VALBUF];
    int   i, j, sign = 1, len = 0;


    if (mach_swap && !do_binary)
        bswap8 ((char *)dp, 1, (char *)dp, 1, sz_double * col->repeat);

    if (do_binary) {
        unsigned int sz_val = 0;
        if (explode) {
            len = sz_double;
            sz_val = htonl(sz_double);
            for (i=1; i <= col->nrows; i++) {
                for (j=1; j <= col->ncols; j++) {
                    memcpy (optr, &sz_val, sz_int);     optr += sz_int;
                    memcpy (optr, dp, sz_double);       optr += len;
                    olen += sz_int + len;
                    dp += sz_double;
                }
            }
        } else {
            len = col->repeat * sz_double;
            sz_val = htonl(col->repeat * sz_double);
            memcpy (optr, &sz_val, sz_int);             optr += sz_int;
            memcpy (optr, dp, sz_double * col->repeat); optr += len;
            olen += sz_int + len;
            dp += col->repeat * sz_double;
        }

    } else {
        for (i=1; i <= col->nrows; i++) {
            for (j=1; j <= col->ncols; j++) {
                memset (valbuf, 0, SZ_VALBUF);
                memcpy (&dval, dp, sizeof(double));

                if (isnan (dval) ) {
                    if (format == TAB_SQLITE || format == TAB_MYSQL)
                        memcpy (optr, "'NaN'", (len = strlen ("'NaN'")));
                    else if (format == TAB_POSTGRES)
                        memcpy (optr, "NaN", (len = strlen ("NaN")));
                    else {
                        sprintf (valbuf, "%.16lf", (double) dval);
                        memcpy (optr, valbuf, (len = strlen (valbuf)));
                    }
                    olen += len, optr += len;

                } else if ((sign = isinf (dval)) ) {
                    if (format == TAB_SQLITE || format == TAB_MYSQL) {
                        char *val = (sign ? "'Infinity'" : "'-Infinity'");
                        memcpy (optr, val, (len = strlen (val)));

                    } else if (format == TAB_POSTGRES) {
                        char *val = (sign ? "Infinity" : "-Infinity");
                        memcpy (optr, val, (len = strlen (val)));

                    } else {
                        sprintf (valbuf, "%.16lf", (double) dval);
                        memcpy (optr, valbuf, (len = strlen (valbuf)));
                    }
                    olen += len, optr += len;

                } else {
                    if (format == TAB_IPAC)
                        sprintf (valbuf, "%*f", col->dispwidth, (double) dval);
                    else
                        sprintf (valbuf, "%.16f", (double) dval);

                    memcpy (optr, valbuf, (len = strlen (valbuf)));
                    olen += len, optr += len;
                }
                dp += sz_double;
                if (col->repeat > 1 && j < col->ncols)
                    *optr++ = delimiter,  olen++;
            }
            if (col->repeat > 1 && i < col->nrows)
                *optr++ = delimiter,  olen++;
        }
    }

    return (dp);
}


/**
 *  DL_PRINTSERIAL -- Print the serial number column as integer values.
 */
static void
dl_printSerial (void)
{
    unsigned int len = 0, ival = serial_number++;
    unsigned int sz_val = htonl(sz_int);
    char  valbuf[SZ_VALBUF];

    if (mach_swap && do_binary)
        bswap4 ((unsigned char *)&ival, 1, (unsigned char *)&ival, 1, sz_int);

    if (do_binary) {
        memcpy (optr, &sz_val, sz_int);         	optr += sz_int;
        ival = htonl(ival);
        memcpy (optr, (char *)&ival, sz_int);   	optr += sz_int;
        olen += (2 * sz_int);

    } else {
        memset (valbuf, 0, SZ_VALBUF);
        //sprintf (valbuf, "%c%d", delimiter, ival);
        sprintf (valbuf, "%d", ival);
        memcpy (optr, valbuf, (len = strlen (valbuf)));
        olen += len;
        optr += len;
    }
}


/**
 *  DL_PRINTRANDOM -- Print the random number column as float values.
 */
static void
dl_printRandom (void)
{
    unsigned int len = 0, sz_val = htonl(sz_float);
    float rval = (((float)rand()/(float)(RAND_MAX)) * RANDOM_SCALE);
    char  valbuf[SZ_VALBUF];


    if (mach_swap && do_binary)
        bswap4 ((unsigned char *)&rval, 1, (unsigned char *)&rval, 1, sz_float);

    if (do_binary) {
        sz_val = htonl(sz_float);
        memcpy (optr, &sz_val, sz_int);           	optr += sz_int;
        memcpy (optr, (char *)&rval, sz_float);   	optr += sz_float;
        olen += (sz_int + sz_float);

    } else {
        memset (valbuf, 0, SZ_VALBUF);
        //sprintf (valbuf, "%c%f", delimiter, rval);
        sprintf (valbuf, "%f", rval);
        memcpy (optr, valbuf, (len = strlen (valbuf)));
        olen += len;
        optr += len;
    }
}


/**
 *  DL_PRINTVALUE -- Print a (integer) value to the output stream.
 */
static void
dl_printValue (int value)
{
    unsigned int len, ival = value;
    unsigned int sz_val = htonl(sz_int);
    char  valbuf[SZ_VALBUF];


    if (do_binary) {
        memcpy (optr, &sz_val, sz_int);         optr += sz_int;
        ival = htonl(ival);
        memcpy (optr, (char *)&ival, sz_int);   optr += sz_int;
        olen += (2 * sz_int);

    } else {
        memset (valbuf, 0, SZ_VALBUF);
        sprintf (valbuf, "%d", ival);
        memcpy (optr, valbuf, (len = strlen (valbuf)));
        olen += len;
        optr += len;
    }
}


/***********************************************************/
/****************** LOCAL UTILITY METHODS ******************/
/***********************************************************/


/**
 *  DL_MAKETABLENAME -- Name a table name from the input file name.
 */
static char *
dl_makeTableName (char *fname)
{
    char *ip, *tp, *np;

    ip = strdup (fname);                // copy the input name
    tp = strchr (ip, (int)'.');         // locate and kill the '.'
    *tp = '\0';

    for (np=ip; *np; np++) {
        if (*np == '-')
            *np = '_';
    }

    return (ip);                        // return the start of the filename
}


/**
 *  DL_ESCAPECSV -- Escape quotes for CSV printing.
 */
static void
dl_escapeCSV (char* in)
{
    //int   in_len = 0;
    char *ip = in, *op = esc_buf;

    memset (esc_buf, 0, SZ_ESCBUF);
    //if (in)
    //    in_len = strlen (in);

    *op++ = quote_char;
    for ( ; *ip; ip++) {
        *op++ = *ip;
        if (*ip == quote_char)
            *op++ = quote_char;
    }
    *op++ = quote_char;
}


/**
 *  DL_QUOTE -- Set quotes for CSV printing.
 */
static void
dl_quote (char* in)
{
    int    in_len = 0;
    char  *op = esc_buf;

    memset (esc_buf, 0, SZ_ESCBUF);
    in_len = (in ? strlen (in) : 0);

    *op++ = quote_char;
    if (in_len) {
        memcpy (op, in, in_len);
        op += in_len;
    }
    *op++ = quote_char;
}



/* BSWAP2 - Move bytes from array "a" to array "b", swapping successive
 * pairs of bytes.  The two arrays may be the same but may not be offset
 * and overlapping.
 */
static void
bswap2 (
  char    *a,         	                        // input array
  char    *b,         	                        // output array
  int     nbytes         	                // number of bytes to swap
)
{
        register char *ip=a, *op=b, *otop;
        register unsigned temp;

        /* Swap successive pairs of bytes.
         */
        for (otop = op + (nbytes & ~1);  op < otop;  ) {
            temp  = *ip++;
            *op++ = *ip++;
            *op++ = temp;
        }

        /* If there is an odd byte left, move it to the output array.
         */
        if (nbytes & 1)
            *op = *ip;
}


/* BSWAP4 - Move bytes from array "a" to array "b", swapping the four bytes
 * in each successive 4 byte group, i.e., 12345678 becomes 43218765.
 * The input and output arrays may be the same but may not partially overlap.
 */
static void
bswap4 (
  char	*a,			                // input array
  int	aoff,			                // first byte in input array
  char	*b,			                // output array
  int	boff,			                // first byte in output array
  int	nbytes			                // number of bytes to swap
)
{
	register char	*ip, *op, *tp;
	register int	n;
	static	char temp[4];

	tp = temp;
	ip = (char *)a + aoff - 1;
	op = (char *)b + boff - 1;

	/* Swap successive four byte groups.
	 */
	for (n = nbytes >> 2;  --n >= 0;  ) {
	    *tp++ = *ip++;
	    *tp++ = *ip++;
	    *tp++ = *ip++;
	    *tp++ = *ip++;
	    *op++ = *--tp;
	    *op++ = *--tp;
	    *op++ = *--tp;
	    *op++ = *--tp;
	}

	/* If there are any odd bytes left, move them to the output array.
	 * Do not bother to swap as it is unclear how to swap a partial
	 * group, and really incorrect if the data is not modulus 4.
	 */
	for (n = nbytes & 03;  --n >= 0;  )
	    *op++ = *ip++;
}


/* BSWAP8 - Move bytes from array "a" to array "b", swapping the eight bytes
 * in each successive 8 byte group, i.e., 12345678 becomes 87654321.
 * The input and output arrays may be the same but may not partially overlap.
 */
static void
bswap8 (
  char  *a,			                // input array
  int	aoff,			                // first byte in input array
  char  *b,			                // output array
  int	boff,			                // first byte in output array
  int	nbytes		                        // number of bytes to swap
)
{
	register char	*ip, *op, *tp;
	register int	n;
	static	char temp[8];

	tp = temp;
	ip = (char *)a + aoff - 1;
	op = (char *)b + boff - 1;

	/* Swap successive eight byte groups.
	 */
	for (n = nbytes >> 3;  --n >= 0;  ) {
	    *tp++ = *ip++;
	    *tp++ = *ip++;
	    *tp++ = *ip++;
	    *tp++ = *ip++;
	    *tp++ = *ip++;
	    *tp++ = *ip++;
	    *tp++ = *ip++;
	    *tp++ = *ip++;
	    *op++ = *--tp;
	    *op++ = *--tp;
	    *op++ = *--tp;
	    *op++ = *--tp;
	    *op++ = *--tp;
	    *op++ = *--tp;
	    *op++ = *--tp;
	    *op++ = *--tp;
	}

	/* If there are any odd bytes left, move them to the output array.
	 * Do not bother to swap as it is unclear how to swap a partial
	 * group, and really incorrect if the data is not modulus 8.
	 */
	for (n = nbytes & 03;  --n >= 0;  )
	    *op++ = *ip++;
}


/* IS_SWAPPED -- See if this is a byte-swapped (relative to Sun) machine.
 */
static int
is_swapped ()
{
        union {
            char ch[4];
            int  i;
        } u;

        u.i = 1;
        return ((int) u.ch[0]);
}


/*  SSTRIP -- Strip leading and trailing spaces in a string.
 */
static char *
sstrip (char *s)
{
    char *ip = s;

    if (!s || !*s)
        return s;

    /* Remove trailing spaces.  */
    for (ip=(s + strlen(s) - 1); *ip == ' ' && ip > s; ip--) *ip = '\0';

    /* Remove leading spaces.   */
    for (ip=s; *ip && *ip == ' '; ip++) ;

    return (ip);
}




/**
 * DL_ERROR - Process an error and exit the program.
 */
static void
dl_error (int exit_code, char *error_message, char *tag)
{
    if (tag != NULL && strlen (tag) > 0)
	fprintf (stderr, "ERROR %s: %s (%s)\n", prog_name, error_message, tag);
    else
	fprintf (stderr, "ERROR %s: %s\n", prog_name, error_message);
    fflush (stdout);
    //exit (exit_code);
}


/**
 *  DL_ISFITS -- Test a file to see if it is a simple FITS file.
 */
static int 
dl_isFITS (char *fits_name)
{
    register FILE *fp;
    int value = 0;
    char keyw[8], fname[SZ_FNAME], *ip, val;


    /*  Remove any filename modifiers from the input specification.
     */
    memset (fname, 0, SZ_FNAME);
    strcpy (fname, fits_name);
    if ((ip = strchr (fname, (int)'[')) )
        *ip = '\0';

    /*  Check for a SIMPLE keyword to identify a FITS file.
     */
    if ((fp = fopen (fname, "r"))) {
        fscanf (fp, "%6s = %c", keyw, &val);
        if (strcmp ("SIMPLE", keyw) == 0 && val == 'T')
            value = 1;
        fclose (fp);
    }
    return value;
}


/**
 *  DL_ISGZIP -- Test a file to see if it is GZip compressed.
 */
static int 
dl_isGZip (char *fname)
{
    int   fp, value = 0;
    unsigned short sval = 0;

    if ((fp = open (fname, O_RDONLY))) {
        read (fp, &sval, 2);
        if (sval == 35615)              // ushort value of "\037\213"
            value = 1;
        close (fp);
    }
    return value;
}


/**
 *  DL_ATOI -- System atoi() with lexical argument checking.
 */
static int
dl_atoi (char *val)
{
    char *ip;

    for (ip = val; *ip; ip++) {
        if (isalpha ((int) *ip)) {
            fprintf (stderr, "Warning: value '%s' is not an integer\n", val);
            break;
        }
    }
    return (atoi (val));
}


/**
 *  Task Parameter Utiltities
 */
#define	MAXARGS		256000
#define	SZ_ARG		256

#define PARAM_DBG (getenv("PARAM_DBG")!=NULL||access("/tmp/PARAM_DBG",F_OK)==0)

    
static  int last_good_index = 0;

/**
 *  DL_PARAMINIT -- Initialize the task parameter vector.
 *
 *  @brief      Initialize the task parameter vector.
 *  @fn         char **dl_paramInit (int argc, char *argv[])
 *
 *  @param  argc        argument count
 *  @param  argv        argument vector
 *  @returns            modified argument vector
 */
static char **
dl_paramInit (int argc, char *argv[], char *opts, struct option long_opts[])
{
    static  char *pargv[MAXARGS], arg[SZ_ARG];
    int  i, j, k, len = 0;

    memset (&pargv[0], 0, MAXARGS);
    last_good_index = 0;

    for (i=0; i < argc; i++) {
	/*  Make a local copy of the arg so we can modify it without side
 	 *  effects.
	 */
	memset (arg, 0, SZ_ARG);
	strcpy (arg, argv[i]);
	len = strlen (arg);

	if (arg[0] != '-') {
	    pargv[i] = calloc (1, strlen (arg) + 6);
	    if (arg[0] == '"' || arg[0] == '\'') {
	        strcpy (pargv[i], arg);
	    } else if (strchr (argv[i], (int) '='))
	        sprintf (pargv[i], "--%s", arg);
	    else if (argv[i][len-1] == '+') {
		arg[len-1] = '\0';
	        sprintf (pargv[i], "--%s=1", arg);
	    } else if (argv[i][len-1] == '-') {
		arg[len-1] = '\0';
	        sprintf (pargv[i], "--%s=0", arg);
	    } else
	        sprintf (pargv[i], "%s", arg);

	} else {
	    if (arg[0] == '-' && arg[1] != '-') {
		if (arg[2] == '=') {
		    /*  Argument is of the form '-f=bar' instead of the form
		     *  "--foo=bar" or "--foo bar" or "foo=bar".  We need to 
		     *  rewrite it to be acceptable to getopt_long().
		     */
		    char new[SZ_ARG];

		    memset (new, 0, SZ_ARG);
		    for (j=0; (char *)long_opts[j].name; j++) {
			if ((int) long_opts[j].val == (int) arg[1]) {
			    sprintf (new, "--%s=%s",long_opts[j].name, &arg[3]);
			    memset (arg, 0, SZ_ARG);
			    strcpy (arg, new);
			    len = strlen (arg);
			}
		    }

	        } else if (arg[2] != '=' && strchr (arg, (int)'=')) {
		    fprintf (stderr, "Illegal flag '%s', skipping.\n", arg);
		    continue;

	        } else {
		    /*  Check for a flag of the form "-all", which should
		     *  really be the long-form of "--all".  Rewrite the
		     *  pargv value so the flag isn't interpreted incorrectly
		     *  as "-a", "-l", "-l".
		     */
		    int found = 0;
		    for (k=0; (char *)long_opts[k].name; k++) {
			if (strcmp (long_opts[k].name, &arg[1]) == 0) {
	    		    pargv[i] = calloc (1, strlen (arg) + 6);
	        	    sprintf (pargv[i], "--%s", &arg[1]);
			    found = 1;
			    break;
			}
		    }
		    if (found) continue;
	        }
	    }

	    pargv[i] = calloc (1, strlen (arg) + 1);
	    sprintf (pargv[i], "%s", arg);
	}
    }

    if (PARAM_DBG) {
	for (i=0; i < argc; i++) 
	    fprintf (stderr, "pargv[%d] = '%s'\n", i, pargv[i]);
    }

    return (pargv);
}


/**
 *  DL_PARAMNEXT -- Get the next parameter value.
 *
 *  @brief      Get the next parameter value.
 *  @fn         int dl_paramNext (char *opts, struct option long_opts[],
 *			int argc, char *argv[], char *optval, int *posindex)
 *
 *  @param  opts        option string
 *  @param  long_opts   long options struct
 *  @param  argc        argument count
 *  @param  argv        argument vector
 *  @param  optval      optional parameter argument
 *  @param  posindex    positional parameter index (0-based)
 *  @returns            nothing
 */
static int
dl_paramNext (char *opts, struct option long_opts[], int argc, char *argv[],
		char *optval, int *posindex)
{
    int  ch = 0, index = 0;
    static  int pos = 0, apos = 0;


    apos++;
    memset (optval, 0, SZ_FNAME);
#ifdef USE_GETOPT_LONG
    ch = getopt_long (argc, argv, opts, long_opts, &index);
#else
    ch = getopt_long_only (argc, argv, opts, long_opts, &index);
#endif

    if (ch == 63) {
        fprintf (stderr, "Error: Invalid argument = '%s'\n", argv[apos]);
        fflush (stderr);
	return (PARG_ERR);
    }

    if (ch >= 0) {
        if (ch > 0 && optarg) {
	    if ((strchr (optarg, (int)'=') != 0) && (optarg[0] != '-') && 
	        (argv[apos][0] == '-' && argv[apos][1] != '-') ) {
		    fprintf (stderr, 
			"Error: invalid argument = '%s' in dl_paramNext()\n",
		        argv[apos]);
                    fflush (stderr);
		    return (PARG_ERR);
	    } else {
		if (optarg[0] == '-') {
		    // optind--;
		    memset (optval, 0, SZ_FNAME);
		} else
	            strcpy (optval, optarg);
	    }

	} else if (ch == 0) {
	    *posindex = index;
	    if (optarg)
	        strcpy (optval, optarg);
        } else if (ch == 63) {
	    optind = last_good_index;
            fprintf (stderr, "Error: Invalid ch = '%s' in dl_paramNext()\n",
	         argv[apos]);
            fflush (stderr);
	    return (-1);
	}

    } else {
	if (argv[optind+pos]) {
	    strcpy (optval, argv[optind+pos]);
	    *posindex = pos++;
	    return (-pos);
	} else
	    return (0);
    }
    last_good_index = optind;

    if (PARAM_DBG) {
	fprintf (stderr, 
	    "paramNext: ch=%d (%c) optval='%s' optarg='%s' index=%d\n",
      	    ch, ch, optval, optarg, index);
    }

    return (ch);
}


/**
 *  DL_PARAMFREE -- Free the allocated parameter vector.
 *
 *  @brief      Free the allocated parameter vector.
 *  @fn         void dl_paramFree (int argc, char *argv[])
 *
 *  @param  argc        argument count
 *  @param  argv        argument vector
 *  @returns            nothing
 */
static void 
dl_paramFree (int argc, char *argv[])
{
    register int i;

    for (i=0; i < argc; i++) {
	if (argv[i] && argv[i][0])
	    free ((void *)argv[i]);
    }
}


/**
 *  DL_FEXTN -- Return the filename extension appropriate for the format.
 */
static char *
dl_fextn (void)
{
    switch (format) {
    case TAB_DELIMITED:
        switch (delimiter) {
        case ' ':       return "asv";
        case '|':       return "bsv";
        case ',':       return "csv";
        case '\t':      return "tsv";
        }
        break;
    case TAB_IPAC:
            return "ipac";
    case TAB_POSTGRES:
    case TAB_MYSQL:
    case TAB_SQLITE:
            return "sql";
    }

    return ("fmt");
}



/**
 *  USAGE -- Print task help summary.
 */
static void
Usage (void)
{
    fprintf (stderr, "\n  Usage:\n\t"
"\n"
"      fits2db [<opts>] [ <input> ... ]\n"
"\n"
"  where <opts> include:\n"
"\n"
"      -h,--help                this message\n"
"      -d,--debug               set debug flag\n"
"      -v,--verbose             set verbose output flag\n"
"      -n,--noop                set no-op flag\n"
"\n"
"                                   INPUT PROCESSING OPTIONS\n"
"      -b,--bundle=<N>          bundle <N> files at a time\n"
"      -c,--chunk=<N>           process <N> rows at a time\n"
"      -e,--extnum=<N>          process table in FITS extension number <N>\n"
"      -E,--extname=<name>      process table in FITS extension name <name>\n"
"      -i,--input=<file>        set input filename\n"
"      -o,--output=<file>       set output filename\n"
"      -r,--rowrange=<range>    convert rows within given <range>\n"
"      -s,--select=<expr>       select rows based on <expr>\n"
"\n"
"                                   PROCESSING OPTIONS\n"
"      -C,--concat              concatenate all input files to output\n"
"      -H,--noheader            suppress CSV column header\n"
"      -N,--nostrip             don't strip strings of whitespace\n"
"      -Q,--noquote             don't quote strings in text formats\n"
"      -S,--singlequote         use single quotes for strings\n"
"      -X,--explode             explode array cols to separate columns\n"
"\n"
"                                   FORMAT OPTIONS\n"
"      --asv                    output an ascii-separated value table\n"
"      --bsv                    output a bar-separated value table\n"
"      --csv                    output a comma-separated value table\n"
"      --tsv                    output a tab-separated value table\n"
"      --ipac                   output an IPAC formatted table\n"
"\n"
"                                   SQL OPTIONS\n"
"      -B,--binary              output binary SQL\n"
"      -O,--oids                create table with OIDs (Postgres only)\n"
"      -t,--table=<name>        create table named <name>\n"
"      -Z,--noload              don't create table load commands\n"
"\n"
"      --sql=<db>               output SQL correct for <db> type\n"
"      --drop                   drop existing DB table before conversion\n"
"      --dbname=<name>          create DB of the given name\n"
"      --create                 create DB table from input table structure\n"
"      --truncate               truncate DB table before loading\n"
"      --pkey=<colname>         create a serial primary key column <colname>\n"
"\n"
"\n"
"  Examples:\n"
"\n"
"    1)  Load all FITS tables in directory to a new Postgres database table\n"
"        named 'mytab' in binary mode, expanding arrays to new columns:\n"
"\n"
"          %% fits2db --sql=postgres --create -B -C -X -t mytab *.fits | psql\n"
"\n"
"        Load all FITS tables to a new MySQL database named 'mydb':\n"
"\n"
"          %% fits2db --sql=mysql --create --drop --dbname=mydb \\\n"
"                       -C -X -t mytab *.fits | mysql\n"
"\n"
"        Load all FITS tables to a new SQLite database file named 'mydb.db':\n"
"\n"
"          %% fits2db --sql=sqlite --create -X -C \\\n"
"                       -t mytab *.fits | sqlite3 mydb.db\n"
"\n"
"        Note that in each of these examples the appropriate third-party\n"
"        database client is used to actually process the data.  This allows\n"
"        for a parallelization of processing the input files as well as\n"
"        providing full access to the database client options.\n"
"\n"
"    2)  Replace the contents of the database table 'mytab' with the contents\n"
"        of the named FITS files:\n"
"\n"
"          %% fits2db --sql=postgres --truncate -t mytab new.fits | psql\n"
"            or\n"
"          %% fits2db --sql=postgres --drop --create -t mytab new.fits | psql\n"
"\n"
"    3)  Convert all FITS tabes to ascii SQL files using the file root name:\n"
"\n"
"          %% fits2db --sql=mysql --create *.fits           # for MySQL\n"
"          %% fits2db --sql=sqlite --create *.fits          # for SQLite\n"
"          %% fits2db --sql=postgres --create *.fits        # for PostgresQL\n"
"\n"
"    4)  Convert FITS bintable to CSV on the standard output:\n"
"\n"
"          %% fits2db --csv test.fits\n"
"\n"
"        Suppress the CSV column header:\n"
"\n"
"          %% fits2db --csv --noheader test.fits\n"
"\n"
"        Use single quotes on strings and don't strip leading/trailing\n"
"        spacces, creating an output file named 'test.csv':\n"
"\n"
"          %% fits2db --csv --singlequote --nostrip -o test.csv test.fits\n"
"\n"
"    5)  Create a database table based on the structure of the FITS bintable\n"
"        but don't actually load the data:\n"
"\n"
"          %% fits2db --sql=postgres --create --noload -t mytab test.fits\n"
"\n"
"    6)  Create a database table of only the r-band values in the table:\n"
"\n"
"          %% fits2db --sql=postgres --select=\'FILTER==\"r\"\' test.fits\n"
"\n"
"        Note in this case the selection expression must be quoted.\n"
"\n"
"  Additionally, filename modifiers may be added in order to select the\n"
"  specific file extension or filter the table for specific rows or columns.\n"
"  Examples of this type of filtering include:\n"
"\n"
"    fits2db tab.fits[sci]                  - list the 'sci' extension\n"
"    fits2db tab.fits[1][#row < 101]        - list first 100 rows of extn 1\n"
"    fits2db tab.fits[col X;Y]              - list X and Y cols only\n"
"    fits2db tab.fits[col -PI,-ETA]         - list all but the PI and ETA cols\n"
"    fits2db tab.fits[col -PI][#row < 101]  - combined case\n"
"\n"
"  For details on table row and column filtering, see the CFITSIO docs.\n"
"\n\n"
    );
}
