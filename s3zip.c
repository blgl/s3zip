/*
 * High-level operation description:
 *
 * 1. Open a single connection using an in-memory main database.
 *
 * 2. Attach each input database in read-only mode.
 *
 * 3. Do a BEGIN IMMEDIATE to acquire locks on all the inputs
 *    as close together in time as possible.  This improves
 *    the chance of getting a consistent multi-database backup.
 *
 * 4. Compress each input database to the output Zip archive,
 *    using the "sqlite_dbpage" virtual table to get pages
 *    from the database or WAL files as appropriate.
 *
 * 5. ROLLBACK the transaction and close the database connection.
 *
 * 6. Write the Zip central directory and finalise the archive.
 */

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <sys/stat.h>

#include <sqlite3.h>
#include <zlib.h>

/*
 * Zip file construction kit, part 1:
 * Unsigned little-endian integers and how to store them.
 */

typedef struct ule16 {
    uint8_t a,b;
} ule16;

typedef struct ule32 {
    uint8_t c,d,e,f;
} ule32;

typedef struct ule64 {
    uint8_t g,h,i,j,k,l,m,n;
} ule64;

#define STORE16(u,v) \
    ((u).a=(uint8_t)(v),       (u).b=(uint8_t)((v)>>8))

#define STORE32(u,v) \
    ((u).c=(uint8_t)(v),       (u).d=(uint8_t)((v)>>8), \
     (u).e=(uint8_t)((v)>>16), (u).f=(uint8_t)((v)>>24))

#define STORE64(u,v) \
    ((u).g=(uint8_t)(v),       (u).h=(uint8_t)((v)>>8), \
     (u).i=(uint8_t)((v)>>16), (u).j=(uint8_t)((v)>>24), \
     (u).k=(uint8_t)((v)>>32), (u).l=(uint8_t)((v)>>40), \
     (u).m=(uint8_t)((v)>>48), (u).n=(uint8_t)((v)>>56))

/*
 * Zip file construction kit, part 2:
 * On-disk data structures.
 */

typedef struct local_entry {
    ule32 sig;
    ule16 needed_version;
    ule16 flags;
    ule16 compression;
    ule16 mod_time;
    ule16 mod_date;
    ule32 crc;
    ule32 compressed_size;
    ule32 size;
    ule16 path_len;
    ule16 extra_len;
} local_entry;

/*
 * A Zip64 local extension always contains both sizes and nothing more.
 */

typedef struct local_zip64 {
    ule16 ext_id;
    ule16 ext_size;
    ule64 size;
    ule64 compressed_size;
} local_zip64;

typedef struct central_entry {
    ule32 sig;
    ule16 creator_version;
    ule16 needed_version;
    ule16 flags;
    ule16 compression;
    ule16 mod_time;
    ule16 mod_date;
    ule32 crc;
    ule32 compressed_size;
    ule32 size;
    ule16 path_len;
    ule16 extra_len;
    ule16 comment_len;
    ule16 first_diskno;
    ule16 internal_attribs;
    ule32 external_attribs;
    ule32 local_offset;
} central_entry;

/*
 * A Zip64 central extension contains up to three 64-bit integers
 * (size, compressed size, local offset),
 * but no disk number since we only write single-part archives.
 */

typedef struct central_zip64 {
    ule16 ext_id;
    ule16 ext_size;
    ule64 data[3];
} central_zip64;

typedef struct eocd64 {
    ule32 sig;
    ule64 size;
    ule16 creator_version;
    ule16 needed_version;
    ule32 this_diskno;
    ule32 cd_diskno;
    ule64 this_entry_cnt;
    ule64 total_entry_cnt;
    ule64 cd_size;
    ule64 cd_offset;
} eocd64;

typedef struct eocd64_locator {
    ule32 sig;
    ule32 eocd_diskno;
    ule64 eocd_offset;
    ule32 disk_cnt;
} eocd64_locator;

typedef struct eocd {
    ule32 sig;
    ule16 this_diskno;
    ule16 cd_diskno;
    ule16 this_entry_cnt;
    ule16 total_entry_cnt;
    ule32 cd_size;
    ule32 cd_offset;
    ule16 comment_len;
} eocd;

static ule32 const local_entry_sig =    { 'P', 'K', 3, 4 };
static ule32 const central_entry_sig =  { 'P', 'K', 1, 2 };
static ule32 const eocd64_sig =         { 'P', 'K', 6, 6 };
static ule32 const eocd64_locator_sig = { 'P', 'K', 6, 7 };
static ule32 const eocd_sig =           { 'P', 'K', 5, 6 };

enum {
    version_classic     = 20,   /* deflate compression needs 2.0 */
    version_zip64       = 45,   /* Zip64 needs 4.5 */

    creator_unix        = 3<<8
};

/*
 * End of Zip stuff.
 */

/*
 * This program's specific data structures.
 */

typedef struct input_info {
    char name[8];
    char const *path;
    size_t path_len;
    dev_t dev;
    ino_t ino;
    off_t local_offset;
    off_t size;
    off_t page_count;
    int page_size;
    uint16_t mode;
    uint16_t dos_mdate;
    uint16_t dos_mtime;
    uint16_t ext_len;
    central_entry entry;
    central_zip64 ext;
} input_info;

typedef struct global_info {
    char const *zip_path;
    FILE *zip;
    off_t cd_offset;
    off_t cd_size;
    off_t total_size;
    sqlite3 *db;
    z_stream deflation;
    char have_output;
    char have_deflation;
    char have_transaction;
    uint8_t output_buf[0x1000B];
    int input_cnt;
    input_info inputs[1];
} global_info;

/*
 * SQL statements.
 *
 * Only the first one needs to be run through snprintf to get
 * the database name as an identifier; every other use is
 * through table-valued functions that take the database name
 * as a text value, letting us use bound parameters.
 *
 * Why the explicit main schema?  Consider what would happen
 * if one of the inputs contained a table named "pragma_page_size".
 */

static char const attach_fmt[] =
    "attach database ?1 as %s";

static char const begin_sql[] =
    "begin immediate";

static char const rollback_sql[] =
    "rollback";

static char const metainfo_sql[] =
    "select page_size, page_count, journal_mode\n"
    "    from main.pragma_page_size(?1),\n"
    "        main.pragma_page_count(?1),\n"
    "        main.pragma_journal_mode(?1)";

static char const pages_sql[] =
    "select data from main.sqlite_dbpage(?1)\n"
    "    order by pgno";

/*
 * Finally, some actual code.
 */

static global_info *make_global(
    int input_cnt)
{
    global_info *g=NULL;

    if (input_cnt>0x7FFFFFFF) {
        fputs("Definitely too many inputs\n",stderr);
        return NULL;
    }
    g=malloc(offsetof(global_info,inputs)+input_cnt*sizeof (input_info));
    if (!g) {
        perror("malloc");
        return NULL;
    }
    g->zip=NULL;
    g->db=NULL;
    g->input_cnt=input_cnt;
    g->have_output=0;
    g->have_deflation=0;
    g->have_transaction=0;
    return g;
}

static int open_db(
    global_info *g)
{
    int status;

    status=sqlite3_open_v2(
        "file:%3Amemory%3A",
        &g->db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
        NULL);
    if (status!=SQLITE_OK) {
        if (g->db) {
            fprintf(stderr,"sqlite3_open: %s\n",sqlite3_errmsg(g->db));
        } else {
            fprintf(stderr,"sqlite3_open: %s\n",sqlite3_errstr(status));
        }
        return -1;
    }
    status=sqlite3_busy_timeout(g->db,999999999);
    if (status!=SQLITE_OK)
        return -1;
    return 0;
}

static uint8_t nobuf[1];

static int init_compression(
    global_info *g)
{
    int status;

    g->deflation.next_in=nobuf;
    g->deflation.avail_in=0;
    g->deflation.next_out=nobuf;
    g->deflation.avail_out=0;
    g->deflation.zalloc=0;
    g->deflation.zfree=0;
    g->deflation.opaque=NULL;
    status=deflateInit2(
        &g->deflation,
        Z_BEST_COMPRESSION,
        Z_DEFLATED,
        -15,
        9,
        Z_DEFAULT_STRATEGY);
    if (status!=Z_OK) {
        fprintf(stderr,"deflateInit2: error %d\n",status);
        return -1;
    }
    g->have_deflation=1;
    return 0;
}

static int attach_inputs(
    global_info *g,
    char **paths)
{
    static char const base_36[36]="1qa2zws3xed4crf5vtg6byh7nuj8mik9ol0p";
    static char const base_16[16]="0123456789ABCDEF";
    int status;
    int ix;
    input_info *input,*inputs_end;
    size_t max_path_len;
    char *uri_buf=NULL;
    sqlite3_stmt *attach=NULL;

    inputs_end=g->inputs+g->input_cnt;
    max_path_len=0;
    for (input=g->inputs,ix=0; input<inputs_end; input++,ix++) {
        char *path;
        size_t path_len;
        struct stat stat_buf;
        int name_num,digit_ix;
        input_info *seen;

        path=paths[ix];
        if (path[0]=='/') {
            fprintf(stderr,"%s: No absolute paths allowed\n",path);
            goto cleanup;
        }
        path_len=strlen(path);
        if (!path_len) {
            fputs("No empty paths allowed\n",stderr);
            goto cleanup;
        }
        if (path_len>0xFFFF) {
            fprintf(stderr,"%s: Path too long\n",path);
            goto cleanup;
        }
        if (stat(path,&stat_buf)) {
            fprintf(stderr,"%s: %s\n",path,strerror(errno));
            goto cleanup;
        }
        if (!S_ISREG(stat_buf.st_mode)) {
            fprintf(stderr,"%s: Not a regular file\n",path);
            goto cleanup;
        }
        for (seen=g->inputs; seen<input; seen++) {
            if (seen->dev==stat_buf.st_dev && seen->ino==stat_buf.st_ino) {
                fprintf(stderr,"%s: Duplicate input\n",path);
                goto cleanup;
            }
        }
        input->path=path;
        input->path_len=path_len;
        input->dev=stat_buf.st_dev;
        input->ino=stat_buf.st_ino;
        input->mode=stat_buf.st_mode;
        if (path_len>max_path_len)
            max_path_len=path_len;
/*
 * Each input needs a unique internal name.  An underscore followed
 * by six base-36 digits makes an identifier that doesn't collide
 * with any keyword.
 */
        name_num=ix;
        input->name[7]=0;
        for (digit_ix=6; digit_ix>0; digit_ix--) {
            input->name[digit_ix]=base_36[name_num%36];
            name_num/=36;
        }
        input->name[0]='_';
    }
    uri_buf=malloc(3*max_path_len+sizeof "file://?mode=ro");
    if (!uri_buf) {
        perror("malloc");
        goto cleanup;
    }
    for (input=g->inputs; input<inputs_end; input++) {
        char attach_sql[sizeof attach_fmt+7];
        int sql_len;
        char const *src;
        char *dst;

        sql_len=snprintf(attach_sql,sizeof attach_sql,attach_fmt,input->name);
        status=sqlite3_prepare_v2(g->db,attach_sql,sql_len+1,&attach,NULL);
        if (status!=SQLITE_OK) {
            fprintf(stderr,"sqlite3_prepare(attach): %s\n",
                    sqlite3_errmsg(g->db));
            goto cleanup;
        }
        src=input->path;
        dst=uri_buf;
        memcpy(dst,"file:",5);
        dst+=5;
        if (src[0]=='/') {
            dst[0]='/';
            dst[1]='/';
            dst+=2;
        }
        for (;;) {
            int c;

            c=src[0];
            src++;
            if (!c)
                break;
            if (c=='%' || c=='#' || c=='?' || c<=' ' || c>=0x7F) {
                dst[0]='%';
                dst[1]=base_16[c>>4 & 0xF];
                dst[2]=base_16[c    & 0xF];
                dst+=3;
            } else {
                dst[0]=c;
                dst++;
            }
        }
        memcpy(dst,"?mode=ro",8);
        dst+=8;
        status=sqlite3_bind_text(attach,1,uri_buf,dst-uri_buf,SQLITE_STATIC);
        if (status!=SQLITE_OK) {
            fprintf(stderr,"sqlite3_bind_text(attach): %s\n",
                    sqlite3_errmsg(g->db));
            goto cleanup;
        }
        status=sqlite3_step(attach);
        if (status!=SQLITE_DONE) {
            fprintf(stderr,"sqlite3_step(attach): %s\n",sqlite3_errmsg(g->db));
            goto cleanup;
        }
        sqlite3_finalize(attach);
        attach=NULL;
    }
    free(uri_buf);
    uri_buf=NULL;
    return 0;

cleanup:
    if (uri_buf)
        free(uri_buf);
    if (attach)
        sqlite3_finalize(attach);
    return -1;
}

static int open_archive(
    global_info *g,
    char const *path)
{
    struct stat stat_buf;

    if (stat(path,&stat_buf)==0) {
        input_info *input,*inputs_end;

        inputs_end=g->inputs+g->input_cnt;
        for (input=g->inputs; input<inputs_end; input++) {
            if (input->dev=stat_buf.st_dev && input->ino==stat_buf.st_ino) {
                fprintf(stderr,"%s: Conflicts with an input file\n", path);
                return -1;
            }
        }
    }
    g->zip_path=path;
    g->zip=fopen(path,"w");
    if (!g->zip) {
        fprintf(stderr,"%s: fopen: %s\n",path,strerror(errno));
        return -1;
    }
    g->have_output=1;
    return 0;
}

static int begin_transaction(
    global_info *g)
{
    int status;
    sqlite3_stmt *begin=NULL;

    status=sqlite3_prepare_v2(g->db,begin_sql,sizeof begin_sql,&begin,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(begin): %s\n",sqlite3_errmsg(g->db));
        goto cleanup;
    }
    status=sqlite3_step(begin);
    if (status!=SQLITE_DONE) {
        fprintf(stderr,"sqlite3_step(begin): %s\n",sqlite3_errmsg(g->db));
        goto cleanup;
    }
    sqlite3_finalize(begin);
    begin=NULL;
    g->have_transaction=1;
    return 0;

cleanup:
    if (begin)
        sqlite3_finalize(begin);
    return -1;
}

static int get_metainfo(
    global_info *g)
{
    int status;
    input_info *input,*inputs_end;
    sqlite3_stmt *metainfo=NULL;

    inputs_end=g->inputs+g->input_cnt;
    status=sqlite3_prepare_v2(
        g->db,metainfo_sql,sizeof metainfo_sql,&metainfo,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(metainfo): %s\n",
                sqlite3_errmsg(g->db));
        goto cleanup;
    }
    for (input=g->inputs; input<inputs_end; input++) {
        struct stat stat_buf;
        struct tm *pieces;
        time_t mtime;
        unsigned char const *journal_mode;
        char const *filename;

        status=sqlite3_bind_text(metainfo,1,input->name,-1,SQLITE_STATIC);
        if (status!=SQLITE_OK) {
            fprintf(stderr,"sqlite3_bind_text(metainfo): %s\n",
                    sqlite3_errmsg(g->db));
            goto cleanup;
        }
        status=sqlite3_step(metainfo);
        if (status!=SQLITE_ROW) {
            fprintf(stderr,"sqlite3_step(metainfo): %s\n",
                    sqlite3_errmsg(g->db));
            goto cleanup;
        }
        input->page_size=sqlite3_column_int(metainfo,0);
        input->page_count=sqlite3_column_int64(metainfo,1);
        journal_mode=sqlite3_column_text(metainfo,2);
        if (!journal_mode) {
            fputs("Out of memory or something\n",stderr);
            goto cleanup;
        }
        if (input->page_size>0x10000) {
            fprintf(stderr,"%s: Unsupported page size %d\n",
                    input->path,input->page_size);
            goto cleanup;
        }
        filename=sqlite3_db_filename(g->db,input->name);
/*
 * stat-ing again because the first time was before we had a lock.
 */
        if (stat(sqlite3_filename_database(filename),&stat_buf)) {
            perror(input->path);
            goto cleanup;
        }
        mtime=stat_buf.st_mtime;
/*
 * If in WAL mode, and the WAL file exists and is newer than the main file,
 * use that mtime instead.
 */
        if (!strcmp((char const *)journal_mode,"wal")
                && stat(sqlite3_filename_wal(filename),&stat_buf)==0
                && stat_buf.st_mtime>mtime) {
            mtime=stat_buf.st_mtime;
        }
        sqlite3_reset(metainfo);
        pieces=localtime(&mtime);
        input->dos_mdate=pieces->tm_year-80<<9
            | pieces->tm_mon+1<<5 & 0x1E0
            | pieces->tm_mday & 0x1F;
        input->dos_mtime=pieces->tm_hour<<11
            | pieces->tm_min<<5 & 0x7E0
            | pieces->tm_sec>>1 & 0x1F;
    }
    sqlite3_finalize(metainfo);
    metainfo=NULL;
    return 0;

cleanup:
    if (metainfo)
        sqlite3_finalize(metainfo);
    return -1;
}

static int compress_inputs(
    global_info *g)
{
    int status;
    sqlite3_stmt *pages=NULL;
    input_info *input,*inputs_end;
    off_t offset;

    inputs_end=g->inputs+g->input_cnt;
    status=sqlite3_prepare_v2(g->db,pages_sql,sizeof pages_sql,&pages,NULL);
    if (status!=SQLITE_OK) {
        fprintf(stderr,"sqlite3_prepare(pages): %s\n",sqlite3_errmsg(g->db));
        goto cleanup;
    }
    offset=0;
    for (input=g->inputs; input<inputs_end; input++) {
        local_entry entry;
        int l64,c64;
        local_zip64 ext;
        off_t size,compressed_size,archived_size;
        off_t page_count;
        uint32_t crc;
        unsigned int version;

/*
 * Compute the worst-case compressed size to see if it fits in 32 bits.
 * If it doesn't, we need to know that in advance.
 */
        size=input->page_count*input->page_size;
        input->size=size;
        compressed_size=input->page_count*
            (input->page_size+(input->page_size+0xFFFE)/0xFFFF*5);
        l64=(size>0xFFFFFFFF || compressed_size>0xFFFFFFFF);
        c64=(l64 || offset>0xFFFFFFFF);
        if (c64) {
            version=version_zip64;
        } else {
            version=version_classic;
        }
/*
 * Writing a preliminary local header followed by the compressed data
 * and then returning to fill in only the CRC and the compressed size
 * is too fiddly.  Instead, leave space for the local header and return
 * to write all of it once everything is known.
 */
        input->local_offset=offset;
        offset+=sizeof (local_entry)+input->path_len;
        if (l64)
            offset+=sizeof (local_zip64);
        if (fseeko(g->zip,offset,SEEK_SET)) {
            fprintf(stderr,"%s: fseeko: %s\n",g->zip_path,strerror(errno));
            goto cleanup;
        }

/*
 * Get, compress, and write pages.
 */
        status=sqlite3_bind_text(pages,1,input->name,-1,SQLITE_STATIC);
        if (status!=SQLITE_OK) {
            fprintf(stderr,"sqlite3_bind_text(pages): %s\n",
                    sqlite3_errmsg(g->db));
            goto cleanup;
        }
        page_count=0;
        compressed_size=0;
        crc=0;
        for (;;) {
            void const *page_data;
            int page_size;
            int flush;
            int got;

            status=sqlite3_step(pages);
            if (status!=SQLITE_ROW)
                break;
            page_data=sqlite3_column_blob(pages,0);
            if (!page_data) {
                fputs("Out of memory or something\n",stderr);
                goto cleanup;
            }
            page_size=sqlite3_column_bytes(pages,0);
            if (page_size!=input->page_size) {
                fprintf(stderr,"%s: Inconsistent page size\n",input->path);
                goto cleanup;
            }
            page_count++;
            if (page_count>input->page_count) {
                fprintf(stderr,"%s: Inconsistent page count\n",input->path);
                goto cleanup;
            }
            crc=crc32(crc,page_data,page_size);
/*
 * For compressible pages, Z_BLOCK consistently yields better compression
 * than Z_NO_FLUSH, even for freshly VACUUMed databases that ought to have
 * similar pages grouped together.  If you need an explanation for this,
 * hire a zlib expert to analyse the problem.
 *
 * On the other hand, a run of incompressible pages (from e.g. a large
 * random blob) should be flushed as seldom as possible in order
 * to minimise overhead.
 *
 * So how do you know in advance whether or not a given page is
 * compressible?  Unfortunately, there's no way that doesn't double
 * the computation cost.
 */
            if (page_count==input->page_count) {
                flush=Z_FINISH;
            } else {
                flush=Z_BLOCK;
            }
            g->deflation.next_in=(uint8_t *)page_data;
            g->deflation.avail_in=page_size;
            g->deflation.next_out=g->output_buf;
            g->deflation.avail_out=sizeof g->output_buf;
            status=deflate(&g->deflation,flush);
            if (status!=Z_OK && status!=Z_STREAM_END) {
                fprintf(stderr,"deflate: error %d\n",status);
                goto cleanup;
            }
            got=g->deflation.next_out-g->output_buf;
            if (got>0) {
                compressed_size+=got;
                if (!fwrite(g->output_buf,got,1,g->zip)) {
                    fprintf(stderr,"%s: fwrite: %s\n",
                            g->zip_path,strerror(errno));
                    goto cleanup;
                }
            }
        }
        if (status!=SQLITE_DONE) {
            fprintf(stderr,"sqlite3_step(pages): %s\n",sqlite3_errmsg(g->db));
            goto cleanup;
        }
        sqlite3_reset(pages);
        if (page_count<input->page_count) {
            fprintf(stderr,"%s: Inconsistent page count\n",input->path);
            goto cleanup;
        }
        g->deflation.next_in=nobuf;
        g->deflation.avail_in=0;
        g->deflation.next_out=nobuf;
        g->deflation.avail_out=0;
        status=deflateReset(&g->deflation);
        if (status!=Z_OK) {
            fprintf(stderr,"deflateReset: error %d\n",status);
            goto cleanup;
        }
        offset+=compressed_size;

/*
 * Prepare and write the local header.
 */
        if (l64) {
            STORE16(entry.needed_version,version);
            STORE32(entry.compressed_size,0xFFFFFFFF);
            STORE32(entry.size,0xFFFFFFFF);
            STORE16(entry.extra_len,sizeof (local_zip64));

            STORE16(ext.ext_id,0x0001);
            STORE16(ext.ext_size,16);
            STORE64(ext.size,size);
            STORE64(ext.compressed_size,compressed_size);
        } else {
            STORE16(entry.needed_version,version);
            STORE32(entry.compressed_size,compressed_size);
            STORE32(entry.size,size);
            STORE16(entry.extra_len,0);
        }
        entry.sig=local_entry_sig;
        STORE16(entry.flags,0x0002);
        STORE16(entry.compression,8);
        STORE16(entry.mod_time,input->dos_mtime);
        STORE16(entry.mod_date,input->dos_mdate);
        STORE32(entry.crc,crc);
        STORE16(entry.path_len,input->path_len);

        if (fseeko(g->zip,input->local_offset,SEEK_SET)) {
            fprintf(stderr,"%s: fseeko: %s\n",g->zip_path,strerror(errno));
            goto cleanup;
        }
        if (!fwrite(&entry,sizeof entry,1,g->zip)) {
            fprintf(stderr,"%s: fwrite: %s\n",g->zip_path,strerror(errno));
            goto cleanup;
        }
        if (!fwrite(input->path,input->path_len,1,g->zip)) {
            fprintf(stderr,"%s: fwrite: %s\n",g->zip_path,strerror(errno));
            goto cleanup;
        }
        if (l64) {
            if (!fwrite(&ext,sizeof ext,1,g->zip)) {
                fprintf(stderr,"%s: fwrite: %s\n",
                        g->zip_path,strerror(errno));
                goto cleanup;
            }
        }

/*
 * Prepare the central directory entry and save it for later.
 *
 * Yes, greater-or-equal comparisons.  Not a bug.
 */
        if (c64) {
            ule64 *ext_data;
            unsigned int ext_size;

            ext_data=input->ext.data;
            if (size>=0xFFFFFFFF) {
                STORE32(input->entry.size,0xFFFFFFFF);
                STORE64(*ext_data,size);
                ext_data++;
            } else {
                STORE32(input->entry.size,size);
            }
            if (compressed_size>=0xFFFFFFFF) {
                STORE32(input->entry.compressed_size,0xFFFFFFFF);
                STORE64(*ext_data,compressed_size);
                ext_data++;
            } else {
                STORE32(input->entry.compressed_size,compressed_size);
            }
            if (input->local_offset>=0xFFFFFFFF) {
                STORE32(input->entry.local_offset,0xFFFFFFFF);
                STORE64(*ext_data,input->local_offset);
                ext_data++;
            } else {
                STORE32(input->entry.local_offset,input->local_offset);
            }
            ext_size=(ext_data-input->ext.data)*8;
            STORE16(input->ext.ext_id,1);
            STORE16(input->ext.ext_size,ext_size);
            input->ext_len=offsetof(central_zip64,data)+ext_size;
        } else {
            STORE32(input->entry.size,size);
            STORE32(input->entry.compressed_size,compressed_size);
            STORE32(input->entry.local_offset,input->local_offset);
            input->ext_len=0;
        }
        input->entry.sig=central_entry_sig;
        STORE16(input->entry.creator_version,version | creator_unix);
        STORE16(input->entry.needed_version,version);
        STORE16(input->entry.flags,0x0002);
        STORE16(input->entry.compression,8);
        STORE16(input->entry.mod_time,input->dos_mtime);
        STORE16(input->entry.mod_date,input->dos_mdate);
        STORE32(input->entry.crc,crc);
        STORE16(input->entry.path_len,input->path_len);
        STORE16(input->entry.extra_len,input->ext_len);
        STORE16(input->entry.comment_len,0);
        STORE16(input->entry.first_diskno,0);
        STORE16(input->entry.internal_attribs,0);
        STORE32(input->entry.external_attribs,input->mode<<16);

        archived_size=offset-input->local_offset
            +sizeof (central_entry)+input->path_len+input->ext_len;
        fprintf(stderr,"%.6f  %s\n",(double)archived_size/size,input->path);
    }

    g->cd_offset=offset;
    sqlite3_finalize(pages);
    pages=NULL;
    return 0;

cleanup:
    if (pages)
        sqlite3_finalize(pages);
    return -1;
}

static void rollback_transaction(
    global_info *g)
{
    int status;
    sqlite3_stmt *rollback=NULL;

    status=sqlite3_prepare_v2(
        g->db,rollback_sql,sizeof rollback_sql,&rollback,NULL);
    if (status==SQLITE_OK)
        sqlite3_step(rollback);
    if (rollback)
        sqlite3_finalize(rollback);
    g->have_transaction=0;
}

static void close_db(
    global_info *g)
{
    sqlite3_close_v2(g->db);
    g->db=NULL;
}

static void finish_compression(
    global_info *g)
{
    deflateEnd(&g->deflation);
    g->have_deflation=0;
}

int write_directory(
    global_info *g)
{
    input_info *input,*inputs_end;
    off_t offset,total_size;

    inputs_end=g->inputs+g->input_cnt;
    offset=g->cd_offset;
    if (fseeko(g->zip,offset,SEEK_SET)) {
        fprintf(stderr,"%s: fseeko: %s\n",g->zip_path,strerror(errno));
        return -1;
    }
    total_size=0;
    for (input=g->inputs; input<inputs_end; input++) {
        if (!fwrite(&input->entry,sizeof (central_entry),1,g->zip)) {
            fprintf(stderr,"%s: fwrite: %s\n",g->zip_path,strerror(errno));
            return -1;
        }
        offset+=sizeof (central_entry);
        if (!fwrite(input->path,input->path_len,1,g->zip)) {
            fprintf(stderr,"%s: fwrite: %s\n",g->zip_path,strerror(errno));
            return -1;
        }
        offset+=input->path_len;
        if (input->ext_len) {
            if (!fwrite(&input->ext,input->ext_len,1,g->zip)) {
                fprintf(stderr,"%s: fwrite: %s\n",
                        g->zip_path,strerror(errno));
                return -1;
            }
            offset+=input->ext_len;
        }
        total_size+=input->size;
    }
    g->cd_size=offset-g->cd_offset;
    g->total_size=total_size;
    return 0;
}

static int write_trailer(
    global_info *g)
{
    eocd end;
    off_t offset;

    offset=g->cd_offset+g->cd_size;
    end.sig=eocd_sig;
    STORE16(end.this_diskno,0);
    STORE16(end.cd_diskno,0);
    STORE16(end.comment_len,0);
    if (g->input_cnt>0xFFFF
            || g->cd_offset>0xFFFFFFFF
            || g->cd_size>0xFFFFFFFF) {
        eocd64 end64;
        eocd64_locator loc64;
        off_t eocd_offset;

        eocd_offset=offset;
        end64.sig=eocd64_sig;
        STORE64(end64.size,sizeof (eocd64)-12);
        STORE16(end64.creator_version,version_zip64 | creator_unix);
        STORE16(end64.needed_version,version_zip64);
        STORE32(end64.this_diskno,0);
        STORE32(end64.cd_diskno,0);
        STORE64(end64.this_entry_cnt,(uint64_t)g->input_cnt);
        STORE64(end64.total_entry_cnt,(uint64_t)g->input_cnt);
        STORE64(end64.cd_size,g->cd_size);
        STORE64(end64.cd_offset,g->cd_offset);
        if (g->input_cnt>0xFFFF) {
            STORE16(end.this_entry_cnt,0xFFFF);
            STORE16(end.total_entry_cnt,0xFFFF);
        } else {
            STORE16(end.this_entry_cnt,g->input_cnt);
            STORE16(end.total_entry_cnt,g->input_cnt);
        }
        if (g->cd_size>0xFFFFFFFF) {
            STORE32(end.cd_size,0xFFFFFFFF);
        } else {
            STORE32(end.cd_size,g->cd_size);
        }
        if (g->cd_offset>0xFFFFFFFF) {
            STORE32(end.cd_offset,0xFFFFFFFF);
        } else {
            STORE32(end.cd_offset,g->cd_size);
        }
        loc64.sig=eocd64_locator_sig;
        STORE32(loc64.eocd_diskno,0);
        STORE64(loc64.eocd_offset,eocd_offset);
        STORE32(loc64.disk_cnt,1);

        if (!fwrite(&end64,sizeof end64,1,g->zip)) {
            fprintf(stderr,"%s: fwrite: %s\n",g->zip_path,strerror(errno));
            return -1;
        }
        offset+=sizeof end64;
        if (!fwrite(&loc64,sizeof loc64,1,g->zip)) {
            fprintf(stderr,"%s: fwrite: %s\n",g->zip_path,strerror(errno));
            return -1;
        }
        offset+=sizeof loc64;
    } else {
        STORE16(end.this_entry_cnt,g->input_cnt);
        STORE16(end.total_entry_cnt,g->input_cnt);
        STORE32(end.cd_size,g->cd_size);
        STORE32(end.cd_offset,g->cd_offset);
    }

    if (!fwrite(&end,sizeof end,1,g->zip)) {
        fprintf(stderr,"%s: fwrite: %s\n",g->zip_path,strerror(errno));
        return -1;
    }
    offset+=sizeof end;
    if (fflush(g->zip)) {
        fprintf(stderr,"%s: fflush: %s\n",g->zip_path,strerror(errno));
        return -1;
    }
    fprintf(stderr,"========\n%.6f  (total)\n",(double)offset/g->total_size);
    return 0;
}

static int close_archive(
    global_info *g)
{
    FILE *zip;

    zip=g->zip;
    g->zip=NULL;
    if (fclose(zip)) {
        fprintf(stderr,"%s: fclose: %s\n",g->zip_path,strerror(errno));
        return -1;
    }
    return 0;
}

static void cleanup_global(
    global_info *g)
{
    if (g->have_deflation)
        finish_compression(g);
    if (g->have_transaction)
        rollback_transaction(g);
    if (g->db)
        close_db(g);
    if (g->zip) {
        fclose(g->zip);
        g->zip=NULL;
    }
    if (g->have_output) {
        remove(g->zip_path);
        g->have_output=0;
    }
}

int main(
    int argc,
    char **argv)
{
    global_info *g=NULL;

    if (argc<3) {
        fputs("Usage: s3zip archive.zip database...\n",stderr);
        return 1;
    }
    g=make_global(argc-2);
    if (!g)
        goto cleanup;
    if (open_db(g))
        goto cleanup;
    if (attach_inputs(g,argv+2))
        goto cleanup;
    if (open_archive(g,argv[1]))
        goto cleanup;
    if (begin_transaction(g))
        goto cleanup;
    if (get_metainfo(g))
        goto cleanup;
    if (init_compression(g))
        goto cleanup;
    if (compress_inputs(g))
        goto cleanup;
    rollback_transaction(g);
    close_db(g);
    finish_compression(g);
    if (write_directory(g))
        goto cleanup;
    if (write_trailer(g))
        goto cleanup;
    if (close_archive(g))
        goto cleanup;
    free(g);
    g=NULL;
    return 0;

cleanup:
    if (g)
        cleanup_global(g);
    return 1;
}
